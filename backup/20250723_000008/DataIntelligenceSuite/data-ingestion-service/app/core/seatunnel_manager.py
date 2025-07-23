"""
Apache SeaTunnel Integration Manager

Manages SeaTunnel jobs for various ingestion types (CDC, Stream, Batch)
"""

import asyncio
import json
import logging
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from enum import Enum

import aiofiles
from jinja2 import Template

from .config import settings

logger = logging.getLogger(__name__)


class JobType(str, Enum):
    """SeaTunnel job types"""
    CDC = "cdc"
    STREAM = "stream"
    BATCH = "batch"


class JobStatus(str, Enum):
    """Job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SeaTunnelManager:
    """Manages SeaTunnel job execution and monitoring"""
    
    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.running_processes: Dict[str, asyncio.subprocess.Process] = {}
        self._ensure_directories()
        
    def _ensure_directories(self):
        """Ensure required directories exist"""
        os.makedirs(settings.seatunnel_config_dir, exist_ok=True)
        os.makedirs(settings.seatunnel_checkpoint_dir, exist_ok=True)
        os.makedirs(f"{settings.seatunnel_config_dir}/jobs", exist_ok=True)
        
    async def create_job(
        self,
        job_type: JobType,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        transform_config: Optional[Dict[str, Any]] = None,
        job_name: Optional[str] = None
    ) -> str:
        """Create a new SeaTunnel job"""
        job_id = str(uuid.uuid4())
        job_name = job_name or f"{job_type.value}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        # Generate SeaTunnel configuration
        config_content = await self._generate_config(
            job_type, source_config, sink_config, transform_config
        )
        
        # Save configuration
        config_path = f"{settings.seatunnel_config_dir}/jobs/{job_id}.conf"
        async with aiofiles.open(config_path, 'w') as f:
            await f.write(config_content)
            
        # Store job metadata
        self.jobs[job_id] = {
            "id": job_id,
            "name": job_name,
            "type": job_type.value,
            "config_path": config_path,
            "status": JobStatus.PENDING.value,
            "created_at": datetime.utcnow(),
            "source": source_config.get("type", "unknown"),
            "sink": sink_config.get("type", "unknown")
        }
        
        logger.info(f"Created SeaTunnel job {job_id} ({job_name})")
        return job_id
        
    async def start_job(self, job_id: str) -> Dict[str, Any]:
        """Start a SeaTunnel job"""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
            
        job = self.jobs[job_id]
        if job["status"] == JobStatus.RUNNING.value:
            raise ValueError(f"Job {job_id} is already running")
            
        try:
            # Build SeaTunnel command
            cmd = [
                f"{settings.seatunnel_home}/bin/seatunnel.sh",
                "--config", job["config_path"],
                "--check-config"  # First validate config
            ]
            
            # Validate configuration
            result = await self._run_command(cmd)
            if result["returncode"] != 0:
                raise ValueError(f"Invalid configuration: {result['stderr']}")
                
            # Run the actual job
            cmd = [
                f"{settings.seatunnel_home}/bin/seatunnel.sh",
                "--config", job["config_path"],
                "--master", "local[*]",  # Local mode for development
                "--deploy-mode", "client",
                "--name", job["name"]
            ]
            
            # Start process
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            self.running_processes[job_id] = process
            job["status"] = JobStatus.RUNNING.value
            job["started_at"] = datetime.utcnow()
            job["pid"] = process.pid
            
            # Start monitoring task
            asyncio.create_task(self._monitor_job(job_id, process))
            
            logger.info(f"Started SeaTunnel job {job_id} (PID: {process.pid})")
            
            return {
                "job_id": job_id,
                "status": "started",
                "pid": process.pid
            }
            
        except Exception as e:
            job["status"] = JobStatus.FAILED.value
            job["error"] = str(e)
            logger.error(f"Failed to start job {job_id}: {e}")
            raise
            
    async def stop_job(self, job_id: str) -> Dict[str, Any]:
        """Stop a running SeaTunnel job"""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
            
        job = self.jobs[job_id]
        if job["status"] != JobStatus.RUNNING.value:
            raise ValueError(f"Job {job_id} is not running")
            
        if job_id in self.running_processes:
            process = self.running_processes[job_id]
            process.terminate()
            await process.wait()
            del self.running_processes[job_id]
            
        job["status"] = JobStatus.CANCELLED.value
        job["stopped_at"] = datetime.utcnow()
        
        logger.info(f"Stopped SeaTunnel job {job_id}")
        
        return {
            "job_id": job_id,
            "status": "stopped"
        }
        
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status and metrics"""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
            
        job = self.jobs[job_id]
        
        # Get checkpoint information if available
        checkpoint_info = await self._get_checkpoint_info(job_id)
        
        return {
            "job_id": job_id,
            "name": job["name"],
            "type": job["type"],
            "status": job["status"],
            "source": job["source"],
            "sink": job["sink"],
            "created_at": job["created_at"].isoformat(),
            "started_at": job.get("started_at", "").isoformat() if job.get("started_at") else None,
            "completed_at": job.get("completed_at", "").isoformat() if job.get("completed_at") else None,
            "checkpoint": checkpoint_info,
            "error": job.get("error")
        }
        
    async def list_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None
    ) -> List[Dict[str, Any]]:
        """List all jobs with optional filtering"""
        jobs = []
        
        for job_id, job in self.jobs.items():
            if job_type and job["type"] != job_type.value:
                continue
            if status and job["status"] != status.value:
                continue
                
            jobs.append(await self.get_job_status(job_id))
            
        return sorted(jobs, key=lambda x: x["created_at"], reverse=True)
        
    async def _generate_config(
        self,
        job_type: JobType,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        transform_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate SeaTunnel configuration file content"""
        
        # Base configuration
        config = {
            "env": {
                "execution.parallelism": settings.seatunnel_parallelism,
                "execution.checkpoint.interval": settings.cdc_checkpoint_interval,
                "execution.checkpoint.data-uri": f"file://{settings.seatunnel_checkpoint_dir}"
            }
        }
        
        # Source configuration
        if job_type == JobType.CDC:
            config["source"] = self._generate_cdc_source(source_config)
        elif job_type == JobType.STREAM:
            config["source"] = self._generate_stream_source(source_config)
        elif job_type == JobType.BATCH:
            config["source"] = self._generate_batch_source(source_config)
            
        # Transform configuration (optional)
        if transform_config:
            config["transform"] = self._generate_transform(transform_config)
            
        # Sink configuration
        config["sink"] = self._generate_sink(sink_config)
        
        # Convert to HOCON format (SeaTunnel config format)
        return self._dict_to_hocon(config)
        
    def _generate_cdc_source(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate CDC source configuration"""
        source_type = config.get("type", "").lower()
        
        if source_type == "postgresql":
            return [{
                "PostgreSQL-CDC": {
                    "hostname": config["hostname"],
                    "port": config.get("port", 5432),
                    "database": config["database"],
                    "username": config["username"],
                    "password": config["password"],
                    "schema-name": config.get("schema", "public"),
                    "table-name": config["tables"],
                    "startup.mode": settings.cdc_snapshot_mode,
                    "incremental.snapshot.enabled": True,
                    "slot.name": f"seatunnel_{config['database']}",
                    "result_table_name": "cdc_source"
                }
            }]
        elif source_type == "mysql":
            return [{
                "MySQL-CDC": {
                    "hostname": config["hostname"],
                    "port": config.get("port", 3306),
                    "database-name": config["database"],
                    "username": config["username"],
                    "password": config["password"],
                    "table-name": config["tables"],
                    "startup.mode": settings.cdc_snapshot_mode,
                    "server-id": config.get("server_id", 5400),
                    "result_table_name": "cdc_source"
                }
            }]
        elif source_type == "mongodb":
            return [{
                "MongoDB-CDC": {
                    "hosts": config["hosts"],
                    "database": config["database"],
                    "collection": config["collection"],
                    "username": config.get("username"),
                    "password": config.get("password"),
                    "startup.mode": settings.cdc_snapshot_mode,
                    "result_table_name": "cdc_source"
                }
            }]
        else:
            raise ValueError(f"Unsupported CDC source type: {source_type}")
            
    def _generate_stream_source(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate stream source configuration"""
        source_type = config.get("type", "").lower()
        
        if source_type == "pulsar":
            return [{
                "Pulsar": {
                    "service-url": settings.pulsar_url,
                    "admin-url": settings.pulsar_admin_url,
                    "topic": config["topics"],
                    "subscription": config.get("subscription", settings.stream_consumer_group),
                    "subscription-type": settings.pulsar_subscription_type,
                    "cursor.startup-mode": config.get("startup_mode", "latest"),
                    "format": config.get("format", "json"),
                    "result_table_name": "stream_source"
                }
            }]
        elif source_type == "kafka":
            return [{
                "Kafka": {
                    "bootstrap.servers": config["bootstrap_servers"],
                    "topic": config["topics"],
                    "consumer.group": config.get("group", settings.stream_consumer_group),
                    "offset.reset": config.get("offset_reset", "latest"),
                    "format": config.get("format", "json"),
                    "result_table_name": "stream_source"
                }
            }]
        else:
            raise ValueError(f"Unsupported stream source type: {source_type}")
            
    def _generate_batch_source(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate batch source configuration"""
        source_type = config.get("type", "").lower()
        file_format = config.get("format", "").lower()
        
        if source_type == "file":
            if file_format == "csv":
                return [{
                    "File": {
                        "path": config["path"],
                        "type": "text",
                        "delimiter": config.get("delimiter", ","),
                        "skip_header_row_number": config.get("skip_header", 1),
                        "schema": config.get("schema", {}),
                        "result_table_name": "batch_source"
                    }
                }]
            elif file_format in ["json", "parquet", "orc"]:
                return [{
                    "File": {
                        "path": config["path"],
                        "type": file_format,
                        "schema": config.get("schema", {}),
                        "result_table_name": "batch_source"
                    }
                }]
        elif source_type == "s3" or source_type == "minio":
            return [{
                "S3File": {
                    "path": config["path"],
                    "type": config.get("format", "json"),
                    "access_key": settings.minio_access_key,
                    "secret_key": settings.minio_secret_key,
                    "endpoint": f"http://{settings.minio_endpoint}",
                    "bucket": config.get("bucket", settings.minio_bucket_raw),
                    "result_table_name": "batch_source"
                }
            }]
        else:
            raise ValueError(f"Unsupported batch source type: {source_type}")
            
    def _generate_transform(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate transform configuration"""
        transforms = []
        
        # Add data quality check if enabled
        if settings.quality_check_enabled:
            transforms.append({
                "sql": {
                    "sql": """
                        SELECT *,
                               CASE WHEN ${quality_check_expression} THEN 1 ELSE 0 END as quality_passed
                        FROM source
                    """,
                    "result_table_name": "quality_checked"
                }
            })
            
        # Add custom transformations
        if "sql" in config:
            transforms.append({
                "sql": {
                    "sql": config["sql"],
                    "result_table_name": "transformed"
                }
            })
            
        return transforms
        
    def _generate_sink(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate sink configuration"""
        sink_type = config.get("type", "").lower()
        sinks = []
        
        # Primary sink
        if sink_type == "cassandra":
            sinks.append({
                "Cassandra": {
                    "host": settings.cassandra_hosts,
                    "keyspace": settings.cassandra_keyspace,
                    "table": config["table"],
                    "consistency_level": settings.cassandra_consistency_level,
                    "source_table_name": config.get("source_table", "source")
                }
            })
        elif sink_type == "pulsar":
            sinks.append({
                "Pulsar": {
                    "service-url": settings.pulsar_url,
                    "topic": config["topic"],
                    "format": config.get("format", "json"),
                    "source_table_name": config.get("source_table", "source")
                }
            })
        elif sink_type == "minio":
            sinks.append({
                "S3File": {
                    "path": config["path"],
                    "bucket": config.get("bucket", settings.minio_bucket_processed),
                    "access_key": settings.minio_access_key,
                    "secret_key": settings.minio_secret_key,
                    "endpoint": f"http://{settings.minio_endpoint}",
                    "file_format_type": config.get("format", "parquet"),
                    "source_table_name": config.get("source_table", "source")
                }
            })
            
        # Add additional sink for data catalog lineage
        sinks.append({
            "Http": {
                "url": "http://data-catalog-service:8017/api/v1/lineage",
                "method": "POST",
                "headers": {
                    "Content-Type": "application/json"
                },
                "source_table_name": config.get("source_table", "source")
            }
        })
        
        return sinks
        
    def _dict_to_hocon(self, data: Dict[str, Any]) -> str:
        """Convert dictionary to HOCON format"""
        # Simple HOCON generation - in production, use pyhocon library
        lines = []
        
        def format_value(value):
            if isinstance(value, str):
                return f'"{value}"'
            elif isinstance(value, list):
                return "[" + ", ".join(format_value(v) for v in value) + "]"
            elif isinstance(value, dict):
                return format_dict(value, indent=2)
            else:
                return str(value)
                
        def format_dict(d, indent=0):
            result = []
            for key, value in d.items():
                if isinstance(value, dict):
                    result.append(f"{' ' * indent}{key} {{")
                    result.extend(format_dict(value, indent + 2).split('\n'))
                    result.append(f"{' ' * indent}}}")
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    for item in value:
                        result.append(f"{' ' * indent}{key} {{")
                        result.extend(format_dict(item, indent + 2).split('\n'))
                        result.append(f"{' ' * indent}}}")
                else:
                    result.append(f"{' ' * indent}{key} = {format_value(value)}")
            return '\n'.join(result)
            
        return format_dict(data)
        
    async def _monitor_job(self, job_id: str, process: asyncio.subprocess.Process):
        """Monitor job execution"""
        job = self.jobs[job_id]
        
        try:
            # Wait for process to complete
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                job["status"] = JobStatus.COMPLETED.value
                logger.info(f"Job {job_id} completed successfully")
            else:
                job["status"] = JobStatus.FAILED.value
                job["error"] = stderr.decode() if stderr else "Unknown error"
                logger.error(f"Job {job_id} failed: {job['error']}")
                
        except asyncio.CancelledError:
            job["status"] = JobStatus.CANCELLED.value
            raise
        finally:
            job["completed_at"] = datetime.utcnow()
            if job_id in self.running_processes:
                del self.running_processes[job_id]
                
    async def _run_command(self, cmd: List[str]) -> Dict[str, Any]:
        """Run a command and return result"""
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        return {
            "returncode": process.returncode,
            "stdout": stdout.decode() if stdout else "",
            "stderr": stderr.decode() if stderr else ""
        }
        
    async def _get_checkpoint_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get checkpoint information for a job"""
        checkpoint_dir = Path(f"{settings.seatunnel_checkpoint_dir}/{job_id}")
        
        if not checkpoint_dir.exists():
            return None
            
        # Get latest checkpoint
        checkpoints = sorted(checkpoint_dir.glob("chk-*"))
        if not checkpoints:
            return None
            
        latest = checkpoints[-1]
        
        return {
            "path": str(latest),
            "timestamp": datetime.fromtimestamp(latest.stat().st_mtime).isoformat(),
            "size_bytes": latest.stat().st_size
        } 