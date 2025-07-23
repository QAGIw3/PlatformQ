"""
Batch Ingestion Manager

Manages batch file ingestion using Apache SeaTunnel
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from enum import Enum

import aiofiles
from fastapi import UploadFile

from .config import settings
from .seatunnel_manager import SeaTunnelManager, JobType
from .schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class BatchSourceType(str, Enum):
    """Supported batch source types"""
    FILE = "file"
    S3 = "s3"
    MINIO = "minio"
    FTP = "ftp"
    HTTP = "http"


class FileFormat(str, Enum):
    """Supported file formats"""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    EXCEL = "excel"


class BatchIngestionManager:
    """Manages batch ingestion operations"""
    
    def __init__(self, config: settings):
        self.config = config
        self.seatunnel = SeaTunnelManager()
        self.schema_registry: Optional[SchemaRegistry] = None
        self.batch_jobs: Dict[str, Dict[str, Any]] = {}
        self._ensure_directories()
        
    def _ensure_directories(self):
        """Ensure required directories exist"""
        os.makedirs(self.config.batch_upload_path, exist_ok=True)
        os.makedirs(f"{self.config.batch_upload_path}/processed", exist_ok=True)
        os.makedirs(f"{self.config.batch_upload_path}/failed", exist_ok=True)
        
    async def cleanup(self):
        """Cleanup resources"""
        # Cancel any running jobs
        for job_id in list(self.batch_jobs.keys()):
            job = self.batch_jobs[job_id]
            if job["status"] == "running":
                await self.seatunnel.stop_job(job_id)
                
    def set_schema_registry(self, registry: SchemaRegistry):
        """Set schema registry reference"""
        self.schema_registry = registry
        
    async def create_batch_job(
        self,
        source_type: BatchSourceType,
        source_config: Dict[str, Any],
        destination_config: Dict[str, Any],
        processing_config: Optional[Dict[str, Any]] = None,
        schedule: Optional[str] = None
    ) -> str:
        """Create a batch ingestion job"""
        
        # Validate file format
        file_format = source_config.get("format", "").lower()
        if file_format not in [f.value for f in FileFormat]:
            raise ValueError(f"Unsupported file format: {file_format}")
            
        # Prepare source configuration
        source_conf = {
            "type": source_type.value,
            "format": file_format,
            **source_config
        }
        
        # Add processing configuration
        transform_config = None
        if processing_config:
            transform_config = self._prepare_transform_config(processing_config)
            
        # Prepare sink configuration
        sink_config = self._prepare_sink_config(destination_config)
        
        # Create SeaTunnel job
        job_id = await self.seatunnel.create_job(
            job_type=JobType.BATCH,
            source_config=source_conf,
            sink_config=sink_config,
            transform_config=transform_config,
            job_name=f"batch_{source_type.value}_{file_format}"
        )
        
        # Store job information
        self.batch_jobs[job_id] = {
            "id": job_id,
            "type": source_type.value,
            "format": file_format,
            "source": source_config,
            "destination": destination_config,
            "processing": processing_config or {},
            "schedule": schedule,
            "created_at": datetime.utcnow(),
            "status": "created"
        }
        
        # If not scheduled, start immediately
        if not schedule:
            await self.start_batch_job(job_id)
        else:
            # Schedule the job (simplified - in production use APScheduler)
            logger.info(f"Job {job_id} scheduled with cron: {schedule}")
            
        return job_id
        
    async def upload_and_process(
        self,
        file: UploadFile,
        destination_config: Dict[str, Any],
        processing_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Upload a file and process it"""
        
        # Validate file
        if file.size > self.config.batch_max_file_size:
            raise ValueError(f"File size exceeds limit of {self.config.batch_max_file_size} bytes")
            
        # Determine file format from extension
        file_extension = Path(file.filename).suffix.lower().lstrip('.')
        if file_extension not in self.config.batch_supported_formats:
            raise ValueError(f"Unsupported file format: {file_extension}")
            
        # Save uploaded file
        upload_id = str(uuid.uuid4())
        file_path = f"{self.config.batch_upload_path}/{upload_id}_{file.filename}"
        
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)
            
        logger.info(f"Saved uploaded file to {file_path}")
        
        # Create batch job for the file
        source_config = {
            "path": file_path,
            "format": file_extension
        }
        
        # Infer schema if possible
        if self.schema_registry and file_extension in ["csv", "json"]:
            schema = await self._infer_schema(file_path, file_extension)
            if schema:
                schema_id = f"upload_{upload_id}"
                await self.schema_registry.register_schema(
                    schema_id=schema_id,
                    schema=schema,
                    schema_type="inferred"
                )
                source_config["schema_id"] = schema_id
                
        # Create and start the job
        job_id = await self.create_batch_job(
            source_type=BatchSourceType.FILE,
            source_config=source_config,
            destination_config=destination_config,
            processing_config=processing_config
        )
        
        # Track the upload
        self.batch_jobs[job_id]["upload_id"] = upload_id
        self.batch_jobs[job_id]["original_filename"] = file.filename
        
        return job_id
        
    async def start_batch_job(self, job_id: str) -> Dict[str, Any]:
        """Start a batch job"""
        if job_id not in self.batch_jobs:
            raise ValueError(f"Job {job_id} not found")
            
        job = self.batch_jobs[job_id]
        
        # Start the SeaTunnel job
        result = await self.seatunnel.start_job(job_id)
        job["status"] = "running"
        job["started_at"] = datetime.utcnow()
        
        # Monitor job completion
        asyncio.create_task(self._monitor_job_completion(job_id))
        
        return result
        
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get batch job status"""
        if job_id not in self.batch_jobs:
            raise ValueError(f"Job {job_id} not found")
            
        job = self.batch_jobs[job_id]
        seatunnel_status = await self.seatunnel.get_job_status(job_id)
        
        # Get processing metrics
        metrics = await self._get_batch_metrics(job_id)
        
        return {
            "job_id": job_id,
            "type": job["type"],
            "format": job["format"],
            "status": seatunnel_status["status"],
            "created_at": job["created_at"].isoformat(),
            "started_at": job.get("started_at", "").isoformat() if job.get("started_at") else None,
            "completed_at": job.get("completed_at", "").isoformat() if job.get("completed_at") else None,
            "source": job["source"],
            "destination": job["destination"],
            "metrics": metrics,
            "error": seatunnel_status.get("error")
        }
        
    async def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List batch jobs"""
        jobs = []
        
        for job_id, job in self.batch_jobs.items():
            if status and job["status"] != status:
                continue
                
            job_status = await self.get_job_status(job_id)
            jobs.append(job_status)
            
            if len(jobs) >= limit:
                break
                
        return sorted(jobs, key=lambda x: x["created_at"], reverse=True)
        
    async def retry_job(self, job_id: str) -> str:
        """Retry a failed batch job"""
        if job_id not in self.batch_jobs:
            raise ValueError(f"Job {job_id} not found")
            
        job = self.batch_jobs[job_id]
        if job["status"] not in ["failed", "completed"]:
            raise ValueError(f"Job {job_id} cannot be retried in status {job['status']}")
            
        # Create new job with same configuration
        new_job_id = await self.create_batch_job(
            source_type=BatchSourceType(job["type"]),
            source_config=job["source"],
            destination_config=job["destination"],
            processing_config=job["processing"]
        )
        
        # Link to original job
        self.batch_jobs[new_job_id]["retry_of"] = job_id
        
        return new_job_id
        
    def _prepare_sink_config(self, destination: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare sink configuration"""
        dest_type = destination.get("type", "minio").lower()
        
        if dest_type == "minio":
            return {
                "type": "minio",
                "bucket": destination.get("bucket", settings.minio_bucket_processed),
                "path": destination.get("path", "batch/${job_name}/${date}"),
                "format": destination.get("format", "parquet")
            }
        elif dest_type == "cassandra":
            return {
                "type": "cassandra",
                "table": destination.get("table", "batch_data"),
                "keyspace": destination.get("keyspace", settings.cassandra_keyspace),
                "batch_size": destination.get("batch_size", 1000)
            }
        elif dest_type == "pulsar":
            topic = destination.get("topic", "batch-processed")
            return {
                "type": "pulsar",
                "topic": f"{settings.pulsar_topic_prefix}{topic}",
                "format": "json",
                "batch_size": destination.get("batch_size", 100)
            }
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
            
    def _prepare_transform_config(self, processing: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare transformation configuration"""
        transforms = []
        
        # Add data quality checks
        if settings.quality_check_enabled:
            quality_sql = processing.get("quality_sql", """
                SELECT *,
                       CASE WHEN ${quality_expression} THEN 1 ELSE 0 END as quality_score
                FROM source
            """)
            transforms.append({"sql": quality_sql})
            
        # Add custom SQL transformations
        if "sql" in processing:
            transforms.append({"sql": processing["sql"]})
            
        # Add field mappings
        if "field_mappings" in processing:
            mapping_sql = self._generate_mapping_sql(processing["field_mappings"])
            transforms.append({"sql": mapping_sql})
            
        return {"transforms": transforms} if transforms else None
        
    def _generate_mapping_sql(self, mappings: Dict[str, str]) -> str:
        """Generate SQL for field mappings"""
        select_fields = []
        
        for source_field, target_field in mappings.items():
            if source_field != target_field:
                select_fields.append(f"{source_field} AS {target_field}")
            else:
                select_fields.append(source_field)
                
        return f"SELECT {', '.join(select_fields)} FROM source"
        
    async def _infer_schema(self, file_path: str, format: str) -> Optional[Dict[str, Any]]:
        """Infer schema from file"""
        try:
            if format == "csv":
                # Simple CSV schema inference
                async with aiofiles.open(file_path, 'r') as f:
                    header = await f.readline()
                    columns = header.strip().split(',')
                    
                return {
                    "type": "record",
                    "name": "inferred_schema",
                    "fields": [
                        {"name": col.strip(), "type": ["null", "string"]}
                        for col in columns
                    ]
                }
            elif format == "json":
                # JSON schema inference would be more complex
                return None
                
        except Exception as e:
            logger.error(f"Failed to infer schema: {e}")
            return None
            
    async def _get_batch_metrics(self, job_id: str) -> Dict[str, Any]:
        """Get batch job metrics"""
        # In production, these would come from monitoring systems
        return {
            "rows_processed": 0,
            "bytes_processed": 0,
            "processing_time_seconds": 0,
            "errors": 0
        }
        
    async def _monitor_job_completion(self, job_id: str):
        """Monitor job completion and cleanup"""
        try:
            job = self.batch_jobs[job_id]
            
            # Wait for job completion
            while job["status"] == "running":
                await asyncio.sleep(5)
                status = await self.seatunnel.get_job_status(job_id)
                job["status"] = status["status"]
                
            job["completed_at"] = datetime.utcnow()
            
            # Move processed file
            if "upload_id" in job and job["status"] == "completed":
                source_path = job["source"]["path"]
                dest_dir = f"{self.config.batch_upload_path}/processed"
                dest_path = f"{dest_dir}/{Path(source_path).name}"
                
                os.rename(source_path, dest_path)
                logger.info(f"Moved processed file to {dest_path}")
                
        except Exception as e:
            logger.error(f"Error monitoring job {job_id}: {e}")
            job["status"] = "failed"
            job["error"] = str(e) 