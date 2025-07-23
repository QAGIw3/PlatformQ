"""
SeaTunnel Orchestrator

Orchestrates data movement using Apache SeaTunnel.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
from pathlib import Path
import uuid
import httpx
import yaml

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class SeaTunnelJobType(Enum):
    """SeaTunnel job types"""
    BATCH = "batch"
    STREAMING = "streaming"
    CDC = "cdc"
    SYNC = "sync"


class SeaTunnelJobStatus(Enum):
    """SeaTunnel job status"""
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ConnectorType(Enum):
    """SeaTunnel connector types"""
    # Sources
    JDBC = "jdbc"
    KAFKA = "kafka"
    PULSAR = "pulsar"
    ELASTICSEARCH = "elasticsearch"
    MONGODB = "mongodb"
    S3 = "s3"
    HDFS = "hdfs"
    
    # Sinks
    CLICKHOUSE = "clickhouse"
    DORIS = "doris"
    HIVE = "hive"
    ICEBERG = "iceberg"


class SeaTunnelOrchestrator:
    """
    Orchestrates data movement using Apache SeaTunnel
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        
        # SeaTunnel API client
        self.api_url = "http://seatunnel-api:8080"
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Job tracking
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.templates: Dict[str, Dict[str, Any]] = {}
        
        # Configuration
        self.config = {
            "api_url": "http://seatunnel-api:8080",
            "templates_path": "/config/seatunnel-templates",
            "job_timeout": 3600,
            "max_concurrent_jobs": 20,
            "checkpoint_interval": 60
        }
        
        # Metrics
        self.metrics = {
            "jobs_submitted": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "data_moved_gb": 0,
            "avg_throughput_mbps": 0
        }
    
    async def initialize(self):
        """Initialize SeaTunnel orchestrator"""
        logger.info("initializing_seatunnel_orchestrator")
        
        # Load configuration
        await self._load_configuration()
        
        # Update API URL
        self.api_url = self.config["api_url"]
        
        # Load pipeline templates
        await self._load_templates()
        
        # Verify SeaTunnel connectivity
        await self._verify_connectivity()
        
        # Start job monitoring
        asyncio.create_task(self._monitor_jobs())
        
        logger.info("seatunnel_orchestrator_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.client.aclose()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/seatunnel-orchestrator")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def create_job(self, job_config: Dict[str, Any]) -> str:
        """
        Create a SeaTunnel job
        
        Args:
            job_config: Job configuration including:
                - name: Job name
                - type: Job type (batch, streaming, cdc, sync)
                - source: Source configuration
                - sink: Sink configuration
                - transform: Transform configuration (optional)
                - parallelism: Parallelism level
                
        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())
        
        # Validate job configuration
        self._validate_job_config(job_config)
        
        # Create SeaTunnel configuration
        seatunnel_config = self._create_seatunnel_config(job_config)
        
        # Create job record
        job = {
            "id": job_id,
            "config": job_config,
            "seatunnel_config": seatunnel_config,
            "status": SeaTunnelJobStatus.CREATED,
            "created_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "seatunnel_job_id": None,
            "metrics": {
                "records_read": 0,
                "records_written": 0,
                "bytes_read": 0,
                "bytes_written": 0,
                "throughput_mbps": 0
            },
            "error": None
        }
        
        # Store job
        self.jobs[job_id] = job
        
        # Submit job to SeaTunnel
        try:
            response = await self.client.post(
                f"{self.api_url}/api/v1/jobs",
                json={
                    "config": seatunnel_config,
                    "name": job_config["name"],
                    "env": {
                        "job.mode": job_config["type"],
                        "checkpoint.interval": self.config["checkpoint_interval"]
                    }
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                job["seatunnel_job_id"] = result["jobId"]
                job["status"] = SeaTunnelJobStatus.RUNNING
                job["started_at"] = datetime.utcnow()
                
                # Update metrics
                self.metrics["jobs_submitted"] += 1
                
                # Emit event
                await self.event_bus.publish(
                    "orchestration.seatunnel.job_started",
                    {
                        "job_id": job_id,
                        "name": job_config["name"],
                        "type": job_config["type"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
                logger.info(f"SeaTunnel job created: {job_id}")
                
            else:
                raise RuntimeError(f"Failed to submit job: {response.text}")
                
        except Exception as e:
            logger.error(f"Error creating SeaTunnel job: {e}")
            job["status"] = SeaTunnelJobStatus.FAILED
            job["error"] = str(e)
            raise
        
        return job_id
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get SeaTunnel job status"""
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        # Get status from SeaTunnel if running
        if job["seatunnel_job_id"] and job["status"] == SeaTunnelJobStatus.RUNNING:
            try:
                response = await self.client.get(
                    f"{self.api_url}/api/v1/jobs/{job['seatunnel_job_id']}"
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Update job metrics
                    job["metrics"].update(result.get("metrics", {}))
                    
                    # Update status if changed
                    seatunnel_status = result.get("status", "").lower()
                    if seatunnel_status == "finished":
                        job["status"] = SeaTunnelJobStatus.FINISHED
                        job["completed_at"] = datetime.utcnow()
                    elif seatunnel_status == "failed":
                        job["status"] = SeaTunnelJobStatus.FAILED
                        job["completed_at"] = datetime.utcnow()
                        job["error"] = result.get("error", "Unknown error")
                        
            except Exception as e:
                logger.error(f"Error getting job status: {e}")
        
        return {
            "id": job_id,
            "name": job["config"]["name"],
            "type": job["config"]["type"],
            "status": job["status"].value,
            "created_at": job["created_at"].isoformat(),
            "started_at": job["started_at"].isoformat() if job["started_at"] else None,
            "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None,
            "metrics": job["metrics"],
            "error": job["error"]
        }
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel SeaTunnel job"""
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        if job["status"] not in [SeaTunnelJobStatus.CREATED, SeaTunnelJobStatus.RUNNING]:
            return False
        
        # Cancel in SeaTunnel
        if job["seatunnel_job_id"]:
            try:
                response = await self.client.post(
                    f"{self.api_url}/api/v1/jobs/{job['seatunnel_job_id']}/cancel"
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to cancel job in SeaTunnel: {response.text}")
                    
            except Exception as e:
                logger.error(f"Error cancelling job: {e}")
        
        # Update job status
        job["status"] = SeaTunnelJobStatus.CANCELLED
        job["completed_at"] = datetime.utcnow()
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.seatunnel.job_cancelled",
            {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"SeaTunnel job cancelled: {job_id}")
        return True
    
    async def create_pipeline(self, pipeline_config: Dict[str, Any]) -> str:
        """
        Create a SeaTunnel pipeline from template
        
        Args:
            pipeline_config: Pipeline configuration
            
        Returns:
            Job ID
        """
        # Get template
        template_name = pipeline_config.get("template")
        if template_name and template_name in self.templates:
            template = self.templates[template_name].copy()
            
            # Merge with pipeline config
            job_config = {
                **template,
                **pipeline_config,
                "name": pipeline_config.get("name", f"pipeline_{template_name}")
            }
        else:
            job_config = pipeline_config
        
        # Create job
        return await self.create_job(job_config)
    
    async def orchestrate_data_movement(self, name: str, movements: List[Dict[str, Any]], 
                                      dependencies: Dict[str, List[str]] = None,
                                      schedule: str = None) -> Dict[str, Any]:
        """
        Orchestrate complex data movements
        
        Args:
            name: Orchestration name
            movements: List of data movements
            dependencies: Movement dependencies
            schedule: Cron schedule (optional)
            
        Returns:
            Orchestration details
        """
        orchestration_id = str(uuid.uuid4())
        
        # Create orchestration plan
        plan = {
            "id": orchestration_id,
            "name": name,
            "movements": movements,
            "dependencies": dependencies or {},
            "schedule": schedule,
            "jobs": {},
            "status": "created",
            "created_at": datetime.utcnow()
        }
        
        # Create jobs for each movement
        for movement in movements:
            job_config = {
                "name": f"{name}_{movement['name']}",
                "type": movement.get("type", "batch"),
                "source": movement["source"],
                "sink": movement["sink"],
                "transform": movement.get("transform"),
                "parallelism": movement.get("parallelism", 1)
            }
            
            job_id = await self.create_job(job_config)
            plan["jobs"][movement["name"]] = job_id
        
        # If schedule is provided, create scheduled workflow
        if schedule:
            # This would integrate with workflow manager to create scheduled execution
            pass
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.seatunnel.orchestration_created",
            {
                "orchestration_id": orchestration_id,
                "name": name,
                "movements": len(movements),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return plan
    
    def _create_seatunnel_config(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create SeaTunnel configuration from job config"""
        config = {
            "env": {
                "execution.parallelism": job_config.get("parallelism", 1),
                "job.mode": job_config["type"].upper()
            },
            "source": [self._create_connector_config(job_config["source"], "source")],
            "sink": [self._create_connector_config(job_config["sink"], "sink")]
        }
        
        # Add transform if specified
        if job_config.get("transform"):
            config["transform"] = self._create_transform_config(job_config["transform"])
        
        return config
    
    def _create_connector_config(self, connector: Dict[str, Any], 
                                connector_type: str) -> Dict[str, Any]:
        """Create connector configuration"""
        config = {
            "plugin_name": connector["type"]
        }
        
        # Add connector-specific configuration
        if connector["type"] == "jdbc":
            config.update({
                "url": connector["config"]["url"],
                "driver": connector["config"].get("driver", "com.mysql.cj.jdbc.Driver"),
                "user": connector["config"]["user"],
                "password": connector["config"]["password"],
                "query": connector["config"].get("query") if connector_type == "source" else None,
                "table": connector["config"].get("table")
            })
        
        elif connector["type"] == "kafka":
            config.update({
                "bootstrap.servers": connector["config"]["bootstrap_servers"],
                "topic": connector["config"]["topic"],
                "consumer.group": connector["config"].get("consumer_group", "seatunnel") if connector_type == "source" else None,
                "format": connector["config"].get("format", "json")
            })
        
        elif connector["type"] == "elasticsearch":
            config.update({
                "hosts": connector["config"]["hosts"],
                "index": connector["config"]["index"],
                "source": connector["config"].get("query") if connector_type == "source" else None
            })
        
        elif connector["type"] == "s3":
            config.update({
                "path": connector["config"]["path"],
                "format": connector["config"].get("format", "parquet"),
                "access_key": connector["config"].get("access_key"),
                "secret_key": connector["config"].get("secret_key"),
                "endpoint": connector["config"].get("endpoint")
            })
        
        return config
    
    def _create_transform_config(self, transforms: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create transform configuration"""
        transform_configs = []
        
        for transform in transforms:
            if transform["type"] == "sql":
                transform_configs.append({
                    "plugin_name": "sql",
                    "query": transform["query"]
                })
            
            elif transform["type"] == "filter":
                transform_configs.append({
                    "plugin_name": "filter",
                    "condition": transform["condition"]
                })
            
            elif transform["type"] == "field_mapper":
                transform_configs.append({
                    "plugin_name": "field_mapper",
                    "field_mapper": transform["mapping"]
                })
        
        return transform_configs
    
    async def _load_templates(self):
        """Load SeaTunnel pipeline templates"""
        templates_path = Path(self.config["templates_path"])
        
        if not templates_path.exists():
            logger.warning(f"Templates directory {templates_path} not found")
            # Create default templates
            self._create_default_templates()
            return
        
        for template_file in templates_path.glob("*.yaml"):
            try:
                with open(template_file, 'r') as f:
                    template = yaml.safe_load(f)
                    template_name = template_file.stem
                    self.templates[template_name] = template
                    logger.info(f"Loaded SeaTunnel template: {template_name}")
            except Exception as e:
                logger.error(f"Failed to load template {template_file}: {e}")
    
    def _create_default_templates(self):
        """Create default SeaTunnel templates"""
        # JDBC to Elasticsearch template
        self.templates["jdbc_to_elasticsearch"] = {
            "type": "batch",
            "source": {
                "type": "jdbc",
                "config": {
                    "driver": "com.mysql.cj.jdbc.Driver",
                    "query": "SELECT * FROM ${table}"
                }
            },
            "sink": {
                "type": "elasticsearch",
                "config": {
                    "hosts": ["http://elasticsearch:9200"],
                    "index": "${index}"
                }
            },
            "parallelism": 4
        }
        
        # Kafka to ClickHouse template
        self.templates["kafka_to_clickhouse"] = {
            "type": "streaming",
            "source": {
                "type": "kafka",
                "config": {
                    "format": "json",
                    "consumer_group": "seatunnel_consumer"
                }
            },
            "sink": {
                "type": "clickhouse",
                "config": {
                    "host": "clickhouse",
                    "database": "default"
                }
            },
            "parallelism": 2
        }
        
        # S3 to Iceberg template
        self.templates["s3_to_iceberg"] = {
            "type": "batch",
            "source": {
                "type": "s3",
                "config": {
                    "format": "parquet"
                }
            },
            "sink": {
                "type": "iceberg",
                "config": {
                    "catalog_name": "iceberg_catalog",
                    "namespace": "default"
                }
            },
            "parallelism": 4
        }
    
    async def _verify_connectivity(self):
        """Verify SeaTunnel API connectivity"""
        try:
            response = await self.client.get(f"{self.api_url}/api/v1/health")
            if response.status_code == 200:
                logger.info("SeaTunnel API connectivity verified")
            else:
                logger.warning(f"SeaTunnel API returned status {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to connect to SeaTunnel API: {e}")
    
    async def _monitor_jobs(self):
        """Monitor running SeaTunnel jobs"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Check running jobs
                for job_id, job in self.jobs.items():
                    if job["status"] == SeaTunnelJobStatus.RUNNING:
                        # Update job status
                        await self.get_job_status(job_id)
                        
                        # Check for completion
                        if job["status"] in [SeaTunnelJobStatus.FINISHED, SeaTunnelJobStatus.FAILED]:
                            # Update metrics
                            if job["status"] == SeaTunnelJobStatus.FINISHED:
                                self.metrics["jobs_completed"] += 1
                                
                                # Update data moved metric
                                bytes_written = job["metrics"].get("bytes_written", 0)
                                self.metrics["data_moved_gb"] += bytes_written / (1024 ** 3)
                                
                            else:
                                self.metrics["jobs_failed"] += 1
                            
                            # Emit completion event
                            await self.event_bus.publish(
                                "orchestration.seatunnel.job_completed",
                                {
                                    "job_id": job_id,
                                    "status": job["status"].value,
                                    "metrics": job["metrics"],
                                    "timestamp": datetime.utcnow().isoformat()
                                }
                            )
                
            except Exception as e:
                logger.error(f"Error monitoring jobs: {e}")
    
    def _validate_job_config(self, config: Dict[str, Any]):
        """Validate job configuration"""
        required_fields = ["name", "type", "source", "sink"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate job type
        job_type = config["type"]
        if job_type not in [t.value for t in SeaTunnelJobType]:
            raise ValueError(f"Invalid job type: {job_type}")
        
        # Validate source and sink
        for connector_type in ["source", "sink"]:
            connector = config[connector_type]
            if "type" not in connector:
                raise ValueError(f"{connector_type} missing type")
            if "config" not in connector:
                raise ValueError(f"{connector_type} missing config")
    
    async def get_seatunnel_metrics(self) -> Dict[str, Any]:
        """Get SeaTunnel orchestrator metrics"""
        # Calculate average throughput
        total_time = sum(
            (job["completed_at"] - job["started_at"]).total_seconds()
            for job in self.jobs.values()
            if job["completed_at"] and job["started_at"]
        )
        
        total_mb = sum(
            job["metrics"].get("bytes_written", 0) / (1024 ** 2)
            for job in self.jobs.values()
        )
        
        avg_throughput = total_mb / total_time if total_time > 0 else 0
        
        return {
            **self.metrics,
            "avg_throughput_mbps": avg_throughput,
            "active_jobs": sum(1 for job in self.jobs.values() 
                             if job["status"] == SeaTunnelJobStatus.RUNNING),
            "total_jobs": len(self.jobs)
        } 