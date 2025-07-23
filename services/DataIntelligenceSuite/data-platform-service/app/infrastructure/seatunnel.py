"""
SeaTunnel Infrastructure Client

Manages interactions with Apache SeaTunnel for data integration
"""

import asyncio
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
import httpx
import yaml

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class JobType(str, Enum):
    """SeaTunnel job types"""
    CDC = "cdc"
    BATCH = "batch"
    STREAMING = "streaming"
    SYNC = "sync"


class JobStatus(str, Enum):
    """SeaTunnel job status"""
    CREATED = "created"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELED = "canceled"


class SeaTunnelClient:
    """Client for interacting with SeaTunnel API"""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
            timeout=30.0
        )
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.aclose()
        
    async def create_job(
        self,
        job_type: JobType,
        source_config: Dict[str, Any],
        sink_config: Dict[str, Any],
        transform_config: Optional[Dict[str, Any]] = None,
        job_name: Optional[str] = None,
        env_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a new SeaTunnel job"""
        
        # Build job configuration
        job_config = {
            "env": env_config or self._get_default_env_config(),
            "source": [self._build_source_config(job_type, source_config)],
            "sink": [self._build_sink_config(sink_config)]
        }
        
        # Add transform if provided
        if transform_config:
            job_config["transform"] = [transform_config]
            
        # Create job request
        request_data = {
            "name": job_name or f"{job_type.value}_job_{datetime.utcnow().isoformat()}",
            "type": job_type.value,
            "config": yaml.dump(job_config),
            "mode": "cluster" if job_type == JobType.CDC else "local"
        }
        
        try:
            response = await self._client.post(
                "/api/v1/jobs",
                json=request_data
            )
            response.raise_for_status()
            
            result = response.json()
            job_id = result["jobId"]
            
            logger.info("seatunnel_job_created",
                       job_id=job_id,
                       job_type=job_type.value,
                       job_name=job_name)
            
            return job_id
            
        except httpx.HTTPError as e:
            logger.error("failed_to_create_seatunnel_job",
                        error=str(e),
                        job_type=job_type.value)
            raise
            
    async def stop_job(self, job_id: str) -> bool:
        """Stop a running SeaTunnel job"""
        try:
            response = await self._client.post(
                f"/api/v1/jobs/{job_id}/stop"
            )
            response.raise_for_status()
            
            logger.info("seatunnel_job_stopped", job_id=job_id)
            return True
            
        except httpx.HTTPError as e:
            logger.error("failed_to_stop_seatunnel_job",
                        job_id=job_id,
                        error=str(e))
            raise
            
    async def restart_job(self, job_id: str) -> bool:
        """Restart a SeaTunnel job"""
        try:
            # Stop the job first
            await self.stop_job(job_id)
            
            # Wait for job to fully stop
            await asyncio.sleep(2)
            
            # Start the job again
            response = await self._client.post(
                f"/api/v1/jobs/{job_id}/start"
            )
            response.raise_for_status()
            
            logger.info("seatunnel_job_restarted", job_id=job_id)
            return True
            
        except httpx.HTTPError as e:
            logger.error("failed_to_restart_seatunnel_job",
                        job_id=job_id,
                        error=str(e))
            raise
            
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a SeaTunnel job"""
        try:
            response = await self._client.get(
                f"/api/v1/jobs/{job_id}/status"
            )
            response.raise_for_status()
            
            return response.json()
            
        except httpx.HTTPError as e:
            logger.error("failed_to_get_job_status",
                        job_id=job_id,
                        error=str(e))
            raise
            
    async def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """Get metrics for a SeaTunnel job"""
        try:
            response = await self._client.get(
                f"/api/v1/jobs/{job_id}/metrics"
            )
            response.raise_for_status()
            
            metrics = response.json()
            
            # Process metrics
            return {
                "throughput": metrics.get("recordsPerSecond", 0),
                "latency": metrics.get("latencyMs", 0),
                "error_rate": metrics.get("errorRate", 0),
                "events_processed": metrics.get("totalRecords", 0),
                "bytes_processed": metrics.get("totalBytes", 0),
                "cpu_seconds": metrics.get("cpuTime", 0),
                "memory_mb_seconds": metrics.get("memoryTime", 0),
                "network_bytes": metrics.get("networkBytes", 0),
                "batch_size": metrics.get("batchSize", 1000),
                "parallelism": metrics.get("parallelism", 1)
            }
            
        except httpx.HTTPError as e:
            logger.error("failed_to_get_job_metrics",
                        job_id=job_id,
                        error=str(e))
            raise
            
    async def check_job_health(self, job_id: str) -> Dict[str, Any]:
        """Check health of a SeaTunnel job"""
        try:
            response = await self._client.get(
                f"/api/v1/jobs/{job_id}/health"
            )
            response.raise_for_status()
            
            health = response.json()
            
            return {
                "healthy": health.get("status") == "healthy",
                "status": health.get("status"),
                "issues": health.get("issues", []),
                "last_heartbeat": health.get("lastHeartbeat")
            }
            
        except httpx.HTTPError as e:
            logger.error("failed_to_check_job_health",
                        job_id=job_id,
                        error=str(e))
            # Return unhealthy status on error
            return {
                "healthy": False,
                "status": "error",
                "issues": [str(e)]
            }
            
    async def update_job_config(self, job_id: str, config_updates: Dict[str, Any]) -> bool:
        """Update configuration of a running job"""
        try:
            response = await self._client.patch(
                f"/api/v1/jobs/{job_id}/config",
                json=config_updates
            )
            response.raise_for_status()
            
            logger.info("seatunnel_job_config_updated",
                       job_id=job_id,
                       updates=list(config_updates.keys()))
            
            return True
            
        except httpx.HTTPError as e:
            logger.error("failed_to_update_job_config",
                        job_id=job_id,
                        error=str(e))
            raise
            
    async def list_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List SeaTunnel jobs"""
        params = {
            "limit": limit
        }
        
        if job_type:
            params["type"] = job_type.value
        if status:
            params["status"] = status.value
            
        try:
            response = await self._client.get(
                "/api/v1/jobs",
                params=params
            )
            response.raise_for_status()
            
            return response.json()["jobs"]
            
        except httpx.HTTPError as e:
            logger.error("failed_to_list_jobs", error=str(e))
            raise
            
    def _get_default_env_config(self) -> Dict[str, Any]:
        """Get default environment configuration"""
        return {
            "job.mode": "STREAMING",
            "checkpoint.interval": 10000,
            "parallelism.default": 4,
            "execution.checkpointing.mode": "EXACTLY_ONCE",
            "execution.checkpointing.timeout": 600000
        }
        
    def _build_source_config(self, job_type: JobType, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build source configuration based on job type"""
        source_type = config.get("type")
        
        if job_type == JobType.CDC:
            # CDC-specific configuration
            if source_type == "postgresql":
                return {
                    "connector": "postgres-cdc",
                    "hostname": config.get("hostname"),
                    "port": config.get("port", 5432),
                    "database": config.get("database"),
                    "schema": config.get("schema", "public"),
                    "table.include.list": ",".join(config.get("tables", [])),
                    "username": config.get("username"),
                    "password": config.get("password"),
                    "slot.name": config.get("slot_name", "seatunnel_slot"),
                    "decoding.plugin.name": "pgoutput",
                    "startup.mode": config.get("mode", "streaming"),
                    "parallelism": config.get("parallelism", 1)
                }
            elif source_type == "mysql":
                return {
                    "connector": "mysql-cdc",
                    "hostname": config.get("hostname"),
                    "port": config.get("port", 3306),
                    "database": config.get("database"),
                    "table.include.list": ",".join(config.get("tables", [])),
                    "username": config.get("username"),
                    "password": config.get("password"),
                    "server.id": config.get("server_id", "5400-5404"),
                    "startup.mode": config.get("mode", "streaming"),
                    "parallelism": config.get("parallelism", 1)
                }
            elif source_type == "mongodb":
                return {
                    "connector": "mongodb-cdc",
                    "hosts": config.get("hosts"),
                    "database": config.get("database"),
                    "collection": config.get("collection"),
                    "username": config.get("username"),
                    "password": config.get("password"),
                    "startup.mode": config.get("mode", "streaming")
                }
                
        # Default source configuration
        return config
        
    def _build_sink_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build sink configuration"""
        sink_type = config.get("type")
        
        if sink_type == "iceberg":
            return {
                "connector": "iceberg",
                "catalog.name": config.get("catalog"),
                "catalog.type": "hive",
                "warehouse": "s3://platform-lakehouse/warehouse",
                "database": config.get("database"),
                "table": config.get("table_prefix", "") + "${table_name}",
                "format": config.get("format", "parquet"),
                "write.format.default": config.get("format", "parquet"),
                "write.parquet.compression-codec": config.get("compression", "snappy"),
                "write.metadata.delete-after-commit.enabled": True,
                "write.metadata.previous-versions-max": 10
            }
        elif sink_type == "pulsar":
            return {
                "connector": "pulsar",
                "service-url": "pulsar://pulsar:6650",
                "admin-url": "http://pulsar:8080",
                "topic": config.get("topic"),
                "tenant": config.get("tenant", "platform"),
                "namespace": config.get("namespace", "cdc"),
                "compression": config.get("compression", "lz4"),
                "format": "json"
            }
        elif sink_type == "elasticsearch":
            return {
                "connector": "elasticsearch",
                "hosts": ["http://elasticsearch:9200"],
                "index": config.get("index", "platform-${table_name}"),
                "document-id.field": config.get("id_field", "id"),
                "batch.size": config.get("batch_size", 1000),
                "socket.timeout": 30000
            }
            
        # Default sink configuration
        return config 