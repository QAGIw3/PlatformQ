"""
Stream Ingestion Manager

Manages real-time stream ingestion using Apache SeaTunnel
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from .config import settings
from .seatunnel_manager import SeaTunnelManager, JobType
from .schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class StreamSourceType(str, Enum):
    """Supported stream source types"""
    PULSAR = "pulsar"
    KAFKA = "kafka"
    KINESIS = "kinesis"
    RABBITMQ = "rabbitmq"


class StreamIngestionManager:
    """Manages stream ingestion operations"""
    
    def __init__(self, config: settings):
        self.config = config
        self.seatunnel = SeaTunnelManager()
        self.schema_registry: Optional[SchemaRegistry] = None
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the stream ingestion manager"""
        logger.info("Starting Stream Ingestion Manager")
        
        # Start monitoring task
        self._monitoring_task = asyncio.create_task(self._monitor_streams())
        
    async def stop(self):
        """Stop the stream ingestion manager"""
        logger.info("Stopping Stream Ingestion Manager")
        
        # Cancel monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            
        # Stop all active streams
        for stream_id in list(self.active_streams.keys()):
            await self.delete_stream(stream_id)
            
    def set_schema_registry(self, registry: SchemaRegistry):
        """Set schema registry reference"""
        self.schema_registry = registry
        
    async def create_stream(
        self,
        source_type: StreamSourceType,
        topics: List[str],
        destination_config: Dict[str, Any],
        consumer_config: Optional[Dict[str, Any]] = None,
        schema_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a new stream ingestion"""
        
        # Prepare source configuration
        source_config = {
            "type": source_type.value,
            "topics": topics
        }
        
        # Add source-specific configuration
        if source_type == StreamSourceType.PULSAR:
            source_config.update({
                "subscription": consumer_config.get("subscription", settings.stream_consumer_group),
                "startup_mode": consumer_config.get("startup_mode", "latest")
            })
        elif source_type == StreamSourceType.KAFKA:
            source_config.update({
                "bootstrap_servers": consumer_config.get("bootstrap_servers", "kafka:9092"),
                "group": consumer_config.get("group", settings.stream_consumer_group),
                "offset_reset": consumer_config.get("offset_reset", "latest")
            })
            
        # Add schema configuration if provided
        if schema_config:
            source_config["format"] = schema_config.get("format", "json")
            if "schema_id" in schema_config and self.schema_registry:
                schema = await self.schema_registry.get_schema(schema_config["schema_id"])
                source_config["schema"] = schema
                
        # Prepare sink configuration
        sink_config = self._prepare_sink_config(destination_config)
        
        # Add transform for data quality if enabled
        transform_config = None
        if settings.quality_check_enabled:
            transform_config = self._prepare_quality_transform(schema_config)
            
        # Create SeaTunnel job
        job_id = await self.seatunnel.create_job(
            job_type=JobType.STREAM,
            source_config=source_config,
            sink_config=sink_config,
            transform_config=transform_config,
            job_name=f"stream_{source_type.value}_{topics[0]}"
        )
        
        # Store stream information
        self.active_streams[job_id] = {
            "id": job_id,
            "type": source_type.value,
            "topics": topics,
            "destination": destination_config,
            "consumer_config": consumer_config or {},
            "schema_config": schema_config or {},
            "created_at": datetime.utcnow(),
            "status": "created"
        }
        
        # Auto-start the job
        await self.seatunnel.start_job(job_id)
        self.active_streams[job_id]["status"] = "running"
        
        logger.info(f"Created stream ingestion {job_id} for {source_type.value}")
        return job_id
        
    async def delete_stream(self, stream_id: str) -> Dict[str, Any]:
        """Delete a stream ingestion"""
        if stream_id not in self.active_streams:
            raise ValueError(f"Stream {stream_id} not found")
            
        # Stop the SeaTunnel job
        await self.seatunnel.stop_job(stream_id)
        
        # Remove from active streams
        stream = self.active_streams.pop(stream_id)
        
        logger.info(f"Deleted stream ingestion {stream_id}")
        
        return {
            "stream_id": stream_id,
            "status": "deleted"
        }
        
    async def pause_stream(self, stream_id: str) -> Dict[str, Any]:
        """Pause a stream ingestion"""
        if stream_id not in self.active_streams:
            raise ValueError(f"Stream {stream_id} not found")
            
        # Stop the job but keep configuration
        await self.seatunnel.stop_job(stream_id)
        self.active_streams[stream_id]["status"] = "paused"
        
        return {
            "stream_id": stream_id,
            "status": "paused"
        }
        
    async def resume_stream(self, stream_id: str) -> Dict[str, Any]:
        """Resume a paused stream ingestion"""
        if stream_id not in self.active_streams:
            raise ValueError(f"Stream {stream_id} not found")
            
        stream = self.active_streams[stream_id]
        if stream["status"] != "paused":
            raise ValueError(f"Stream {stream_id} is not paused")
            
        # Restart the job
        await self.seatunnel.start_job(stream_id)
        stream["status"] = "running"
        
        return {
            "stream_id": stream_id,
            "status": "resumed"
        }
        
    async def get_stream_status(self, stream_id: str) -> Dict[str, Any]:
        """Get status of a stream ingestion"""
        if stream_id not in self.active_streams:
            raise ValueError(f"Stream {stream_id} not found")
            
        stream = self.active_streams[stream_id]
        job_status = await self.seatunnel.get_job_status(stream_id)
        
        # Get stream-specific metrics
        metrics = await self._get_stream_metrics(stream_id)
        
        return {
            "stream_id": stream_id,
            "type": stream["type"],
            "topics": stream["topics"],
            "destination": stream["destination"],
            "status": job_status["status"],
            "created_at": stream["created_at"].isoformat(),
            "checkpoint": job_status.get("checkpoint"),
            "metrics": metrics,
            "error": job_status.get("error")
        }
        
    async def list_streams(
        self,
        source_type: Optional[StreamSourceType] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List all stream ingestions"""
        streams = []
        
        for stream_id, stream in self.active_streams.items():
            if source_type and stream["type"] != source_type.value:
                continue
            if status and stream["status"] != status:
                continue
                
            stream_status = await self.get_stream_status(stream_id)
            streams.append(stream_status)
            
        return streams
        
    async def update_stream_topics(
        self,
        stream_id: str,
        topics: List[str],
        operation: str = "add"  # add, remove, or replace
    ) -> Dict[str, Any]:
        """Update topics for a stream ingestion"""
        if stream_id not in self.active_streams:
            raise ValueError(f"Stream {stream_id} not found")
            
        stream = self.active_streams[stream_id]
        current_topics = set(stream["topics"])
        
        if operation == "add":
            new_topics = current_topics.union(set(topics))
        elif operation == "remove":
            new_topics = current_topics - set(topics)
        elif operation == "replace":
            new_topics = set(topics)
        else:
            raise ValueError(f"Invalid operation: {operation}")
            
        if new_topics == current_topics:
            return {"status": "no_change"}
            
        # Update the stream configuration
        stream["topics"] = list(new_topics)
        stream["updated_at"] = datetime.utcnow()
        
        # Restart the job with new configuration
        await self.seatunnel.stop_job(stream_id)
        
        # Recreate with updated topics
        source_config = {
            "type": stream["type"],
            "topics": list(new_topics),
            **stream["consumer_config"]
        }
        
        # Create new job configuration
        # Note: In production, SeaTunnel would support dynamic topic updates
        
        await self.seatunnel.start_job(stream_id)
        
        return {
            "stream_id": stream_id,
            "topics": list(new_topics),
            "status": "updated"
        }
        
    def _prepare_sink_config(self, destination: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare sink configuration based on destination type"""
        dest_type = destination.get("type", "cassandra").lower()
        
        if dest_type == "cassandra":
            return {
                "type": "cassandra",
                "table": destination.get("table", "stream_data"),
                "keyspace": destination.get("keyspace", settings.cassandra_keyspace)
            }
        elif dest_type == "pulsar":
            topic = destination.get("topic", "processed-events")
            return {
                "type": "pulsar",
                "topic": f"{settings.pulsar_topic_prefix}{topic}",
                "format": "json"
            }
        elif dest_type == "minio":
            return {
                "type": "minio",
                "bucket": destination.get("bucket", settings.minio_bucket_raw),
                "path": destination.get("path", "streams/${topic}/${date}/${hour}"),
                "format": destination.get("format", "parquet"),
                "file_size": destination.get("file_size", "128MB"),
                "rolling_interval": destination.get("rolling_interval", "1h")
            }
        elif dest_type == "ignite":
            return {
                "type": "ignite",
                "cache": destination.get("cache", "stream_cache"),
                "ttl": destination.get("ttl", settings.cache_ttl_seconds)
            }
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
            
    def _prepare_quality_transform(self, schema_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare data quality transformation"""
        transforms = []
        
        # Add basic quality checks
        quality_sql = """
        SELECT *,
               CASE 
                   WHEN _value IS NOT NULL 
                   AND LENGTH(CAST(_value AS STRING)) > 0 
                   THEN 1 
                   ELSE 0 
               END as quality_score,
               CURRENT_TIMESTAMP as ingestion_timestamp
        FROM source
        """
        
        # Add schema validation if schema is provided
        if schema_config and "schema_id" in schema_config:
            quality_sql += " WHERE schema_valid = 1"
            
        return {
            "sql": quality_sql
        }
        
    async def _get_stream_metrics(self, stream_id: str) -> Dict[str, Any]:
        """Get stream-specific metrics"""
        # In production, these would come from monitoring systems
        return {
            "messages_processed": 0,
            "bytes_processed": 0,
            "lag": 0,
            "throughput_msg_per_sec": 0,
            "errors": 0,
            "last_message_timestamp": None
        }
        
    async def _monitor_streams(self):
        """Monitor active stream ingestions"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for stream_id in list(self.active_streams.keys()):
                    try:
                        status = await self.seatunnel.get_job_status(stream_id)
                        
                        # Update stream status
                        self.active_streams[stream_id]["status"] = status["status"]
                        
                        # Handle failed jobs
                        if status["status"] == "failed":
                            logger.error(f"Stream ingestion {stream_id} failed: {status.get('error')}")
                            
                            # Auto-restart if configured
                            if self.active_streams[stream_id].get("auto_restart", True):
                                logger.info(f"Auto-restarting stream {stream_id}")
                                await self.seatunnel.start_job(stream_id)
                                
                    except Exception as e:
                        logger.error(f"Error monitoring stream {stream_id}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stream monitoring: {e}")
                await asyncio.sleep(60)  # Wait longer on error 