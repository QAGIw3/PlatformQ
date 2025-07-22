"""
Change Data Capture (CDC) Manager

Manages CDC operations using Apache SeaTunnel
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


class CDCSourceType(str, Enum):
    """Supported CDC source types"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MONGODB = "mongodb"


class CDCManager:
    """Manages CDC sources and operations"""
    
    def __init__(self, config: settings):
        self.config = config
        self.seatunnel = SeaTunnelManager()
        self.schema_registry: Optional[SchemaRegistry] = None
        self.active_sources: Dict[str, Dict[str, Any]] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the CDC manager"""
        logger.info("Starting CDC Manager")
        
        # Start monitoring task
        self._monitoring_task = asyncio.create_task(self._monitor_sources())
        
    async def stop(self):
        """Stop the CDC manager"""
        logger.info("Stopping CDC Manager")
        
        # Cancel monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            
        # Stop all active sources
        for source_id in list(self.active_sources.keys()):
            await self.delete_source(source_id)
            
    def set_schema_registry(self, registry: SchemaRegistry):
        """Set schema registry reference"""
        self.schema_registry = registry
        
    async def create_source(
        self,
        source_type: CDCSourceType,
        connection_config: Dict[str, Any],
        tables: List[str],
        destination_config: Dict[str, Any],
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """Create a new CDC source"""
        
        # Validate source type
        if source_type == CDCSourceType.POSTGRESQL and not self.config.cdc_postgres_enabled:
            raise ValueError("PostgreSQL CDC is not enabled")
        elif source_type == CDCSourceType.MYSQL and not self.config.cdc_mysql_enabled:
            raise ValueError("MySQL CDC is not enabled")
        elif source_type == CDCSourceType.MONGODB and not self.config.cdc_mongodb_enabled:
            raise ValueError("MongoDB CDC is not enabled")
            
        # Prepare source configuration
        source_config = {
            "type": source_type.value,
            **connection_config,
            "tables": tables
        }
        
        # Add default options
        if options:
            source_config.update(options)
            
        # Prepare sink configuration
        sink_config = self._prepare_sink_config(destination_config)
        
        # Create SeaTunnel job
        job_id = await self.seatunnel.create_job(
            job_type=JobType.CDC,
            source_config=source_config,
            sink_config=sink_config,
            job_name=f"cdc_{source_type.value}_{connection_config.get('database', 'db')}"
        )
        
        # Store source information
        self.active_sources[job_id] = {
            "id": job_id,
            "type": source_type.value,
            "database": connection_config.get("database"),
            "tables": tables,
            "destination": destination_config,
            "created_at": datetime.utcnow(),
            "status": "created"
        }
        
        # Auto-start the job
        await self.seatunnel.start_job(job_id)
        self.active_sources[job_id]["status"] = "running"
        
        # Register schemas if schema registry is available
        if self.schema_registry:
            await self._register_table_schemas(source_type, connection_config, tables)
            
        logger.info(f"Created CDC source {job_id} for {source_type.value}")
        return job_id
        
    async def delete_source(self, source_id: str) -> Dict[str, Any]:
        """Delete a CDC source"""
        if source_id not in self.active_sources:
            raise ValueError(f"Source {source_id} not found")
            
        # Stop the SeaTunnel job
        await self.seatunnel.stop_job(source_id)
        
        # Remove from active sources
        source = self.active_sources.pop(source_id)
        
        logger.info(f"Deleted CDC source {source_id}")
        
        return {
            "source_id": source_id,
            "status": "deleted"
        }
        
    async def get_source_status(self, source_id: str) -> Dict[str, Any]:
        """Get status of a CDC source"""
        if source_id not in self.active_sources:
            raise ValueError(f"Source {source_id} not found")
            
        source = self.active_sources[source_id]
        job_status = await self.seatunnel.get_job_status(source_id)
        
        # Get CDC-specific metrics
        metrics = await self._get_cdc_metrics(source_id)
        
        return {
            "source_id": source_id,
            "type": source["type"],
            "database": source["database"],
            "tables": source["tables"],
            "status": job_status["status"],
            "created_at": source["created_at"].isoformat(),
            "checkpoint": job_status.get("checkpoint"),
            "metrics": metrics,
            "error": job_status.get("error")
        }
        
    async def list_sources(
        self,
        source_type: Optional[CDCSourceType] = None
    ) -> List[Dict[str, Any]]:
        """List all CDC sources"""
        sources = []
        
        for source_id, source in self.active_sources.items():
            if source_type and source["type"] != source_type.value:
                continue
                
            status = await self.get_source_status(source_id)
            sources.append(status)
            
        return sources
        
    async def update_source_tables(
        self,
        source_id: str,
        tables: List[str],
        operation: str = "add"  # add or remove
    ) -> Dict[str, Any]:
        """Update tables for a CDC source"""
        if source_id not in self.active_sources:
            raise ValueError(f"Source {source_id} not found")
            
        source = self.active_sources[source_id]
        current_tables = set(source["tables"])
        
        if operation == "add":
            new_tables = current_tables.union(set(tables))
        elif operation == "remove":
            new_tables = current_tables - set(tables)
        else:
            raise ValueError(f"Invalid operation: {operation}")
            
        if new_tables == current_tables:
            return {"status": "no_change"}
            
        # Stop current job
        await self.seatunnel.stop_job(source_id)
        
        # Create new job with updated tables
        source_config = {
            "type": source["type"],
            "database": source["database"],
            "tables": list(new_tables)
        }
        
        # Recreate the job
        new_job_id = await self.seatunnel.create_job(
            job_type=JobType.CDC,
            source_config=source_config,
            sink_config=self._prepare_sink_config(source["destination"]),
            job_name=f"cdc_{source['type']}_{source['database']}_updated"
        )
        
        # Update source information
        self.active_sources.pop(source_id)
        self.active_sources[new_job_id] = {
            **source,
            "id": new_job_id,
            "tables": list(new_tables),
            "updated_at": datetime.utcnow()
        }
        
        # Start new job
        await self.seatunnel.start_job(new_job_id)
        
        return {
            "source_id": new_job_id,
            "tables": list(new_tables),
            "status": "updated"
        }
        
    def _prepare_sink_config(self, destination: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare sink configuration based on destination type"""
        dest_type = destination.get("type", "pulsar").lower()
        
        if dest_type == "pulsar":
            topic = destination.get("topic", "cdc-events")
            return {
                "type": "pulsar",
                "topic": f"{settings.pulsar_topic_prefix}{topic}",
                "format": "json"
            }
        elif dest_type == "cassandra":
            return {
                "type": "cassandra",
                "table": destination.get("table", "cdc_data"),
                "keyspace": destination.get("keyspace", settings.cassandra_keyspace)
            }
        elif dest_type == "minio":
            return {
                "type": "minio",
                "bucket": destination.get("bucket", settings.minio_bucket_raw),
                "path": destination.get("path", "cdc/${database}/${table}/${date}"),
                "format": destination.get("format", "parquet")
            }
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
            
    async def _register_table_schemas(
        self,
        source_type: CDCSourceType,
        connection_config: Dict[str, Any],
        tables: List[str]
    ):
        """Register table schemas with schema registry"""
        try:
            for table in tables:
                schema_id = f"{source_type.value}_{connection_config.get('database', 'db')}_{table}"
                
                # Get table schema (simplified - in production, query the database)
                schema = {
                    "type": "record",
                    "name": table,
                    "namespace": f"cdc.{source_type.value}",
                    "fields": []  # Would be populated from database metadata
                }
                
                await self.schema_registry.register_schema(
                    schema_id=schema_id,
                    schema=schema,
                    schema_type="avro"
                )
                
        except Exception as e:
            logger.error(f"Failed to register schemas: {e}")
            
    async def _get_cdc_metrics(self, source_id: str) -> Dict[str, Any]:
        """Get CDC-specific metrics"""
        # In production, these would come from monitoring systems
        return {
            "records_processed": 0,
            "lag_seconds": 0,
            "errors": 0,
            "last_record_timestamp": None
        }
        
    async def _monitor_sources(self):
        """Monitor active CDC sources"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for source_id in list(self.active_sources.keys()):
                    try:
                        status = await self.seatunnel.get_job_status(source_id)
                        
                        # Update source status
                        self.active_sources[source_id]["status"] = status["status"]
                        
                        # Handle failed jobs
                        if status["status"] == "failed":
                            logger.error(f"CDC source {source_id} failed: {status.get('error')}")
                            # Could implement auto-restart logic here
                            
                    except Exception as e:
                        logger.error(f"Error monitoring source {source_id}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in CDC monitoring: {e}")
                await asyncio.sleep(60)  # Wait longer on error 