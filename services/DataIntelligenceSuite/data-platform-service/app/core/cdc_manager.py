"""
Change Data Capture (CDC) Manager

Manages CDC operations using Apache SeaTunnel with enhanced capabilities
from DataIntelligenceSuite v2.0
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from enum import Enum
import json

from data_intelligence_common import (
    BaseProcessor,
    ProcessorConfig,
    ProcessingResult,
    ProcessingStatus,
    MetricsCollector,
    StructuredLogger,
    cached,
    CacheStrategy
)
from data_intelligence_common.core.events import EventBus, Event
from platformq_shared.vault.vault_client import VaultClient

from ..infrastructure.seatunnel import SeaTunnelClient, JobType
from ..domain.models import CDCSource, CDCEvent, CDCStatus
from ..utils.validators import validate_connection_config

logger = StructuredLogger.get_logger(__name__)


class CDCSourceType(str, Enum):
    """Supported CDC source types"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MONGODB = "mongodb"
    CASSANDRA = "cassandra"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"
    DB2 = "db2"


class CDCMode(str, Enum):
    """CDC operation modes"""
    FULL_SYNC = "full_sync"
    INCREMENTAL = "incremental"
    SNAPSHOT = "snapshot"
    STREAMING = "streaming"


class CDCManager(BaseProcessor):
    """
    Enhanced CDC Manager with v2.0 capabilities:
    - Multi-source CDC support
    - Real-time schema evolution
    - Automatic backpressure handling
    - ML-based optimization
    - Cost tracking
    """
    
    def __init__(
        self,
        seatunnel_client: SeaTunnelClient,
        vault_client: VaultClient,
        event_bus: EventBus,
        metrics: MetricsCollector,
        config: ProcessorConfig
    ):
        super().__init__(config)
        self.seatunnel = seatunnel_client
        self.vault = vault_client
        self.event_bus = event_bus
        self.metrics = metrics
        
        # Active CDC sources
        self.active_sources: Dict[str, CDCSource] = {}
        self.source_metrics: Dict[str, Dict[str, Any]] = {}
        
        # Monitoring
        self._monitoring_task: Optional[asyncio.Task] = None
        self._health_check_interval = config.get("health_check_interval", 30)
        
        # Performance optimization
        self._optimization_enabled = config.get("ml_optimization", True)
        self._cost_tracking_enabled = config.get("cost_tracking", True)
        
    async def initialize(self):
        """Initialize CDC manager"""
        logger.info("initializing_cdc_manager", 
                   optimization=self._optimization_enabled,
                   cost_tracking=self._cost_tracking_enabled)
        
        # Start monitoring
        self._monitoring_task = asyncio.create_task(self._monitor_sources())
        
        # Subscribe to relevant events
        await self.event_bus.subscribe("schema.changed", self._handle_schema_change)
        await self.event_bus.subscribe("source.failed", self._handle_source_failure)
        
    async def shutdown(self):
        """Shutdown CDC manager"""
        logger.info("shutting_down_cdc_manager")
        
        # Cancel monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            
        # Stop all active sources
        for source_id in list(self.active_sources.keys()):
            await self.stop_source(source_id)
            
    async def create_source(
        self,
        name: str,
        source_type: CDCSourceType,
        connection_config: Dict[str, Any],
        tables: List[str],
        destination_config: Dict[str, Any],
        mode: CDCMode = CDCMode.STREAMING,
        options: Optional[Dict[str, Any]] = None
    ) -> CDCSource:
        """Create a new CDC source with enhanced capabilities"""
        
        # Validate configuration
        validate_connection_config(source_type, connection_config)
        
        # Get credentials from Vault
        credentials = await self._get_credentials(source_type, connection_config)
        connection_config.update(credentials)
        
        # Prepare source configuration
        source_config = {
            "type": source_type.value,
            "mode": mode.value,
            **connection_config,
            "tables": tables,
            "schema_evolution": options.get("schema_evolution", True),
            "batch_size": options.get("batch_size", 1000),
            "parallelism": options.get("parallelism", 4)
        }
        
        # Add ML optimization if enabled
        if self._optimization_enabled:
            source_config["optimization"] = {
                "auto_scaling": True,
                "adaptive_batch_size": True,
                "smart_partitioning": True
            }
            
        # Prepare sink configuration
        sink_config = self._prepare_sink_config(destination_config)
        
        # Create SeaTunnel job
        job_id = await self.seatunnel.create_job(
            job_type=JobType.CDC,
            source_config=source_config,
            sink_config=sink_config,
            job_name=f"cdc_{name}_{source_type.value}"
        )
        
        # Create CDC source object
        cdc_source = CDCSource(
            id=job_id,
            name=name,
            source_type=source_type,
            tables=tables,
            mode=mode,
            status=CDCStatus.RUNNING,
            created_at=datetime.utcnow(),
            metrics={}
        )
        
        # Store source
        self.active_sources[job_id] = cdc_source
        
        # Emit event
        await self.event_bus.publish(Event(
            type="cdc.source.created",
            data={
                "source_id": job_id,
                "name": name,
                "type": source_type.value,
                "tables": tables
            }
        ))
        
        # Track metrics
        self.metrics.increment("cdc.sources.created", 
                             tags={"type": source_type.value})
        
        logger.info("cdc_source_created",
                   source_id=job_id,
                   name=name,
                   type=source_type.value,
                   tables_count=len(tables))
        
        return cdc_source
        
    async def stop_source(self, source_id: str) -> bool:
        """Stop a CDC source"""
        if source_id not in self.active_sources:
            raise ValueError(f"Source {source_id} not found")
            
        source = self.active_sources[source_id]
        
        # Stop SeaTunnel job
        await self.seatunnel.stop_job(source_id)
        
        # Update status
        source.status = CDCStatus.STOPPED
        source.stopped_at = datetime.utcnow()
        
        # Remove from active sources
        del self.active_sources[source_id]
        
        # Emit event
        await self.event_bus.publish(Event(
            type="cdc.source.stopped",
            data={"source_id": source_id, "name": source.name}
        ))
        
        logger.info("cdc_source_stopped", source_id=source_id)
        return True
        
    @cached(ttl=60, strategy=CacheStrategy.CACHE_ASIDE)
    async def get_source_metrics(self, source_id: str) -> Dict[str, Any]:
        """Get metrics for a CDC source"""
        if source_id not in self.active_sources:
            raise ValueError(f"Source {source_id} not found")
            
        # Get metrics from SeaTunnel
        job_metrics = await self.seatunnel.get_job_metrics(source_id)
        
        # Get custom metrics
        custom_metrics = self.source_metrics.get(source_id, {})
        
        # Combine metrics
        metrics = {
            **job_metrics,
            **custom_metrics,
            "status": self.active_sources[source_id].status.value,
            "uptime": self._calculate_uptime(source_id)
        }
        
        # Add cost metrics if enabled
        if self._cost_tracking_enabled:
            metrics["cost"] = await self._calculate_cost(source_id)
            
        return metrics
        
    async def _monitor_sources(self):
        """Monitor active CDC sources"""
        while True:
            try:
                for source_id, source in self.active_sources.items():
                    try:
                        # Check health
                        health = await self.seatunnel.check_job_health(source_id)
                        
                        if not health["healthy"]:
                            await self._handle_unhealthy_source(source_id, health)
                            
                        # Update metrics
                        metrics = await self.get_source_metrics(source_id)
                        source.metrics = metrics
                        
                        # Check for optimization opportunities
                        if self._optimization_enabled:
                            await self._optimize_source(source_id, metrics)
                            
                    except Exception as e:
                        logger.error("error_monitoring_source",
                                   source_id=source_id,
                                   error=str(e))
                        
                await asyncio.sleep(self._health_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("error_in_monitoring_loop", error=str(e))
                await asyncio.sleep(self._health_check_interval)
                
    async def _handle_schema_change(self, event: Event):
        """Handle schema change events"""
        source_id = event.data.get("source_id")
        if source_id in self.active_sources:
            logger.info("handling_schema_change", source_id=source_id)
            
            # Update source configuration
            source = self.active_sources[source_id]
            
            # Restart job with new schema
            await self.seatunnel.update_job_config(
                source_id,
                {"schema_version": event.data.get("new_version")}
            )
            
    async def _handle_source_failure(self, event: Event):
        """Handle source failure events"""
        source_id = event.data.get("source_id")
        if source_id in self.active_sources:
            logger.error("handling_source_failure", source_id=source_id)
            
            source = self.active_sources[source_id]
            source.status = CDCStatus.FAILED
            
            # Attempt automatic recovery
            if source.retry_count < 3:
                source.retry_count += 1
                await self._restart_source(source_id)
            else:
                # Notify administrators
                await self.event_bus.publish(Event(
                    type="cdc.source.failed.permanently",
                    data={"source_id": source_id, "name": source.name}
                ))
                
    async def _get_credentials(
        self, 
        source_type: CDCSourceType, 
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get database credentials from Vault"""
        path = f"database/creds/{source_type.value}-{config.get('database', 'default')}"
        
        try:
            creds = await self.vault.read_secret(path)
            return {
                "username": creds["username"],
                "password": creds["password"]
            }
        except Exception as e:
            logger.error("failed_to_get_credentials", 
                        source_type=source_type.value,
                        error=str(e))
            raise
            
    def _prepare_sink_config(self, destination_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare sink configuration based on destination type"""
        dest_type = destination_config.get("type", "lakehouse")
        
        if dest_type == "lakehouse":
            return {
                "type": "iceberg",
                "catalog": destination_config.get("catalog", "platform_catalog"),
                "database": destination_config.get("database", "raw"),
                "table_prefix": destination_config.get("table_prefix", "cdc_"),
                "format": destination_config.get("format", "parquet"),
                "compression": "snappy",
                "partitioning": destination_config.get("partitioning", ["date"])
            }
        elif dest_type == "stream":
            return {
                "type": "pulsar",
                "topic": destination_config.get("topic"),
                "tenant": destination_config.get("tenant", "platform"),
                "namespace": destination_config.get("namespace", "cdc"),
                "compression": "lz4"
            }
        else:
            return destination_config
            
    async def _optimize_source(self, source_id: str, metrics: Dict[str, Any]):
        """Apply ML-based optimization to CDC source"""
        # Check if optimization is needed
        throughput = metrics.get("throughput", 0)
        latency = metrics.get("latency", 0)
        error_rate = metrics.get("error_rate", 0)
        
        if error_rate > 0.05:  # 5% error threshold
            # Reduce batch size
            await self.seatunnel.update_job_config(
                source_id,
                {"batch_size": max(100, metrics.get("batch_size", 1000) // 2)}
            )
        elif latency > 1000:  # 1 second latency threshold
            # Increase parallelism
            await self.seatunnel.update_job_config(
                source_id,
                {"parallelism": min(16, metrics.get("parallelism", 4) + 2)}
            )
        elif throughput < 1000:  # Low throughput
            # Increase batch size
            await self.seatunnel.update_job_config(
                source_id,
                {"batch_size": min(10000, metrics.get("batch_size", 1000) * 2)}
            )
            
    def _calculate_uptime(self, source_id: str) -> float:
        """Calculate source uptime in seconds"""
        source = self.active_sources.get(source_id)
        if source:
            return (datetime.utcnow() - source.created_at).total_seconds()
        return 0
        
    async def _calculate_cost(self, source_id: str) -> Dict[str, float]:
        """Calculate cost metrics for CDC source"""
        metrics = await self.seatunnel.get_job_metrics(source_id)
        
        # Simple cost model (customize based on your infrastructure)
        cpu_hours = metrics.get("cpu_seconds", 0) / 3600
        memory_gb_hours = metrics.get("memory_mb_seconds", 0) / 1024 / 3600
        network_gb = metrics.get("network_bytes", 0) / 1024 / 1024 / 1024
        
        return {
            "cpu_cost": cpu_hours * 0.05,  # $0.05 per CPU hour
            "memory_cost": memory_gb_hours * 0.01,  # $0.01 per GB hour
            "network_cost": network_gb * 0.02,  # $0.02 per GB
            "total_cost": (cpu_hours * 0.05) + (memory_gb_hours * 0.01) + (network_gb * 0.02)
        }
        
    async def _restart_source(self, source_id: str):
        """Restart a failed CDC source"""
        logger.info("restarting_cdc_source", source_id=source_id)
        
        try:
            await self.seatunnel.restart_job(source_id)
            
            source = self.active_sources[source_id]
            source.status = CDCStatus.RUNNING
            
            await self.event_bus.publish(Event(
                type="cdc.source.restarted",
                data={"source_id": source_id, "name": source.name}
            ))
        except Exception as e:
            logger.error("failed_to_restart_source",
                        source_id=source_id,
                        error=str(e))
            raise
            
    async def _handle_unhealthy_source(self, source_id: str, health: Dict[str, Any]):
        """Handle unhealthy CDC source"""
        logger.warning("unhealthy_cdc_source",
                      source_id=source_id,
                      health=health)
        
        source = self.active_sources[source_id]
        source.status = CDCStatus.UNHEALTHY
        
        # Emit warning event
        await self.event_bus.publish(Event(
            type="cdc.source.unhealthy",
            data={
                "source_id": source_id,
                "name": source.name,
                "health": health
            }
        )) 