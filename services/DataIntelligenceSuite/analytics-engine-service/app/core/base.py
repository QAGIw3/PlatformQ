"""
Analytics Service Base Classes

Migrated to use the unified data-intelligence-common library.
"""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig, DatabaseConnectionConfig
from data_intelligence_common.core.processing import (
    UnifiedProcessor, ProcessingConfig, ProcessingMode, ProcessingEngine,
    DataSource, DataSink, ProcessingStage, ProcessingContext,
    FileSource, DatabaseSource, EventBusSource,
    FileSink, DatabaseSink, EventBusSink,
    QualityCheckStage, SchemaValidationStage, DataCleaningStage,
    CommonQualityRules
)
from data_intelligence_common.core.events import Event, EventType, create_processing_event
from data_intelligence_common.core.mixins import ServiceMixin
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class AnalyticsMode(str, Enum):
    """Analytics execution mode"""
    BATCH = "batch"         # Use Trino for complex queries
    REALTIME = "realtime"   # Use Druid/Ignite for low latency
    STREAM = "stream"       # Use streaming engine
    AUTO = "auto"           # Automatically choose based on query


@dataclass
class AnalyticsServiceConfig(UnifiedServiceConfig):
    """Configuration for analytics service"""
    # Analytics specific
    default_mode: AnalyticsMode = AnalyticsMode.AUTO
    enable_caching: bool = True
    cache_ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Engines
    trino_config: Optional[DatabaseConnectionConfig] = None
    druid_config: Optional[DatabaseConnectionConfig] = None
    ignite_config: Optional[DatabaseConnectionConfig] = None
    clickhouse_config: Optional[DatabaseConnectionConfig] = None
    
    # ML settings
    enable_ml_predictions: bool = True
    ml_model_registry_url: str = ""
    
    # Streaming
    enable_streaming: bool = True
    stream_batch_size: int = 1000
    stream_window_size: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    def __post_init__(self):
        super().__post_init__()
        
        # Set default engine configs if not provided
        if not self.trino_config:
            self.trino_config = DatabaseConnectionConfig(
                host="trino.analytics.local",
                port=8080,
                database="analytics"
            )
            
        if not self.ignite_config:
            self.ignite_config = DatabaseConnectionConfig(
                host="ignite.analytics.local",
                port=10800,
                database="analytics",
                cache_mode="PARTITIONED"
            )


class AnalyticsBaseService(DataIntelligenceBaseService):
    """
    Base service for analytics operations.
    
    Inherits from DataIntelligenceBaseService which uses ServiceMixin,
    providing all common functionality like metrics, caching, events, etc.
    """
    
    def __init__(self, config: AnalyticsServiceConfig):
        super().__init__(config)
        self.config = config
        
        # Analytics engines (initialized in _initialize_internal)
        self._batch_engine = None
        self._realtime_engine = None
        self._stream_processor = None
        self._ml_engine = None
        
    async def _initialize_internal(self):
        """Initialize analytics-specific components"""
        await super()._initialize_internal()
        
        # Initialize analytics engines
        await self._initialize_engines()
        
        # Register analytics-specific health checks
        self.register_health_check(
            "batch_engine",
            self._check_batch_engine_health,
            critical=False
        )
        
        self.register_health_check(
            "realtime_engine", 
            self._check_realtime_engine_health,
            critical=True
        )
        
        logger.info("Analytics service initialized")
        
    async def _initialize_engines(self):
        """Initialize analytics engines"""
        # Initialize batch engine (Trino)
        if self.config.trino_config:
            from ..engines.trino_engine import TrinoEngine
            self._batch_engine = TrinoEngine(self.config.trino_config)
            await self._batch_engine.initialize()
            
        # Initialize real-time engines
        if self.config.ignite_config:
            from ..engines.ignite_engine import IgniteEngine
            self._realtime_engine = IgniteEngine(self.config.ignite_config)
            await self._realtime_engine.initialize()
            
        # Initialize stream processor if enabled
        if self.config.enable_streaming:
            self._stream_processor = await self._create_stream_processor()
            
        # Initialize ML engine if enabled
        if self.config.enable_ml_predictions:
            from ..ml.ml_engine import MLEngine
            self._ml_engine = MLEngine(
                model_registry_url=self.config.ml_model_registry_url
            )
            await self._ml_engine.initialize()
            
    async def _create_stream_processor(self) -> UnifiedProcessor:
        """Create unified stream processor for analytics"""
        # Create processing configuration
        processing_config = ProcessingConfig(
            name=f"{self.config.name}_stream_processor",
            mode=ProcessingMode.STREAM,
            engine=ProcessingEngine.AUTO,
            batch_size=self.config.stream_batch_size,
            checkpoint_interval=timedelta(minutes=1),
            enable_quality_checks=True,
            enable_lineage_tracking=True
        )
        
        # Create event source
        source = EventBusSource(
            event_bus=self.event_bus,
            topic="analytics.events",
            subscription="analytics_processor"
        )
        
        # Create sink to real-time database
        sink = DatabaseSink(
            client=self._realtime_engine,
            table="analytics_results",
            mode="append"
        )
        
        # Build processing pipeline
        processor = UnifiedProcessor.pipeline(processing_config)\
            .from_source(source)\
            .transform(DataCleaningStage(
                trim_strings=True,
                remove_nulls=True
            ))\
            .transform(SchemaValidationStage({
                "required": ["timestamp", "metric_name", "value"],
                "properties": {
                    "timestamp": {"type": "string"},
                    "metric_name": {"type": "string"},
                    "value": {"type": "number"},
                    "dimensions": {"type": "object"}
                }
            }))\
            .transform(QualityCheckStage([
                CommonQualityRules.not_null("metric_name"),
                CommonQualityRules.not_null("value"),
                CommonQualityRules.in_range("value", -1e9, 1e9)
            ]))\
            .transform(self._create_analytics_stage())\
            .to_sink(sink)\
            .build(
                metrics_collector=self.metrics,
                event_bus=self.event_bus,
                cache_manager=self.cache
            )
            
        return processor
        
    def _create_analytics_stage(self) -> ProcessingStage:
        """Create custom analytics processing stage"""
        class AnalyticsStage(ProcessingStage):
            def __init__(self, ml_engine):
                self.ml_engine = ml_engine
                
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                # Add analytics metadata
                data["_processed_at"] = datetime.utcnow().isoformat()
                data["_processor_id"] = context.job_id
                
                # Apply ML predictions if available
                if self.ml_engine and data.get("metric_name") in ["cpu_usage", "memory_usage"]:
                    prediction = await self.ml_engine.predict_anomaly(data)
                    data["_anomaly_score"] = prediction.get("score", 0)
                    data["_is_anomaly"] = prediction.get("is_anomaly", False)
                    
                return data
                
        return AnalyticsStage(self._ml_engine)
        
    async def execute_query(
        self,
        query: str,
        mode: AnalyticsMode = AnalyticsMode.AUTO,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Execute analytics query using appropriate engine.
        
        Uses caching mixin for automatic result caching.
        """
        # Generate cache key
        cache_key = f"analytics_query:{query}:{mode}:{kwargs}"
        
        # Try to get from cache
        cached_result = await self.get_cached(cache_key)
        if cached_result:
            return cached_result
            
        # Determine execution mode
        if mode == AnalyticsMode.AUTO:
            mode = self._determine_mode(query, **kwargs)
            
        # Execute based on mode
        if mode == AnalyticsMode.BATCH:
            result = await self._execute_batch_query(query, **kwargs)
        elif mode == AnalyticsMode.REALTIME:
            result = await self._execute_realtime_query(query, **kwargs)
        else:
            result = await self._execute_stream_query(query, **kwargs)
            
        # Cache result
        await self.cache_result(cache_key, result, ttl=self.config.cache_ttl.total_seconds())
        
        # Emit analytics event
        await self.publish_event(
            event_type="analytics.query_executed",
            data={
                "mode": mode.value,
                "query_hash": hash(query),
                "result_count": len(result.get("data", [])),
                "execution_time": result.get("execution_time", 0)
            }
        )
        
        return result
        
    def _determine_mode(self, query: str, **kwargs) -> AnalyticsMode:
        """Determine optimal execution mode based on query characteristics"""
        # Simple heuristics - can be enhanced
        query_lower = query.lower()
        
        # Check for real-time indicators
        if any(keyword in query_lower for keyword in ["last", "current", "now", "real-time"]):
            return AnalyticsMode.REALTIME
            
        # Check for complex analytics
        if any(keyword in query_lower for keyword in ["join", "window", "over", "partition"]):
            return AnalyticsMode.BATCH
            
        # Check time range
        time_range = kwargs.get("time_range", "7d")
        if time_range in ["1m", "5m", "15m", "1h"]:
            return AnalyticsMode.REALTIME
            
        return AnalyticsMode.BATCH
        
    async def _execute_batch_query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute query using batch engine"""
        if not self._batch_engine:
            raise ValueError("Batch engine not initialized")
            
        start_time = datetime.utcnow()
        
        try:
            result = await self._batch_engine.execute(query, **kwargs)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            # Record metrics
            self.record_operation("batch_query_executed", {
                "execution_time": execution_time,
                "result_count": len(result)
            })
            
            return {
                "data": result,
                "mode": "batch",
                "execution_time": execution_time,
                "engine": "trino"
            }
            
        except Exception as e:
            self.record_error("batch_query_failed", e)
            raise
            
    async def _execute_realtime_query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute query using real-time engine"""
        if not self._realtime_engine:
            raise ValueError("Real-time engine not initialized")
            
        start_time = datetime.utcnow()
        
        try:
            result = await self._realtime_engine.execute(query, **kwargs)
            
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            # Record metrics
            self.record_operation("realtime_query_executed", {
                "execution_time": execution_time,
                "result_count": len(result)
            })
            
            return {
                "data": result,
                "mode": "realtime",
                "execution_time": execution_time,
                "engine": "ignite"
            }
            
        except Exception as e:
            self.record_error("realtime_query_failed", e)
            raise
            
    async def _execute_stream_query(self, query: str, **kwargs) -> Dict[str, Any]:
        """Execute streaming query"""
        if not self._stream_processor:
            raise ValueError("Stream processor not initialized")
            
        # For streaming, we return current state
        # In a real implementation, this would set up continuous query
        return {
            "data": [],
            "mode": "stream",
            "status": "streaming",
            "subscription_id": str(uuid.uuid4())
        }
        
    async def _check_batch_engine_health(self) -> Dict[str, Any]:
        """Check batch engine health"""
        if not self._batch_engine:
            return {"healthy": False, "reason": "Not initialized"}
            
        try:
            await self._batch_engine.execute("SELECT 1")
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "reason": str(e)}
            
    async def _check_realtime_engine_health(self) -> Dict[str, Any]:
        """Check real-time engine health"""
        if not self._realtime_engine:
            return {"healthy": False, "reason": "Not initialized"}
            
        try:
            await self._realtime_engine.health_check()
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "reason": str(e)}
            
    async def _start_internal(self):
        """Start analytics-specific components"""
        await super()._start_internal()
        
        # Start stream processor if configured
        if self._stream_processor:
            await self._stream_processor.start()
            
        logger.info("Analytics service started")
        
    async def _stop_internal(self):
        """Stop analytics-specific components"""
        # Stop stream processor
        if self._stream_processor:
            await self._stream_processor.stop()
            
        # Close engine connections
        if self._batch_engine:
            await self._batch_engine.close()
            
        if self._realtime_engine:
            await self._realtime_engine.close()
            
        if self._ml_engine:
            await self._ml_engine.close()
            
        await super()._stop_internal()
        
        logger.info("Analytics service stopped")


# Export main components
__all__ = [
    'AnalyticsMode',
    'AnalyticsServiceConfig', 
    'AnalyticsBaseService'
] 