"""
Unified Processing Framework for DataIntelligenceSuite

Combines batch and stream processing with a unified API and intelligent mode selection.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, AsyncIterator, TypeVar, Generic, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import uuid
from contextlib import asynccontextmanager

from ..mixins import (
    LifecycleMixin, MetricsMixin, CachingMixin, 
    EventMixin, ResilienceMixin, MonitoringMixin,
    ConfigurationMixin, StateMixin, ResourceMixin
)
from ..patterns.resilience import RetryConfig, CircuitBreakerConfig
from ..config.unified import UnifiedServiceConfig, ScalableConfig
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class ProcessingMode(str, Enum):
    """Processing modes"""
    BATCH = "batch"
    STREAM = "stream"
    MICRO_BATCH = "micro_batch"
    HYBRID = "hybrid"  # Lambda architecture
    ADAPTIVE = "adaptive"  # Auto-select based on data


class ProcessingEngine(str, Enum):
    """Available processing engines"""
    # Batch engines
    SPARK = "spark"
    RAY = "ray"
    DASK = "dask"
    PANDAS = "pandas"
    
    # Stream engines
    FLINK = "flink"
    BEAM = "beam"
    BYTEWAX = "bytewax"
    
    # Universal
    NATIVE = "native"  # Pure Python
    AUTO = "auto"  # Auto-select


class WindowType(str, Enum):
    """Window types for processing"""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"
    NONE = "none"


@dataclass
class ProcessingWindow:
    """Window configuration"""
    type: WindowType = WindowType.NONE
    size: Optional[timedelta] = None
    slide: Optional[timedelta] = None
    gap: Optional[timedelta] = None  # For session windows
    
    # Watermark handling
    watermark_delay: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    allowed_lateness: timedelta = field(default_factory=lambda: timedelta(minutes=1))
    
    # Triggering
    early_firing: bool = False
    late_firing: bool = True
    accumulating: bool = True


@dataclass
class ProcessingConfig(UnifiedServiceConfig, ScalableConfig):
    """Unified processing configuration"""
    # Processing settings
    mode: ProcessingMode = ProcessingMode.ADAPTIVE
    engine: ProcessingEngine = ProcessingEngine.AUTO
    
    # Batch settings
    batch_size: int = 10000
    micro_batch_interval: timedelta = field(default_factory=lambda: timedelta(seconds=1))
    
    # Stream settings
    buffer_size: int = 1000
    checkpoint_interval: timedelta = field(default_factory=lambda: timedelta(minutes=1))
    
    # Window configuration
    window: ProcessingWindow = field(default_factory=ProcessingWindow)
    
    # Parallelism
    parallelism: int = 4
    max_parallelism: int = 128
    
    # Quality
    enable_quality_checks: bool = True
    quality_sample_rate: float = 0.1
    
    # State management
    enable_stateful_processing: bool = False
    state_backend: str = "memory"  # memory, rocksdb, ignite
    
    # Optimization
    enable_optimization: bool = True
    optimization_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Lineage
    enable_lineage_tracking: bool = True
    
    # Cost management
    max_cost_per_hour: Optional[float] = None
    preferred_regions: List[str] = field(default_factory=list)


class ProcessingContext:
    """Context for processing operations"""
    
    def __init__(
        self,
        job_id: str,
        config: ProcessingConfig,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.job_id = job_id
        self.config = config
        self.metadata = metadata or {}
        
        # Runtime state
        self.start_time = datetime.utcnow()
        self.checkpoints: List[datetime] = []
        self.metrics: Dict[str, Any] = {}
        self.state: Dict[str, Any] = {}
        
    def checkpoint(self):
        """Mark a checkpoint"""
        self.checkpoints.append(datetime.utcnow())
        
    def get_elapsed_time(self) -> timedelta:
        """Get elapsed processing time"""
        return datetime.utcnow() - self.start_time


class DataSource(ABC):
    """Abstract data source"""
    
    @abstractmethod
    async def read(self) -> AsyncIterator[T]:
        """Read data from source"""
        pass
        
    @abstractmethod
    async def get_schema(self) -> Dict[str, Any]:
        """Get data schema"""
        pass
        
    @abstractmethod
    async def estimate_size(self) -> int:
        """Estimate data size in bytes"""
        pass


class DataSink(ABC):
    """Abstract data sink"""
    
    @abstractmethod
    async def write(self, data: Union[T, List[T]]) -> None:
        """Write data to sink"""
        pass
        
    @abstractmethod
    async def commit(self) -> None:
        """Commit written data"""
        pass
        
    @abstractmethod
    async def rollback(self) -> None:
        """Rollback uncommitted data"""
        pass


class ProcessingStage(ABC):
    """Abstract processing stage"""
    
    @abstractmethod
    async def process(self, data: T, context: ProcessingContext) -> Optional[R]:
        """Process single item"""
        pass
        
    async def process_batch(self, batch: List[T], context: ProcessingContext) -> List[R]:
        """Process batch of items"""
        results = []
        for item in batch:
            result = await self.process(item, context)
            if result is not None:
                results.append(result)
        return results


class ProcessorMixin(
    LifecycleMixin,
    MetricsMixin,
    CachingMixin,
    EventMixin,
    ResilienceMixin,
    MonitoringMixin,
    ConfigurationMixin,
    StateMixin,
    ResourceMixin
):
    """Combined mixin for processors"""
    pass


class UnifiedProcessor(ProcessorMixin, Generic[T, R]):
    """
    Unified processor that handles both batch and stream processing.
    
    Features:
    - Automatic mode selection based on data characteristics
    - Unified API for batch and stream
    - Built-in optimization and resource management
    - Quality checks and lineage tracking
    - State management and checkpointing
    - Cost optimization
    """
    
    def __init__(
        self,
        config: ProcessingConfig,
        source: DataSource[T],
        sink: DataSink[R],
        stages: List[ProcessingStage[T, R]],
        **kwargs
    ):
        super().__init__(
            config=config,
            service_name=config.name,
            **kwargs
        )
        
        self.config = config
        self.source = source
        self.sink = sink
        self.stages = stages
        
        # Processing state
        self._current_job: Optional[str] = None
        self._processing_task: Optional[asyncio.Task] = None
        
        # Engine instances (lazy loaded)
        self._engine = None
        self._quality_monitor = None
        self._lineage_tracker = None
        
    async def _start_internal(self):
        """Initialize processor"""
        await super()._start_internal()
        
        # Initialize engine
        self._engine = await self._create_engine()
        
        # Initialize quality monitor if enabled
        if self.config.enable_quality_checks:
            self._quality_monitor = await self._create_quality_monitor()
            
        # Initialize lineage tracker if enabled
        if self.config.enable_lineage_tracking:
            self._lineage_tracker = await self._create_lineage_tracker()
            
        logger.info(f"Unified processor initialized with mode: {self.config.mode}")
        
    async def _stop_internal(self):
        """Cleanup processor"""
        # Stop any running jobs
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
                
        # Cleanup engine
        if self._engine:
            await self._cleanup_engine()
            
        await super()._stop_internal()
        
    async def process(
        self,
        job_id: Optional[str] = None,
        context_metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute processing job.
        
        Returns processing results and metrics.
        """
        job_id = job_id or str(uuid.uuid4())
        self._current_job = job_id
        
        # Create context
        context = ProcessingContext(
            job_id=job_id,
            config=self.config,
            metadata=context_metadata
        )
        
        # Emit start event
        await self.publish_event(
            event_type="processing.started",
            data={
                "job_id": job_id,
                "mode": self.config.mode.value,
                "engine": self.config.engine.value
            }
        )
        
        try:
            # Determine processing mode
            mode = await self._determine_mode()
            
            # Execute processing
            if mode in (ProcessingMode.BATCH, ProcessingMode.MICRO_BATCH):
                result = await self._process_batch(context)
            elif mode == ProcessingMode.STREAM:
                result = await self._process_stream(context)
            elif mode == ProcessingMode.HYBRID:
                result = await self._process_hybrid(context)
            else:
                raise ValueError(f"Unsupported mode: {mode}")
                
            # Record metrics
            self.record_operation("processing_completed", {
                "job_id": job_id,
                "mode": mode.value,
                "duration": context.get_elapsed_time().total_seconds()
            })
            
            # Emit completion event
            await self.publish_event(
                event_type="processing.completed",
                data={
                    "job_id": job_id,
                    "result": result
                }
            )
            
            return result
            
        except Exception as e:
            # Record error
            self.record_error("processing_failed", e)
            
            # Emit failure event
            await self.publish_event(
                event_type="processing.failed",
                data={
                    "job_id": job_id,
                    "error": str(e)
                }
            )
            
            raise
            
    async def _determine_mode(self) -> ProcessingMode:
        """Determine optimal processing mode"""
        if self.config.mode != ProcessingMode.ADAPTIVE:
            return self.config.mode
            
        # Estimate data characteristics
        try:
            data_size = await self.source.estimate_size()
            
            # Simple heuristics (can be enhanced)
            if data_size < 100 * 1024 * 1024:  # < 100MB
                return ProcessingMode.BATCH
            elif data_size < 1024 * 1024 * 1024:  # < 1GB
                return ProcessingMode.MICRO_BATCH
            else:
                return ProcessingMode.STREAM
                
        except Exception:
            # Default to stream for safety
            return ProcessingMode.STREAM
            
    async def _process_batch(self, context: ProcessingContext) -> Dict[str, Any]:
        """Process data in batch mode"""
        metrics = {
            "records_processed": 0,
            "records_failed": 0,
            "batches_processed": 0
        }
        
        batch = []
        async for item in self.source.read():
            batch.append(item)
            
            if len(batch) >= self.config.batch_size:
                # Process batch
                processed = await self._process_batch_items(batch, context)
                await self.sink.write(processed)
                
                metrics["records_processed"] += len(processed)
                metrics["batches_processed"] += 1
                
                batch = []
                
        # Process remaining items
        if batch:
            processed = await self._process_batch_items(batch, context)
            await self.sink.write(processed)
            
            metrics["records_processed"] += len(processed)
            metrics["batches_processed"] += 1
            
        # Commit results
        await self.sink.commit()
        
        return metrics
        
    async def _process_stream(self, context: ProcessingContext) -> Dict[str, Any]:
        """Process data in stream mode"""
        metrics = {
            "records_processed": 0,
            "records_failed": 0,
            "checkpoints": 0
        }
        
        checkpoint_interval = self.config.checkpoint_interval
        last_checkpoint = datetime.utcnow()
        
        async for item in self.source.read():
            try:
                # Process through stages
                result = item
                for stage in self.stages:
                    result = await stage.process(result, context)
                    if result is None:
                        break
                        
                if result is not None:
                    await self.sink.write(result)
                    metrics["records_processed"] += 1
                    
            except Exception as e:
                metrics["records_failed"] += 1
                logger.error(f"Failed to process record: {e}")
                
            # Checkpoint if needed
            if datetime.utcnow() - last_checkpoint >= checkpoint_interval:
                await self.sink.commit()
                context.checkpoint()
                metrics["checkpoints"] += 1
                last_checkpoint = datetime.utcnow()
                
        # Final commit
        await self.sink.commit()
        
        return metrics
        
    async def _process_hybrid(self, context: ProcessingContext) -> Dict[str, Any]:
        """Process data in hybrid mode (Lambda architecture)"""
        # Run batch and stream processing in parallel
        batch_task = asyncio.create_task(self._process_batch(context))
        stream_task = asyncio.create_task(self._process_stream(context))
        
        batch_result, stream_result = await asyncio.gather(
            batch_task, stream_task
        )
        
        return {
            "batch": batch_result,
            "stream": stream_result
        }
        
    async def _process_batch_items(
        self,
        batch: List[T],
        context: ProcessingContext
    ) -> List[R]:
        """Process a batch of items through all stages"""
        results = batch
        
        for stage in self.stages:
            results = await stage.process_batch(results, context)
            if not results:
                break
                
        return results
        
    async def _create_engine(self):
        """Create processing engine based on configuration"""
        # This would create actual engine instances
        # For now, return a placeholder
        return None
        
    async def _cleanup_engine(self):
        """Cleanup processing engine"""
        # Cleanup engine resources
        pass
        
    async def _create_quality_monitor(self):
        """Create quality monitoring component"""
        # This would create quality monitor
        return None
        
    async def _create_lineage_tracker(self):
        """Create lineage tracking component"""
        # This would create lineage tracker
        return None
        
    # Pipeline builder interface
    @classmethod
    def pipeline(cls, config: ProcessingConfig) -> 'PipelineBuilder':
        """Create a pipeline builder"""
        return PipelineBuilder(config)


class PipelineBuilder:
    """Fluent interface for building processing pipelines"""
    
    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.source = None
        self.sink = None
        self.stages = []
        
    def from_source(self, source: DataSource) -> 'PipelineBuilder':
        """Set data source"""
        self.source = source
        return self
        
    def to_sink(self, sink: DataSink) -> 'PipelineBuilder':
        """Set data sink"""
        self.sink = sink
        return self
        
    def transform(self, stage: ProcessingStage) -> 'PipelineBuilder':
        """Add transformation stage"""
        self.stages.append(stage)
        return self
        
    def filter(self, predicate: Callable[[T], bool]) -> 'PipelineBuilder':
        """Add filter stage"""
        class FilterStage(ProcessingStage):
            async def process(self, data: T, context: ProcessingContext) -> Optional[T]:
                return data if predicate(data) else None
                
        self.stages.append(FilterStage())
        return self
        
    def map(self, mapper: Callable[[T], R]) -> 'PipelineBuilder':
        """Add map stage"""
        class MapStage(ProcessingStage):
            async def process(self, data: T, context: ProcessingContext) -> R:
                return mapper(data)
                
        self.stages.append(MapStage())
        return self
        
    def build(self, **kwargs) -> UnifiedProcessor:
        """Build the processor"""
        if not self.source:
            raise ValueError("Source not specified")
        if not self.sink:
            raise ValueError("Sink not specified")
            
        return UnifiedProcessor(
            config=self.config,
            source=self.source,
            sink=self.sink,
            stages=self.stages,
            **kwargs
        )


# Export main components
__all__ = [
    'ProcessingMode',
    'ProcessingEngine',
    'WindowType',
    'ProcessingWindow',
    'ProcessingConfig',
    'ProcessingContext',
    'DataSource',
    'DataSink',
    'ProcessingStage',
    'UnifiedProcessor',
    'PipelineBuilder'
] 