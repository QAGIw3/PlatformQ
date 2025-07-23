"""
Stream Processor for real-time data processing.
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import deque
import uuid

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class ProcessingMode(str, Enum):
    """Stream processing modes."""
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"
    BATCH = "batch"


class WindowType(str, Enum):
    """Window types for stream processing."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"


class AggregationType(str, Enum):
    """Aggregation types."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    DISTINCT_COUNT = "distinct_count"
    PERCENTILE = "percentile"
    STDDEV = "stddev"


@dataclass
class StreamConfig:
    """Configuration for stream processing."""
    processing_mode: ProcessingMode = ProcessingMode.STREAMING
    parallelism: int = 4
    checkpoint_interval: int = 60000  # ms
    watermark_interval: int = 1000  # ms
    max_out_of_orderness: int = 5000  # ms
    enable_exactly_once: bool = True
    state_backend: str = "rocksdb"
    buffer_timeout: int = 100  # ms
    max_batch_size: int = 1000


@dataclass
class StreamMetrics:
    """Metrics for stream processing."""
    events_processed: int = 0
    events_per_second: float = 0.0
    processing_latency_ms: float = 0.0
    watermark_lag_ms: float = 0.0
    checkpoint_duration_ms: float = 0.0
    state_size_bytes: int = 0
    backpressure: float = 0.0
    errors_count: int = 0


@dataclass
class StreamWindow:
    """Represents a processing window."""
    window_id: str
    start_time: datetime
    end_time: datetime
    window_type: WindowType
    events: List[Dict[str, Any]] = field(default_factory=list)
    aggregates: Dict[str, Any] = field(default_factory=dict)
    is_closed: bool = False


class StreamProcessor:
    """
    Main stream processing engine for real-time data processing.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        ignite_client: Optional[IgniteClient] = None,
        config: Optional[StreamConfig] = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        self.config = config or StreamConfig()
        
        # Processing state
        self.jobs: Dict[str, "StreamJob"] = {}
        self.windows: Dict[str, StreamWindow] = {}
        self.event_buffer = deque(maxlen=10000)
        self.metrics = StreamMetrics()
        
        # Background tasks
        self._running = True
        self._background_tasks = []
        
        # Watermark tracking
        self.current_watermark = datetime.utcnow()
        self.max_event_time = datetime.utcnow()
        
        logger.info(f"Stream Processor initialized with mode: {self.config.processing_mode}")
        
    async def initialize(self):
        """Initialize stream processor."""
        # Start background tasks
        tasks = [
            self._process_events(),
            self._update_watermarks(),
            self._checkpoint_state(),
            self._calculate_metrics()
        ]
        
        for task_coro in tasks:
            task = asyncio.create_task(task_coro)
            self._background_tasks.append(task)
        
        # Subscribe to events
        await self.event_bus.subscribe("stream.submit", self._handle_job_submit)
        await self.event_bus.subscribe("stream.data", self._handle_stream_data)
        
        logger.info("Stream Processor initialized")
        
    async def create_job(
        self,
        job_id: str,
        name: str,
        source_topic: str,
        sink_topic: str,
        transformations: List[Dict[str, Any]],
        window_config: Optional[Dict[str, Any]] = None,
        parallelism: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a new streaming job."""
        if job_id in self.jobs:
            raise ValueError(f"Job {job_id} already exists")
        
        # Create job
        job = StreamJob(
            job_id=job_id,
            name=name,
            source_topic=source_topic,
            sink_topic=sink_topic,
            transformations=transformations,
            window_config=window_config,
            parallelism=parallelism or self.config.parallelism,
            created_at=datetime.utcnow()
        )
        
        self.jobs[job_id] = job
        
        # Subscribe to source topic
        await self.event_bus.subscribe(source_topic, 
            lambda data: self._handle_job_event(job_id, data))
        
        # Cache job metadata
        job_metadata = job.to_dict()
        await self.cache_manager.set(f"stream:job:{job_id}", job_metadata)
        
        # Publish job created event
        await self.event_bus.publish("stream.job.created", job_metadata)
        
        logger.info(f"Created streaming job {job_id}: {name}")
        
        return job_metadata
        
    async def process_event(
        self,
        job_id: str,
        event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Process a single event through a job."""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
        
        job = self.jobs[job_id]
        
        # Extract event time
        event_time = self._extract_event_time(event)
        
        # Check watermark
        if event_time < self.current_watermark:
            logger.warning(f"Late event detected: {event_time} < {self.current_watermark}")
            self.metrics.errors_count += 1
            
            # Handle late event based on configuration
            if not job.allow_late_events:
                return None
        
        # Apply transformations
        transformed = event
        for transformation in job.transformations:
            transformed = await self._apply_transformation(transformed, transformation)
            if transformed is None:
                return None  # Filtered out
        
        # Handle windowing if configured
        if job.window_config:
            window_result = await self._process_windowed_event(
                job_id,
                transformed,
                event_time,
                job.window_config
            )
            if window_result:
                transformed = window_result
            else:
                return None  # Added to window, not yet emitted
        
        # Update metrics
        self.metrics.events_processed += 1
        
        # Publish to sink
        await self.event_bus.publish(job.sink_topic, transformed)
        
        return transformed
        
    async def _apply_transformation(
        self,
        event: Dict[str, Any],
        transformation: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Apply a transformation to an event."""
        transform_type = transformation.get("type")
        
        if transform_type == "filter":
            # Filter transformation
            condition = transformation.get("condition")
            if not self._evaluate_condition(event, condition):
                return None
                
        elif transform_type == "map":
            # Map transformation
            mapping = transformation.get("mapping", {})
            for target_field, source_expr in mapping.items():
                event[target_field] = self._evaluate_expression(event, source_expr)
                
        elif transform_type == "flatmap":
            # FlatMap transformation (returns multiple events)
            # For simplicity, returning single event here
            field = transformation.get("field")
            if field in event and isinstance(event[field], list):
                # Would normally emit multiple events
                pass
                
        elif transform_type == "keyby":
            # Key by transformation
            key_field = transformation.get("field")
            if key_field in event:
                event["_key"] = event[key_field]
                
        elif transform_type == "aggregate":
            # Aggregation transformation
            # This would normally be handled in windowing
            pass
            
        return event
        
    async def _process_windowed_event(
        self,
        job_id: str,
        event: Dict[str, Any],
        event_time: datetime,
        window_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Process event in a window."""
        window_type = WindowType(window_config.get("type", "tumbling"))
        window_size = window_config.get("size", 60000)  # ms
        
        # Determine window
        window_id = self._get_window_id(event_time, window_type, window_size)
        
        # Get or create window
        if window_id not in self.windows:
            window_start = self._get_window_start(event_time, window_size)
            window_end = window_start + timedelta(milliseconds=window_size)
            
            self.windows[window_id] = StreamWindow(
                window_id=window_id,
                start_time=window_start,
                end_time=window_end,
                window_type=window_type
            )
        
        window = self.windows[window_id]
        
        # Add event to window
        window.events.append(event)
        
        # Update aggregates
        aggregations = window_config.get("aggregations", [])
        for agg in aggregations:
            self._update_aggregate(window, event, agg)
        
        # Check if window should be emitted
        if self._should_emit_window(window, event_time):
            result = self._finalize_window(window)
            window.is_closed = True
            return result
        
        return None
        
    def _update_aggregate(
        self,
        window: StreamWindow,
        event: Dict[str, Any],
        aggregation: Dict[str, Any]
    ):
        """Update window aggregates."""
        agg_type = AggregationType(aggregation.get("type", "count"))
        field = aggregation.get("field")
        alias = aggregation.get("alias", f"{agg_type.value}_{field}")
        
        if field and field not in event:
            return
        
        value = event.get(field, 1) if field else 1
        
        if agg_type == AggregationType.COUNT:
            window.aggregates[alias] = window.aggregates.get(alias, 0) + 1
            
        elif agg_type == AggregationType.SUM:
            window.aggregates[alias] = window.aggregates.get(alias, 0) + value
            
        elif agg_type == AggregationType.AVG:
            sum_key = f"_sum_{alias}"
            count_key = f"_count_{alias}"
            window.aggregates[sum_key] = window.aggregates.get(sum_key, 0) + value
            window.aggregates[count_key] = window.aggregates.get(count_key, 0) + 1
            window.aggregates[alias] = window.aggregates[sum_key] / window.aggregates[count_key]
            
        elif agg_type == AggregationType.MIN:
            current = window.aggregates.get(alias)
            window.aggregates[alias] = min(current, value) if current is not None else value
            
        elif agg_type == AggregationType.MAX:
            current = window.aggregates.get(alias)
            window.aggregates[alias] = max(current, value) if current is not None else value
            
    def _should_emit_window(self, window: StreamWindow, event_time: datetime) -> bool:
        """Check if window should be emitted."""
        # Emit if watermark passed window end
        return self.current_watermark >= window.end_time
        
    def _finalize_window(self, window: StreamWindow) -> Dict[str, Any]:
        """Finalize window and create result."""
        return {
            "window_start": window.start_time.isoformat(),
            "window_end": window.end_time.isoformat(),
            "event_count": len(window.events),
            "aggregates": window.aggregates,
            "window_type": window.window_type.value
        }
        
    def _extract_event_time(self, event: Dict[str, Any]) -> datetime:
        """Extract event time from event."""
        # Try common timestamp fields
        for field in ["timestamp", "event_time", "time", "ts"]:
            if field in event:
                ts = event[field]
                if isinstance(ts, str):
                    return datetime.fromisoformat(ts)
                elif isinstance(ts, (int, float)):
                    return datetime.fromtimestamp(ts / 1000)  # Assume ms
        
        # Default to processing time
        return datetime.utcnow()
        
    def _get_window_id(
        self,
        event_time: datetime,
        window_type: WindowType,
        window_size: int
    ) -> str:
        """Get window ID for event."""
        if window_type == WindowType.TUMBLING:
            window_start = self._get_window_start(event_time, window_size)
            return f"tumbling_{window_start.timestamp()}"
        else:
            # Simplified for other window types
            return f"{window_type.value}_{event_time.timestamp()}"
            
    def _get_window_start(self, event_time: datetime, window_size: int) -> datetime:
        """Get window start time."""
        ts = int(event_time.timestamp() * 1000)  # Convert to ms
        window_start_ms = (ts // window_size) * window_size
        return datetime.fromtimestamp(window_start_ms / 1000)
        
    def _evaluate_condition(self, event: Dict[str, Any], condition: Dict[str, Any]) -> bool:
        """Evaluate filter condition."""
        # Simple condition evaluation
        field = condition.get("field")
        operator = condition.get("operator", "==")
        value = condition.get("value")
        
        if field not in event:
            return False
        
        event_value = event[field]
        
        if operator == "==":
            return event_value == value
        elif operator == "!=":
            return event_value != value
        elif operator == ">":
            return event_value > value
        elif operator == ">=":
            return event_value >= value
        elif operator == "<":
            return event_value < value
        elif operator == "<=":
            return event_value <= value
        elif operator == "in":
            return event_value in value
        elif operator == "contains":
            return value in str(event_value)
        
        return True
        
    def _evaluate_expression(self, event: Dict[str, Any], expression: str) -> Any:
        """Evaluate mapping expression."""
        # Simple expression evaluation
        if expression.startswith("$"):
            # Field reference
            field = expression[1:]
            return event.get(field)
        else:
            # Literal value
            return expression
            
    async def _handle_job_event(self, job_id: str, event_data: Dict[str, Any]):
        """Handle event for a specific job."""
        try:
            await self.process_event(job_id, event_data)
        except Exception as e:
            logger.error(f"Error processing event for job {job_id}: {e}")
            self.metrics.errors_count += 1
            
    async def _handle_job_submit(self, job_data: Dict[str, Any]):
        """Handle job submission event."""
        try:
            await self.create_job(**job_data)
        except Exception as e:
            logger.error(f"Error submitting job: {e}")
            
    async def _handle_stream_data(self, data: Dict[str, Any]):
        """Handle incoming stream data."""
        # Add to buffer for processing
        self.event_buffer.append(data)
        
    async def _process_events(self):
        """Background task to process events."""
        while self._running:
            try:
                if self.event_buffer:
                    # Process batch
                    batch_size = min(len(self.event_buffer), self.config.max_batch_size)
                    batch = [self.event_buffer.popleft() for _ in range(batch_size)]
                    
                    # Process each event
                    for event in batch:
                        job_id = event.get("_job_id")
                        if job_id and job_id in self.jobs:
                            await self.process_event(job_id, event)
                            
                await asyncio.sleep(self.config.buffer_timeout / 1000)
                
            except Exception as e:
                logger.error(f"Error in event processing: {e}")
                await asyncio.sleep(1)
                
    async def _update_watermarks(self):
        """Background task to update watermarks."""
        while self._running:
            try:
                # Update watermark based on max event time and allowed lateness
                new_watermark = self.max_event_time - timedelta(
                    milliseconds=self.config.max_out_of_orderness
                )
                
                if new_watermark > self.current_watermark:
                    self.current_watermark = new_watermark
                    
                    # Check for windows to emit
                    for window_id, window in list(self.windows.items()):
                        if not window.is_closed and self._should_emit_window(window, datetime.utcnow()):
                            result = self._finalize_window(window)
                            window.is_closed = True
                            
                            # Emit window result
                            for job_id, job in self.jobs.items():
                                if job.window_config:
                                    await self.event_bus.publish(job.sink_topic, result)
                                    
                await asyncio.sleep(self.config.watermark_interval / 1000)
                
            except Exception as e:
                logger.error(f"Error updating watermarks: {e}")
                await asyncio.sleep(1)
                
    async def _checkpoint_state(self):
        """Background task to checkpoint state."""
        while self._running:
            try:
                await asyncio.sleep(self.config.checkpoint_interval / 1000)
                
                # Checkpoint state
                checkpoint_data = {
                    "jobs": {job_id: job.to_dict() for job_id, job in self.jobs.items()},
                    "windows": {
                        window_id: {
                            "start_time": window.start_time.isoformat(),
                            "end_time": window.end_time.isoformat(),
                            "event_count": len(window.events),
                            "aggregates": window.aggregates
                        }
                        for window_id, window in self.windows.items()
                        if not window.is_closed
                    },
                    "watermark": self.current_watermark.isoformat(),
                    "metrics": {
                        "events_processed": self.metrics.events_processed,
                        "errors_count": self.metrics.errors_count
                    }
                }
                
                # Save checkpoint
                checkpoint_id = f"checkpoint_{datetime.utcnow().timestamp()}"
                await self.cache_manager.set(
                    f"stream:checkpoint:{checkpoint_id}",
                    checkpoint_data,
                    ttl=86400  # 24 hours
                )
                
                logger.debug(f"Checkpoint saved: {checkpoint_id}")
                
            except Exception as e:
                logger.error(f"Error during checkpointing: {e}")
                
    async def _calculate_metrics(self):
        """Background task to calculate metrics."""
        last_count = 0
        
        while self._running:
            try:
                await asyncio.sleep(1)  # Calculate every second
                
                # Calculate events per second
                current_count = self.metrics.events_processed
                self.metrics.events_per_second = current_count - last_count
                last_count = current_count
                
                # Calculate watermark lag
                self.metrics.watermark_lag_ms = (
                    self.max_event_time - self.current_watermark
                ).total_seconds() * 1000
                
                # Update metrics cache
                await self.cache_manager.set(
                    "stream:metrics:current",
                    {
                        "events_processed": self.metrics.events_processed,
                        "events_per_second": self.metrics.events_per_second,
                        "processing_latency_ms": self.metrics.processing_latency_ms,
                        "watermark_lag_ms": self.metrics.watermark_lag_ms,
                        "errors_count": self.metrics.errors_count,
                        "timestamp": datetime.utcnow().isoformat()
                    },
                    ttl=60
                )
                
            except Exception as e:
                logger.error(f"Error calculating metrics: {e}")
                
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get job status."""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
        
        job = self.jobs[job_id]
        return {
            **job.to_dict(),
            "metrics": {
                "events_processed": self.metrics.events_processed,
                "events_per_second": self.metrics.events_per_second,
                "errors": self.metrics.errors_count
            }
        }
        
    async def cancel_job(self, job_id: str):
        """Cancel a streaming job."""
        if job_id not in self.jobs:
            raise ValueError(f"Job {job_id} not found")
        
        job = self.jobs[job_id]
        
        # Unsubscribe from source topic
        # (In real implementation, would properly unsubscribe)
        
        # Remove job
        del self.jobs[job_id]
        
        # Remove from cache
        await self.cache_manager.delete(f"stream:job:{job_id}")
        
        # Publish event
        await self.event_bus.publish("stream.job.cancelled", {"job_id": job_id})
        
        logger.info(f"Cancelled job {job_id}")
        
    async def close(self):
        """Clean up resources."""
        self._running = False
        
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        
        logger.info("Stream processor closed")


@dataclass
class StreamJob:
    """Represents a streaming job."""
    job_id: str
    name: str
    source_topic: str
    sink_topic: str
    transformations: List[Dict[str, Any]]
    window_config: Optional[Dict[str, Any]]
    parallelism: int
    created_at: datetime
    status: str = "RUNNING"
    allow_late_events: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_id": self.job_id,
            "name": self.name,
            "source_topic": self.source_topic,
            "sink_topic": self.sink_topic,
            "transformations": self.transformations,
            "window_config": self.window_config,
            "parallelism": self.parallelism,
            "created_at": self.created_at.isoformat(),
            "status": self.status,
            "allow_late_events": self.allow_late_events
        } 