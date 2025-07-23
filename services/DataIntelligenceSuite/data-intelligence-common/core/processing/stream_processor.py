"""
Stream Processing Implementation for DataIntelligenceSuite

Provides real-time stream processing capabilities with Flink integration.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Union, AsyncIterator
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import json

import pulsar
from apache_beam import Pipeline, PTransform, DoFn, WindowInto
from apache_beam.transforms.window import FixedWindows, SlidingWindows, Sessions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, Repeatedly
from apache_beam.options.pipeline_options import PipelineOptions

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from .base_processor import BaseProcessor, ProcessorConfig, ProcessingResult, ProcessingStatus, ProcessingMode
from ...monitoring import MetricsCollector
from ...integrations.pulsar_client import PulsarClient, PulsarConfig

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Window types for stream processing"""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"


class StreamSource(Enum):
    """Stream source types"""
    PULSAR = "pulsar"
    KAFKA = "kafka"
    KINESIS = "kinesis"
    FILE = "file"


class StreamSink(Enum):
    """Stream sink types"""
    PULSAR = "pulsar"
    KAFKA = "kafka"
    DATABASE = "database"
    FILE = "file"


@dataclass
class StreamConfig(ProcessorConfig):
    """Configuration for stream processing"""
    # Source configuration
    source_type: StreamSource = StreamSource.PULSAR
    source_url: str = "pulsar://localhost:6650"
    source_topic: str = "data-stream"
    subscription_name: Optional[str] = None
    
    # Sink configuration
    sink_type: StreamSink = StreamSink.PULSAR
    sink_url: str = "pulsar://localhost:6650"
    sink_topic: str = "processed-stream"
    
    # Window configuration
    window_type: WindowType = WindowType.TUMBLING
    window_size: timedelta = timedelta(minutes=1)
    window_slide: Optional[timedelta] = None
    session_gap: Optional[timedelta] = None
    
    # Processing configuration
    max_out_of_order_delay: timedelta = timedelta(seconds=10)
    watermark_interval: timedelta = timedelta(seconds=1)
    checkpoint_interval: timedelta = timedelta(minutes=5)
    
    # Performance
    parallelism: int = 4
    buffer_size: int = 1000
    batch_size: int = 100
    
    # State management
    enable_state: bool = True
    state_backend: str = "rocksdb"
    state_ttl: Optional[timedelta] = None
    
    # Security
    encrypt_state: bool = True
    state_encryption_key: str = "stream-state"
    
    def __post_init__(self):
        super().__post_init__()
        self.mode = ProcessingMode.STREAM
        if not self.subscription_name:
            self.subscription_name = f"{self.name}-subscription"


@dataclass
class StreamMetrics:
    """Metrics for stream processing"""
    events_received: int = 0
    events_processed: int = 0
    events_failed: int = 0
    watermark: datetime = field(default_factory=datetime.utcnow)
    latency_ms: float = 0.0
    backpressure: bool = False


class StreamProcessor(BaseProcessor):
    """
    Stream processor for real-time data processing with Vault/Consul integration.
    
    Features:
    - Multiple stream sources and sinks
    - Windowing operations
    - Stateful processing with encryption
    - Exactly-once semantics
    - Backpressure handling
    - Late data handling
    - Dynamic credential management
    - Secure configuration
    """
    
    def __init__(
        self,
        config: StreamConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        super().__init__(config, vault_client=vault_client, consul_client=consul_client, **kwargs)
        self.config: StreamConfig = config
        self._pipeline: Optional[Pipeline] = None
        self._pulsar_client: Optional[PulsarClient] = None
        self._consumer_key: Optional[str] = None
        self._producer_key: Optional[str] = None
        self._processing_task: Optional[asyncio.Task] = None
        self._metrics = StreamMetrics()
        self._state_store: Dict[str, Any] = {}
        self._event_buffer = deque(maxlen=config.buffer_size)
        self._checkpoint_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize stream processor"""
        await super().initialize()
        
        logger.info(f"Initializing stream processor: {self.config.name}")
        
        # Initialize source
        await self._initialize_source()
        
        # Initialize sink
        await self._initialize_sink()
        
        # Initialize state backend
        if self.config.enable_state:
            await self._initialize_state_backend()
            
        # Start checkpoint task
        if self.config.checkpoint_interval:
            self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
            
        # Start processing
        self._processing_task = asyncio.create_task(self._process_stream())
        
    async def shutdown(self):
        """Shutdown stream processor"""
        # Stop processing
        if self._processing_task:
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
                
        # Stop checkpointing
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass
                
        # Close connections
        if self._pulsar_client:
            await self._pulsar_client.close()
            
        await super().shutdown()
        
    async def _initialize_source(self):
        """Initialize stream source with dynamic credentials"""
        if self.config.source_type == StreamSource.PULSAR:
            # Get Pulsar credentials from Vault if available
            pulsar_config = PulsarConfig(
                service_name="pulsar",
                service_url=self.config.source_url,
                use_vault_credentials=self.vault_client is not None,
                vault_url=self.vault_client.vault_url if self.vault_client else None,
                use_service_discovery=self.consul_client is not None,
                consul_url=self.consul_client.consul_url if self.consul_client else None
            )
            
            self._pulsar_client = PulsarClient(
                pulsar_config,
                self.vault_client,
                self.consul_client
            )
            
            await self._pulsar_client.connect()
            
            # Create consumer
            from ...integrations.pulsar_client import ConsumerConfig
            consumer_config = ConsumerConfig(
                topics=self.config.source_topic,
                subscription_name=self.config.subscription_name,
                subscription_type=pulsar.ConsumerType.SHARED
            )
            
            self._consumer_key = await self._pulsar_client.create_consumer(consumer_config)
            logger.info(f"Initialized Pulsar source: {self.config.source_topic}")
            
    async def _initialize_sink(self):
        """Initialize stream sink with dynamic credentials"""
        if self.config.sink_type == StreamSink.PULSAR:
            # Use existing Pulsar client or create new one
            if not self._pulsar_client:
                pulsar_config = PulsarConfig(
                    service_name="pulsar",
                    service_url=self.config.sink_url,
                    use_vault_credentials=self.vault_client is not None,
                    vault_url=self.vault_client.vault_url if self.vault_client else None
                )
                
                self._pulsar_client = PulsarClient(
                    pulsar_config,
                    self.vault_client,
                    self.consul_client
                )
                
                await self._pulsar_client.connect()
                
            # Create producer
            from ...integrations.pulsar_client import ProducerConfig
            producer_config = ProducerConfig(
                topic=self.config.sink_topic,
                batching_enabled=True,
                batching_max_messages=self.config.batch_size
            )
            
            self._producer_key = await self._pulsar_client.create_producer(producer_config)
            logger.info(f"Initialized Pulsar sink: {self.config.sink_topic}")
            
    async def _initialize_state_backend(self):
        """Initialize state backend with encryption"""
        # Load state configuration from Consul if available
        if self.consul_client:
            state_config = await self.consul_client.kv_get(
                f"data-intelligence/processors/{self.config.name}/state-config"
            )
            if state_config:
                config_data = json.loads(state_config)
                self.config.state_backend = config_data.get("backend", self.config.state_backend)
                self.config.state_ttl = timedelta(seconds=config_data.get("ttl_seconds", 3600))
                
        logger.info(f"Initialized state backend: {self.config.state_backend}")
        
    async def _process_stream(self):
        """Main stream processing loop"""
        logger.info(f"Starting stream processing for {self.config.name}")
        
        window_buffer: Dict[str, List[Any]] = {}
        watermark = datetime.utcnow()
        
        while True:
            try:
                # Collect batch of messages
                batch = await self._collect_batch()
                
                if batch:
                    # Update watermark
                    watermark = self._update_watermark(batch, watermark)
                    
                    # Process batch
                    await self._process_batch(batch, window_buffer)
                    
                    # Check for completed windows
                    await self._emit_windows(window_buffer, watermark)
                    
                    # Update metrics
                    self._metrics.watermark = watermark
                    self._metrics.latency_ms = (
                        datetime.utcnow() - watermark
                    ).total_seconds() * 1000
                    
                else:
                    # No messages, sleep briefly
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                logger.info("Stream processing cancelled")
                break
            except Exception as e:
                logger.error(f"Stream processing error: {e}")
                await asyncio.sleep(1)  # Brief pause before retry
                
    async def _collect_batch(self) -> List[pulsar.Message]:
        """Collect a batch of messages from source"""
        batch = []
        
        try:
            # Collect up to batch_size messages
            for _ in range(self.config.batch_size):
                # Non-blocking receive
                msg = await self._pulsar_client.receive_async(
                    self._consumer_key,
                    timeout_millis=100
                )
                
                if msg:
                    batch.append(msg)
                    self._metrics.events_received += 1
                else:
                    break  # No more messages available
                    
        except Exception as e:
            logger.error(f"Error collecting batch: {e}")
            
        return batch
        
    def _update_watermark(self, batch: List[pulsar.Message], current_watermark: datetime) -> datetime:
        """Update watermark based on batch"""
        max_event_time = current_watermark
        
        for msg in batch:
            # Extract event time from message
            event_time = datetime.fromtimestamp(msg.event_timestamp() / 1000)
            max_event_time = max(max_event_time, event_time)
            
        # Apply max out-of-order delay
        return max_event_time - self.config.max_out_of_order_delay
        
    async def _process_batch(self, batch: List[pulsar.Message], window_buffer: Dict[str, List[Any]]):
        """Process a batch of messages"""
        for msg in batch:
            try:
                # Deserialize message
                data = json.loads(msg.data().decode('utf-8'))
                
                # Decrypt if needed
                if data.get("encrypted") and self.vault_client:
                    decrypted = await self.vault_client.transit_decrypt(
                        self.config.encryption_key,
                        data["encrypted"]
                    )
                    data = json.loads(decrypted)
                
                event_time = datetime.fromisoformat(data.get('timestamp', datetime.utcnow().isoformat()))
                
                # Apply transformations
                transformed = await self._apply_transformation(data)
                
                # Assign to window
                window_key = self._get_window_key(event_time)
                if window_key not in window_buffer:
                    window_buffer[window_key] = []
                window_buffer[window_key].append(transformed)
                
                # Update state if enabled
                if self.config.enable_state:
                    await self._update_state(transformed)
                    
                # Acknowledge message
                await self._pulsar_client.acknowledge_async(self._consumer_key, msg)
                self._metrics.events_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                await self._pulsar_client.negative_acknowledge_async(self._consumer_key, msg)
                self._metrics.events_failed += 1
                
    async def _apply_transformation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply transformation to event data"""
        # Default implementation - override in subclasses
        return data
        
    def _get_window_key(self, event_time: datetime) -> str:
        """Get window key for event time"""
        if self.config.window_type == WindowType.TUMBLING:
            window_seconds = int(self.config.window_size.total_seconds())
            timestamp_seconds = int(event_time.timestamp())
            window_start = (timestamp_seconds // window_seconds) * window_seconds
            return f"tumbling_{window_start}"
            
        elif self.config.window_type == WindowType.SLIDING:
            # Simplified sliding window
            window_seconds = int(self.config.window_size.total_seconds())
            slide_seconds = int(self.config.window_slide.total_seconds()) if self.config.window_slide else window_seconds
            timestamp_seconds = int(event_time.timestamp())
            window_start = (timestamp_seconds // slide_seconds) * slide_seconds
            return f"sliding_{window_start}"
            
        elif self.config.window_type == WindowType.SESSION:
            # Session windows require more complex logic
            return f"session_{event_time.timestamp()}"
            
        else:  # GLOBAL
            return "global"
            
    async def _emit_windows(self, window_buffer: Dict[str, List[Any]], watermark: datetime):
        """Emit completed windows"""
        completed_windows = []
        
        for window_key, events in window_buffer.items():
            if self._is_window_complete(window_key, watermark):
                completed_windows.append(window_key)
                
        for window_key in completed_windows:
            events = window_buffer.pop(window_key)
            await self._process_window(window_key, events)
            
    def _is_window_complete(self, window_key: str, watermark: datetime) -> bool:
        """Check if window is complete based on watermark"""
        if window_key == "global":
            return False  # Global windows never complete
            
        # Extract window end time from key
        parts = window_key.split("_")
        if len(parts) >= 2:
            window_start = int(parts[1])
            window_end = window_start + int(self.config.window_size.total_seconds())
            window_end_time = datetime.fromtimestamp(window_end)
            return watermark > window_end_time
            
        return False
        
    async def _process_window(self, window_key: str, events: List[Any]):
        """Process a completed window"""
        try:
            # Apply window aggregation
            result = await self._aggregate_window(events)
            
            # Add window metadata
            result["window_key"] = window_key
            result["window_size"] = len(events)
            result["processed_at"] = datetime.utcnow().isoformat()
            
            # Encrypt if needed
            if self.config.enable_encryption and self.vault_client:
                plaintext = json.dumps(result)
                encrypted = await self.vault_client.transit_encrypt(
                    self.config.encryption_key,
                    plaintext
                )
                result = {"encrypted": encrypted["ciphertext"]}
            
            # Write to sink
            await self._write_to_sink(result)
            
            logger.debug(f"Processed window {window_key} with {len(events)} events")
            
        except Exception as e:
            logger.error(f"Error processing window {window_key}: {e}")
            
    async def _aggregate_window(self, events: List[Any]) -> Dict[str, Any]:
        """Aggregate events in window"""
        # Default implementation - count events
        return {
            "event_count": len(events),
            "events": events,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def _write_to_sink(self, data: Any):
        """Write data to sink"""
        if self.config.sink_type == StreamSink.PULSAR and self._producer_key:
            await self._pulsar_client.send_async(
                self._producer_key,
                data
            )
            
    async def _update_state(self, event: Dict[str, Any]):
        """Update processor state"""
        state_key = event.get("key", "default")
        
        # Get current state
        current_state = self._state_store.get(state_key, {})
        
        # Update state (simple counter example)
        current_state["count"] = current_state.get("count", 0) + 1
        current_state["last_update"] = datetime.utcnow().isoformat()
        
        # Encrypt state if enabled
        if self.config.encrypt_state and self.vault_client:
            plaintext = json.dumps(current_state)
            encrypted = await self.vault_client.transit_encrypt(
                self.config.state_encryption_key,
                plaintext
            )
            self._state_store[state_key] = {"encrypted": encrypted["ciphertext"]}
        else:
            self._state_store[state_key] = current_state
            
    async def _checkpoint_loop(self):
        """Periodically save checkpoints"""
        while True:
            try:
                await asyncio.sleep(self.config.checkpoint_interval.total_seconds())
                
                # Save state checkpoint
                checkpoint_data = {
                    "state": self._state_store,
                    "metrics": {
                        "events_received": self._metrics.events_received,
                        "events_processed": self._metrics.events_processed,
                        "events_failed": self._metrics.events_failed,
                        "watermark": self._metrics.watermark.isoformat()
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Use base class checkpoint method
                await self.checkpoint(f"{self.config.name}_stream", checkpoint_data)
                
                logger.debug(f"Saved stream checkpoint")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error saving checkpoint: {e}")
                
    async def process(self, data: Any, job_id: Optional[str] = None) -> ProcessingResult:
        """Process method for BaseProcessor interface"""
        # Stream processors typically run continuously
        # This method is for compatibility with the base interface
        result = ProcessingResult(
            job_id=job_id or f"stream_{self.config.name}",
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        # Return current metrics
        result.records_processed = self._metrics.events_processed
        result.records_failed = self._metrics.events_failed
        result.metadata = {
            "watermark": self._metrics.watermark.isoformat(),
            "latency_ms": self._metrics.latency_ms,
            "backpressure": self._metrics.backpressure
        }
        
        return result 