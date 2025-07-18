"""
Base Event Processor

Provides standardized event processing patterns with built-in retry,
error handling, and monitoring capabilities.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable, Set, Type
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import traceback
import uuid

import pulsar
from pulsar.schema import AvroSchema, JsonSchema
from tenacity import retry, stop_after_attempt, wait_exponential

from .monitoring import EventMetrics, EventTracer
from .retry import RetryPolicy, ExponentialBackoff

logger = logging.getLogger(__name__)


class EventProcessingStatus(Enum):
    """Status of event processing"""
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    FILTERED = "filtered"
    SKIPPED = "skipped"


@dataclass
class EventContext:
    """Context information for event processing"""
    event_id: str
    event_type: str
    source_topic: str
    timestamp: datetime
    tenant_id: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    span_id: Optional[str] = None
    trace_id: Optional[str] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_message(cls, msg: pulsar.Message, tenant_id: Optional[str] = None) -> "EventContext":
        """Create context from Pulsar message"""
        properties = msg.properties() or {}
        
        return cls(
            event_id=properties.get("event_id", str(msg.message_id())),
            event_type=properties.get("event_type", "unknown"),
            source_topic=msg.topic_name(),
            timestamp=datetime.fromtimestamp(msg.publish_timestamp() / 1000, tz=timezone.utc),
            tenant_id=tenant_id or properties.get("tenant_id"),
            correlation_id=properties.get("correlation_id"),
            causation_id=properties.get("causation_id"),
            span_id=properties.get("span_id"),
            trace_id=properties.get("trace_id"),
            retry_count=int(properties.get("retry_count", 0)),
            metadata={
                "partition_key": msg.partition_key(),
                "event_timestamp": msg.event_timestamp(),
                "properties": properties
            }
        )


@dataclass
class EventProcessingResult:
    """Result of event processing"""
    status: EventProcessingStatus
    message: Optional[str] = None
    error: Optional[Exception] = None
    data: Optional[Any] = None
    retry_after: Optional[int] = None  # Seconds to wait before retry


class BaseEventProcessor(ABC):
    """
    Base class for event processors with standardized patterns
    """
    
    def __init__(self, 
                 service_name: str,
                 pulsar_url: str,
                 metrics: Optional[EventMetrics] = None,
                 tracer: Optional[EventTracer] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 enable_dead_letter: bool = True,
                 enable_event_store: bool = True):
        
        self.service_name = service_name
        self.pulsar_url = pulsar_url
        self.metrics = metrics or EventMetrics(service_name)
        self.tracer = tracer or EventTracer(service_name)
        self.retry_policy = retry_policy or ExponentialBackoff()
        self.enable_dead_letter = enable_dead_letter
        self.enable_event_store = enable_event_store
        
        self.client: Optional[pulsar.Client] = None
        self.consumers: Dict[str, pulsar.Consumer] = {}
        self.producers: Dict[str, pulsar.Producer] = {}
        self.handlers: Dict[str, List[Callable]] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
    async def initialize(self):
        """Initialize the event processor"""
        logger.info(f"Initializing {self.service_name} event processor...")
        
        # Create Pulsar client
        self.client = pulsar.Client(
            self.pulsar_url,
            operation_timeout_seconds=30,
            connection_timeout_seconds=30
        )
        
        # Initialize metrics and tracing
        await self.metrics.initialize()
        await self.tracer.initialize()
        
        # Initialize dead letter producer if enabled
        if self.enable_dead_letter:
            await self._create_dead_letter_producer()
            
        logger.info(f"{self.service_name} event processor initialized")
        
    async def _create_dead_letter_producer(self):
        """Create producer for dead letter queue"""
        dlq_topic = f"persistent://platformq/dead-letter/{self.service_name}"
        
        self.producers["dead_letter"] = self.client.create_producer(
            dlq_topic,
            producer_name=f"{self.service_name}-dlq-producer",
            batching_enabled=True,
            batching_max_messages=100,
            batching_max_publish_delay_ms=10
        )
        
    def register_handler(self, 
                        topic_pattern: str,
                        handler: Callable,
                        subscription_name: Optional[str] = None,
                        consumer_type: pulsar.ConsumerType = pulsar.ConsumerType.Shared,
                        schema: Optional[Type] = None):
        """Register an event handler for a topic pattern"""
        
        # Generate subscription name if not provided
        if not subscription_name:
            subscription_name = f"{self.service_name}-{topic_pattern.split('/')[-1]}"
            
        # Store handler
        if topic_pattern not in self.handlers:
            self.handlers[topic_pattern] = []
        self.handlers[topic_pattern].append(handler)
        
        # Create consumer
        consumer_config = {
            "topic": topic_pattern if not self._is_pattern(topic_pattern) else None,
            "topics_pattern": topic_pattern if self._is_pattern(topic_pattern) else None,
            "subscription_name": subscription_name,
            "consumer_type": consumer_type,
            "is_regex": self._is_pattern(topic_pattern)
        }
        
        # Add schema if provided
        if schema:
            if hasattr(schema, "__avro_schema__"):
                consumer_config["schema"] = AvroSchema(schema)
            else:
                consumer_config["schema"] = JsonSchema(schema)
                
        # Remove None values
        consumer_config = {k: v for k, v in consumer_config.items() if v is not None}
        
        consumer = self.client.subscribe(**consumer_config)
        self.consumers[topic_pattern] = consumer
        
        logger.info(f"Registered handler for topic pattern: {topic_pattern}")
        
    def _is_pattern(self, topic: str) -> bool:
        """Check if topic is a pattern"""
        return "*" in topic or ".*" in topic
        
    async def start(self):
        """Start processing events"""
        if self._running:
            logger.warning(f"{self.service_name} is already running")
            return
            
        self._running = True
        logger.info(f"Starting {self.service_name} event processing...")
        
        # Create processing tasks for each consumer
        for topic_pattern, consumer in self.consumers.items():
            task = asyncio.create_task(
                self._process_consumer(topic_pattern, consumer)
            )
            self._tasks.append(task)
            
        logger.info(f"Started {len(self._tasks)} consumer tasks")
        
    async def _process_consumer(self, topic_pattern: str, consumer: pulsar.Consumer):
        """Process events from a consumer"""
        handlers = self.handlers.get(topic_pattern, [])
        
        while self._running:
            try:
                # Receive message with timeout
                msg = consumer.receive(timeout_millis=1000)
                
                # Create context
                context = EventContext.from_message(msg)
                
                # Start tracing span
                with self.tracer.trace_event(context) as span:
                    # Process message
                    result = await self._process_message(msg, handlers, context)
                    
                    # Handle result
                    if result.status == EventProcessingStatus.SUCCESS:
                        consumer.acknowledge(msg)
                        await self.metrics.record_processed(context, success=True)
                        
                    elif result.status == EventProcessingStatus.RETRY:
                        # Handle retry
                        await self._handle_retry(msg, context, result)
                        
                    elif result.status == EventProcessingStatus.DEAD_LETTER:
                        # Send to dead letter queue
                        await self._send_to_dead_letter(msg, context, result)
                        consumer.acknowledge(msg)
                        
                    elif result.status == EventProcessingStatus.FILTERED:
                        # Message was filtered, acknowledge
                        consumer.acknowledge(msg)
                        await self.metrics.record_filtered(context)
                        
                    else:
                        # Failed - send to DLQ or negative acknowledge
                        if self.enable_dead_letter:
                            await self._send_to_dead_letter(msg, context, result)
                            consumer.acknowledge(msg)
                        else:
                            consumer.negative_acknowledge(msg)
                            
                        await self.metrics.record_processed(context, success=False, error=result.error)
                        
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error in consumer loop for {topic_pattern}: {e}")
                    await self.metrics.record_error("consumer_error", e)
                    
    async def _process_message(self, 
                             msg: pulsar.Message,
                             handlers: List[Callable],
                             context: EventContext) -> EventProcessingResult:
        """Process a message through handlers"""
        
        # Get message data
        try:
            if hasattr(msg, 'value'):
                data = msg.value()
            else:
                import json
                data = json.loads(msg.data())
        except Exception as e:
            logger.error(f"Failed to deserialize message: {e}")
            return EventProcessingResult(
                status=EventProcessingStatus.FAILED,
                error=e,
                message="Failed to deserialize message"
            )
            
        # Process through each handler
        for handler in handlers:
            try:
                # Check if handler is async
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(data, context)
                else:
                    result = handler(data, context)
                    
                # If handler returns a result, use it
                if isinstance(result, EventProcessingResult):
                    return result
                    
                # Otherwise, assume success if no exception
                
            except Exception as e:
                logger.error(f"Handler error: {e}\n{traceback.format_exc()}")
                
                # Check if we should retry
                if self._should_retry(e, context):
                    return EventProcessingResult(
                        status=EventProcessingStatus.RETRY,
                        error=e,
                        retry_after=self.retry_policy.get_delay(context.retry_count)
                    )
                else:
                    return EventProcessingResult(
                        status=EventProcessingStatus.FAILED,
                        error=e,
                        message=str(e)
                    )
                    
        # All handlers succeeded
        return EventProcessingResult(status=EventProcessingStatus.SUCCESS)
        
    def _should_retry(self, error: Exception, context: EventContext) -> bool:
        """Determine if error should trigger retry"""
        # Check retry count
        if context.retry_count >= self.retry_policy.max_attempts:
            return False
            
        # Check if error is retryable
        retryable_errors = (
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
            # Add more retryable errors
        )
        
        return isinstance(error, retryable_errors)
        
    async def _handle_retry(self, 
                          msg: pulsar.Message,
                          context: EventContext,
                          result: EventProcessingResult):
        """Handle message retry"""
        retry_delay = result.retry_after or self.retry_policy.get_delay(context.retry_count)
        
        logger.info(f"Retrying message {context.event_id} after {retry_delay}s (attempt {context.retry_count + 1})")
        
        # Update retry count
        context.retry_count += 1
        
        # Requeue message with delay
        # Note: Pulsar doesn't support native delay, so we use acknowledge with delay
        consumer = msg.consumer()
        consumer.acknowledge(msg)
        
        # Schedule retry
        asyncio.create_task(self._retry_message(msg, context, retry_delay))
        
    async def _retry_message(self, 
                           msg: pulsar.Message,
                           context: EventContext,
                           delay: int):
        """Retry a message after delay"""
        await asyncio.sleep(delay)
        
        # Get original topic
        topic = msg.topic_name()
        
        # Create producer if needed
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic)
            
        # Resend message with updated context
        producer = self.producers[topic]
        
        await producer.send_async(
            msg.data(),
            properties={
                **msg.properties(),
                "retry_count": str(context.retry_count),
                "original_event_id": context.event_id
            }
        )
        
    async def _send_to_dead_letter(self,
                                 msg: pulsar.Message,
                                 context: EventContext,
                                 result: EventProcessingResult):
        """Send message to dead letter queue"""
        if "dead_letter" not in self.producers:
            logger.error("Dead letter producer not initialized")
            return
            
        dlq_producer = self.producers["dead_letter"]
        
        # Prepare DLQ message
        dlq_data = {
            "original_message": msg.data().decode('utf-8') if msg.data() else None,
            "original_topic": context.source_topic,
            "error": str(result.error) if result.error else result.message,
            "error_type": type(result.error).__name__ if result.error else None,
            "stack_trace": traceback.format_exc() if result.error else None,
            "context": context.__dict__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name
        }
        
        # Send to DLQ
        await dlq_producer.send_async(
            json.dumps(dlq_data).encode('utf-8'),
            properties={
                "original_event_id": context.event_id,
                "original_topic": context.source_topic,
                "error_type": type(result.error).__name__ if result.error else "unknown"
            }
        )
        
        logger.warning(f"Sent message {context.event_id} to dead letter queue")
        await self.metrics.record_dead_letter(context, result.error)
        
    async def stop(self):
        """Stop the event processor"""
        logger.info(f"Stopping {self.service_name} event processor...")
        
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close consumers
        for consumer in self.consumers.values():
            await consumer.close()
            
        # Close producers
        for producer in self.producers.values():
            await producer.close()
            
        # Close client
        if self.client:
            self.client.close()
            
        # Shutdown metrics and tracing
        await self.metrics.shutdown()
        await self.tracer.shutdown()
        
        logger.info(f"{self.service_name} event processor stopped")
        
    @abstractmethod
    async def process_event(self, event_data: Any, context: EventContext) -> EventProcessingResult:
        """
        Process an event. Must be implemented by subclasses.
        
        Args:
            event_data: The event data
            context: Event context information
            
        Returns:
            EventProcessingResult indicating success/failure/retry
        """
        pass 