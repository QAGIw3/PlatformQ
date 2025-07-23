"""
Unified Event Bus for DataIntelligenceSuite

Provides pub/sub capabilities with multiple backend support.
"""

import asyncio
import logging
import json
from typing import Any, Dict, Optional, List, Callable, Set, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import uuid

import pulsar
from pulsar.schema import JsonSchema

from ...monitoring import MetricsCollector

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class EventDeliveryMode(Enum):
    """Event delivery guarantees"""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class Event:
    """Base event class"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source: str = ""
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    priority: EventPriority = EventPriority.NORMAL
    headers: Dict[str, str] = field(default_factory=dict)
    payload: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "priority": self.priority.value,
            "headers": self.headers,
            "payload": self.payload
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary"""
        data = data.copy()
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['priority'] = EventPriority(data['priority'])
        return cls(**data)


@dataclass
class EventSubscription:
    """Event subscription configuration"""
    subscription_id: str
    topic_pattern: str
    handler: Callable[[Event], Any]
    filter_expression: Optional[str] = None
    delivery_mode: EventDeliveryMode = EventDeliveryMode.AT_LEAST_ONCE
    max_retries: int = 3
    retry_delay_ms: int = 1000
    dead_letter_topic: Optional[str] = None
    
    # Runtime state
    active: bool = True
    error_count: int = 0
    last_error: Optional[str] = None
    last_event_time: Optional[datetime] = None


class EventBus:
    """
    Unified event bus with support for multiple messaging backends.
    
    Features:
    - Topic-based pub/sub
    - Pattern-based subscriptions
    - Multiple delivery guarantees
    - Dead letter queues
    - Event filtering
    - Metrics and monitoring
    """
    
    def __init__(
        self,
        pulsar_url: str = "pulsar://localhost:6650",
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.pulsar_url = pulsar_url
        self.metrics = metrics_collector
        
        # Pulsar client
        self._client: Optional[pulsar.Client] = None
        
        # Producers cache
        self._producers: Dict[str, pulsar.Producer] = {}
        
        # Subscriptions
        self._subscriptions: Dict[str, EventSubscription] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        
        # Event handlers by pattern
        self._handlers: Dict[str, List[EventSubscription]] = {}
        
        # Background tasks
        self._tasks: Set[asyncio.Task] = set()
        
    async def initialize(self):
        """Initialize event bus"""
        if not self._client:
            self._client = pulsar.Client(self.pulsar_url)
            logger.info(f"Initialized Pulsar client: {self.pulsar_url}")
            
    async def shutdown(self):
        """Shutdown event bus"""
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        # Close consumers
        for consumer in self._consumers.values():
            consumer.close()
            
        # Close producers
        for producer in self._producers.values():
            producer.close()
            
        # Close client
        if self._client:
            self._client.close()
            
        logger.info("Shutdown event bus")
        
    async def publish(
        self,
        topic: str,
        event: Union[Event, Dict[str, Any]],
        partition_key: Optional[str] = None
    ):
        """Publish event to topic"""
        if not self._client:
            await self.initialize()
            
        # Convert dict to Event if needed
        if isinstance(event, dict):
            event = Event(**event)
            
        # Get or create producer
        producer = self._get_producer(topic)
        
        # Prepare message
        message_data = event.to_dict()
        
        # Send message
        try:
            message_id = producer.send(
                json.dumps(message_data).encode('utf-8'),
                partition_key=partition_key,
                event_timestamp=int(event.timestamp.timestamp() * 1000)
            )
            
            # Record metrics
            if self.metrics:
                self.metrics.increment_counter(
                    "event_bus_published_total",
                    {"topic": topic, "event_type": event.event_type}
                )
                
            logger.debug(f"Published event {event.event_id} to {topic}")
            return message_id
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            if self.metrics:
                self.metrics.increment_counter(
                    "event_bus_publish_errors_total",
                    {"topic": topic, "event_type": event.event_type}
                )
            raise
            
    async def publish_batch(
        self,
        topic: str,
        events: List[Union[Event, Dict[str, Any]]]
    ):
        """Publish batch of events"""
        if not self._client:
            await self.initialize()
            
        producer = self._get_producer(topic)
        
        # Send all events
        futures = []
        for event in events:
            if isinstance(event, dict):
                event = Event(**event)
                
            future = producer.send_async(
                json.dumps(event.to_dict()).encode('utf-8'),
                callback=lambda res, msg: logger.debug(f"Sent {event.event_id}")
            )
            futures.append(future)
            
        # Wait for all sends to complete
        for future in futures:
            future.result()
            
        if self.metrics:
            self.metrics.increment_counter(
                "event_bus_batch_published_total",
                {"topic": topic},
                len(events)
            )
            
    async def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], Any],
        subscription_name: Optional[str] = None,
        **kwargs
    ) -> EventSubscription:
        """Subscribe to events matching topic pattern"""
        if not self._client:
            await self.initialize()
            
        # Create subscription
        subscription_id = subscription_name or f"sub_{uuid.uuid4().hex[:8]}"
        
        subscription = EventSubscription(
            subscription_id=subscription_id,
            topic_pattern=topic_pattern,
            handler=handler,
            **kwargs
        )
        
        self._subscriptions[subscription_id] = subscription
        
        # Create consumer
        consumer = self._client.subscribe(
            topic_pattern,
            subscription_name=subscription_id,
            consumer_type=pulsar.ConsumerType.Shared,
            message_listener=lambda consumer, msg: self._handle_message(consumer, msg, subscription)
        )
        
        self._consumers[subscription_id] = consumer
        
        # Start consumer task
        task = asyncio.create_task(self._consume_messages(subscription))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        
        logger.info(f"Created subscription {subscription_id} for pattern {topic_pattern}")
        return subscription
        
    async def unsubscribe(self, subscription_id: str):
        """Unsubscribe from events"""
        if subscription_id in self._subscriptions:
            # Mark as inactive
            self._subscriptions[subscription_id].active = False
            
            # Close consumer
            if subscription_id in self._consumers:
                self._consumers[subscription_id].close()
                del self._consumers[subscription_id]
                
            # Remove subscription
            del self._subscriptions[subscription_id]
            
            logger.info(f"Unsubscribed {subscription_id}")
            
    def _get_producer(self, topic: str) -> pulsar.Producer:
        """Get or create producer for topic"""
        if topic not in self._producers:
            self._producers[topic] = self._client.create_producer(
                topic,
                batching_enabled=True,
                batching_max_messages=100,
                batching_max_publish_delay_ms=10
            )
        return self._producers[topic]
        
    def _handle_message(self, consumer: pulsar.Consumer, msg: pulsar.Message, subscription: EventSubscription):
        """Handle incoming message"""
        try:
            # Parse message
            data = json.loads(msg.data().decode('utf-8'))
            event = Event.from_dict(data)
            
            # Update subscription stats
            subscription.last_event_time = datetime.utcnow()
            
            # Apply filter if configured
            if subscription.filter_expression:
                if not self._evaluate_filter(event, subscription.filter_expression):
                    consumer.acknowledge(msg)
                    return
                    
            # Call handler
            asyncio.create_task(self._process_event(event, subscription, consumer, msg))
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            subscription.error_count += 1
            subscription.last_error = str(e)
            
            # Handle based on delivery mode
            if subscription.delivery_mode == EventDeliveryMode.AT_MOST_ONCE:
                consumer.acknowledge(msg)
            else:
                consumer.negative_acknowledge(msg)
                
    async def _process_event(
        self,
        event: Event,
        subscription: EventSubscription,
        consumer: pulsar.Consumer,
        msg: pulsar.Message
    ):
        """Process event with retry logic"""
        retries = 0
        
        while retries <= subscription.max_retries:
            try:
                # Call handler
                if asyncio.iscoroutinefunction(subscription.handler):
                    await subscription.handler(event)
                else:
                    subscription.handler(event)
                    
                # Acknowledge message
                consumer.acknowledge(msg)
                
                # Record metrics
                if self.metrics:
                    self.metrics.increment_counter(
                        "event_bus_processed_total",
                        {"subscription": subscription.subscription_id, "event_type": event.event_type}
                    )
                    
                return
                
            except Exception as e:
                retries += 1
                logger.error(f"Error processing event (attempt {retries}): {e}")
                
                if retries <= subscription.max_retries:
                    await asyncio.sleep(subscription.retry_delay_ms / 1000)
                else:
                    # Send to dead letter queue if configured
                    if subscription.dead_letter_topic:
                        await self._send_to_dead_letter(event, subscription, str(e))
                        
                    # Acknowledge to prevent redelivery
                    consumer.acknowledge(msg)
                    
                    # Record failure
                    if self.metrics:
                        self.metrics.increment_counter(
                            "event_bus_processing_failures_total",
                            {"subscription": subscription.subscription_id, "event_type": event.event_type}
                        )
                        
    async def _consume_messages(self, subscription: EventSubscription):
        """Background task to consume messages"""
        consumer = self._consumers.get(subscription.subscription_id)
        if not consumer:
            return
            
        while subscription.active:
            try:
                # Receive will be handled by message listener
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)
                
    def _evaluate_filter(self, event: Event, filter_expression: str) -> bool:
        """Evaluate filter expression against event"""
        # Simple implementation - can be extended with more complex expressions
        try:
            # Create evaluation context
            context = {
                "event": event,
                "event_type": event.event_type,
                "source": event.source,
                "priority": event.priority,
                "headers": event.headers,
                "payload": event.payload
            }
            
            # Evaluate expression
            return eval(filter_expression, {"__builtins__": {}}, context)
            
        except Exception as e:
            logger.error(f"Filter evaluation error: {e}")
            return False
            
    async def _send_to_dead_letter(self, event: Event, subscription: EventSubscription, error: str):
        """Send event to dead letter queue"""
        try:
            # Add error information
            event.headers["dead_letter_reason"] = error
            event.headers["original_subscription"] = subscription.subscription_id
            event.headers["retry_count"] = str(subscription.max_retries)
            
            # Publish to dead letter topic
            await self.publish(subscription.dead_letter_topic, event)
            
            logger.warning(f"Sent event {event.event_id} to dead letter queue")
            
        except Exception as e:
            logger.error(f"Failed to send to dead letter queue: {e}")
            
    async def get_subscription_stats(self) -> Dict[str, Any]:
        """Get subscription statistics"""
        stats = {}
        
        for sub_id, subscription in self._subscriptions.items():
            stats[sub_id] = {
                "active": subscription.active,
                "topic_pattern": subscription.topic_pattern,
                "error_count": subscription.error_count,
                "last_error": subscription.last_error,
                "last_event_time": subscription.last_event_time.isoformat() if subscription.last_event_time else None
            }
            
        return stats 