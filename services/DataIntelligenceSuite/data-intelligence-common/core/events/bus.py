"""
Unified event bus implementation with multiple backend support.

Combines functionality from core/events/event_bus.py and event_handlers/pulsar_bus.py.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Set, Pattern
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import json
import re
import uuid

try:
    import pulsar
    PULSAR_AVAILABLE = True
except ImportError:
    PULSAR_AVAILABLE = False

from .base import Event, EventDeliveryMode, EventPriority
from ...monitoring import MetricsCollector, StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class SubscriptionType(str, Enum):
    """Subscription types"""
    EXCLUSIVE = "exclusive"
    SHARED = "shared"
    FAILOVER = "failover"
    KEY_SHARED = "key_shared"


@dataclass
class EventSubscription:
    """Event subscription details"""
    subscription_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic_pattern: str = ""
    handler: Callable = None
    subscription_name: Optional[str] = None
    subscription_type: SubscriptionType = SubscriptionType.SHARED
    
    # Filtering
    filter_expression: Optional[str] = None
    event_types: Optional[List[str]] = None
    
    # Delivery
    delivery_mode: EventDeliveryMode = EventDeliveryMode.AT_LEAST_ONCE
    max_redeliveries: int = 3
    ack_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Dead letter
    dead_letter_topic: Optional[str] = None
    
    # State
    active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_event_time: Optional[datetime] = None
    events_processed: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    
    def matches_event(self, event: Event) -> bool:
        """Check if subscription matches event"""
        # Check event types filter
        if self.event_types and event.event_type not in self.event_types:
            return False
            
        # Check filter expression
        if self.filter_expression:
            # Simple property matching for now
            # Could be extended to support SQL-like expressions
            try:
                return eval(self.filter_expression, {"event": event})
            except:
                return True
                
        return True


class EventBackend(ABC):
    """Abstract base for event bus backends"""
    
    @abstractmethod
    async def initialize(self):
        """Initialize backend"""
        pass
        
    @abstractmethod
    async def shutdown(self):
        """Shutdown backend"""
        pass
        
    @abstractmethod
    async def publish(self, topic: str, event: Event) -> bool:
        """Publish event to topic"""
        pass
        
    @abstractmethod
    async def subscribe(
        self,
        topic_pattern: str,
        handler: Callable,
        subscription: EventSubscription
    ) -> str:
        """Subscribe to topic pattern"""
        pass
        
    @abstractmethod
    async def unsubscribe(self, subscription_id: str):
        """Unsubscribe from topic"""
        pass
        
    @abstractmethod
    async def acknowledge(self, message_id: Any):
        """Acknowledge message processing"""
        pass
        
    @abstractmethod
    async def negative_acknowledge(self, message_id: Any):
        """Negative acknowledge for retry"""
        pass


class PulsarBackend(EventBackend):
    """Apache Pulsar event backend"""
    
    def __init__(self, pulsar_url: str = "pulsar://localhost:6650"):
        self.pulsar_url = pulsar_url
        self._client: Optional[pulsar.Client] = None
        self._producers: Dict[str, pulsar.Producer] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        self._subscriptions: Dict[str, EventSubscription] = {}
        
    async def initialize(self):
        """Initialize Pulsar client"""
        if not PULSAR_AVAILABLE:
            raise ImportError("Pulsar client not available")
            
        self._client = pulsar.Client(self.pulsar_url)
        logger.info(f"Initialized Pulsar backend: {self.pulsar_url}")
        
    async def shutdown(self):
        """Shutdown Pulsar client"""
        # Close consumers
        for consumer in self._consumers.values():
            consumer.close()
            
        # Close producers
        for producer in self._producers.values():
            producer.close()
            
        # Close client
        if self._client:
            self._client.close()
            
        logger.info("Shutdown Pulsar backend")
        
    async def publish(self, topic: str, event: Event) -> bool:
        """Publish event to Pulsar topic"""
        try:
            # Get or create producer
            if topic not in self._producers:
                self._producers[topic] = self._client.create_producer(
                    topic,
                    compression_type=pulsar.CompressionType.LZ4,
                    batching_enabled=True,
                    batching_max_publish_delay_ms=100
                )
                
            producer = self._producers[topic]
            
            # Serialize event
            message_data = json.dumps(event.to_dict()).encode('utf-8')
            
            # Set message properties
            properties = {
                "event_type": event.event_type,
                "source": event.source,
                "priority": event.priority.value
            }
            
            if event.correlation_id:
                properties["correlation_id"] = event.correlation_id
                
            # Send message
            message_id = producer.send(
                message_data,
                properties=properties,
                event_timestamp=int(event.timestamp.timestamp() * 1000)
            )
            
            logger.debug(f"Published event {event.event_id} to {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False
            
    async def subscribe(
        self,
        topic_pattern: str,
        handler: Callable,
        subscription: EventSubscription
    ) -> str:
        """Subscribe to Pulsar topic pattern"""
        try:
            # Create consumer
            consumer_config = {
                "topics": [topic_pattern] if not topic_pattern.startswith("regex:") else None,
                "topics_pattern": topic_pattern[6:] if topic_pattern.startswith("regex:") else None,
                "subscription_name": subscription.subscription_name or f"sub-{subscription.subscription_id}",
                "consumer_type": self._map_subscription_type(subscription.subscription_type),
                "message_listener": lambda consumer, msg: asyncio.create_task(
                    self._handle_message(consumer, msg, subscription, handler)
                )
            }
            
            # Add dead letter policy if configured
            if subscription.dead_letter_topic:
                consumer_config["dead_letter_policy"] = pulsar.ConsumerDeadLetterPolicy(
                    max_redeliver_count=subscription.max_redeliveries,
                    dead_letter_topic=subscription.dead_letter_topic
                )
                
            consumer = self._client.subscribe(**{k: v for k, v in consumer_config.items() if v is not None})
            
            self._consumers[subscription.subscription_id] = consumer
            self._subscriptions[subscription.subscription_id] = subscription
            
            logger.info(f"Created subscription {subscription.subscription_id} for pattern {topic_pattern}")
            return subscription.subscription_id
            
        except Exception as e:
            logger.error(f"Failed to create subscription: {e}")
            raise
            
    async def unsubscribe(self, subscription_id: str):
        """Unsubscribe from topic"""
        if subscription_id in self._consumers:
            consumer = self._consumers.pop(subscription_id)
            consumer.close()
            
        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]
            
        logger.info(f"Unsubscribed {subscription_id}")
        
    async def acknowledge(self, message_id: Any):
        """Acknowledge message"""
        # In Pulsar, acknowledgment is done on the consumer
        # This is handled in _handle_message
        pass
        
    async def negative_acknowledge(self, message_id: Any):
        """Negative acknowledge message"""
        # In Pulsar, negative acknowledgment is done on the consumer
        # This is handled in _handle_message
        pass
        
    def _map_subscription_type(self, sub_type: SubscriptionType) -> pulsar.ConsumerType:
        """Map subscription type to Pulsar consumer type"""
        mapping = {
            SubscriptionType.EXCLUSIVE: pulsar.ConsumerType.Exclusive,
            SubscriptionType.SHARED: pulsar.ConsumerType.Shared,
            SubscriptionType.FAILOVER: pulsar.ConsumerType.Failover,
            SubscriptionType.KEY_SHARED: pulsar.ConsumerType.KeyShared
        }
        return mapping.get(sub_type, pulsar.ConsumerType.Shared)
        
    async def _handle_message(
        self,
        consumer: pulsar.Consumer,
        msg: pulsar.Message,
        subscription: EventSubscription,
        handler: Callable
    ):
        """Handle incoming Pulsar message"""
        try:
            # Parse message
            data = json.loads(msg.data().decode('utf-8'))
            event = Event.from_dict(data)
            
            # Check if subscription matches event
            if not subscription.matches_event(event):
                consumer.acknowledge(msg)
                return
                
            # Update subscription stats
            subscription.last_event_time = datetime.utcnow()
            subscription.events_processed += 1
            
            # Call handler
            await handler(event)
            
            # Acknowledge based on delivery mode
            if subscription.delivery_mode != EventDeliveryMode.AT_MOST_ONCE:
                consumer.acknowledge(msg)
                
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            subscription.error_count += 1
            subscription.last_error = str(e)
            
            # Handle based on delivery mode
            if subscription.delivery_mode == EventDeliveryMode.AT_MOST_ONCE:
                consumer.acknowledge(msg)
            else:
                consumer.negative_acknowledge(msg)


class UnifiedEventBus:
    """
    Unified event bus with support for multiple messaging backends.
    
    Features:
    - Multiple backend support (Pulsar, Kafka, etc.)
    - Topic-based pub/sub
    - Pattern-based subscriptions
    - Multiple delivery guarantees
    - Dead letter queues
    - Event filtering
    - Metrics and monitoring
    """
    
    def __init__(
        self,
        backend: Optional[EventBackend] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.backend = backend or PulsarBackend()
        self.metrics = metrics_collector
        
        # Subscriptions tracking
        self._subscriptions: Dict[str, EventSubscription] = {}
        self._handlers_by_pattern: Dict[str, List[EventSubscription]] = {}
        
        # Metrics
        self._event_metrics = {
            "published": 0,
            "received": 0,
            "processed": 0,
            "failed": 0
        }
        
        # Background tasks
        self._tasks: Set[asyncio.Task] = set()
        
    async def initialize(self):
        """Initialize event bus"""
        await self.backend.initialize()
        
        # Start monitoring task
        task = asyncio.create_task(self._monitor_subscriptions())
        self._tasks.add(task)
        
        logger.info("Initialized unified event bus")
        
    async def shutdown(self):
        """Shutdown event bus"""
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
            
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        # Shutdown backend
        await self.backend.shutdown()
        
        logger.info("Shutdown unified event bus")
        
    async def publish(
        self,
        event: Event,
        topic: Optional[str] = None
    ) -> bool:
        """
        Publish event to bus.
        
        Args:
            event: Event to publish
            topic: Optional topic override (default uses event type)
            
        Returns:
            Success status
        """
        # Determine topic
        if not topic:
            topic = f"events.{event.event_type.replace('.', '-')}"
            
        # Publish via backend
        success = await self.backend.publish(topic, event)
        
        if success:
            self._event_metrics["published"] += 1
            
            # Record metrics
            if self.metrics:
                self.metrics.increment(
                    "events_published",
                    tags={
                        "event_type": event.event_type,
                        "topic": topic,
                        "priority": event.priority.value
                    }
                )
        else:
            self._event_metrics["failed"] += 1
            
        return success
        
    async def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], Any],
        subscription_name: Optional[str] = None,
        subscription_type: SubscriptionType = SubscriptionType.SHARED,
        filter_expression: Optional[str] = None,
        event_types: Optional[List[str]] = None,
        delivery_mode: EventDeliveryMode = EventDeliveryMode.AT_LEAST_ONCE,
        dead_letter_topic: Optional[str] = None
    ) -> EventSubscription:
        """
        Subscribe to events.
        
        Args:
            topic_pattern: Topic pattern to subscribe to (supports wildcards and regex)
            handler: Async function to handle events
            subscription_name: Optional subscription name
            subscription_type: Type of subscription
            filter_expression: Optional filter expression
            event_types: Optional list of event types to filter
            delivery_mode: Delivery guarantee mode
            dead_letter_topic: Optional dead letter topic
            
        Returns:
            Event subscription
        """
        # Create subscription
        subscription = EventSubscription(
            topic_pattern=topic_pattern,
            handler=handler,
            subscription_name=subscription_name,
            subscription_type=subscription_type,
            filter_expression=filter_expression,
            event_types=event_types,
            delivery_mode=delivery_mode,
            dead_letter_topic=dead_letter_topic
        )
        
        # Wrap handler to add metrics
        async def wrapped_handler(event: Event):
            self._event_metrics["received"] += 1
            
            try:
                await handler(event)
                self._event_metrics["processed"] += 1
                
                if self.metrics:
                    self.metrics.increment(
                        "events_processed",
                        tags={"event_type": event.event_type}
                    )
                    
            except Exception as e:
                self._event_metrics["failed"] += 1
                
                if self.metrics:
                    self.metrics.increment(
                        "events_failed",
                        tags={
                            "event_type": event.event_type,
                            "error": type(e).__name__
                        }
                    )
                raise
                
        # Subscribe via backend
        subscription_id = await self.backend.subscribe(
            topic_pattern,
            wrapped_handler,
            subscription
        )
        
        subscription.subscription_id = subscription_id
        
        # Track subscription
        self._subscriptions[subscription_id] = subscription
        
        if topic_pattern not in self._handlers_by_pattern:
            self._handlers_by_pattern[topic_pattern] = []
        self._handlers_by_pattern[topic_pattern].append(subscription)
        
        logger.info(f"Created subscription {subscription_id} for pattern {topic_pattern}")
        return subscription
        
    async def unsubscribe(self, subscription_id: str):
        """Unsubscribe from events"""
        if subscription_id not in self._subscriptions:
            return
            
        subscription = self._subscriptions.pop(subscription_id)
        
        # Remove from pattern tracking
        for pattern, subs in self._handlers_by_pattern.items():
            self._handlers_by_pattern[pattern] = [
                s for s in subs if s.subscription_id != subscription_id
            ]
            
        # Unsubscribe via backend
        await self.backend.unsubscribe(subscription_id)
        
        logger.info(f"Unsubscribed {subscription_id}")
        
    async def _monitor_subscriptions(self):
        """Monitor subscription health"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Check subscription health
                for subscription in self._subscriptions.values():
                    if subscription.active:
                        # Check if subscription is stale
                        if subscription.last_event_time:
                            time_since_last = datetime.utcnow() - subscription.last_event_time
                            if time_since_last > timedelta(minutes=30):
                                logger.warning(
                                    f"Subscription {subscription.subscription_id} "
                                    f"hasn't received events for {time_since_last}"
                                )
                                
                        # Check error rate
                        if subscription.events_processed > 0:
                            error_rate = subscription.error_count / subscription.events_processed
                            if error_rate > 0.1:  # 10% error rate
                                logger.warning(
                                    f"High error rate for subscription {subscription.subscription_id}: "
                                    f"{error_rate:.2%}"
                                )
                                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring subscriptions: {e}")
                
    def get_metrics(self) -> Dict[str, Any]:
        """Get event bus metrics"""
        return {
            "events": self._event_metrics.copy(),
            "subscriptions": {
                "total": len(self._subscriptions),
                "active": sum(1 for s in self._subscriptions.values() if s.active),
                "by_pattern": {
                    pattern: len(subs)
                    for pattern, subs in self._handlers_by_pattern.items()
                }
            }
        } 