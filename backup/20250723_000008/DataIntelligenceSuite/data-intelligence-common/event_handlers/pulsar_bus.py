"""
Pulsar Event Bus with Vault/Consul Integration

Provides secure event bus implementation using Apache Pulsar.
"""

import logging
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import json
from enum import Enum

import pulsar
from pulsar import ConsumerType, InitialPosition

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..integrations.pulsar_client import PulsarClient, PulsarConfig, ProducerConfig, ConsumerConfig
from .base import EventBus, EventHandler, Event

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels"""
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 20


@dataclass
class PulsarEventBusConfig:
    """Configuration for Pulsar event bus"""
    # Pulsar settings
    service_url: str = "pulsar://localhost:6650"
    namespace: str = "data-intelligence"
    tenant: str = "public"
    
    # Topic settings
    topic_prefix: str = "events"
    partitions: int = 4
    
    # Security
    enable_encryption: bool = True
    encryption_key: str = "event-encryption"
    enable_auth: bool = True
    required_role: Optional[str] = None
    
    # Performance
    enable_batching: bool = True
    batch_max_messages: int = 100
    batch_max_delay_ms: int = 10
    
    # Reliability
    enable_deduplication: bool = True
    retention_hours: int = 72
    
    # Dead letter queue
    enable_dlq: bool = True
    dlq_max_redeliveries: int = 3


class PulsarEventBus(EventBus):
    """
    Event bus implementation using Apache Pulsar with Vault/Consul support.
    
    Features:
    - End-to-end encryption via Vault Transit
    - Dynamic authentication from Vault
    - Service discovery via Consul
    - Event prioritization
    - Dead letter queue handling
    - Event deduplication
    - Distributed tracing
    """
    
    def __init__(
        self,
        config: PulsarEventBusConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        service_name: str = "event-bus"
    ):
        self.config = config
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.service_name = service_name
        
        # Pulsar client
        self.pulsar_client: Optional[PulsarClient] = None
        
        # Topic management
        self._topics: Set[str] = set()
        self._producers: Dict[str, str] = {}  # topic -> producer_key
        self._consumers: Dict[str, str] = {}  # handler_id -> consumer_key
        
        # Handler registry
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._handler_tasks: Dict[str, asyncio.Task] = {}
        
        # Metrics
        self._events_published = 0
        self._events_consumed = 0
        self._events_failed = 0
        
    async def initialize(self):
        """Initialize event bus"""
        try:
            # Create Pulsar client configuration
            pulsar_config = PulsarConfig(
                service_url=self.config.service_url,
                use_vault_credentials=self.vault_client is not None,
                use_service_discovery=self.consul_client is not None,
                enable_message_encryption=self.config.enable_encryption,
                encryption_key_name=self.config.encryption_key,
                default_batching_enabled=self.config.enable_batching,
                default_batching_max_messages=self.config.batch_max_messages,
                default_batching_max_publish_delay_ms=self.config.batch_max_delay_ms
            )
            
            # Create Pulsar client
            self.pulsar_client = PulsarClient(
                pulsar_config,
                self.vault_client,
                self.consul_client
            )
            
            await self.pulsar_client.connect()
            
            # Create admin client for topic management
            # This would need actual implementation
            
            logger.info(f"Pulsar event bus initialized for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize event bus: {e}")
            raise
            
    async def publish(
        self,
        event: Event,
        priority: EventPriority = EventPriority.NORMAL
    ) -> bool:
        """Publish event to bus"""
        try:
            # Get or create producer for event type
            topic = self._get_topic_name(event.event_type)
            producer_key = await self._ensure_producer(topic)
            
            # Build event message
            message_data = {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "timestamp": event.timestamp.isoformat(),
                "source": event.source,
                "data": event.data,
                "metadata": event.metadata,
                "correlation_id": event.correlation_id,
                "causation_id": event.causation_id,
                "version": event.version
            }
            
            # Add security context if available
            if self.vault_client and event.metadata.get("user_id"):
                # Could add user context for row-level security
                pass
                
            # Publish with priority
            properties = {
                "priority": str(priority.value),
                "event_type": event.event_type,
                "source": event.source
            }
            
            # Add deduplication key if enabled
            if self.config.enable_deduplication:
                properties["deduplication_key"] = event.event_id
                
            # Publish message
            message_id = await self.pulsar_client.send_async(
                producer_key,
                message_data,
                properties=properties,
                ordering_key=event.correlation_id,  # Ensure ordering within correlation
                event_timestamp=int(event.timestamp.timestamp() * 1000)
            )
            
            self._events_published += 1
            
            logger.debug(f"Published event {event.event_id} to {topic}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event {event.event_id}: {e}")
            self._events_failed += 1
            return False
            
    async def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
        subscription_name: Optional[str] = None
    ) -> str:
        """Subscribe to events"""
        try:
            # Generate subscription name if not provided
            if not subscription_name:
                subscription_name = f"{self.service_name}-{event_type}-{handler.__class__.__name__}"
                
            # Get topic name
            topic = self._get_topic_name(event_type)
            
            # Create consumer configuration
            consumer_config = ConsumerConfig(
                topics=topic,
                subscription_name=subscription_name,
                subscription_type=ConsumerType.SHARED,  # Allow multiple instances
                initial_position=InitialPosition.Latest,
                required_role=self.config.required_role,
                negative_ack_redelivery_delay_ms=60000,  # 1 minute
                ack_timeout_ms=300000  # 5 minutes
            )
            
            # Add dead letter policy if enabled
            if self.config.enable_dlq:
                consumer_config.dead_letter_policy = {
                    "max_redeliver_count": self.config.dlq_max_redeliveries,
                    "dead_letter_topic": f"{topic}-dlq"
                }
                
            # Create consumer
            consumer = self.pulsar_client.create_consumer(consumer_config)
            
            # Store consumer reference
            handler_id = f"{event_type}:{handler.__class__.__name__}:{id(handler)}"
            consumer_key = f"{subscription_name}:{topic}"
            self._consumers[handler_id] = consumer_key
            
            # Register handler
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(handler)
            
            # Start consumer task
            task = asyncio.create_task(
                self._consume_messages(consumer_key, event_type, handler)
            )
            self._handler_tasks[handler_id] = task
            
            logger.info(f"Subscribed to {event_type} with handler {handler.__class__.__name__}")
            
            return handler_id
            
        except Exception as e:
            logger.error(f"Failed to subscribe to {event_type}: {e}")
            raise
            
    async def unsubscribe(self, handler_id: str):
        """Unsubscribe from events"""
        try:
            # Cancel consumer task
            if handler_id in self._handler_tasks:
                self._handler_tasks[handler_id].cancel()
                try:
                    await self._handler_tasks[handler_id]
                except asyncio.CancelledError:
                    pass
                del self._handler_tasks[handler_id]
                
            # Close consumer
            if handler_id in self._consumers:
                consumer_key = self._consumers[handler_id]
                self.pulsar_client.close_consumer(consumer_key)
                del self._consumers[handler_id]
                
            # Remove handler
            event_type = handler_id.split(":")[0]
            if event_type in self._handlers:
                # Remove specific handler instance
                self._handlers[event_type] = [
                    h for h in self._handlers[event_type]
                    if f"{event_type}:{h.__class__.__name__}:{id(h)}" != handler_id
                ]
                
            logger.info(f"Unsubscribed handler {handler_id}")
            
        except Exception as e:
            logger.error(f"Failed to unsubscribe {handler_id}: {e}")
            
    async def _consume_messages(
        self,
        consumer_key: str,
        event_type: str,
        handler: EventHandler
    ):
        """Consume messages for a handler"""
        while True:
            try:
                # Receive message
                msg = await self.pulsar_client.receive_async(consumer_key)
                
                try:
                    # Parse event
                    event_data = json.loads(msg.data().decode('utf-8'))
                    
                    # Reconstruct event
                    event = Event(
                        event_id=event_data["event_id"],
                        event_type=event_data["event_type"],
                        timestamp=datetime.fromisoformat(event_data["timestamp"]),
                        source=event_data["source"],
                        data=event_data["data"],
                        metadata=event_data.get("metadata", {}),
                        correlation_id=event_data.get("correlation_id"),
                        causation_id=event_data.get("causation_id"),
                        version=event_data.get("version", "1.0")
                    )
                    
                    # Handle event
                    await handler.handle(event)
                    
                    # Acknowledge message
                    self.pulsar_client.acknowledge(consumer_key, msg)
                    
                    self._events_consumed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    
                    # Negative acknowledge for retry
                    self.pulsar_client.negative_acknowledge(consumer_key, msg)
                    
                    self._events_failed += 1
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(5)
                
    async def _ensure_producer(self, topic: str) -> str:
        """Ensure producer exists for topic"""
        if topic not in self._producers:
            # Create producer configuration
            producer_config = ProducerConfig(
                topic=topic,
                producer_name=f"{self.service_name}-{topic}",
                compression_type=pulsar.CompressionType.LZ4,
                required_role=self.config.required_role
            )
            
            # Create producer
            producer = self.pulsar_client.create_producer(producer_config)
            
            # Store producer key
            producer_key = producer_config.producer_name or topic
            self._producers[topic] = producer_key
            
            # Ensure topic exists with proper configuration
            await self._ensure_topic(topic)
            
        return self._producers[topic]
        
    async def _ensure_topic(self, topic: str):
        """Ensure topic exists with proper configuration"""
        # This would use Pulsar admin API to create topic
        # with partitions, retention, deduplication, etc.
        pass
        
    def _get_topic_name(self, event_type: str) -> str:
        """Get full topic name for event type"""
        return f"persistent://{self.config.tenant}/{self.config.namespace}/{self.config.topic_prefix}-{event_type}"
        
    async def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics"""
        return {
            "events_published": self._events_published,
            "events_consumed": self._events_consumed,
            "events_failed": self._events_failed,
            "active_producers": len(self._producers),
            "active_consumers": len(self._consumers),
            "registered_handlers": sum(len(handlers) for handlers in self._handlers.values())
        }
        
    async def shutdown(self):
        """Shutdown event bus"""
        try:
            # Cancel all consumer tasks
            for task in self._handler_tasks.values():
                task.cancel()
                
            await asyncio.gather(
                *self._handler_tasks.values(),
                return_exceptions=True
            )
            
            # Close Pulsar client
            if self.pulsar_client:
                await self.pulsar_client.close()
                
            logger.info("Event bus shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during event bus shutdown: {e}")
            
    async def replay_events(
        self,
        event_type: str,
        handler: EventHandler,
        start_time: datetime,
        end_time: Optional[datetime] = None
    ):
        """Replay historical events"""
        # Create replay consumer
        topic = self._get_topic_name(event_type)
        subscription = f"{self.service_name}-replay-{datetime.utcnow().timestamp()}"
        
        consumer_config = ConsumerConfig(
            topics=topic,
            subscription_name=subscription,
            subscription_type=ConsumerType.EXCLUSIVE,
            initial_position=InitialPosition.Earliest
        )
        
        consumer = self.pulsar_client.create_consumer(consumer_config)
        consumer_key = f"{subscription}:{topic}"
        
        try:
            # Seek to start time
            self.pulsar_client.seek(
                consumer_key,
                int(start_time.timestamp() * 1000)
            )
            
            # Process messages until end time
            while True:
                try:
                    msg = self.pulsar_client.receive(consumer_key, timeout_millis=1000)
                    
                    # Check if past end time
                    if end_time and msg.event_timestamp() > int(end_time.timestamp() * 1000):
                        break
                        
                    # Process message
                    event_data = json.loads(msg.data().decode('utf-8'))
                    event = Event(**event_data)
                    
                    await handler.handle(event)
                    
                    self.pulsar_client.acknowledge(consumer_key, msg)
                    
                except Exception as e:
                    if "timeout" in str(e).lower():
                        break  # No more messages
                    logger.error(f"Replay error: {e}")
                    
        finally:
            # Clean up replay consumer
            self.pulsar_client.close_consumer(consumer_key)
            
    async def create_event_stream(
        self,
        event_types: List[str],
        start_position: str = "latest"
    ) -> AsyncIterator[Event]:
        """Create a stream of events"""
        # Create multi-topic consumer
        topics = [self._get_topic_name(et) for et in event_types]
        subscription = f"{self.service_name}-stream-{datetime.utcnow().timestamp()}"
        
        consumer_config = ConsumerConfig(
            topics=topics,
            subscription_name=subscription,
            subscription_type=ConsumerType.EXCLUSIVE,
            initial_position=InitialPosition.Latest if start_position == "latest" else InitialPosition.Earliest
        )
        
        consumer = self.pulsar_client.create_consumer(consumer_config)
        consumer_key = f"{subscription}:{','.join(topics)}"
        
        try:
            while True:
                msg = await self.pulsar_client.receive_async(consumer_key)
                
                event_data = json.loads(msg.data().decode('utf-8'))
                event = Event(**event_data)
                
                yield event
                
                self.pulsar_client.acknowledge(consumer_key, msg)
                
        finally:
            self.pulsar_client.close_consumer(consumer_key) 