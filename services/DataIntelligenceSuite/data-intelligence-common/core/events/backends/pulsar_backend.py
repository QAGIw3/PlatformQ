"""
Apache Pulsar Event Backend

Implements event backend interface for Apache Pulsar.
"""

from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from datetime import datetime
import asyncio
import uuid
import json

import pulsar
from pulsar.schema import JsonSchema

from .base_backend import (
    EventBackend, EventBackendConfig, BackendType,
    Event, PublishResult, ConsumerConfig
)
from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class PulsarBackend(EventBackend):
    """
    Apache Pulsar event backend implementation.
    
    Features:
    - Multi-tenancy support
    - Schema registry
    - Geo-replication
    - Tiered storage
    - Built-in functions
    """
    
    def __init__(self, config: EventBackendConfig):
        super().__init__(config)
        self._client: Optional[pulsar.Client] = None
        self._producers: Dict[str, pulsar.Producer] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        self._subscriptions: Dict[str, asyncio.Task] = {}
        
    async def connect(self) -> None:
        """Connect to Pulsar cluster"""
        try:
            # Build client configuration
            client_config = {
                'service_url': self.config.connection_url,
                'operation_timeout_seconds': self.config.timeout_seconds,
                'io_threads': 4,
                'message_listener_threads': 4,
            }
            
            # Add authentication if configured
            if self.config.auth_mechanism == "token":
                client_config['authentication'] = pulsar.AuthenticationToken(
                    self.config.credentials.get('token')
                )
            elif self.config.auth_mechanism == "oauth2":
                client_config['authentication'] = pulsar.AuthenticationOauth2(
                    self.config.credentials
                )
            
            # Add TLS if enabled
            if self.config.use_tls:
                client_config['tls_trust_certs_file_path'] = self.config.credentials.get(
                    'ca_cert_path'
                )
                client_config['tls_allow_insecure_connection'] = False
            
            # Create client
            self._client = pulsar.Client(**client_config)
            self._connected = True
            
            logger.info(f"Connected to Pulsar: {self.config.connection_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Pulsar"""
        try:
            # Close all producers
            for producer in self._producers.values():
                producer.close()
            self._producers.clear()
            
            # Close all consumers
            for consumer in self._consumers.values():
                consumer.close()
            self._consumers.clear()
            
            # Cancel all subscriptions
            for task in self._subscriptions.values():
                task.cancel()
            self._subscriptions.clear()
            
            # Close client
            if self._client:
                self._client.close()
                self._client = None
            
            self._connected = False
            logger.info("Disconnected from Pulsar")
            
        except Exception as e:
            logger.error(f"Error disconnecting from Pulsar: {e}")
            raise
    
    async def publish(
        self,
        event: Event,
        timeout: Optional[float] = None
    ) -> PublishResult:
        """Publish a single event"""
        try:
            producer = await self._get_or_create_producer(event.topic)
            
            # Prepare message
            message_data = json.dumps(event.data).encode('utf-8')
            
            # Build message properties
            properties = event.headers.copy()
            properties['event_id'] = event.id
            properties['timestamp'] = event.timestamp.isoformat()
            
            # Send message
            message_id = producer.send(
                message_data,
                properties=properties,
                partition_key=event.partition_key,
                event_timestamp=int(event.timestamp.timestamp() * 1000),
                deliver_after_ms=None,
                deliver_at_ms=None
            )
            
            # Wait for acknowledgment
            if timeout:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, message_id.result),
                    timeout=timeout
                )
            else:
                message_id.result()
            
            self._record_publish(True)
            
            return PublishResult(
                success=True,
                message_id=str(message_id),
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            self._record_publish(False)
            self._record_error(str(e))
            
            return PublishResult(
                success=False,
                error=str(e),
                timestamp=datetime.now()
            )
    
    async def publish_batch(
        self,
        events: List[Event],
        timeout: Optional[float] = None
    ) -> List[PublishResult]:
        """Publish multiple events"""
        results = []
        
        # Group events by topic
        events_by_topic: Dict[str, List[Event]] = {}
        for event in events:
            if event.topic not in events_by_topic:
                events_by_topic[event.topic] = []
            events_by_topic[event.topic].append(event)
        
        # Publish to each topic
        for topic, topic_events in events_by_topic.items():
            producer = await self._get_or_create_producer(topic)
            
            # Use batch builder for efficiency
            batch_builder = producer.create_batch()
            
            for event in topic_events:
                try:
                    message_data = json.dumps(event.data).encode('utf-8')
                    properties = event.headers.copy()
                    properties['event_id'] = event.id
                    properties['timestamp'] = event.timestamp.isoformat()
                    
                    batch_builder.append(
                        message_data,
                        properties=properties,
                        partition_key=event.partition_key,
                        event_timestamp=int(event.timestamp.timestamp() * 1000)
                    )
                    
                except Exception as e:
                    results.append(PublishResult(
                        success=False,
                        error=str(e),
                        timestamp=datetime.now()
                    ))
            
            # Send batch
            try:
                message_id = producer.send(batch_builder)
                
                if timeout:
                    await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(None, message_id.result),
                        timeout=timeout
                    )
                else:
                    message_id.result()
                
                # All messages in batch succeeded
                for event in topic_events:
                    results.append(PublishResult(
                        success=True,
                        message_id=str(message_id),
                        timestamp=datetime.now()
                    ))
                    self._record_publish(True)
                    
            except Exception as e:
                logger.error(f"Failed to publish batch: {e}")
                for event in topic_events:
                    results.append(PublishResult(
                        success=False,
                        error=str(e),
                        timestamp=datetime.now()
                    ))
                    self._record_publish(False)
                self._record_error(str(e))
        
        return results
    
    async def subscribe(
        self,
        config: ConsumerConfig,
        handler: Callable[[Event], Any]
    ) -> str:
        """Subscribe to topics with a handler"""
        subscription_id = str(uuid.uuid4())
        
        # Create consumer
        consumer = await self._create_consumer(config)
        self._consumers[subscription_id] = consumer
        
        # Start consumption task
        task = asyncio.create_task(
            self._consume_loop(subscription_id, consumer, handler, config)
        )
        self._subscriptions[subscription_id] = task
        
        logger.info(f"Created subscription {subscription_id} for topics: {config.topics}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from topics"""
        # Cancel task
        if subscription_id in self._subscriptions:
            self._subscriptions[subscription_id].cancel()
            del self._subscriptions[subscription_id]
        
        # Close consumer
        if subscription_id in self._consumers:
            self._consumers[subscription_id].close()
            del self._consumers[subscription_id]
        
        logger.info(f"Unsubscribed: {subscription_id}")
    
    async def consume_batch(
        self,
        config: ConsumerConfig,
        max_messages: int = 100,
        timeout: Optional[float] = None
    ) -> List[Event]:
        """Consume a batch of messages"""
        consumer = await self._create_consumer(config)
        events = []
        
        try:
            end_time = datetime.now().timestamp() + (timeout or self.config.timeout_seconds)
            
            while len(events) < max_messages and datetime.now().timestamp() < end_time:
                try:
                    # Receive with timeout
                    remaining_timeout = max(0, end_time - datetime.now().timestamp())
                    msg = consumer.receive(timeout_millis=int(remaining_timeout * 1000))
                    
                    # Convert to Event
                    event = self._message_to_event(msg)
                    events.append(event)
                    
                    # Acknowledge if auto-commit enabled
                    if config.auto_commit:
                        consumer.acknowledge(msg)
                    
                    self._record_consume(1)
                    
                except Exception as e:
                    if "timeout" in str(e).lower():
                        break
                    else:
                        logger.error(f"Error consuming message: {e}")
                        self._record_error(str(e))
            
            return events
            
        finally:
            consumer.close()
    
    async def acknowledge(
        self,
        event: Event,
        success: bool = True
    ) -> None:
        """Acknowledge event processing"""
        # In Pulsar, acknowledgment is handled at the consumer level
        # This would typically be called within the consume loop
        pass
    
    async def create_topic(
        self,
        topic: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Create a topic"""
        try:
            # Pulsar creates topics automatically on first use
            # For explicit creation, use admin API
            admin_url = self.config.metadata.get('admin_url')
            if admin_url:
                # TODO: Implement admin API calls
                pass
            
            logger.info(f"Topic will be created on first use: {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            return False
    
    async def delete_topic(self, topic: str) -> bool:
        """Delete a topic"""
        try:
            # Requires admin API
            admin_url = self.config.metadata.get('admin_url')
            if admin_url:
                # TODO: Implement admin API calls
                pass
            
            logger.warning(f"Topic deletion requires admin API: {topic}")
            return False
            
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            return False
    
    async def list_topics(self) -> List[str]:
        """List all topics"""
        try:
            # Requires admin API
            admin_url = self.config.metadata.get('admin_url')
            if admin_url:
                # TODO: Implement admin API calls
                pass
            
            # Return empty list for now
            return []
            
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic information"""
        try:
            # Requires admin API
            return {
                "topic": topic,
                "partitions": 1,
                "replication_factor": 1,
                "message_count": 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get topic info: {e}")
            return {}
    
    async def stream(
        self,
        config: ConsumerConfig
    ) -> AsyncIterator[Event]:
        """Stream events as async iterator"""
        consumer = await self._create_consumer(config)
        
        try:
            while True:
                try:
                    msg = consumer.receive(timeout_millis=1000)
                    event = self._message_to_event(msg)
                    
                    if config.auto_commit:
                        consumer.acknowledge(msg)
                    
                    self._record_consume(1)
                    yield event
                    
                except Exception as e:
                    if "timeout" not in str(e).lower():
                        logger.error(f"Error in stream: {e}")
                        self._record_error(str(e))
                    
        finally:
            consumer.close()
    
    async def _get_or_create_producer(self, topic: str) -> pulsar.Producer:
        """Get or create producer for topic"""
        if topic not in self._producers:
            producer_config = {
                'topic': topic,
                'producer_name': f"{self.config.metadata.get('service_name', 'producer')}_{topic}",
                'send_timeout_millis': int(self.config.timeout_seconds * 1000),
                'compression_type': self._get_compression_type(),
                'batching_enabled': True,
                'batching_max_messages': self.config.batch_size,
                'batching_max_publish_delay_ms': self.config.batch_timeout_ms,
                'max_pending_messages': 1000,
                'block_if_queue_full': True
            }
            
            self._producers[topic] = self._client.create_producer(**producer_config)
        
        return self._producers[topic]
    
    async def _create_consumer(self, config: ConsumerConfig) -> pulsar.Consumer:
        """Create consumer"""
        consumer_config = {
            'topics': config.topics,
            'subscription_name': config.consumer_group,
            'consumer_type': pulsar.ConsumerType.Shared,
            'message_listener': None,
            'receiver_queue_size': config.max_poll_records,
            'max_total_receiver_queue_size_across_partitions': config.max_poll_records * 10,
            'consumer_name': f"{config.consumer_group}_{uuid.uuid4().hex[:8]}",
            'unacked_messages_timeout_ms': int(self.config.timeout_seconds * 1000),
            'broker_consumer_stats_cache_time_ms': 30000,
            'negative_ack_redelivery_delay_ms': 1000,
            'auto_ack_oldest_chunked_message_on_queue_full': True,
            'start_message_id_inclusive': True
        }
        
        # Set initial position
        if config.start_from == "earliest":
            consumer_config['initial_position'] = pulsar.InitialPosition.Earliest
        elif config.start_from == "latest":
            consumer_config['initial_position'] = pulsar.InitialPosition.Latest
        
        # Add dead letter policy if enabled
        if config.enable_dead_letter:
            consumer_config['dead_letter_policy'] = pulsar.ConsumerDeadLetterPolicy(
                max_redeliver_count=config.max_redeliveries,
                dead_letter_topic=config.dead_letter_topic
            )
        
        return self._client.subscribe(**consumer_config)
    
    async def _consume_loop(
        self,
        subscription_id: str,
        consumer: pulsar.Consumer,
        handler: Callable[[Event], Any],
        config: ConsumerConfig
    ):
        """Consumer loop for subscription"""
        while subscription_id in self._subscriptions:
            try:
                msg = consumer.receive(timeout_millis=1000)
                event = self._message_to_event(msg)
                
                # Process event
                try:
                    result = handler(event)
                    if asyncio.iscoroutine(result):
                        await result
                    
                    # Acknowledge on success
                    consumer.acknowledge(msg)
                    self._record_consume(1)
                    
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    # Negative acknowledge for redelivery
                    consumer.negative_acknowledge(msg)
                    self._record_error(str(e))
                    
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error in consume loop: {e}")
                    self._record_error(str(e))
                    await asyncio.sleep(1)
    
    def _message_to_event(self, msg: pulsar.Message) -> Event:
        """Convert Pulsar message to Event"""
        try:
            data = json.loads(msg.data().decode('utf-8'))
        except:
            data = {"raw_data": msg.data().decode('utf-8')}
        
        properties = msg.properties() or {}
        
        return Event(
            id=properties.get('event_id', str(msg.message_id())),
            topic=msg.topic_name(),
            data=data,
            timestamp=datetime.fromtimestamp(msg.event_timestamp() / 1000),
            headers=properties,
            key=msg.partition_key(),
            partition_key=msg.partition_key()
        )
    
    def _get_compression_type(self) -> pulsar.CompressionType:
        """Get Pulsar compression type"""
        compression_map = {
            'lz4': pulsar.CompressionType.LZ4,
            'zlib': pulsar.CompressionType.ZLIB,
            'zstd': pulsar.CompressionType.ZSTD,
            'snappy': pulsar.CompressionType.SNAPPY
        }
        
        compression = self.config.compression
        if compression and compression.lower() in compression_map:
            return compression_map[compression.lower()]
        
        return pulsar.CompressionType.NONE


# Register backend
from .base_backend import EventBackendFactory
EventBackendFactory.register_backend(BackendType.PULSAR, PulsarBackend) 