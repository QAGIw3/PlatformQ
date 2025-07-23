"""
Apache Kafka Event Backend

Implements event backend interface for Apache Kafka.
"""

from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from datetime import datetime
import asyncio
import uuid
import json

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError

from .base_backend import (
    EventBackend, EventBackendConfig, BackendType,
    Event, PublishResult, ConsumerConfig
)
from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class KafkaBackend(EventBackend):
    """
    Apache Kafka event backend implementation.
    
    Features:
    - High throughput
    - Horizontal scalability
    - Durable message storage
    - Exactly-once semantics
    - Stream processing
    """
    
    def __init__(self, config: EventBackendConfig):
        super().__init__(config)
        self._producers: Dict[str, AIOKafkaProducer] = {}
        self._consumers: Dict[str, AIOKafkaConsumer] = {}
        self._admin_client: Optional[KafkaAdminClient] = None
        
    async def connect(self) -> None:
        """Connect to Kafka cluster"""
        try:
            # Parse connection URL
            bootstrap_servers = self.config.connection_url.replace("kafka://", "")
            
            # Create admin client for topic management
            admin_config = {
                'bootstrap_servers': bootstrap_servers,
                'client_id': f"{self.config.metadata.get('service_name', 'kafka-client')}_admin"
            }
            
            # Add security config
            if self.config.auth_mechanism == "sasl":
                admin_config.update({
                    'security_protocol': 'SASL_SSL' if self.config.use_tls else 'SASL_PLAINTEXT',
                    'sasl_mechanism': self.config.credentials.get('mechanism', 'PLAIN'),
                    'sasl_plain_username': self.config.credentials.get('username'),
                    'sasl_plain_password': self.config.credentials.get('password')
                })
            elif self.config.use_tls:
                admin_config.update({
                    'security_protocol': 'SSL',
                    'ssl_cafile': self.config.credentials.get('ca_cert_path'),
                    'ssl_certfile': self.config.credentials.get('cert_path'),
                    'ssl_keyfile': self.config.credentials.get('key_path')
                })
            
            self._admin_client = KafkaAdminClient(**admin_config)
            self._connected = True
            
            logger.info(f"Connected to Kafka: {bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Kafka"""
        try:
            # Close all producers
            for producer in self._producers.values():
                await producer.stop()
            self._producers.clear()
            
            # Close all consumers
            for consumer in self._consumers.values():
                await consumer.stop()
            self._consumers.clear()
            
            # Close admin client
            if self._admin_client:
                self._admin_client.close()
                self._admin_client = None
            
            self._connected = False
            logger.info("Disconnected from Kafka")
            
        except Exception as e:
            logger.error(f"Error disconnecting from Kafka: {e}")
            raise
    
    async def publish(
        self,
        event: Event,
        timeout: Optional[float] = None
    ) -> PublishResult:
        """Publish a single event"""
        try:
            producer = await self._get_or_create_producer()
            
            # Prepare message
            key = event.key.encode('utf-8') if event.key else None
            value = json.dumps(event.data).encode('utf-8')
            headers = [(k, v.encode('utf-8')) for k, v in event.headers.items()]
            headers.append(('event_id', event.id.encode('utf-8')))
            headers.append(('timestamp', event.timestamp.isoformat().encode('utf-8')))
            
            # Send message
            future = await producer.send(
                event.topic,
                key=key,
                value=value,
                headers=headers,
                partition=None,  # Let Kafka decide based on key
                timestamp_ms=int(event.timestamp.timestamp() * 1000)
            )
            
            # Wait for result
            if timeout:
                result = await asyncio.wait_for(future, timeout=timeout)
            else:
                result = await future
            
            self._record_publish(True)
            
            return PublishResult(
                success=True,
                message_id=f"{result.topic}:{result.partition}:{result.offset}",
                partition=result.partition,
                offset=result.offset,
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
        producer = await self._get_or_create_producer()
        results = []
        futures = []
        
        # Send all messages
        for event in events:
            try:
                key = event.key.encode('utf-8') if event.key else None
                value = json.dumps(event.data).encode('utf-8')
                headers = [(k, v.encode('utf-8')) for k, v in event.headers.items()]
                headers.append(('event_id', event.id.encode('utf-8')))
                headers.append(('timestamp', event.timestamp.isoformat().encode('utf-8')))
                
                future = await producer.send(
                    event.topic,
                    key=key,
                    value=value,
                    headers=headers,
                    timestamp_ms=int(event.timestamp.timestamp() * 1000)
                )
                futures.append((event, future))
                
            except Exception as e:
                results.append(PublishResult(
                    success=False,
                    error=str(e),
                    timestamp=datetime.now()
                ))
                self._record_publish(False)
        
        # Wait for all results
        if timeout:
            done, pending = await asyncio.wait(
                [f[1] for f in futures],
                timeout=timeout
            )
            
            # Process completed
            for event, future in futures:
                if future in done:
                    try:
                        result = await future
                        results.append(PublishResult(
                            success=True,
                            message_id=f"{result.topic}:{result.partition}:{result.offset}",
                            partition=result.partition,
                            offset=result.offset,
                            timestamp=datetime.now()
                        ))
                        self._record_publish(True)
                    except Exception as e:
                        results.append(PublishResult(
                            success=False,
                            error=str(e),
                            timestamp=datetime.now()
                        ))
                        self._record_publish(False)
                else:
                    results.append(PublishResult(
                        success=False,
                        error="Timeout",
                        timestamp=datetime.now()
                    ))
                    self._record_publish(False)
        else:
            # Wait for all without timeout
            for event, future in futures:
                try:
                    result = await future
                    results.append(PublishResult(
                        success=True,
                        message_id=f"{result.topic}:{result.partition}:{result.offset}",
                        partition=result.partition,
                        offset=result.offset,
                        timestamp=datetime.now()
                    ))
                    self._record_publish(True)
                except Exception as e:
                    results.append(PublishResult(
                        success=False,
                        error=str(e),
                        timestamp=datetime.now()
                    ))
                    self._record_publish(False)
        
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
            await self._consumers[subscription_id].stop()
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
                    # Fetch messages
                    remaining_timeout = max(0, end_time - datetime.now().timestamp())
                    records = await consumer.getmany(
                        timeout_ms=int(remaining_timeout * 1000),
                        max_records=max_messages - len(events)
                    )
                    
                    # Process records
                    for topic_partition, messages in records.items():
                        for msg in messages:
                            event = self._message_to_event(msg)
                            events.append(event)
                    
                    # Commit if auto-commit enabled
                    if config.auto_commit and events:
                        await consumer.commit()
                    
                    self._record_consume(len(events))
                    
                    # Break if no more messages
                    if not records:
                        break
                        
                except asyncio.TimeoutError:
                    break
                except Exception as e:
                    logger.error(f"Error consuming messages: {e}")
                    self._record_error(str(e))
                    break
            
            return events
            
        finally:
            await consumer.stop()
    
    async def acknowledge(
        self,
        event: Event,
        success: bool = True
    ) -> None:
        """Acknowledge event processing"""
        # In Kafka, acknowledgment is done via offset commits
        # This would typically be handled in the consume loop
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
            new_topic = NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=replication_factor,
                topic_configs=config or {}
            )
            
            self._admin_client.create_topics([new_topic], validate_only=False)
            logger.info(f"Created topic: {topic}")
            return True
            
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists: {topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            return False
    
    async def delete_topic(self, topic: str) -> bool:
        """Delete a topic"""
        try:
            self._admin_client.delete_topics([topic], timeout_ms=30000)
            logger.info(f"Deleted topic: {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            return False
    
    async def list_topics(self) -> List[str]:
        """List all topics"""
        try:
            metadata = self._admin_client.list_topics()
            return list(metadata)
            
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic information"""
        try:
            # Get topic metadata
            metadata = self._admin_client.describe_topics([topic])[0]
            
            # Get topic configuration
            resource = ConfigResource(ConfigResourceType.TOPIC, topic)
            configs = self._admin_client.describe_configs([resource])
            
            config_dict = {}
            for config_resource, config_response in configs.items():
                for config_name, config_value in config_response.configs.items():
                    config_dict[config_name] = config_value
            
            return {
                "topic": topic,
                "partitions": len(metadata.partitions),
                "replication_factor": len(metadata.partitions[0].replicas) if metadata.partitions else 0,
                "config": config_dict
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
            async for msg in consumer:
                event = self._message_to_event(msg)
                
                if config.auto_commit:
                    await consumer.commit()
                
                self._record_consume(1)
                yield event
                
        except Exception as e:
            logger.error(f"Error in stream: {e}")
            self._record_error(str(e))
        finally:
            await consumer.stop()
    
    async def _get_or_create_producer(self) -> AIOKafkaProducer:
        """Get or create producer"""
        producer_id = "default"
        
        if producer_id not in self._producers:
            bootstrap_servers = self.config.connection_url.replace("kafka://", "")
            
            producer_config = {
                'bootstrap_servers': bootstrap_servers,
                'client_id': f"{self.config.metadata.get('service_name', 'kafka-client')}_producer",
                'acks': 'all' if self.config.delivery_guarantee == "exactly_once" else 1,
                'retries': self.config.max_retries,
                'max_batch_size': 16384,
                'linger_ms': self.config.batch_timeout_ms,
                'compression_type': self.config.compression or 'none',
                'enable_idempotence': self.config.delivery_guarantee == "exactly_once",
                'request_timeout_ms': int(self.config.timeout_seconds * 1000),
                'retry_backoff_ms': int(self.config.retry_delay_seconds * 1000)
            }
            
            # Add security config
            if self.config.auth_mechanism == "sasl":
                producer_config.update({
                    'security_protocol': 'SASL_SSL' if self.config.use_tls else 'SASL_PLAINTEXT',
                    'sasl_mechanism': self.config.credentials.get('mechanism', 'PLAIN'),
                    'sasl_plain_username': self.config.credentials.get('username'),
                    'sasl_plain_password': self.config.credentials.get('password')
                })
            elif self.config.use_tls:
                producer_config.update({
                    'security_protocol': 'SSL',
                    'ssl_cafile': self.config.credentials.get('ca_cert_path'),
                    'ssl_certfile': self.config.credentials.get('cert_path'),
                    'ssl_keyfile': self.config.credentials.get('key_path')
                })
            
            producer = AIOKafkaProducer(**producer_config)
            await producer.start()
            self._producers[producer_id] = producer
        
        return self._producers[producer_id]
    
    async def _create_consumer(self, config: ConsumerConfig) -> AIOKafkaConsumer:
        """Create consumer"""
        bootstrap_servers = self.config.connection_url.replace("kafka://", "")
        
        consumer_config = {
            'bootstrap_servers': bootstrap_servers,
            'client_id': f"{self.config.metadata.get('service_name', 'kafka-client')}_consumer",
            'group_id': config.consumer_group,
            'auto_offset_reset': config.start_from,
            'enable_auto_commit': config.auto_commit,
            'auto_commit_interval_ms': config.commit_interval_ms,
            'max_poll_records': config.max_poll_records,
            'session_timeout_ms': 30000,
            'heartbeat_interval_ms': 3000,
            'request_timeout_ms': int(self.config.timeout_seconds * 1000),
            'retry_backoff_ms': int(self.config.retry_delay_seconds * 1000),
            'isolation_level': 'read_committed' if self.config.delivery_guarantee == "exactly_once" else 'read_uncommitted'
        }
        
        # Add security config
        if self.config.auth_mechanism == "sasl":
            consumer_config.update({
                'security_protocol': 'SASL_SSL' if self.config.use_tls else 'SASL_PLAINTEXT',
                'sasl_mechanism': self.config.credentials.get('mechanism', 'PLAIN'),
                'sasl_plain_username': self.config.credentials.get('username'),
                'sasl_plain_password': self.config.credentials.get('password')
            })
        elif self.config.use_tls:
            consumer_config.update({
                'security_protocol': 'SSL',
                'ssl_cafile': self.config.credentials.get('ca_cert_path'),
                'ssl_certfile': self.config.credentials.get('cert_path'),
                'ssl_keyfile': self.config.credentials.get('key_path')
            })
        
        consumer = AIOKafkaConsumer(*config.topics, **consumer_config)
        await consumer.start()
        
        # Seek to timestamp if specified
        if config.start_from == "timestamp" and config.start_timestamp:
            partitions = consumer.assignment()
            timestamps = {
                tp: int(config.start_timestamp.timestamp() * 1000)
                for tp in partitions
            }
            offsets = await consumer.offsets_for_times(timestamps)
            for tp, offset in offsets.items():
                if offset:
                    consumer.seek(tp, offset.offset)
        
        return consumer
    
    async def _consume_loop(
        self,
        subscription_id: str,
        consumer: AIOKafkaConsumer,
        handler: Callable[[Event], Any],
        config: ConsumerConfig
    ):
        """Consumer loop for subscription"""
        while subscription_id in self._subscriptions:
            try:
                # Consume messages
                async for msg in consumer:
                    event = self._message_to_event(msg)
                    
                    # Process event
                    try:
                        result = handler(event)
                        if asyncio.iscoroutine(result):
                            await result
                        
                        self._record_consume(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                        self._record_error(str(e))
                        
                        # Handle dead letter if enabled
                        if config.enable_dead_letter and config.dead_letter_topic:
                            # TODO: Send to dead letter topic
                            pass
                    
                    # Commit offset if auto-commit is disabled
                    if not config.auto_commit:
                        await consumer.commit()
                        
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                self._record_error(str(e))
                await asyncio.sleep(1)
    
    def _message_to_event(self, msg) -> Event:
        """Convert Kafka message to Event"""
        try:
            data = json.loads(msg.value.decode('utf-8'))
        except:
            data = {"raw_data": msg.value.decode('utf-8')}
        
        headers = {}
        event_id = None
        timestamp_str = None
        
        if msg.headers:
            for key, value in msg.headers:
                header_value = value.decode('utf-8') if value else None
                if key == 'event_id':
                    event_id = header_value
                elif key == 'timestamp':
                    timestamp_str = header_value
                else:
                    headers[key] = header_value
        
        # Parse timestamp
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
            except:
                timestamp = datetime.fromtimestamp(msg.timestamp / 1000)
        else:
            timestamp = datetime.fromtimestamp(msg.timestamp / 1000)
        
        return Event(
            id=event_id or f"{msg.topic}:{msg.partition}:{msg.offset}",
            topic=msg.topic,
            data=data,
            timestamp=timestamp,
            headers=headers,
            key=msg.key.decode('utf-8') if msg.key else None,
            partition_key=msg.key.decode('utf-8') if msg.key else None
        )


# Register backend
from .base_backend import EventBackendFactory
EventBackendFactory.register_backend(BackendType.KAFKA, KafkaBackend) 