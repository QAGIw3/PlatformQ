"""
Redis Streams Event Backend

Implements event backend interface for Redis Streams.
"""

from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from datetime import datetime
import asyncio
import uuid
import json
import aioredis

from .base_backend import (
    EventBackend, EventBackendConfig, BackendType,
    Event, PublishResult, ConsumerConfig
)
from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class RedisStreamsBackend(EventBackend):
    """
    Redis Streams event backend implementation.
    
    Features:
    - Low latency messaging
    - Consumer groups
    - Message persistence
    - At-least-once delivery
    - Stream trimming
    """
    
    def __init__(self, config: EventBackendConfig):
        super().__init__(config)
        self._redis: Optional[aioredis.Redis] = None
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        
    async def connect(self) -> None:
        """Connect to Redis"""
        try:
            # Parse connection URL
            url = self.config.connection_url
            if not url.startswith("redis://"):
                url = f"redis://{url}"
            
            # Connection options
            options = {
                'encoding': 'utf-8',
                'decode_responses': False,
                'health_check_interval': 30,
                'socket_keepalive': True,
                'socket_connect_timeout': self.config.timeout_seconds,
                'retry_on_timeout': True,
                'retry_on_error': [aioredis.ConnectionError],
                'max_connections': 50
            }
            
            # Add authentication
            if self.config.credentials:
                if 'password' in self.config.credentials:
                    options['password'] = self.config.credentials['password']
                if 'username' in self.config.credentials:
                    options['username'] = self.config.credentials['username']
            
            # SSL/TLS configuration
            if self.config.use_tls:
                options['ssl'] = True
                if 'ca_cert_path' in self.config.credentials:
                    options['ssl_ca_certs'] = self.config.credentials['ca_cert_path']
                if 'cert_path' in self.config.credentials:
                    options['ssl_certfile'] = self.config.credentials['cert_path']
                if 'key_path' in self.config.credentials:
                    options['ssl_keyfile'] = self.config.credentials['key_path']
            
            # Create connection
            self._redis = await aioredis.from_url(url, **options)
            
            # Test connection
            await self._redis.ping()
            
            self._connected = True
            logger.info(f"Connected to Redis: {url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from Redis"""
        try:
            # Cancel all consumer tasks
            for task in self._consumer_tasks.values():
                task.cancel()
            
            # Wait for tasks to complete
            if self._consumer_tasks:
                await asyncio.gather(*self._consumer_tasks.values(), return_exceptions=True)
            
            self._consumer_tasks.clear()
            
            # Close Redis connection
            if self._redis:
                await self._redis.close()
                self._redis = None
            
            self._connected = False
            logger.info("Disconnected from Redis")
            
        except Exception as e:
            logger.error(f"Error disconnecting from Redis: {e}")
            raise
    
    async def publish(
        self,
        event: Event,
        timeout: Optional[float] = None
    ) -> PublishResult:
        """Publish a single event"""
        try:
            # Prepare stream data
            stream_data = {
                'event_id': event.id,
                'data': json.dumps(event.data),
                'timestamp': event.timestamp.isoformat(),
                'key': event.key or '',
                'partition_key': event.partition_key or ''
            }
            
            # Add headers
            for k, v in event.headers.items():
                stream_data[f'header_{k}'] = v
            
            # Add to stream
            stream_key = f"stream:{event.topic}"
            
            # Use XADD with optional max length to prevent unbounded growth
            max_len = self.config.metadata.get('stream_max_length', 100000)
            
            message_id = await asyncio.wait_for(
                self._redis.xadd(
                    stream_key,
                    stream_data,
                    maxlen=max_len,
                    approximate=True
                ),
                timeout=timeout or self.config.timeout_seconds
            )
            
            self._record_publish(True)
            
            return PublishResult(
                success=True,
                message_id=message_id.decode() if isinstance(message_id, bytes) else str(message_id),
                timestamp=datetime.now()
            )
            
        except asyncio.TimeoutError:
            logger.error("Publish timeout")
            self._record_publish(False)
            self._record_error("Timeout")
            
            return PublishResult(
                success=False,
                error="Timeout",
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
        # Redis doesn't have native batch publish for streams
        # Use pipeline for better performance
        results = []
        
        try:
            async with self._redis.pipeline(transaction=True) as pipe:
                futures = []
                
                for event in events:
                    stream_data = {
                        'event_id': event.id,
                        'data': json.dumps(event.data),
                        'timestamp': event.timestamp.isoformat(),
                        'key': event.key or '',
                        'partition_key': event.partition_key or ''
                    }
                    
                    for k, v in event.headers.items():
                        stream_data[f'header_{k}'] = v
                    
                    stream_key = f"stream:{event.topic}"
                    max_len = self.config.metadata.get('stream_max_length', 100000)
                    
                    pipe.xadd(stream_key, stream_data, maxlen=max_len, approximate=True)
                    futures.append(event)
                
                # Execute pipeline
                message_ids = await asyncio.wait_for(
                    pipe.execute(),
                    timeout=timeout or self.config.timeout_seconds
                )
                
                # Process results
                for i, (event, message_id) in enumerate(zip(futures, message_ids)):
                    results.append(PublishResult(
                        success=True,
                        message_id=message_id.decode() if isinstance(message_id, bytes) else str(message_id),
                        timestamp=datetime.now()
                    ))
                    self._record_publish(True)
                    
        except asyncio.TimeoutError:
            # Timeout - mark remaining as failed
            for i in range(len(results), len(events)):
                results.append(PublishResult(
                    success=False,
                    error="Timeout",
                    timestamp=datetime.now()
                ))
                self._record_publish(False)
            self._record_error("Timeout")
            
        except Exception as e:
            logger.error(f"Failed to publish batch: {e}")
            # Error - mark all as failed
            results = [
                PublishResult(
                    success=False,
                    error=str(e),
                    timestamp=datetime.now()
                ) for _ in events
            ]
            for _ in events:
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
        
        # Create consumer task
        task = asyncio.create_task(
            self._consumer_loop(subscription_id, config, handler)
        )
        self._consumer_tasks[subscription_id] = task
        
        logger.info(f"Created subscription {subscription_id} for topics: {config.topics}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from topics"""
        if subscription_id in self._consumer_tasks:
            self._consumer_tasks[subscription_id].cancel()
            try:
                await self._consumer_tasks[subscription_id]
            except asyncio.CancelledError:
                pass
            del self._consumer_tasks[subscription_id]
        
        logger.info(f"Unsubscribed: {subscription_id}")
    
    async def consume_batch(
        self,
        config: ConsumerConfig,
        max_messages: int = 100,
        timeout: Optional[float] = None
    ) -> List[Event]:
        """Consume a batch of messages"""
        events = []
        end_time = datetime.now().timestamp() + (timeout or self.config.timeout_seconds)
        
        # Create consumer group if needed
        for topic in config.topics:
            stream_key = f"stream:{topic}"
            try:
                await self._redis.xgroup_create(
                    stream_key,
                    config.consumer_group,
                    id='0' if config.start_from == 'earliest' else '$',
                    mkstream=True
                )
            except aioredis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
        
        # Consume from streams
        consumer_name = f"{config.consumer_group}_{uuid.uuid4().hex[:8]}"
        
        while len(events) < max_messages and datetime.now().timestamp() < end_time:
            try:
                # Calculate remaining timeout
                remaining_timeout = max(0, end_time - datetime.now().timestamp())
                if remaining_timeout == 0:
                    break
                
                # Read from streams
                stream_keys = [f"stream:{topic}" for topic in config.topics]
                
                messages = await self._redis.xreadgroup(
                    config.consumer_group,
                    consumer_name,
                    {key: '>' for key in stream_keys},
                    count=max_messages - len(events),
                    block=int(remaining_timeout * 1000)
                )
                
                if not messages:
                    break
                
                # Process messages
                for stream_key, stream_messages in messages:
                    topic = stream_key.decode().replace('stream:', '')
                    
                    for message_id, data in stream_messages:
                        event = self._stream_data_to_event(topic, message_id, data)
                        events.append(event)
                        
                        # Acknowledge if auto-commit
                        if config.auto_commit:
                            await self._redis.xack(stream_key, config.consumer_group, message_id)
                
                self._record_consume(len(events))
                
            except asyncio.TimeoutError:
                break
            except Exception as e:
                logger.error(f"Error consuming messages: {e}")
                self._record_error(str(e))
                break
        
        return events
    
    async def acknowledge(
        self,
        event: Event,
        success: bool = True
    ) -> None:
        """Acknowledge event processing"""
        # Extract message ID from event
        if ':' in event.id:
            stream_key = f"stream:{event.topic}"
            message_id = event.id.split(':', 1)[1]
            
            # Get consumer group from event metadata
            consumer_group = event.headers.get('_consumer_group')
            if consumer_group:
                if success:
                    await self._redis.xack(stream_key, consumer_group, message_id)
                else:
                    # Redis doesn't have negative acknowledgment
                    # Could implement custom dead letter handling here
                    pass
    
    async def create_topic(
        self,
        topic: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Create a topic (stream)"""
        try:
            # Redis Streams are created automatically on first write
            # We can pre-create by adding a dummy message and then deleting it
            stream_key = f"stream:{topic}"
            
            # Add initialization message
            message_id = await self._redis.xadd(stream_key, {'_init': 'true'})
            
            # Delete the initialization message
            await self._redis.xdel(stream_key, message_id)
            
            logger.info(f"Created stream: {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            return False
    
    async def delete_topic(self, topic: str) -> bool:
        """Delete a topic (stream)"""
        try:
            stream_key = f"stream:{topic}"
            await self._redis.delete(stream_key)
            
            logger.info(f"Deleted stream: {topic}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            return False
    
    async def list_topics(self) -> List[str]:
        """List all topics (streams)"""
        try:
            # Scan for all stream keys
            topics = []
            cursor = 0
            
            while True:
                cursor, keys = await self._redis.scan(
                    cursor,
                    match='stream:*',
                    count=100
                )
                
                for key in keys:
                    topic = key.decode().replace('stream:', '')
                    topics.append(topic)
                
                if cursor == 0:
                    break
            
            return topics
            
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic (stream) information"""
        try:
            stream_key = f"stream:{topic}"
            
            # Get stream info
            info = await self._redis.xinfo_stream(stream_key)
            
            # Get consumer groups
            groups = []
            try:
                groups_info = await self._redis.xinfo_groups(stream_key)
                groups = [g['name'].decode() for g in groups_info]
            except:
                pass
            
            return {
                "topic": topic,
                "length": info.get('length', 0),
                "first_entry": info.get('first-entry'),
                "last_entry": info.get('last-entry'),
                "consumer_groups": groups,
                "radix_tree_keys": info.get('radix-tree-keys', 0),
                "radix_tree_nodes": info.get('radix-tree-nodes', 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to get topic info: {e}")
            return {}
    
    async def stream(
        self,
        config: ConsumerConfig
    ) -> AsyncIterator[Event]:
        """Stream events as async iterator"""
        # Create consumer group
        for topic in config.topics:
            stream_key = f"stream:{topic}"
            try:
                await self._redis.xgroup_create(
                    stream_key,
                    config.consumer_group,
                    id='0' if config.start_from == 'earliest' else '$',
                    mkstream=True
                )
            except aioredis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
        
        consumer_name = f"{config.consumer_group}_{uuid.uuid4().hex[:8]}"
        stream_keys = [f"stream:{topic}" for topic in config.topics]
        
        while True:
            try:
                messages = await self._redis.xreadgroup(
                    config.consumer_group,
                    consumer_name,
                    {key: '>' for key in stream_keys},
                    count=config.max_poll_records,
                    block=1000  # 1 second
                )
                
                if messages:
                    for stream_key, stream_messages in messages:
                        topic = stream_key.decode().replace('stream:', '')
                        
                        for message_id, data in stream_messages:
                            event = self._stream_data_to_event(topic, message_id, data)
                            
                            if config.auto_commit:
                                await self._redis.xack(stream_key, config.consumer_group, message_id)
                            
                            self._record_consume(1)
                            yield event
                            
            except Exception as e:
                logger.error(f"Error in stream: {e}")
                self._record_error(str(e))
                await asyncio.sleep(1)
    
    async def _consumer_loop(
        self,
        subscription_id: str,
        config: ConsumerConfig,
        handler: Callable[[Event], Any]
    ):
        """Consumer loop for subscription"""
        # Create consumer group
        for topic in config.topics:
            stream_key = f"stream:{topic}"
            try:
                await self._redis.xgroup_create(
                    stream_key,
                    config.consumer_group,
                    id='0' if config.start_from == 'earliest' else '$',
                    mkstream=True
                )
            except aioredis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise
        
        consumer_name = f"{config.consumer_group}_{subscription_id[:8]}"
        stream_keys = [f"stream:{topic}" for topic in config.topics]
        
        # Track pending messages for retry
        pending_messages = {}
        
        while subscription_id in self._consumer_tasks:
            try:
                # Check for pending messages to retry
                if config.enable_dead_letter and pending_messages:
                    for (stream_key, message_id), (event, attempts) in list(pending_messages.items()):
                        if attempts >= config.max_redeliveries:
                            # Send to dead letter
                            if config.dead_letter_topic:
                                event.headers['_original_topic'] = event.topic
                                event.headers['_failure_count'] = str(attempts)
                                event.topic = config.dead_letter_topic
                                await self.publish(event)
                            
                            # Acknowledge to remove from pending
                            await self._redis.xack(stream_key, config.consumer_group, message_id)
                            del pending_messages[(stream_key, message_id)]
                
                # Read new messages
                messages = await self._redis.xreadgroup(
                    config.consumer_group,
                    consumer_name,
                    {key: '>' for key in stream_keys},
                    count=config.max_poll_records,
                    block=1000  # 1 second
                )
                
                if messages:
                    for stream_key, stream_messages in messages:
                        topic = stream_key.decode().replace('stream:', '')
                        
                        for message_id, data in stream_messages:
                            event = self._stream_data_to_event(topic, message_id, data)
                            event.headers['_consumer_group'] = config.consumer_group
                            
                            try:
                                # Process event
                                result = handler(event)
                                if asyncio.iscoroutine(result):
                                    await result
                                
                                # Acknowledge on success
                                await self._redis.xack(stream_key, config.consumer_group, message_id)
                                self._record_consume(1)
                                
                                # Remove from pending if it was there
                                pending_key = (stream_key, message_id)
                                if pending_key in pending_messages:
                                    del pending_messages[pending_key]
                                    
                            except Exception as e:
                                logger.error(f"Error processing event: {e}")
                                self._record_error(str(e))
                                
                                # Track for retry
                                pending_key = (stream_key, message_id)
                                if pending_key in pending_messages:
                                    pending_messages[pending_key] = (event, pending_messages[pending_key][1] + 1)
                                else:
                                    pending_messages[pending_key] = (event, 1)
                
                # Check for idle messages (claimed by other consumers that died)
                if len(pending_messages) < config.max_poll_records:
                    for stream_key in stream_keys:
                        try:
                            # Claim idle messages older than 30 seconds
                            idle_messages = await self._redis.xautoclaim(
                                stream_key,
                                config.consumer_group,
                                consumer_name,
                                30000,  # 30 seconds
                                start_id='-',
                                count=10
                            )
                            
                            if idle_messages and len(idle_messages) > 1:
                                for message_id, data in idle_messages[1]:
                                    if data:  # Sometimes returns empty data
                                        topic = stream_key.decode().replace('stream:', '')
                                        event = self._stream_data_to_event(topic, message_id, data)
                                        event.headers['_consumer_group'] = config.consumer_group
                                        
                                        # Add to pending for processing in next iteration
                                        pending_messages[(stream_key, message_id)] = (event, 0)
                                        
                        except Exception as e:
                            # XAUTOCLAIM might not be available in older Redis versions
                            pass
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                self._record_error(str(e))
                await asyncio.sleep(1)
    
    def _stream_data_to_event(self, topic: str, message_id: bytes, data: Dict[bytes, bytes]) -> Event:
        """Convert Redis stream data to Event"""
        # Decode data
        decoded_data = {}
        headers = {}
        event_id = None
        timestamp_str = None
        key = None
        partition_key = None
        
        for k, v in data.items():
            k_str = k.decode() if isinstance(k, bytes) else k
            v_str = v.decode() if isinstance(v, bytes) else v
            
            if k_str == 'event_id':
                event_id = v_str
            elif k_str == 'data':
                try:
                    decoded_data = json.loads(v_str)
                except:
                    decoded_data = {'raw_data': v_str}
            elif k_str == 'timestamp':
                timestamp_str = v_str
            elif k_str == 'key':
                key = v_str if v_str else None
            elif k_str == 'partition_key':
                partition_key = v_str if v_str else None
            elif k_str.startswith('header_'):
                header_name = k_str[7:]  # Remove 'header_' prefix
                headers[header_name] = v_str
        
        # Parse timestamp
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
            except:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # Generate event ID if not provided
        if not event_id:
            message_id_str = message_id.decode() if isinstance(message_id, bytes) else str(message_id)
            event_id = f"{topic}:{message_id_str}"
        
        return Event(
            id=event_id,
            topic=topic,
            data=decoded_data,
            timestamp=timestamp,
            headers=headers,
            key=key,
            partition_key=partition_key
        )


# Register backend
from .base_backend import EventBackendFactory
EventBackendFactory.register_backend(BackendType.REDIS_STREAMS, RedisStreamsBackend) 