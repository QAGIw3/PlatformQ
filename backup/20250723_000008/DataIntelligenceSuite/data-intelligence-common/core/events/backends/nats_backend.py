"""
NATS Event Backend

Implements event backend interface for NATS/NATS Streaming.
"""

from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from datetime import datetime
import asyncio
import uuid
import json
import nats
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.errors import NotFoundError

from .base_backend import (
    EventBackend, EventBackendConfig, BackendType,
    Event, PublishResult, ConsumerConfig
)
from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class NATSBackend(EventBackend):
    """
    NATS/JetStream event backend implementation.
    
    Features:
    - High performance messaging
    - JetStream for persistence
    - Subject-based routing
    - Distributed queuing
    - Exactly-once delivery
    """
    
    def __init__(self, config: EventBackendConfig):
        super().__init__(config)
        self._nc: Optional[NATS] = None
        self._js: Optional[JetStreamContext] = None
        self._subscriptions: Dict[str, Any] = {}
        
    async def connect(self) -> None:
        """Connect to NATS server"""
        try:
            # Parse connection URL
            servers = [self.config.connection_url]
            if not any(s.startswith("nats://") for s in servers):
                servers = [f"nats://{s}" for s in servers]
            
            # Connection options
            options = {
                "servers": servers,
                "name": self.config.metadata.get('client_name', 'nats-client'),
                "pedantic": False,
                "verbose": False,
                "allow_reconnect": True,
                "connect_timeout": self.config.timeout_seconds,
                "reconnect_time_wait": self.config.retry_delay_seconds,
                "max_reconnect_attempts": self.config.max_retries,
                "ping_interval": 60,
                "max_outstanding_pings": 2,
                "dont_randomize": False,
                "flusher_queue_size": 1024,
                "no_echo": False,
                "drain_timeout": 30
            }
            
            # Add authentication
            if self.config.auth_mechanism == "token":
                options["token"] = self.config.credentials.get('token')
            elif self.config.auth_mechanism == "user_pass":
                options["user"] = self.config.credentials.get('username')
                options["password"] = self.config.credentials.get('password')
            elif self.config.auth_mechanism == "nkey":
                options["nkeys_seed"] = self.config.credentials.get('seed')
            elif self.config.auth_mechanism == "jwt":
                options["user_jwt_cb"] = self._jwt_cb
                options["signature_cb"] = self._sig_cb
            
            # TLS configuration
            if self.config.use_tls:
                import ssl
                ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                
                if 'ca_cert_path' in self.config.credentials:
                    ssl_ctx.load_verify_locations(self.config.credentials['ca_cert_path'])
                if 'cert_path' in self.config.credentials:
                    ssl_ctx.load_cert_chain(
                        self.config.credentials['cert_path'],
                        self.config.credentials.get('key_path')
                    )
                
                options["tls"] = ssl_ctx
            
            # Create connection
            self._nc = await nats.connect(**options)
            
            # Create JetStream context
            self._js = self._nc.jetstream()
            
            self._connected = True
            logger.info(f"Connected to NATS: {servers}")
            
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect from NATS"""
        try:
            # Cancel all subscriptions
            for sub in self._subscriptions.values():
                await sub.unsubscribe()
            self._subscriptions.clear()
            
            # Drain and close connection
            if self._nc:
                await self._nc.drain()
                await self._nc.close()
                self._nc = None
                self._js = None
            
            self._connected = False
            logger.info("Disconnected from NATS")
            
        except Exception as e:
            logger.error(f"Error disconnecting from NATS: {e}")
            raise
    
    async def publish(
        self,
        event: Event,
        timeout: Optional[float] = None
    ) -> PublishResult:
        """Publish a single event"""
        try:
            # Prepare message
            message_data = json.dumps(event.data).encode('utf-8')
            
            # Create headers
            headers = {}
            headers['event_id'] = event.id
            headers['timestamp'] = event.timestamp.isoformat()
            for k, v in event.headers.items():
                headers[k] = v
            
            # Publish to JetStream
            subject = self._topic_to_subject(event.topic)
            
            ack = await asyncio.wait_for(
                self._js.publish(
                    subject,
                    message_data,
                    headers=headers,
                    stream=self._get_stream_name(event.topic)
                ),
                timeout=timeout or self.config.timeout_seconds
            )
            
            self._record_publish(True)
            
            return PublishResult(
                success=True,
                message_id=str(ack.seq),
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
        results = []
        futures = []
        
        # Publish all messages
        for event in events:
            try:
                message_data = json.dumps(event.data).encode('utf-8')
                
                headers = {}
                headers['event_id'] = event.id
                headers['timestamp'] = event.timestamp.isoformat()
                for k, v in event.headers.items():
                    headers[k] = v
                
                subject = self._topic_to_subject(event.topic)
                
                future = self._js.publish(
                    subject,
                    message_data,
                    headers=headers,
                    stream=self._get_stream_name(event.topic)
                )
                futures.append((event, future))
                
            except Exception as e:
                results.append(PublishResult(
                    success=False,
                    error=str(e),
                    timestamp=datetime.now()
                ))
                self._record_publish(False)
        
        # Wait for results
        if timeout:
            # Wait with timeout
            for event, future in futures:
                try:
                    ack = await asyncio.wait_for(future, timeout=timeout)
                    results.append(PublishResult(
                        success=True,
                        message_id=str(ack.seq),
                        timestamp=datetime.now()
                    ))
                    self._record_publish(True)
                except asyncio.TimeoutError:
                    results.append(PublishResult(
                        success=False,
                        error="Timeout",
                        timestamp=datetime.now()
                    ))
                    self._record_publish(False)
                except Exception as e:
                    results.append(PublishResult(
                        success=False,
                        error=str(e),
                        timestamp=datetime.now()
                    ))
                    self._record_publish(False)
        else:
            # Wait without timeout
            for event, future in futures:
                try:
                    ack = await future
                    results.append(PublishResult(
                        success=True,
                        message_id=str(ack.seq),
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
        
        # Create durable consumer
        subjects = [self._topic_to_subject(topic) for topic in config.topics]
        
        # JetStream consumer configuration
        consumer_config = {
            "durable_name": f"{config.consumer_group}_{subscription_id[:8]}",
            "deliver_subject": f"deliver.{config.consumer_group}.{subscription_id[:8]}",
            "ack_policy": "explicit",
            "max_deliver": config.max_redeliveries,
            "max_ack_pending": config.max_poll_records,
            "replay_policy": "instant" if config.start_from == "earliest" else "new_only"
        }
        
        # Create pull subscription
        sub = await self._js.pull_subscribe(
            subjects[0] if len(subjects) == 1 else subjects,
            **consumer_config
        )
        
        self._subscriptions[subscription_id] = sub
        
        # Start consumer task
        task = asyncio.create_task(
            self._consume_loop(subscription_id, sub, handler, config)
        )
        self._consumer_tasks[subscription_id] = task
        
        logger.info(f"Created subscription {subscription_id} for subjects: {subjects}")
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from topics"""
        # Cancel consumer task
        if subscription_id in self._consumer_tasks:
            self._consumer_tasks[subscription_id].cancel()
            del self._consumer_tasks[subscription_id]
        
        # Unsubscribe
        if subscription_id in self._subscriptions:
            await self._subscriptions[subscription_id].unsubscribe()
            del self._subscriptions[subscription_id]
        
        logger.info(f"Unsubscribed: {subscription_id}")
    
    async def consume_batch(
        self,
        config: ConsumerConfig,
        max_messages: int = 100,
        timeout: Optional[float] = None
    ) -> List[Event]:
        """Consume a batch of messages"""
        events = []
        
        # Create temporary subscription
        subjects = [self._topic_to_subject(topic) for topic in config.topics]
        
        consumer_config = {
            "durable_name": f"{config.consumer_group}_batch_{uuid.uuid4().hex[:8]}",
            "ack_policy": "explicit",
            "max_deliver": config.max_redeliveries,
            "replay_policy": "instant" if config.start_from == "earliest" else "new_only"
        }
        
        sub = await self._js.pull_subscribe(
            subjects[0] if len(subjects) == 1 else subjects,
            **consumer_config
        )
        
        try:
            # Fetch messages
            messages = await sub.fetch(
                max_messages,
                timeout=timeout or self.config.timeout_seconds
            )
            
            for msg in messages:
                event = self._message_to_event(msg)
                events.append(event)
                
                # Acknowledge if auto-commit
                if config.auto_commit:
                    await msg.ack()
            
            self._record_consume(len(events))
            
        finally:
            await sub.unsubscribe()
        
        return events
    
    async def acknowledge(
        self,
        event: Event,
        success: bool = True
    ) -> None:
        """Acknowledge event processing"""
        # NATS acknowledgment is handled at message level
        # This would be called within consume loop
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
            stream_name = self._get_stream_name(topic)
            subject = self._topic_to_subject(topic)
            
            # Stream configuration
            stream_config = {
                "name": stream_name,
                "subjects": [subject],
                "retention": "limits",
                "max_consumers": -1,
                "max_msgs": config.get('max_messages', -1) if config else -1,
                "max_bytes": config.get('max_bytes', -1) if config else -1,
                "max_age": config.get('max_age_seconds', 0) if config else 0,
                "max_msg_size": config.get('max_msg_size', -1) if config else -1,
                "storage": "file",
                "num_replicas": min(replication_factor, 5),  # NATS max is 5
                "duplicate_window": 120000000000  # 2 minutes in nanoseconds
            }
            
            # Create stream
            await self._js.add_stream(**stream_config)
            
            logger.info(f"Created stream: {stream_name}")
            return True
            
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Stream already exists: {topic}")
                return True
            logger.error(f"Failed to create topic: {e}")
            return False
    
    async def delete_topic(self, topic: str) -> bool:
        """Delete a topic (stream)"""
        try:
            stream_name = self._get_stream_name(topic)
            await self._js.delete_stream(stream_name)
            
            logger.info(f"Deleted stream: {stream_name}")
            return True
            
        except NotFoundError:
            logger.info(f"Stream not found: {stream_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            return False
    
    async def list_topics(self) -> List[str]:
        """List all topics (streams)"""
        try:
            streams = await self._js.streams_info()
            topics = []
            
            for stream in streams:
                # Extract topic from stream name or subjects
                if stream.config.subjects:
                    for subject in stream.config.subjects:
                        topic = self._subject_to_topic(subject)
                        if topic not in topics:
                            topics.append(topic)
            
            return topics
            
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """Get topic (stream) information"""
        try:
            stream_name = self._get_stream_name(topic)
            info = await self._js.stream_info(stream_name)
            
            return {
                "topic": topic,
                "stream_name": stream_name,
                "messages": info.state.messages,
                "bytes": info.state.bytes,
                "first_seq": info.state.first_seq,
                "last_seq": info.state.last_seq,
                "consumer_count": info.state.consumer_count,
                "subjects": info.config.subjects,
                "retention": info.config.retention,
                "max_age": info.config.max_age
            }
            
        except Exception as e:
            logger.error(f"Failed to get topic info: {e}")
            return {}
    
    async def stream(
        self,
        config: ConsumerConfig
    ) -> AsyncIterator[Event]:
        """Stream events as async iterator"""
        # Create subscription
        subjects = [self._topic_to_subject(topic) for topic in config.topics]
        
        consumer_config = {
            "durable_name": f"{config.consumer_group}_stream_{uuid.uuid4().hex[:8]}",
            "ack_policy": "explicit",
            "max_deliver": config.max_redeliveries,
            "replay_policy": "instant" if config.start_from == "earliest" else "new_only"
        }
        
        sub = await self._js.pull_subscribe(
            subjects[0] if len(subjects) == 1 else subjects,
            **consumer_config
        )
        
        try:
            while True:
                try:
                    # Fetch messages
                    messages = await sub.fetch(
                        config.max_poll_records,
                        timeout=1.0
                    )
                    
                    for msg in messages:
                        event = self._message_to_event(msg)
                        
                        if config.auto_commit:
                            await msg.ack()
                        
                        self._record_consume(1)
                        yield event
                        
                except asyncio.TimeoutError:
                    # Normal timeout, continue
                    continue
                except Exception as e:
                    logger.error(f"Error in stream: {e}")
                    self._record_error(str(e))
                    await asyncio.sleep(1)
                    
        finally:
            await sub.unsubscribe()
    
    async def _consume_loop(
        self,
        subscription_id: str,
        sub: Any,
        handler: Callable[[Event], Any],
        config: ConsumerConfig
    ):
        """Consumer loop for subscription"""
        while subscription_id in self._consumer_tasks:
            try:
                # Fetch messages
                messages = await sub.fetch(
                    config.max_poll_records,
                    timeout=1.0
                )
                
                for msg in messages:
                    event = self._message_to_event(msg)
                    
                    try:
                        # Process event
                        result = handler(event)
                        if asyncio.iscoroutine(result):
                            await result
                        
                        # Acknowledge on success
                        await msg.ack()
                        self._record_consume(1)
                        
                    except Exception as e:
                        logger.error(f"Error processing event: {e}")
                        self._record_error(str(e))
                        
                        # Negative acknowledge for redelivery
                        await msg.nak()
                        
                        # Handle dead letter if max redeliveries exceeded
                        if msg.metadata.num_delivered >= config.max_redeliveries:
                            if config.enable_dead_letter and config.dead_letter_topic:
                                event.headers['_original_topic'] = event.topic
                                event.headers['_failure_count'] = str(msg.metadata.num_delivered)
                                event.topic = config.dead_letter_topic
                                await self.publish(event)
                                
            except asyncio.TimeoutError:
                # Normal timeout, continue
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consume loop: {e}")
                self._record_error(str(e))
                await asyncio.sleep(1)
    
    def _message_to_event(self, msg: Any) -> Event:
        """Convert NATS message to Event"""
        try:
            data = json.loads(msg.data.decode('utf-8'))
        except:
            data = {"raw_data": msg.data.decode('utf-8')}
        
        # Extract headers
        headers = {}
        event_id = None
        timestamp_str = None
        
        if msg.headers:
            for k, v in msg.headers.items():
                if k == 'event_id':
                    event_id = v
                elif k == 'timestamp':
                    timestamp_str = v
                else:
                    headers[k] = v
        
        # Parse timestamp
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
            except:
                timestamp = datetime.now()
        else:
            timestamp = datetime.now()
        
        # Extract topic from subject
        topic = self._subject_to_topic(msg.subject)
        
        return Event(
            id=event_id or str(msg.metadata.sequence.stream),
            topic=topic,
            data=data,
            timestamp=timestamp,
            headers=headers,
            key=None,
            partition_key=None
        )
    
    def _topic_to_subject(self, topic: str) -> str:
        """Convert topic to NATS subject"""
        # Replace special characters with dots for NATS subjects
        return topic.replace('/', '.').replace('_', '.')
    
    def _subject_to_topic(self, subject: str) -> str:
        """Convert NATS subject to topic"""
        # This is a simple conversion - could be more sophisticated
        return subject.replace('.', '_')
    
    def _get_stream_name(self, topic: str) -> str:
        """Get stream name for topic"""
        # Use prefix from config or default
        prefix = self.config.metadata.get('stream_prefix', 'EVENTS')
        return f"{prefix}_{topic.upper().replace('-', '_')}"
    
    async def _jwt_cb(self) -> str:
        """JWT callback for authentication"""
        return self.config.credentials.get('jwt', '')
    
    async def _sig_cb(self, nonce: bytes) -> bytes:
        """Signature callback for authentication"""
        import nacl.signing
        import nacl.encoding
        
        seed = self.config.credentials.get('seed')
        if seed:
            signing_key = nacl.signing.SigningKey(
                seed.encode(),
                encoder=nacl.encoding.Base64Encoder
            )
            return signing_key.sign(nonce).signature
        return b''


# Register backend
from .base_backend import EventBackendFactory
EventBackendFactory.register_backend(BackendType.NATS, NATSBackend) 