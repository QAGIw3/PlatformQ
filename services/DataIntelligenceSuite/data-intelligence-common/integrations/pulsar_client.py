"""
Apache Pulsar Client Integration

Provides high-level client for Apache Pulsar operations with Vault/Consul support.
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
from enum import Enum
import json

import pulsar
from pulsar import Schema

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


class SubscriptionType(Enum):
    """Pulsar subscription types"""
    EXCLUSIVE = pulsar.SubscriptionType.Exclusive
    SHARED = pulsar.SubscriptionType.Shared
    FAILOVER = pulsar.SubscriptionType.Failover
    KEY_SHARED = pulsar.SubscriptionType.KeyShared


class CompressionType(Enum):
    """Message compression types"""
    NONE = pulsar.CompressionType.NONE
    LZ4 = pulsar.CompressionType.LZ4
    ZLIB = pulsar.CompressionType.ZLIB
    ZSTD = pulsar.CompressionType.ZSTD
    SNAPPY = pulsar.CompressionType.SNAPPY


@dataclass
class PulsarConfig(ClientConfig):
    """Configuration for Pulsar client with Vault/Consul support"""
    # Pulsar specific settings
    service_url: str = "pulsar://localhost:6650"
    
    # TLS
    use_tls: bool = False
    tls_trust_certs_file_path: Optional[str] = None
    tls_allow_insecure_connection: bool = False
    
    # Connection settings
    operation_timeout_seconds: int = 30
    io_threads: int = 1
    message_listener_threads: int = 1
    concurrent_lookup_requests: int = 50000
    
    # Default producer settings
    default_compression: CompressionType = CompressionType.NONE
    default_batching_enabled: bool = True
    default_batching_max_messages: int = 1000
    default_batching_max_allowed_size_in_bytes: int = 128 * 1024
    default_batching_max_publish_delay_ms: int = 10
    
    # Default consumer settings
    default_subscription_type: SubscriptionType = SubscriptionType.SHARED
    default_receiver_queue_size: int = 1000
    default_max_total_receiver_queue_size_across_partitions: int = 50000
    
    # Vault specific
    vault_auth_mount: str = "auth/pulsar"
    vault_auth_role: str = "pulsar-client"
    vault_pki_mount: str = "pki"
    vault_pki_role: str = "pulsar-client"
    
    # Message encryption
    enable_message_encryption: bool = False
    encryption_key_name: str = "pulsar-messages"
    
    def __post_init__(self):
        # Set service name for base client
        if not hasattr(self, 'service_name'):
            self.service_name = "pulsar"


@dataclass
class ProducerConfig:
    """Configuration for Pulsar producer"""
    topic: str
    producer_name: Optional[str] = None
    
    # Message settings
    schema: Optional[Schema] = None
    compression_type: Optional[CompressionType] = None
    max_pending_messages: int = 1000
    block_if_queue_full: bool = True
    
    # Batching
    batching_enabled: Optional[bool] = None
    batching_max_messages: Optional[int] = None
    batching_max_allowed_size_in_bytes: Optional[int] = None
    batching_max_publish_delay_ms: Optional[int] = None
    
    # Routing
    message_routing_mode: pulsar.PartitionsRoutingMode = pulsar.PartitionsRoutingMode.RoundRobinDistribution
    hashing_scheme: pulsar.HashingScheme = pulsar.HashingScheme.JavaStringHash
    
    # Encryption
    encryption_key: Optional[str] = None
    crypto_key_reader: Optional[Any] = None
    
    # Access control
    required_role: Optional[str] = None


@dataclass
class ConsumerConfig:
    """Configuration for Pulsar consumer"""
    topics: Union[str, List[str]]
    subscription_name: str
    consumer_name: Optional[str] = None
    
    # Subscription settings
    subscription_type: Optional[SubscriptionType] = None
    schema: Optional[Schema] = None
    
    # Queue settings
    receiver_queue_size: Optional[int] = None
    max_total_receiver_queue_size_across_partitions: Optional[int] = None
    
    # Acknowledgment
    negative_ack_redelivery_delay_ms: int = 60000
    ack_timeout_ms: int = 0
    
    # Dead letter policy
    dead_letter_policy: Optional[Dict[str, Any]] = None
    
    # Initial position
    initial_position: pulsar.InitialPosition = pulsar.InitialPosition.Latest
    
    # Access control
    required_role: Optional[str] = None


class PulsarClient(BaseServiceClient):
    """
    High-level client for Apache Pulsar operations with Vault/Consul support.
    
    Features:
    - Dynamic authentication via Vault
    - Service discovery via Consul
    - Message encryption/decryption
    - mTLS support
    - Producer/Consumer management
    - Schema support
    - Async operations
    - Access control
    """
    
    def __init__(
        self,
        config: PulsarConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        super().__init__(config, vault_client, consul_client)
        self.pulsar_config = config
        self._client: Optional[pulsar.Client] = None
        self._producers: Dict[str, pulsar.Producer] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        self._auth_token: Optional[str] = None
        self._tls_certs: Optional[Dict[str, str]] = None
        
    async def connect(self):
        """Connect to Pulsar cluster with dynamic authentication"""
        # Initialize base client
        await super().connect()
        
        try:
            # Get Pulsar service URL from discovery
            service_url = await self._get_pulsar_url()
            
            # Build client configuration
            conf = {
                'service_url': service_url,
                'operation_timeout_seconds': self.pulsar_config.operation_timeout_seconds,
                'io_threads': self.pulsar_config.io_threads,
                'message_listener_threads': self.pulsar_config.message_listener_threads,
                'concurrent_lookup_requests': self.pulsar_config.concurrent_lookup_requests,
            }
            
            # Add authentication from Vault
            if self.vault_client and self.config.use_vault_credentials:
                auth = await self._get_pulsar_auth()
                if auth:
                    conf['authentication'] = auth
                    
            # Add TLS settings
            if self.pulsar_config.use_tls:
                tls_config = await self._get_tls_config()
                conf.update(tls_config)
                
            # Create client
            self._client = pulsar.Client(**conf)
            
            logger.info(f"Connected to Pulsar cluster: {service_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar: {e}")
            await self.close()
            raise
            
    async def _get_pulsar_url(self) -> str:
        """Get Pulsar service URL from Consul or config"""
        if self.config.use_service_discovery and self._service_instances:
            # Use first healthy instance
            instance = self._service_instances[0]
            
            # Check for TLS port in metadata
            if self.pulsar_config.use_tls:
                port = instance.get('meta', {}).get('tls_port', 6651)
                return f"pulsar+ssl://{instance['address']}:{port}"
            else:
                port = instance.get('port', 6650)
                return f"pulsar://{instance['address']}:{port}"
        else:
            return self.pulsar_config.service_url
            
    async def _get_pulsar_auth(self) -> Optional[pulsar.Authentication]:
        """Get Pulsar authentication from Vault"""
        try:
            # Get auth token from Vault
            auth_data = await self.vault_client.read_secret(
                f"{self.pulsar_config.vault_auth_mount}/creds/{self.pulsar_config.vault_auth_role}"
            )
            
            if auth_data and 'token' in auth_data:
                self._auth_token = auth_data['token']
                return pulsar.AuthenticationToken(self._auth_token)
            elif auth_data and 'oauth2' in auth_data:
                # OAuth2 authentication
                return pulsar.AuthenticationOauth2(auth_data['oauth2'])
                
        except Exception as e:
            logger.error(f"Failed to get Pulsar auth from Vault: {e}")
            
        return None
        
    async def _get_tls_config(self) -> Dict[str, Any]:
        """Get TLS configuration from Vault PKI"""
        config = {}
        
        if self.pulsar_config.tls_trust_certs_file_path:
            config['tls_trust_certs_file_path'] = self.pulsar_config.tls_trust_certs_file_path
            
        config['tls_allow_insecure_connection'] = self.pulsar_config.tls_allow_insecure_connection
        
        # Get client certificates from Vault if using mTLS
        if self.config.use_mtls and self.vault_client:
            try:
                # Issue certificate from Vault PKI
                cert_data = await self.vault_client.issue_certificate(
                    self.pulsar_config.vault_pki_role,
                    common_name=f"{self.config.service_name}.pulsar.local",
                    ttl="24h"
                )
                
                if cert_data:
                    # Store certificates temporarily
                    import tempfile
                    import os
                    
                    cert_dir = tempfile.mkdtemp()
                    cert_file = os.path.join(cert_dir, "client.crt")
                    key_file = os.path.join(cert_dir, "client.key")
                    
                    with open(cert_file, 'w') as f:
                        f.write(cert_data['certificate'])
                    with open(key_file, 'w') as f:
                        f.write(cert_data['private_key'])
                        
                    config['tls_certificate_file'] = cert_file
                    config['tls_private_key_file'] = key_file
                    
                    self._tls_certs = {
                        'cert_file': cert_file,
                        'key_file': key_file,
                        'cert_dir': cert_dir
                    }
                    
            except Exception as e:
                logger.error(f"Failed to get TLS certs from Vault: {e}")
                
        return config
        
    async def close(self):
        """Close Pulsar connection"""
        # Close all producers
        for producer in self._producers.values():
            producer.close()
        self._producers.clear()
        
        # Close all consumers
        for consumer in self._consumers.values():
            consumer.close()
        self._consumers.clear()
        
        # Close client
        if self._client:
            self._client.close()
            self._client = None
            
        # Clean up TLS certificates
        if self._tls_certs:
            import shutil
            shutil.rmtree(self._tls_certs['cert_dir'], ignore_errors=True)
            
        # Close base client
        await super().close()
        
        logger.info("Disconnected from Pulsar")
        
    def create_producer(self, config: ProducerConfig) -> pulsar.Producer:
        """Create a producer with configuration"""
        if not self._client:
            raise RuntimeError("Not connected to Pulsar")
            
        # Check access control
        if config.required_role and self._user_context:
            user_roles = self._user_context.get('roles', [])
            if config.required_role not in user_roles:
                raise PermissionError(f"Required role '{config.required_role}' not found")
                
        # Build producer configuration
        producer_config = {
            'producer_name': config.producer_name,
            'max_pending_messages': config.max_pending_messages,
            'block_if_queue_full': config.block_if_queue_full,
            'message_routing_mode': config.message_routing_mode,
            'hashing_scheme': config.hashing_scheme,
        }
        
        # Add schema if provided
        if config.schema:
            producer_config['schema'] = config.schema
            
        # Add compression
        compression = config.compression_type or self.pulsar_config.default_compression
        producer_config['compression_type'] = compression.value
        
        # Add batching settings
        if config.batching_enabled is not None:
            producer_config['batching_enabled'] = config.batching_enabled
        else:
            producer_config['batching_enabled'] = self.pulsar_config.default_batching_enabled
            
        if config.batching_max_messages:
            producer_config['batching_max_messages'] = config.batching_max_messages
        elif self.pulsar_config.default_batching_max_messages:
            producer_config['batching_max_messages'] = self.pulsar_config.default_batching_max_messages
            
        if config.batching_max_allowed_size_in_bytes:
            producer_config['batching_max_allowed_size_in_bytes'] = config.batching_max_allowed_size_in_bytes
        elif self.pulsar_config.default_batching_max_allowed_size_in_bytes:
            producer_config['batching_max_allowed_size_in_bytes'] = self.pulsar_config.default_batching_max_allowed_size_in_bytes
            
        if config.batching_max_publish_delay_ms:
            producer_config['batching_max_publish_delay_ms'] = config.batching_max_publish_delay_ms
        elif self.pulsar_config.default_batching_max_publish_delay_ms:
            producer_config['batching_max_publish_delay_ms'] = self.pulsar_config.default_batching_max_publish_delay_ms
            
        # Add encryption if configured
        if config.encryption_key and self.vault_client:
            # This would need actual Pulsar encryption implementation
            pass
            
        # Create producer
        producer = self._client.create_producer(config.topic, **producer_config)
        
        # Store producer
        key = config.producer_name or config.topic
        self._producers[key] = producer
        
        logger.info(f"Created producer for topic: {config.topic}")
        
        return producer
        
    def create_consumer(self, config: ConsumerConfig) -> pulsar.Consumer:
        """Create a consumer with configuration"""
        if not self._client:
            raise RuntimeError("Not connected to Pulsar")
            
        # Check access control
        if config.required_role and self._user_context:
            user_roles = self._user_context.get('roles', [])
            if config.required_role not in user_roles:
                raise PermissionError(f"Required role '{config.required_role}' not found")
                
        # Build consumer configuration
        consumer_config = {
            'consumer_name': config.consumer_name,
            'initial_position': config.initial_position,
            'negative_ack_redelivery_delay_ms': config.negative_ack_redelivery_delay_ms,
            'ack_timeout_ms': config.ack_timeout_ms,
        }
        
        # Add subscription type
        subscription_type = config.subscription_type or self.pulsar_config.default_subscription_type
        consumer_config['subscription_type'] = subscription_type.value
        
        # Add schema if provided
        if config.schema:
            consumer_config['schema'] = config.schema
            
        # Add queue settings
        if config.receiver_queue_size:
            consumer_config['receiver_queue_size'] = config.receiver_queue_size
        elif self.pulsar_config.default_receiver_queue_size:
            consumer_config['receiver_queue_size'] = self.pulsar_config.default_receiver_queue_size
            
        if config.max_total_receiver_queue_size_across_partitions:
            consumer_config['max_total_receiver_queue_size_across_partitions'] = config.max_total_receiver_queue_size_across_partitions
        elif self.pulsar_config.default_max_total_receiver_queue_size_across_partitions:
            consumer_config['max_total_receiver_queue_size_across_partitions'] = self.pulsar_config.default_max_total_receiver_queue_size_across_partitions
            
        # Add dead letter policy
        if config.dead_letter_policy:
            consumer_config['dead_letter_policy'] = config.dead_letter_policy
            
        # Create consumer
        topics = config.topics if isinstance(config.topics, list) else [config.topics]
        consumer = self._client.subscribe(
            topics,
            config.subscription_name,
            **consumer_config
        )
        
        # Store consumer
        key = f"{config.subscription_name}:{','.join(topics)}"
        self._consumers[key] = consumer
        
        logger.info(f"Created consumer for topics: {topics}")
        
        return consumer
        
    async def send_async(
        self,
        producer_key: str,
        message: Any,
        properties: Optional[Dict[str, str]] = None,
        partition_key: Optional[str] = None,
        ordering_key: Optional[str] = None,
        replication_clusters: Optional[List[str]] = None,
        event_timestamp: Optional[int] = None,
        deliver_at: Optional[int] = None,
        deliver_after: Optional[timedelta] = None
    ) -> pulsar.MessageId:
        """Send message asynchronously with optional encryption"""
        producer = self._producers.get(producer_key)
        if not producer:
            raise ValueError(f"Producer '{producer_key}' not found")
            
        # Encrypt message if configured
        if self.pulsar_config.enable_message_encryption and self.vault_client:
            message = await self._encrypt_message(message)
            
        # Build message
        msg_builder = producer.create_message(message)
        
        if properties:
            for key, value in properties.items():
                msg_builder.property(key, value)
                
        if partition_key:
            msg_builder.partition_key(partition_key)
            
        if ordering_key:
            msg_builder.ordering_key(ordering_key)
            
        if replication_clusters:
            msg_builder.replication_clusters(replication_clusters)
            
        if event_timestamp:
            msg_builder.event_timestamp(event_timestamp)
            
        if deliver_at:
            msg_builder.deliver_at(deliver_at)
        elif deliver_after:
            msg_builder.deliver_after(deliver_after)
            
        # Send asynchronously
        future = producer.send_async(msg_builder.build())
        
        # Wait for result
        return await asyncio.get_event_loop().run_in_executor(
            None,
            future.result
        )
        
    def send(
        self,
        producer_key: str,
        message: Any,
        **kwargs
    ) -> pulsar.MessageId:
        """Send message synchronously"""
        producer = self._producers.get(producer_key)
        if not producer:
            raise ValueError(f"Producer '{producer_key}' not found")
            
        # For sync send, encryption would need to be handled differently
        return producer.send(message, **kwargs)
        
    async def receive_async(
        self,
        consumer_key: str,
        timeout_millis: Optional[int] = None
    ) -> pulsar.Message:
        """Receive message asynchronously with optional decryption"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        # Receive message
        msg = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: consumer.receive(timeout_millis) if timeout_millis else consumer.receive()
        )
        
        # Decrypt if needed
        if self.pulsar_config.enable_message_encryption and self.vault_client:
            await self._decrypt_message(msg)
            
        return msg
        
    def receive(
        self,
        consumer_key: str,
        timeout_millis: Optional[int] = None
    ) -> pulsar.Message:
        """Receive message synchronously"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        if timeout_millis:
            return consumer.receive(timeout_millis)
        else:
            return consumer.receive()
            
    def acknowledge(
        self,
        consumer_key: str,
        message: Union[pulsar.Message, pulsar.MessageId]
    ):
        """Acknowledge message"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        consumer.acknowledge(message)
        
    def acknowledge_cumulative(
        self,
        consumer_key: str,
        message: Union[pulsar.Message, pulsar.MessageId]
    ):
        """Acknowledge messages cumulatively"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        consumer.acknowledge_cumulative(message)
        
    def negative_acknowledge(
        self,
        consumer_key: str,
        message: Union[pulsar.Message, pulsar.MessageId]
    ):
        """Negative acknowledge message"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        consumer.negative_acknowledge(message)
        
    async def batch_receive_async(
        self,
        consumer_key: str,
        max_messages: int = 100,
        timeout_millis: int = 1000
    ) -> List[pulsar.Message]:
        """Receive batch of messages asynchronously"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        messages = []
        start_time = datetime.utcnow()
        
        while len(messages) < max_messages:
            try:
                remaining_timeout = timeout_millis - int((datetime.utcnow() - start_time).total_seconds() * 1000)
                if remaining_timeout <= 0:
                    break
                    
                msg = await self.receive_async(consumer_key, remaining_timeout)
                messages.append(msg)
                
            except Exception:
                break
                
        return messages
        
    def seek(
        self,
        consumer_key: str,
        message_id: Union[pulsar.MessageId, int]
    ):
        """Seek to specific message or timestamp"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        if isinstance(message_id, int):
            # Seek by timestamp
            consumer.seek(message_id)
        else:
            # Seek by message ID
            consumer.seek(message_id)
            
    def pause(self, consumer_key: str):
        """Pause message consumption"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        consumer.pause_message_listener()
        
    def resume(self, consumer_key: str):
        """Resume message consumption"""
        consumer = self._consumers.get(consumer_key)
        if not consumer:
            raise ValueError(f"Consumer '{consumer_key}' not found")
            
        consumer.resume_message_listener()
        
    def get_producer(self, key: str) -> Optional[pulsar.Producer]:
        """Get producer by key"""
        return self._producers.get(key)
        
    def get_consumer(self, key: str) -> Optional[pulsar.Consumer]:
        """Get consumer by key"""
        return self._consumers.get(key)
        
    def close_producer(self, key: str):
        """Close and remove producer"""
        if key in self._producers:
            self._producers[key].close()
            del self._producers[key]
            
    def close_consumer(self, key: str):
        """Close and remove consumer"""
        if key in self._consumers:
            self._consumers[key].close()
            del self._consumers[key]
            
    async def _encrypt_message(self, message: Any) -> bytes:
        """Encrypt message using Vault Transit"""
        if isinstance(message, bytes):
            data = message
        elif isinstance(message, str):
            data = message.encode('utf-8')
        else:
            data = json.dumps(message).encode('utf-8')
            
        # Encrypt using Vault Transit
        encrypted = await self.vault_client.transit_encrypt(
            self.pulsar_config.encryption_key_name,
            data.decode('utf-8')
        )
        
        return encrypted['ciphertext'].encode('utf-8')
        
    async def _decrypt_message(self, msg: pulsar.Message):
        """Decrypt message using Vault Transit"""
        try:
            ciphertext = msg.data().decode('utf-8')
            
            # Decrypt using Vault Transit
            decrypted = await self.vault_client.transit_decrypt(
                self.pulsar_config.encryption_key_name,
                ciphertext
            )
            
            # Update message data (this would need actual implementation)
            # Pulsar messages are immutable, so we'd need a wrapper
            
        except Exception as e:
            logger.error(f"Failed to decrypt message: {e}")
            
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Pulsar-specific configuration from Consul"""
        if not self.consul_client:
            return {}
            
        try:
            # Get Pulsar-specific config
            config = await self.consul_client.get_config(
                f"data-intelligence/pulsar/config"
            )
            
            return config or {}
            
        except Exception as e:
            logger.error(f"Failed to get Pulsar config from Consul: {e}")
            return {}
            
    def set_user_context(self, context: Dict[str, Any]):
        """Set user context for access control"""
        self._user_context = context
        
    @asynccontextmanager
    async def transaction(self):
        """Transaction context manager for Pulsar transactions"""
        # Pulsar transaction support would be implemented here
        yield 