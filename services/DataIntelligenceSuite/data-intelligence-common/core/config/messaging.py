"""
Messaging Configuration Classes

Provides configurations for messaging systems using unified approach.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import timedelta

from .unified import MessagingConnectionConfig
from .base import MessagingConfig


@dataclass
class PulsarConfig(MessagingConnectionConfig):
    """Apache Pulsar configuration"""
    # Pulsar specific
    service_url: str = ""
    admin_url: str = ""
    
    # Authentication
    auth_plugin: Optional[str] = None
    auth_params: Optional[str] = None
    tls_trust_certs_file_path: Optional[str] = None
    tls_allow_insecure_connection: bool = False
    
    # Producer settings
    producer_name: Optional[str] = None
    send_timeout_ms: int = 30000
    block_if_queue_full: bool = True
    max_pending_messages: int = 1000
    
    # Consumer settings
    subscription_initial_position: str = "Latest"
    ack_timeout_ms: int = 10000
    negative_ack_redelivery_delay_ms: int = 60000
    
    def __post_init__(self):
        if not self.port:
            self.port = 6650
        if not self.service_url:
            protocol = "pulsar+ssl" if self.use_ssl else "pulsar"
            self.service_url = f"{protocol}://{self.host}:{self.port}"
        if not self.admin_url:
            protocol = "https" if self.use_ssl else "http"
            admin_port = 443 if self.use_ssl else 8080
            self.admin_url = f"{protocol}://{self.host}:{admin_port}"
        super().__post_init__()


@dataclass
class EventBusConfig(MessagingConnectionConfig):
    """Event bus configuration (can use different backends)"""
    # Backend selection
    backend: str = "pulsar"  # pulsar, kafka, nats, ignite
    
    # Event bus specific
    default_topic: str = "events"
    enable_schema_validation: bool = True
    enable_dead_letter_queue: bool = True
    dead_letter_topic: str = "dead-letter"
    
    # Processing
    enable_deduplication: bool = True
    deduplication_window_minutes: int = 5
    enable_ordering: bool = False
    ordering_key_field: str = "partition_key"
    
    # Monitoring
    enable_event_metrics: bool = True
    metrics_topic: str = "event-metrics"
    
    def __post_init__(self):
        # Set default ports based on backend
        if not self.port:
            port_map = {
                "pulsar": 6650,
                "kafka": 9092,
                "nats": 4222,
                "ignite": 10800
            }
            self.port = port_map.get(self.backend, 6650)
        super().__post_init__()


@dataclass
class StreamingConfig(MessagingConnectionConfig):
    """Streaming configuration for real-time processing"""
    # Streaming engine
    engine: str = "flink"  # flink, spark, pulsar-functions
    
    # Checkpointing
    enable_checkpointing: bool = True
    checkpoint_interval_ms: int = 60000
    min_pause_between_checkpoints_ms: int = 5000
    checkpoint_timeout_ms: int = 600000
    
    # State management
    state_backend: str = "rocksdb"
    state_ttl_hours: int = 24
    enable_incremental_checkpointing: bool = True
    
    # Watermarks
    enable_watermarks: bool = True
    watermark_interval_ms: int = 200
    max_out_of_orderness_ms: int = 10000
    
    # Resources
    parallelism: int = 4
    task_slots: int = 4
    job_manager_memory_mb: int = 1024
    task_manager_memory_mb: int = 2048


@dataclass
class KafkaConfig(MessagingConnectionConfig):
    """Apache Kafka configuration"""
    # Kafka specific
    bootstrap_servers: List[str] = field(default_factory=list)
    
    # Producer settings
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 10
    buffer_memory: int = 33554432
    
    # Consumer settings
    group_id: str = ""
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    
    # Security
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    def __post_init__(self):
        if not self.port:
            self.port = 9092
        if not self.bootstrap_servers and self.host:
            self.bootstrap_servers = [f"{self.host}:{self.port}"]
        super().__post_init__()


@dataclass
class NATSConfig(MessagingConnectionConfig):
    """NATS configuration"""
    # NATS specific
    servers: List[str] = field(default_factory=list)
    
    # Connection
    name: Optional[str] = None
    pedantic: bool = False
    allow_reconnect: bool = True
    max_reconnect_attempts: int = 60
    reconnect_time_wait_seconds: int = 2
    
    # JetStream
    enable_jetstream: bool = True
    jetstream_prefix: str = "$JS.API"
    
    # Security
    user_credentials: Optional[str] = None
    nkey_seed: Optional[str] = None
    
    def __post_init__(self):
        if not self.port:
            self.port = 4222
        if not self.servers and self.host:
            protocol = "tls" if self.use_ssl else "nats"
            self.servers = [f"{protocol}://{self.host}:{self.port}"]
        super().__post_init__()


# Legacy support
@dataclass
class LegacyPulsarConfig(MessagingConfig):
    """Legacy Pulsar config for backward compatibility"""
    service_url: str = "pulsar://localhost:6650"
    
    def to_unified(self) -> PulsarConfig:
        """Convert to unified config"""
        # Parse service URL
        from urllib.parse import urlparse
        parsed = urlparse(self.service_url)
        
        return PulsarConfig(
            host=parsed.hostname or "localhost",
            port=parsed.port or 6650,
            use_ssl="ssl" in parsed.scheme
        )


# Re-export
__all__ = [
    'PulsarConfig',
    'EventBusConfig',
    'StreamingConfig',
    'KafkaConfig',
    'NATSConfig',
    'LegacyPulsarConfig'
] 