"""Service configuration for DataIntelligenceSuite services."""

import os
from typing import List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import timedelta


@dataclass
class ServiceConfig:
    """Configuration for enhanced base service"""
    name: str
    version: str = "1.0.0"
    
    # Vault & Consul
    vault_addr: str = field(default_factory=lambda: os.getenv("VAULT_ADDR", "http://localhost:8200"))
    vault_token: Optional[str] = field(default_factory=lambda: os.getenv("VAULT_TOKEN"))
    consul_addr: str = field(default_factory=lambda: os.getenv("CONSUL_ADDR", "http://localhost:8500"))
    
    # Ignite
    ignite_nodes: List[Tuple[str, int]] = field(default_factory=lambda: [("localhost", 10800)])
    enable_caching: bool = True
    
    # Pulsar
    pulsar_url: str = field(default_factory=lambda: os.getenv("PULSAR_URL", "pulsar://localhost:6650"))
    enable_events: bool = True
    
    # Rate limiting
    enable_rate_limiting: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: timedelta = field(default_factory=lambda: timedelta(minutes=1))
    
    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_failures: int = 5
    circuit_breaker_timeout: int = 60
    circuit_breaker_expected_exception: type = Exception
    
    # Health check
    health_check_interval: int = 30
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    
    # CORS
    cors_origins: List[str] = field(default_factory=lambda: ["*"])


@dataclass  
class CacheConfig:
    """Configuration for cache settings."""
    
    # Cache names and their configurations
    session_cache: str = "session_cache"
    session_ttl: timedelta = field(default_factory=lambda: timedelta(hours=1))
    
    configuration_cache: str = "configuration_cache"
    configuration_ttl: timedelta = field(default_factory=lambda: timedelta(hours=24))
    
    query_results_cache: str = "query_results_cache"
    query_results_ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    ml_models_cache: str = "ml_models_cache"
    ml_models_ttl: timedelta = field(default_factory=lambda: timedelta(hours=12))
    
    # Cache behavior
    enable_distributed_cache: bool = True
    cache_key_prefix: Optional[str] = None
    
    def __post_init__(self):
        if self.cache_key_prefix is None:
            self.cache_key_prefix = os.getenv("SERVICE_NAME", "data_intelligence")


@dataclass
class EventConfig:
    """Configuration for event processing."""
    
    # Topics
    data_events_topic: str = "data-intelligence-events"
    model_events_topic: str = "ml-model-events"
    system_events_topic: str = "system-events"
    
    # Consumer settings
    subscription_name: Optional[str] = None
    consumer_type: str = "shared"  # shared, exclusive, failover
    
    # Producer settings
    producer_batching: bool = True
    producer_compression: str = "lz4"
    
    # Processing
    max_concurrent_messages: int = 100
    message_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    def __post_init__(self):
        if self.subscription_name is None:
            self.subscription_name = f"{os.getenv('SERVICE_NAME', 'data-intelligence')}-subscription" 