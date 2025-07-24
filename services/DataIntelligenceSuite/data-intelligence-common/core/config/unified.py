"""
Unified configuration system using composition.

Reduces duplication by providing reusable configuration components.
"""

from typing import Dict, Any, Optional, List, Union, Type
from dataclasses import dataclass, field, fields
from datetime import timedelta
from enum import Enum
import os
import json
import yaml
from abc import ABC, abstractmethod

from .base import BaseConfig, Environment, LogLevel
from ..patterns.resilience import RetryConfig, CircuitBreakerConfig


@dataclass
class ConnectionConfig:
    """Reusable connection configuration"""
    host: str = "localhost"
    port: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    
    # Connection pooling
    min_connections: int = 1
    max_connections: int = 10
    connection_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    idle_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=10))
    
    # SSL/TLS
    use_ssl: bool = True
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    ssl_ca_path: Optional[str] = None
    
    # Additional options
    options: Dict[str, Any] = field(default_factory=dict)
    
    def to_url(self, protocol: str = "tcp") -> str:
        """Convert to connection URL"""
        auth = ""
        if self.username:
            auth = f"{self.username}"
            if self.password:
                auth += f":{self.password}"
            auth += "@"
            
        url = f"{protocol}://{auth}{self.host}:{self.port}"
        if self.database:
            url += f"/{self.database}"
            
        return url


@dataclass
class RetryableConfig:
    """Mixin for retry configuration"""
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker_config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    
    # Timeout settings
    connect_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    read_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    write_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))


@dataclass
class CacheableConfig:
    """Mixin for cache configuration"""
    enable_cache: bool = True
    cache_ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    cache_size: int = 1000
    cache_eviction_policy: str = "lru"
    
    # Cache warming
    enable_cache_warming: bool = False
    cache_warm_keys: List[str] = field(default_factory=list)
    
    # Cache invalidation
    cache_invalidation_patterns: List[str] = field(default_factory=list)


@dataclass
class ObservableConfig:
    """Mixin for observability configuration"""
    # Metrics
    enable_metrics: bool = True
    metrics_prefix: str = ""
    custom_metrics: Dict[str, str] = field(default_factory=dict)
    
    # Tracing
    enable_tracing: bool = True
    trace_sample_rate: float = 0.1
    trace_propagation_format: str = "w3c"
    
    # Logging
    enable_structured_logging: bool = True
    log_level: LogLevel = LogLevel.INFO
    log_sampling_rate: float = 1.0
    sensitive_fields: List[str] = field(default_factory=list)


@dataclass
class SecurableConfig:
    """Mixin for security configuration"""
    # Authentication
    enable_auth: bool = True
    auth_type: str = "jwt"
    auth_header: str = "Authorization"
    
    # Authorization
    enable_rbac: bool = True
    default_role: str = "viewer"
    role_mappings: Dict[str, List[str]] = field(default_factory=dict)
    
    # Encryption
    enable_encryption: bool = True
    encryption_algorithm: str = "AES-256-GCM"
    key_rotation_interval: timedelta = field(default_factory=lambda: timedelta(days=90))
    
    # Security headers
    security_headers: Dict[str, str] = field(default_factory=lambda: {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block"
    })


@dataclass
class ScalableConfig:
    """Mixin for scalability configuration"""
    # Auto-scaling
    enable_autoscaling: bool = True
    min_instances: int = 1
    max_instances: int = 10
    target_cpu_percent: float = 70.0
    target_memory_percent: float = 80.0
    
    # Rate limiting
    enable_rate_limiting: bool = True
    rate_limit_requests: int = 1000
    rate_limit_window: timedelta = field(default_factory=lambda: timedelta(minutes=1))
    
    # Resource limits
    max_memory_mb: int = 1024
    max_cpu_cores: float = 2.0
    max_disk_gb: int = 10


@dataclass
class DatabaseConnectionConfig(ConnectionConfig, RetryableConfig, CacheableConfig):
    """Unified database connection configuration"""
    # Database specific
    connection_pool_size: int = 10
    statement_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    lock_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    
    # Query optimization
    enable_query_cache: bool = True
    enable_prepared_statements: bool = True
    batch_size: int = 1000


@dataclass
class MessagingConnectionConfig(ConnectionConfig, RetryableConfig):
    """Unified messaging connection configuration"""
    # Messaging specific
    topic_prefix: str = ""
    consumer_group: str = ""
    enable_auto_commit: bool = True
    max_poll_records: int = 500
    
    # Delivery guarantees
    delivery_mode: str = "at_least_once"
    enable_idempotence: bool = True
    
    # Compression
    compression_type: str = "snappy"
    batch_size: int = 16384


@dataclass
class ServiceConnectionConfig(ConnectionConfig, RetryableConfig, ObservableConfig):
    """Unified service-to-service connection configuration"""
    # Service discovery
    enable_service_discovery: bool = True
    service_name: str = ""
    service_version: str = "v1"
    
    # Load balancing
    load_balancer_type: str = "round_robin"
    health_check_path: str = "/health"
    health_check_interval: timedelta = field(default_factory=lambda: timedelta(seconds=10))


@dataclass
class UnifiedServiceConfig(BaseConfig, ObservableConfig, SecurableConfig, ScalableConfig):
    """
    Unified service configuration combining all common patterns.
    
    Use this as a base for service-specific configurations.
    """
    # Service dependencies
    database_config: Optional[DatabaseConnectionConfig] = None
    cache_config: Optional[DatabaseConnectionConfig] = None
    messaging_config: Optional[MessagingConnectionConfig] = None
    
    # External service connections
    external_services: Dict[str, ServiceConnectionConfig] = field(default_factory=dict)
    
    # Vault/Consul integration
    vault_enabled: bool = True
    vault_path: str = ""
    consul_enabled: bool = True
    consul_service_name: str = ""
    
    def add_database(self, name: str, config: DatabaseConnectionConfig):
        """Add database configuration"""
        if not hasattr(self, 'databases'):
            self.databases = {}
        self.databases[name] = config
        
    def add_messaging(self, name: str, config: MessagingConnectionConfig):
        """Add messaging configuration"""
        if not hasattr(self, 'messaging_systems'):
            self.messaging_systems = {}
        self.messaging_systems[name] = config
        
    def add_external_service(self, name: str, config: ServiceConnectionConfig):
        """Add external service configuration"""
        self.external_services[name] = config
        
    def merge_with(self, other: 'UnifiedServiceConfig'):
        """Merge with another configuration"""
        for field in fields(self):
            other_value = getattr(other, field.name, None)
            if other_value is not None:
                if isinstance(other_value, dict):
                    current_value = getattr(self, field.name, {})
                    if isinstance(current_value, dict):
                        current_value.update(other_value)
                else:
                    setattr(self, field.name, other_value)


class ConfigBuilder:
    """Builder for creating configurations with common patterns"""
    
    def __init__(self, config_class: Type[BaseConfig]):
        self.config_class = config_class
        self.config_dict = {}
        
    def with_connection(self, **kwargs) -> 'ConfigBuilder':
        """Add connection configuration"""
        self.config_dict.update(kwargs)
        return self
        
    def with_retry(self, max_retries: int = 3, backoff_factor: float = 2.0) -> 'ConfigBuilder':
        """Add retry configuration"""
        self.config_dict['retry_config'] = RetryConfig(
            max_retries=max_retries,
            backoff_factor=backoff_factor
        )
        return self
        
    def with_circuit_breaker(self, failure_threshold: int = 5, recovery_timeout: int = 60) -> 'ConfigBuilder':
        """Add circuit breaker configuration"""
        self.config_dict['circuit_breaker_config'] = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=timedelta(seconds=recovery_timeout)
        )
        return self
        
    def with_caching(self, ttl_minutes: int = 5, size: int = 1000) -> 'ConfigBuilder':
        """Add caching configuration"""
        self.config_dict.update({
            'enable_cache': True,
            'cache_ttl': timedelta(minutes=ttl_minutes),
            'cache_size': size
        })
        return self
        
    def with_security(self, auth_type: str = "jwt", enable_rbac: bool = True) -> 'ConfigBuilder':
        """Add security configuration"""
        self.config_dict.update({
            'enable_auth': True,
            'auth_type': auth_type,
            'enable_rbac': enable_rbac
        })
        return self
        
    def build(self) -> BaseConfig:
        """Build the configuration"""
        return self.config_class(**self.config_dict)


def load_config_from_env(config_class: Type[BaseConfig], prefix: str = "") -> BaseConfig:
    """Load configuration from environment variables"""
    config_dict = {}
    
    for field in fields(config_class):
        env_var = f"{prefix}{field.name}".upper()
        value = os.getenv(env_var)
        
        if value is not None:
            # Type conversion
            if field.type == int:
                value = int(value)
            elif field.type == float:
                value = float(value)
            elif field.type == bool:
                value = value.lower() in ('true', '1', 'yes')
            elif field.type == timedelta:
                # Assume seconds
                value = timedelta(seconds=int(value))
                
            config_dict[field.name] = value
            
    return config_class(**config_dict)


def load_config_from_file(config_class: Type[BaseConfig], file_path: str) -> BaseConfig:
    """Load configuration from YAML or JSON file"""
    with open(file_path, 'r') as f:
        if file_path.endswith('.yaml') or file_path.endswith('.yml'):
            data = yaml.safe_load(f)
        else:
            data = json.load(f)
            
    return config_class(**data)


# Example usage configurations
@dataclass
class AnalyticsServiceConfig(UnifiedServiceConfig):
    """Example: Analytics service configuration"""
    # Analytics specific
    enable_real_time: bool = True
    batch_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    retention_days: int = 90
    
    def __post_init__(self):
        super().__post_init__()
        
        # Set up default connections
        if not self.database_config:
            self.database_config = DatabaseConnectionConfig(
                host="ignite.analytics.local",
                port=10800,
                database="analytics"
            )
            
        if not self.messaging_config:
            self.messaging_config = MessagingConnectionConfig(
                host="pulsar.messaging.local",
                port=6650,
                topic_prefix="analytics"
            )


@dataclass
class MLPlatformServiceConfig(UnifiedServiceConfig):
    """Example: ML Platform service configuration"""
    # ML specific
    model_registry_url: str = ""
    feature_store_url: str = ""
    experiment_tracking_url: str = ""
    
    # Training
    default_gpu_count: int = 0
    max_training_hours: int = 24
    checkpoint_interval: timedelta = field(default_factory=lambda: timedelta(hours=1))


# Re-export commonly used items
__all__ = [
    # Base components
    'ConnectionConfig', 'RetryableConfig', 'CacheableConfig',
    'ObservableConfig', 'SecurableConfig', 'ScalableConfig',
    
    # Unified configs
    'DatabaseConnectionConfig', 'MessagingConnectionConfig',
    'ServiceConnectionConfig', 'UnifiedServiceConfig',
    
    # Builder and utilities
    'ConfigBuilder', 'load_config_from_env', 'load_config_from_file',
    
    # Examples
    'AnalyticsServiceConfig', 'MLPlatformServiceConfig'
] 