"""
Base Configuration Classes for DataIntelligenceSuite

Provides common configuration patterns and base classes.
"""

from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
import os
import json
import yaml
from abc import ABC, abstractmethod
import logging

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"


class LogLevel(str, Enum):
    """Log levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class BaseConfig(ABC):
    """
    Base configuration class with common fields.
    
    All service configurations should inherit from this.
    """
    # Service identification
    name: str
    version: str = "1.0.0"
    description: str = ""
    environment: Environment = field(default_factory=lambda: Environment(os.getenv("ENVIRONMENT", "development")))
    
    # Timeouts
    startup_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    shutdown_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    request_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=60))
    
    # Resource limits
    max_memory_mb: Optional[int] = None
    max_cpu_percent: Optional[float] = None
    max_concurrent_requests: int = 100
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = True
    enable_profiling: bool = False
    metrics_port: int = 9090
    
    # Logging
    log_level: LogLevel = field(default_factory=lambda: LogLevel(os.getenv("LOG_LEVEL", "INFO")))
    structured_logging: bool = True
    log_format: str = "json"
    
    # Health checks
    enable_health_check: bool = True
    health_check_interval: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    health_check_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    
    # Security
    enable_auth: bool = True
    enable_tls: bool = True
    enable_encryption: bool = True
    
    # Feature flags
    feature_flags: Dict[str, bool] = field(default_factory=dict)
    
    # Tags and metadata
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-initialization validation and setup"""
        self.validate()
        
    def validate(self):
        """Validate configuration"""
        if not self.name:
            raise ValueError("Service name is required")
            
        if self.max_memory_mb and self.max_memory_mb < 128:
            raise ValueError("Minimum memory requirement is 128MB")
            
        if self.max_cpu_percent and not 0 < self.max_cpu_percent <= 100:
            raise ValueError("CPU percent must be between 0 and 100")
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Enum):
                result[key] = value.value
            elif isinstance(value, timedelta):
                result[key] = value.total_seconds()
            elif hasattr(value, "to_dict"):
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseConfig":
        """Create from dictionary"""
        # Convert timedelta fields
        for field_name in ["startup_timeout", "shutdown_timeout", "request_timeout", "health_check_interval", "health_check_timeout"]:
            if field_name in data and isinstance(data[field_name], (int, float)):
                data[field_name] = timedelta(seconds=data[field_name])
                
        # Convert enum fields
        if "environment" in data and isinstance(data["environment"], str):
            data["environment"] = Environment(data["environment"])
        if "log_level" in data and isinstance(data["log_level"], str):
            data["log_level"] = LogLevel(data["log_level"])
            
        return cls(**data)
        
    def merge(self, other: "BaseConfig") -> "BaseConfig":
        """Merge with another config, other takes precedence"""
        merged_data = self.to_dict()
        other_data = other.to_dict()
        merged_data.update(other_data)
        return self.__class__.from_dict(merged_data)


@dataclass
class ServiceConfig(BaseConfig):
    """Configuration for services with Vault/Consul integration"""
    # Service discovery
    use_service_discovery: bool = True
    service_host: str = field(default_factory=lambda: os.getenv("SERVICE_HOST", "0.0.0.0"))
    service_port: int = field(default_factory=lambda: int(os.getenv("SERVICE_PORT", "8000")))
    advertise_address: Optional[str] = None
    
    # Consul
    consul_enabled: bool = True
    consul_url: str = field(default_factory=lambda: os.getenv("CONSUL_URL", "http://localhost:8500"))
    consul_token: Optional[str] = field(default_factory=lambda: os.getenv("CONSUL_TOKEN"))
    consul_datacenter: str = "dc1"
    
    # Vault
    vault_enabled: bool = True
    vault_url: str = field(default_factory=lambda: os.getenv("VAULT_URL", "http://localhost:8200"))
    vault_token: Optional[str] = field(default_factory=lambda: os.getenv("VAULT_TOKEN"))
    vault_role: str = "service"
    vault_mount: str = "secret"
    
    # Rate limiting
    enable_rate_limiting: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: timedelta = field(default_factory=lambda: timedelta(minutes=1))
    
    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_failures: int = 5
    circuit_breaker_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=60))


@dataclass
class DatabaseConfig(BaseConfig):
    """Base database configuration"""
    host: str = "localhost"
    port: int = 5432
    database: str = "platformq"
    username: Optional[str] = None
    password: Optional[str] = None
    
    # Connection pool
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # SSL/TLS
    ssl_enabled: bool = True
    ssl_mode: str = "require"
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_ca: Optional[str] = None
    
    # Performance
    statement_timeout: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    connect_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    
    def get_connection_string(self, include_password: bool = False) -> str:
        """Get database connection string"""
        if include_password and self.password:
            auth = f"{self.username}:{self.password}"
        elif self.username:
            auth = self.username
        else:
            auth = ""
            
        return f"postgresql://{auth}@{self.host}:{self.port}/{self.database}"


@dataclass
class CacheConfig(BaseConfig):
    """Base cache configuration"""
    enabled: bool = True
    backend: str = "ignite"  # ignite, redis, memcached
    
    # TTL settings
    default_ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    max_ttl: timedelta = field(default_factory=lambda: timedelta(hours=24))
    
    # Size limits
    max_entries: Optional[int] = None
    max_memory_mb: Optional[int] = None
    
    # Behavior
    enable_compression: bool = False
    enable_encryption: bool = False
    eviction_policy: str = "LRU"


@dataclass
class MessagingConfig(BaseConfig):
    """Base messaging configuration"""
    enabled: bool = True
    backend: str = "pulsar"  # pulsar, kafka, rabbitmq
    
    # Connection
    broker_url: str = field(default_factory=lambda: os.getenv("BROKER_URL", "pulsar://localhost:6650"))
    
    # Producer settings
    producer_batching: bool = True
    producer_compression: str = "lz4"
    producer_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Consumer settings
    consumer_type: str = "shared"  # shared, exclusive, failover
    consumer_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    max_concurrent_messages: int = 100


@dataclass
class SecurityConfig(BaseConfig):
    """Base security configuration"""
    # Authentication
    auth_enabled: bool = True
    auth_type: str = "jwt"  # jwt, oauth2, basic, api_key
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    token_expiry: timedelta = field(default_factory=lambda: timedelta(hours=1))
    
    # Authorization
    rbac_enabled: bool = True
    default_role: str = "viewer"
    
    # Encryption
    encryption_enabled: bool = True
    encryption_algorithm: str = "AES256"
    
    # API Security
    cors_enabled: bool = True
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    csrf_enabled: bool = True
    
    # Rate limiting
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 60


@dataclass
class ObservabilityConfig(BaseConfig):
    """Base observability configuration"""
    # Metrics
    metrics_enabled: bool = True
    metrics_backend: str = "prometheus"
    metrics_endpoint: str = "/metrics"
    custom_metrics: Dict[str, str] = field(default_factory=dict)
    
    # Tracing
    tracing_enabled: bool = True
    tracing_backend: str = "jaeger"
    tracing_endpoint: str = field(default_factory=lambda: os.getenv("JAEGER_ENDPOINT", "http://localhost:14268/api/traces"))
    tracing_sample_rate: float = 0.1
    
    # Logging
    logging_backend: str = "elasticsearch"
    log_retention_days: int = 30
    
    # Alerting
    alerting_enabled: bool = True
    alert_endpoints: List[str] = field(default_factory=list)


class ConfigLoader:
    """Utility class for loading configurations"""
    
    def __init__(
        self,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.vault_client = vault_client
        self.consul_client = consul_client
        
    async def load(
        self,
        config_class: type,
        sources: List[str] = ["env", "file", "consul", "vault"],
        config_file: Optional[str] = None,
        consul_key: Optional[str] = None,
        vault_path: Optional[str] = None
    ) -> BaseConfig:
        """
        Load configuration from multiple sources.
        
        Sources are applied in order, later sources override earlier ones.
        """
        config_data = {}
        
        for source in sources:
            if source == "env":
                config_data.update(self._load_from_env(config_class))
            elif source == "file" and config_file:
                config_data.update(self._load_from_file(config_file))
            elif source == "consul" and self.consul_client and consul_key:
                config_data.update(await self._load_from_consul(consul_key))
            elif source == "vault" and self.vault_client and vault_path:
                config_data.update(await self._load_from_vault(vault_path))
                
        return config_class.from_dict(config_data)
        
    def _load_from_env(self, config_class: type) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config_data = {}
        
        # Get field names from config class
        if hasattr(config_class, "__dataclass_fields__"):
            for field_name in config_class.__dataclass_fields__:
                env_var = field_name.upper()
                if env_var in os.environ:
                    config_data[field_name] = os.environ[env_var]
                    
        return config_data
        
    def _load_from_file(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from file (JSON or YAML)"""
        with open(config_file, 'r') as f:
            if config_file.endswith('.json'):
                return json.load(f)
            elif config_file.endswith(('.yml', '.yaml')):
                return yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported config file format: {config_file}")
                
    async def _load_from_consul(self, key: str) -> Dict[str, Any]:
        """Load configuration from Consul"""
        try:
            data = await self.consul_client.kv_get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to load config from Consul: {e}")
        return {}
        
    async def _load_from_vault(self, path: str) -> Dict[str, Any]:
        """Load configuration from Vault"""
        try:
            secret = await self.vault_client.get_secret(path)
            if secret:
                return secret
        except Exception as e:
            logger.error(f"Failed to load config from Vault: {e}")
        return {}


class ConfigValidator:
    """Configuration validator"""
    
    @staticmethod
    def validate(config: BaseConfig) -> List[str]:
        """
        Validate configuration and return list of errors.
        
        Returns empty list if valid.
        """
        errors = []
        
        # Call the config's own validation
        try:
            config.validate()
        except Exception as e:
            errors.append(str(e))
            
        # Additional validation rules can be added here
        
        return errors


class ConfigurationError(Exception):
    """Configuration-related errors"""
    pass 