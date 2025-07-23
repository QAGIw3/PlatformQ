"""
Base configuration classes and utilities.

Provides foundation for all configuration management in the platform.
"""

import os
import json
import yaml
from typing import Any, Dict, List, Optional, Type, TypeVar, Union
from dataclasses import dataclass, field, asdict
from datetime import timedelta
from abc import ABC, abstractmethod
from pathlib import Path
import logging
from enum import Enum

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T', bound='BaseConfig')


class ConfigurationError(Exception):
    """Configuration related errors"""
    pass


class Environment(Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"


@dataclass
class BaseConfig(ABC):
    """
    Base configuration class with common functionality.
    
    Features:
    - Automatic validation
    - Environment variable support
    - Type conversion
    - Serialization/deserialization
    """
    
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create config from dictionary"""
        return cls(**data)
        
    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """Create config from JSON string"""
        data = json.loads(json_str)
        return cls.from_dict(data)
        
    @classmethod
    def from_yaml(cls: Type[T], yaml_str: str) -> T:
        """Create config from YAML string"""
        data = yaml.safe_load(yaml_str)
        return cls.from_dict(data)
        
    @classmethod
    def from_file(cls: Type[T], file_path: Union[str, Path]) -> T:
        """Load config from file"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise ConfigurationError(f"Config file not found: {file_path}")
            
        content = file_path.read_text()
        
        if file_path.suffix == '.json':
            return cls.from_json(content)
        elif file_path.suffix in ['.yaml', '.yml']:
            return cls.from_yaml(content)
        else:
            raise ConfigurationError(f"Unsupported file format: {file_path.suffix}")
            
    @classmethod
    def from_env(cls: Type[T], prefix: str = "") -> T:
        """Create config from environment variables"""
        data = {}
        prefix = prefix.upper()
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and convert to lowercase
                config_key = key[len(prefix):].lstrip('_').lower()
                
                # Handle nested keys (e.g., DATABASE_HOST -> database.host)
                if '__' in config_key:
                    config_key = config_key.replace('__', '.')
                    
                data[config_key] = value
                
        return cls.from_dict(data)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        return asdict(self)
        
    def to_json(self, indent: int = 2) -> str:
        """Convert config to JSON string"""
        return json.dumps(self.to_dict(), indent=indent, default=str)
        
    def to_yaml(self) -> str:
        """Convert config to YAML string"""
        return yaml.dump(self.to_dict(), default_flow_style=False)
        
    def validate(self) -> List[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Override in subclasses for custom validation
        errors.extend(self._validate())
        
        return errors
        
    def _validate(self) -> List[str]:
        """Custom validation logic (override in subclasses)"""
        return []
        
    def merge(self, other: 'BaseConfig') -> 'BaseConfig':
        """Merge with another config (other takes precedence)"""
        self_dict = self.to_dict()
        other_dict = other.to_dict()
        
        merged = {**self_dict, **other_dict}
        return self.__class__.from_dict(merged)


@dataclass
class ServiceConfig(BaseConfig):
    """Base configuration for services"""
    name: str
    version: str = "1.0.0"
    environment: Environment = Environment.DEVELOPMENT
    
    # Network
    host: str = "0.0.0.0"
    port: int = 8000
    base_url: Optional[str] = None
    
    # Timeouts
    request_timeout: timedelta = timedelta(seconds=30)
    shutdown_timeout: timedelta = timedelta(seconds=30)
    
    # Features
    debug: bool = False
    enable_docs: bool = True
    enable_metrics: bool = True
    enable_tracing: bool = True
    
    # Resource limits
    max_connections: int = 1000
    max_workers: int = 10
    
    def _validate(self) -> List[str]:
        """Validate service configuration"""
        errors = []
        
        if not self.name:
            errors.append("Service name is required")
            
        if self.port < 1 or self.port > 65535:
            errors.append(f"Invalid port: {self.port}")
            
        if self.max_workers < 1:
            errors.append(f"Invalid max_workers: {self.max_workers}")
            
        return errors


@dataclass
class DatabaseConfig(BaseConfig):
    """Database connection configuration"""
    type: str  # cassandra, ignite, elasticsearch, janusgraph
    host: str = "localhost"
    port: int = 9042
    username: Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None
    keyspace: Optional[str] = None
    
    # Connection pool
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: timedelta = timedelta(seconds=30)
    
    # SSL/TLS
    ssl_enabled: bool = False
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None
    ssl_ca_path: Optional[str] = None
    
    # Options
    options: Dict[str, Any] = field(default_factory=dict)
    
    def get_connection_string(self) -> str:
        """Build connection string"""
        if self.type == "cassandra":
            return f"cassandra://{self.host}:{self.port}/{self.keyspace}"
        elif self.type == "ignite":
            return f"ignite://{self.host}:{self.port}"
        elif self.type == "elasticsearch":
            return f"http://{self.host}:{self.port}"
        elif self.type == "janusgraph":
            return f"janusgraph://{self.host}:{self.port}"
        else:
            raise ValueError(f"Unknown database type: {self.type}")


@dataclass
class CacheConfig(BaseConfig):
    """Cache configuration"""
    type: str = "ignite"  # ignite, redis, memory
    enabled: bool = True
    
    # TTL settings
    default_ttl: timedelta = timedelta(minutes=5)
    max_ttl: timedelta = timedelta(hours=24)
    
    # Size limits
    max_size: int = 10000
    eviction_policy: str = "LRU"
    
    # Connection
    host: str = "localhost"
    port: int = 10800
    
    # Serialization
    serializer: str = "json"  # json, pickle, msgpack


@dataclass
class MessagingConfig(BaseConfig):
    """Messaging system configuration"""
    type: str = "pulsar"  # pulsar, kafka
    brokers: List[str] = field(default_factory=lambda: ["localhost:6650"])
    
    # Authentication
    auth_enabled: bool = False
    auth_type: str = "token"  # token, oauth2, tls
    auth_params: Dict[str, Any] = field(default_factory=dict)
    
    # Producer settings
    producer_batch_size: int = 1000
    producer_timeout: timedelta = timedelta(milliseconds=100)
    compression_type: str = "lz4"
    
    # Consumer settings
    consumer_group: Optional[str] = None
    subscription_type: str = "shared"  # shared, exclusive, failover
    ack_timeout: timedelta = timedelta(seconds=10)
    
    # Topics
    default_topic: Optional[str] = None
    topic_prefix: str = "platformq"


@dataclass
class SecurityConfig(BaseConfig):
    """Security configuration"""
    # Vault
    vault_enabled: bool = True
    vault_url: str = "http://localhost:8200"
    vault_token: Optional[str] = None
    vault_namespace: Optional[str] = None
    
    # Consul
    consul_enabled: bool = True
    consul_url: str = "http://localhost:8500"
    consul_token: Optional[str] = None
    
    # Encryption
    encryption_enabled: bool = True
    encryption_algorithm: str = "AES256"
    key_rotation_interval: timedelta = timedelta(days=30)
    
    # Authentication
    auth_provider: str = "internal"  # internal, oauth2, ldap
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry: timedelta = timedelta(hours=1)
    
    # Authorization
    rbac_enabled: bool = True
    default_role: str = "viewer"
    
    # Security headers
    cors_enabled: bool = True
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    csp_enabled: bool = True


@dataclass
class ObservabilityConfig(BaseConfig):
    """Observability configuration"""
    # Metrics
    metrics_enabled: bool = True
    metrics_endpoint: str = "/metrics"
    metrics_port: int = 9090
    
    # Tracing
    tracing_enabled: bool = True
    tracing_endpoint: str = "http://localhost:4317"
    tracing_sample_rate: float = 0.1
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"
    log_file: Optional[str] = None
    log_rotation: bool = True
    log_max_size: int = 100  # MB
    log_max_files: int = 10
    
    # Alerting
    alerting_enabled: bool = True
    alert_webhook: Optional[str] = None
    alert_email: Optional[str] = None


class ConfigLoader:
    """
    Configuration loader with multiple source support.
    
    Features:
    - Load from files
    - Load from environment
    - Load from Consul
    - Load from Vault
    - Merge configurations
    - Watch for changes
    """
    
    def __init__(
        self,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.vault_client = vault_client
        self.consul_client = consul_client
        
    async def load(
        self,
        config_class: Type[T],
        sources: List[str],
        env_prefix: str = ""
    ) -> T:
        """
        Load configuration from multiple sources.
        
        Args:
            config_class: Configuration class
            sources: List of sources (file paths, "env", "consul", "vault")
            env_prefix: Environment variable prefix
            
        Returns:
            Loaded configuration
        """
        configs = []
        
        for source in sources:
            if source == "env":
                config = config_class.from_env(env_prefix)
                configs.append(config)
                
            elif source == "consul":
                if self.consul_client:
                    config = await self._load_from_consul(config_class)
                    configs.append(config)
                    
            elif source == "vault":
                if self.vault_client:
                    config = await self._load_from_vault(config_class)
                    configs.append(config)
                    
            elif Path(source).exists():
                config = config_class.from_file(source)
                configs.append(config)
                
            else:
                logger.warning(f"Unknown config source: {source}")
                
        # Merge configurations (later sources override earlier)
        if not configs:
            raise ConfigurationError("No configuration loaded")
            
        result = configs[0]
        for config in configs[1:]:
            result = result.merge(config)
            
        # Validate final configuration
        errors = result.validate()
        if errors:
            raise ConfigurationError(f"Configuration validation failed: {errors}")
            
        return result
        
    async def _load_from_consul(self, config_class: Type[T]) -> T:
        """Load configuration from Consul"""
        key = f"config/{config_class.__name__.lower()}"
        data = await self.consul_client.kv_get(key)
        
        if not data:
            raise ConfigurationError(f"No configuration found in Consul at {key}")
            
        return config_class.from_json(data)
        
    async def _load_from_vault(self, config_class: Type[T]) -> T:
        """Load configuration from Vault"""
        path = f"secret/config/{config_class.__name__.lower()}"
        data = await self.vault_client.read_secret(path)
        
        if not data:
            raise ConfigurationError(f"No configuration found in Vault at {path}")
            
        return config_class.from_dict(data)


class ConfigValidator:
    """Configuration validator with custom rules"""
    
    @staticmethod
    def validate_port(port: int) -> bool:
        """Validate port number"""
        return 1 <= port <= 65535
        
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate URL format"""
        from urllib.parse import urlparse
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
            
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format"""
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None 