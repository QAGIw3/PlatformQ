"""
Configuration Module

Integrates with the configuration management system for service-specific settings.
"""

import os
from typing import Any, Optional, Dict, List, Callable
from pydantic import BaseSettings, Field, validator
import logging

from .config_manager import (
    init_config_manager,
    get_config_manager,
    ConfigurationManager,
    config,
    feature_flag
)

logger = logging.getLogger(__name__)


class ServiceConfig(BaseSettings):
    """Base configuration for all services"""
    
    # Service identification
    service_name: str = Field(..., env="SERVICE_NAME")
    environment: str = Field("development", env="ENVIRONMENT")
    
    # Vault configuration
    vault_url: Optional[str] = Field(None, env="VAULT_URL")
    vault_token: Optional[str] = Field(None, env="VAULT_TOKEN")
    vault_role_id: Optional[str] = Field(None, env="VAULT_ROLE_ID")
    vault_secret_id: Optional[str] = Field(None, env="VAULT_SECRET_ID")
    
    # Ignite configuration for distributed config
    ignite_config_host: str = Field("ignite", env="IGNITE_CONFIG_HOST")
    ignite_config_port: int = Field(10800, env="IGNITE_CONFIG_PORT")
    
    # Configuration settings
    config_dir: str = Field("/etc/platformq", env="CONFIG_DIR")
    enable_hot_reload: bool = Field(True, env="ENABLE_HOT_RELOAD")
    config_cache_ttl: int = Field(300, env="CONFIG_CACHE_TTL")
    
    # Database configuration
    database_url: Optional[str] = Field(None, env="DATABASE_URL")
    database_pool_size: int = Field(10, env="DATABASE_POOL_SIZE")
    database_max_overflow: int = Field(20, env="DATABASE_MAX_OVERFLOW")
    
    # Pulsar configuration
    pulsar_url: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_tls_enabled: bool = Field(False, env="PULSAR_TLS_ENABLED")
    pulsar_token: Optional[str] = Field(None, env="PULSAR_TOKEN")
    
    # Ignite configuration
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_username: Optional[str] = Field(None, env="IGNITE_USERNAME")
    ignite_password: Optional[str] = Field(None, env="IGNITE_PASSWORD")
    
    # MinIO configuration
    minio_endpoint: str = Field("minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(False, env="MINIO_SECURE")
    
    # API configuration
    api_prefix: str = Field("/api/v1", env="API_PREFIX")
    cors_origins: List[str] = Field(["*"], env="CORS_ORIGINS")
    rate_limit_enabled: bool = Field(True, env="RATE_LIMIT_ENABLED")
    rate_limit_requests: int = Field(100, env="RATE_LIMIT_REQUESTS")
    rate_limit_period: int = Field(60, env="RATE_LIMIT_PERIOD")
    
    # Monitoring configuration
    metrics_enabled: bool = Field(True, env="METRICS_ENABLED")
    tracing_enabled: bool = Field(True, env="TRACING_ENABLED")
    jaeger_endpoint: Optional[str] = Field(None, env="JAEGER_ENDPOINT")
    
    # Security configuration
    jwt_secret_key: Optional[str] = Field(None, env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="JWT_ALGORITHM")
    jwt_expiration_minutes: int = Field(1440, env="JWT_EXPIRATION_MINUTES")
    
    # Feature flags
    feature_flags_enabled: bool = Field(True, env="FEATURE_FLAGS_ENABLED")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
    
    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v


class ConfigLoader:
    """Configuration loader with hot-reload support"""
    
    def __init__(self, service_name: str, base_config: Optional[ServiceConfig] = None):
        self.service_name = service_name
        self.base_config = base_config or ServiceConfig(service_name=service_name)
        self.config_manager: Optional[ConfigurationManager] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize the configuration manager"""
        if self._initialized:
            return
        
        # Initialize configuration manager
        self.config_manager = init_config_manager(
            service_name=self.service_name,
            config_dir=self.base_config.config_dir,
            vault_url=self.base_config.vault_url,
            vault_token=self.base_config.vault_token,
            ignite_host=self.base_config.ignite_config_host,
            ignite_port=self.base_config.ignite_config_port,
            enable_hot_reload=self.base_config.enable_hot_reload,
            cache_ttl=self.base_config.config_cache_ttl
        )
        
        # Load all configurations
        await self.config_manager.load_all_configs()
        
        # Load secrets from Vault
        await self._load_secrets()
        
        self._initialized = True
        logger.info(f"Configuration initialized for service: {self.service_name}")
    
    async def _load_secrets(self):
        """Load service-specific secrets from Vault"""
        if not self.config_manager.vault_client:
            return
        
        # Load database credentials
        db_secrets = self.config_manager.get_vault_secret(f"{self.service_name}/database")
        if db_secrets:
            self.base_config.database_url = self._build_database_url(db_secrets)
        
        # Load JWT secret
        jwt_secret = self.config_manager.get_vault_secret("shared/jwt")
        if jwt_secret:
            self.base_config.jwt_secret_key = jwt_secret.get("secret_key")
        
        # Load service-specific secrets
        service_secrets = self.config_manager.get_vault_secret(f"{self.service_name}/secrets")
        if service_secrets:
            for key, value in service_secrets.items():
                setattr(self.base_config, key, value)
    
    def _build_database_url(self, db_secrets: Dict[str, Any]) -> str:
        """Build database URL from secrets"""
        return (
            f"postgresql://{db_secrets.get('username')}:{db_secrets.get('password')}"
            f"@{db_secrets.get('host', 'localhost')}:{db_secrets.get('port', 5432)}"
            f"/{db_secrets.get('database', self.service_name)}"
        )
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        # Check base config first
        if hasattr(self.base_config, key):
            return getattr(self.base_config, key)
        
        # Then check configuration manager
        if self.config_manager:
            return self.config_manager.get(key, default)
        
        return default
    
    def get_feature_flag(self, flag_name: str, default: bool = False) -> bool:
        """Get feature flag value"""
        if not self.base_config.feature_flags_enabled:
            return default
        
        if self.config_manager:
            return self.config_manager.get_feature_flag(flag_name, default)
        
        return default
    
    def register_callback(self, key_pattern: str, callback: Callable):
        """Register callback for configuration changes"""
        if self.config_manager:
            self.config_manager.register_callback(key_pattern, callback)
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return {
            "url": self.base_config.database_url,
            "pool_size": self.base_config.database_pool_size,
            "max_overflow": self.base_config.database_max_overflow,
            "pool_pre_ping": True,
            "pool_recycle": 3600
        }
    
    def get_pulsar_config(self) -> Dict[str, Any]:
        """Get Pulsar configuration"""
        config = {
            "service_url": self.base_config.pulsar_url,
            "authentication": None
        }
        
        if self.base_config.pulsar_token:
            config["authentication"] = {
                "type": "token",
                "token": self.base_config.pulsar_token
            }
        
        if self.base_config.pulsar_tls_enabled:
            config["tls_trust_certs_file_path"] = "/etc/ssl/certs/ca-certificates.crt"
            config["tls_validate_hostname"] = True
        
        return config
    
    def get_ignite_config(self) -> Dict[str, Any]:
        """Get Ignite configuration"""
        return {
            "host": self.base_config.ignite_host,
            "port": self.base_config.ignite_port,
            "username": self.base_config.ignite_username,
            "password": self.base_config.ignite_password,
            "use_ssl": self.config_manager.get("ignite.use_ssl", False) if self.config_manager else False
        }
    
    def get_minio_config(self) -> Dict[str, Any]:
        """Get MinIO configuration"""
        return {
            "endpoint": self.base_config.minio_endpoint,
            "access_key": self.base_config.minio_access_key,
            "secret_key": self.base_config.minio_secret_key,
            "secure": self.base_config.minio_secure,
            "region": self.config_manager.get("minio.region", "us-east-1") if self.config_manager else "us-east-1"
        }
    
    async def close(self):
        """Close configuration manager"""
        if self.config_manager:
            await self.config_manager.close()


# Global configuration loader instance
_config_loader: Optional[ConfigLoader] = None


def init_config(service_name: str, base_config: Optional[ServiceConfig] = None) -> ConfigLoader:
    """Initialize the global configuration loader"""
    global _config_loader
    _config_loader = ConfigLoader(service_name, base_config)
    return _config_loader


def get_config() -> ConfigLoader:
    """Get the global configuration loader"""
    if _config_loader is None:
        raise RuntimeError("Configuration loader not initialized")
    return _config_loader


# Export common functions
__all__ = [
    "ServiceConfig",
    "ConfigLoader",
    "init_config",
    "get_config",
    "config",
    "feature_flag",
    "ConfigurationManager",
    "init_config_manager",
    "get_config_manager"
] 