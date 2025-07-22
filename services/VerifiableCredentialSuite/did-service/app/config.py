"""
Configuration for DID Service
"""

from pydantic import BaseSettings, Field
from typing import Optional, List, Dict
import os


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service identification
    service_name: str = Field(default="did-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration  
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8051)
    api_prefix: str = Field(default="/api/v1")
    
    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/dids"
    )
    database_pool_size: int = Field(default=20)
    database_max_overflow: int = Field(default=10)
    
    # Apache Ignite cache
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800) 
    cache_ttl_seconds: int = Field(default=3600)
    enable_cache: bool = Field(default=True)
    
    # Key management service
    key_management_url: str = Field(default="http://key-management-service:8088")
    
    # Blockchain connector (for did:ethr)
    blockchain_connector_url: str = Field(
        default="http://blockchain-connector-service:8086"
    )
    enable_did_ethr: bool = Field(default=False)
    
    # DID configuration
    default_did_method: str = Field(default="key")
    supported_did_methods: List[str] = Field(
        default_factory=lambda: ["key", "web", "platformq", "ethr"]
    )
    max_keys_per_did: int = Field(default=10)
    
    # did:web configuration
    did_web_domain: str = Field(default="platformq.com")
    did_web_path_prefix: str = Field(default=".well-known")
    
    # did:platformq configuration
    did_platformq_prefix: str = Field(default="did:platformq")
    did_platformq_network: str = Field(default="mainnet")
    
    # Security
    require_authentication: bool = Field(default=True)
    allowed_key_types: List[str] = Field(
        default_factory=lambda: ["Ed25519", "secp256k1"]
    )
    
    # Performance
    async_operations: bool = Field(default=True)
    batch_size: int = Field(default=100)
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9051)
    log_level: str = Field(default="INFO")
    
    # Consul integration
    consul_host: str = Field(default="localhost")
    consul_port: int = Field(default=8500)
    service_health_interval: int = Field(default=10)
    enable_consul_config: bool = Field(default=True)
    
    # HashiCorp Vault
    vault_addr: str = Field(default="http://vault:8200")
    vault_token: Optional[str] = Field(default=None)
    vault_namespace: Optional[str] = Field(default=None)
    vault_mount_path: str = Field(default="dids")
    
    # Key rotation
    enable_key_rotation: bool = Field(default=True)
    key_rotation_days: int = Field(default=90)
    
    class Config:
        env_prefix = "DID_"
        case_sensitive = False
        env_file = ".env"


# Global settings instance
settings = Settings() 