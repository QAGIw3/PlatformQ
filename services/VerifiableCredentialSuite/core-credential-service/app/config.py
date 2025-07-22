"""
Configuration for Core Credential Service
"""

from pydantic import BaseSettings, Field
from typing import Optional, List
import os


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service identification
    service_name: str = Field(default="core-credential-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration  
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8050)
    api_prefix: str = Field(default="/api/v1")
    
    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/credentials"
    )
    database_pool_size: int = Field(default=20)
    database_max_overflow: int = Field(default=10)
    
    # Apache Ignite cache
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800) 
    cache_ttl_seconds: int = Field(default=3600)
    enable_cache: bool = Field(default=True)
    
    # Apache Pulsar
    pulsar_url: str = Field(default="pulsar://localhost:6650")
    credential_events_topic: str = Field(
        default="persistent://public/default/credential-events"
    )
    
    # Storage service integration
    storage_service_url: str = Field(default="http://storage-service:8000")
    enable_ipfs_storage: bool = Field(default=True)
    encrypt_credentials: bool = Field(default=True)
    
    # Blockchain integration
    blockchain_connector_url: str = Field(
        default="http://blockchain-connector-service:8086"
    )
    enable_blockchain_anchoring: bool = Field(default=True)
    anchor_chains: List[str] = Field(default_factory=lambda: ["ethereum", "polygon"])
    
    # Key management service
    key_management_url: str = Field(default="http://key-management-service:8088")
    signing_timeout_seconds: int = Field(default=30)
    
    # DID service integration
    did_service_url: str = Field(default="http://did-service:8051")
    
    # Credential defaults
    credential_default_validity_days: int = Field(default=365)
    credential_namespace: str = Field(default="urn:uuid")
    supported_proof_types: List[str] = Field(
        default_factory=lambda: ["Ed25519Signature2020"]
    )
    
    # Batch operations
    max_batch_size: int = Field(default=100)
    batch_timeout_seconds: int = Field(default=60)
    
    # Security
    require_authenticated_issuers: bool = Field(default=True)
    allowed_issuer_pattern: Optional[str] = Field(default=None)
    
    # Performance
    async_storage_enabled: bool = Field(default=True)
    connection_pool_size: int = Field(default=10)
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9050)
    log_level: str = Field(default="INFO")
    
    # Consul integration
    consul_host: str = Field(default="localhost")
    consul_port: int = Field(default=8500)
    service_health_interval: int = Field(default=10)
    
    # HashiCorp Vault
    vault_addr: str = Field(default="http://vault:8200")
    vault_token: Optional[str] = Field(default=None)
    vault_namespace: Optional[str] = Field(default=None)
    
    class Config:
        env_prefix = "CREDENTIAL_"
        case_sensitive = False
        env_file = ".env"


# Global settings instance
settings = Settings() 