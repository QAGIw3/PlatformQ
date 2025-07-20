"""
Configuration for SBT Service
"""

from pydantic import BaseSettings, Field
from typing import Optional, List, Dict
import os


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service identification
    service_name: str = Field(default="sbt-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8053)
    api_prefix: str = Field(default="/api/v1")
    
    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/sbt"
    )
    database_pool_size: int = Field(default=20)
    database_max_overflow: int = Field(default=10)
    
    # Blockchain connector service
    blockchain_connector_url: str = Field(
        default="http://blockchain-connector-service:8086"
    )
    
    # Supported blockchain networks
    supported_chains: List[str] = Field(
        default_factory=lambda: [
            "ethereum",
            "polygon", 
            "arbitrum",
            "optimism",
            "base"
        ]
    )
    default_chain: str = Field(default="polygon")
    
    # Contract addresses per chain
    sbt_contract_addresses: Dict[str, str] = Field(
        default_factory=lambda: {
            "ethereum": "",
            "polygon": "",
            "arbitrum": "",
            "optimism": "",
            "base": ""
        }
    )
    
    # Apache Ignite cache
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800)
    enable_cache: bool = Field(default=True)
    cache_ttl_seconds: int = Field(default=3600)
    
    # Storage service (MinIO/IPFS)
    storage_service_url: str = Field(default="http://storage-service:8084")
    ipfs_gateway: str = Field(default="https://ipfs.io/ipfs/")
    use_ipfs: bool = Field(default=True)
    
    # Event streaming (Pulsar)
    pulsar_url: str = Field(default="pulsar://localhost:6650")
    event_topic: str = Field(default="sbt-events")
    enable_events: bool = Field(default=True)
    
    # Core credential service
    credential_service_url: str = Field(default="http://core-credential-service:8050")
    
    # Gas management
    gas_price_multiplier: float = Field(default=1.2)
    max_gas_price_gwei: int = Field(default=500)
    gas_estimation_buffer: float = Field(default=1.3)
    
    # Batch operations
    max_batch_size: int = Field(default=100)
    batch_timeout_seconds: int = Field(default=60)
    
    # Recovery settings
    recovery_timeout_hours: int = Field(default=72)
    min_recovery_signatures: int = Field(default=2)
    recovery_fee_percentage: float = Field(default=0.1)
    
    # Performance settings
    async_operations: bool = Field(default=True)
    worker_pool_size: int = Field(default=10)
    queue_size: int = Field(default=1000)
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9053)
    log_level: str = Field(default="INFO")
    enable_tracing: bool = Field(default=True)
    
    # Consul integration
    consul_host: str = Field(default="localhost")
    consul_port: int = Field(default=8500)
    service_health_interval: int = Field(default=10)
    enable_consul_config: bool = Field(default=True)
    
    # HashiCorp Vault
    vault_addr: str = Field(default="http://vault:8200")
    vault_token: Optional[str] = Field(default=None)
    vault_namespace: Optional[str] = Field(default=None)
    vault_mount_path: str = Field(default="sbt")
    
    # Rate limiting
    rate_limit_per_minute: int = Field(default=100)
    rate_limit_per_address: int = Field(default=10)
    
    # Security
    require_signature: bool = Field(default=True)
    allowed_issuers: List[str] = Field(default_factory=list)
    metadata_encryption: bool = Field(default=True)
    
    class Config:
        env_prefix = "SBT_"
        case_sensitive = False
        env_file = ".env"


# Global settings instance
settings = Settings() 