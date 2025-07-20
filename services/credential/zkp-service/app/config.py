"""
Configuration for ZKP Service
"""

from pydantic import BaseSettings, Field
from typing import Optional, List
import os


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service identification
    service_name: str = Field(default="zkp-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8052)
    api_prefix: str = Field(default="/api/v1")
    
    # Database
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/zkp"
    )
    database_pool_size: int = Field(default=20)
    database_max_overflow: int = Field(default=10)
    
    # Apache Ignite
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800)
    enable_compute_grid: bool = Field(default=True)
    worker_threads: int = Field(default=4)
    compute_timeout_seconds: int = Field(default=300)
    
    # Caching
    cache_ttl_seconds: int = Field(default=3600)
    enable_proof_cache: bool = Field(default=True)
    max_cached_proofs: int = Field(default=10000)
    
    # Key management service
    key_management_url: str = Field(default="http://key-management-service:8088")
    
    # Core credential service
    credential_service_url: str = Field(default="http://core-credential-service:8050")
    
    # Proof generation settings
    max_batch_size: int = Field(default=100)
    default_proof_expiry_hours: int = Field(default=24)
    enable_parallel_generation: bool = Field(default=True)
    
    # BBS+ specific settings
    bbs_signature_length: int = Field(default=112)  # BLS12-381 signature size
    max_messages_per_signature: int = Field(default=100)
    
    # Range proof settings
    range_proof_bits: int = Field(default=32)
    enable_range_proof_optimization: bool = Field(default=True)
    
    # Security settings
    require_nonce: bool = Field(default=True)
    min_nonce_length: int = Field(default=16)
    proof_verification_timeout: int = Field(default=30)
    
    # Performance settings
    async_operations: bool = Field(default=True)
    queue_size: int = Field(default=1000)
    worker_pool_size: int = Field(default=10)
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9052)
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
    vault_mount_path: str = Field(default="zkp")
    
    # Resource limits
    max_proof_size_kb: int = Field(default=100)
    max_concurrent_proofs: int = Field(default=100)
    rate_limit_per_minute: int = Field(default=1000)
    
    class Config:
        env_prefix = "ZKP_"
        case_sensitive = False
        env_file = ".env"


# Global settings instance
settings = Settings() 