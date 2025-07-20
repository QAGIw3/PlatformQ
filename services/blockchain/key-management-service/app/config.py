"""
Configuration for Key Management Service
"""

from typing import List, Dict, Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    SERVICE_NAME: str = "key-management"
    SERVICE_PORT: int = 8012
    
    # Consul & Vault
    CONSUL_HOST: str = Field("consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(8500, env="CONSUL_PORT")
    VAULT_ADDR: str = Field("http://vault:8200", env="VAULT_ADDR")
    VAULT_TOKEN: Optional[str] = Field(None, env="VAULT_TOKEN")
    
    # Vault configuration
    VAULT_TRANSIT_PATH: str = "transit"
    VAULT_KV_PATH: str = "secret"
    VAULT_POLICY_PATH: str = "policies"
    VAULT_AUTO_UNSEAL: bool = True
    
    # Infrastructure
    IGNITE_ADDRESSES: List[str] = Field(["ignite:10800"], env="IGNITE_ADDRESSES")
    
    # Security
    ENABLE_MFA: bool = True
    MFA_ISSUER: str = "PlatformQ"
    
    # Key management
    KEY_ROTATION_INTERVAL_DAYS: int = 90
    KEY_VERSION_LIMIT: int = 5
    AUTO_ROTATE_KEYS: bool = True
    
    # Access control
    REQUIRE_APPROVAL_FOR_SIGNING: bool = False
    MAX_SIGNING_RATE_PER_MINUTE: int = 60
    AUDIT_ALL_OPERATIONS: bool = True
    MAX_TRANSACTION_VALUE_WEI: str = "1000000000000000000000"  # 1000 ETH
    
    # Caching
    KEY_CACHE_TTL_SECONDS: int = 300
    PERMISSION_CACHE_TTL_SECONDS: int = 60
    
    # Monitoring
    METRICS_ENABLED: bool = True
    METRICS_PORT: int = 9093
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings() 