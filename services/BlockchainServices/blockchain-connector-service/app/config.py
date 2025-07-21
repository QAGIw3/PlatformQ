"""
Configuration for Blockchain Connector Service
"""

from typing import List, Dict, Optional
from pydantic import BaseSettings, Field


class ChainEndpointConfig(BaseSettings):
    """Configuration for a blockchain endpoint"""
    url: str
    priority: int = 1
    rate_limit: Optional[int] = None  # requests per second
    health_check_interval: int = 60
    timeout: int = 30
    is_archive: bool = False


class ChainConfig(BaseSettings):
    """Configuration for a blockchain network"""
    chain_type: str
    chain_id: int
    name: str
    symbol: str
    explorer_url: str
    endpoints: List[ChainEndpointConfig] = []
    confirmations_required: int = 1
    gas_price_multiplier: float = 1.1
    max_gas_price: Optional[float] = None
    features: Dict[str, bool] = Field(default_factory=dict)


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    SERVICE_NAME: str = "blockchain-connector"
    SERVICE_PORT: int = 8010
    
    # Consul & Vault
    CONSUL_HOST: str = Field("consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(8500, env="CONSUL_PORT")
    VAULT_ADDR: str = Field("http://vault:8200", env="VAULT_ADDR")
    VAULT_TOKEN: Optional[str] = Field(None, env="VAULT_TOKEN")
    
    # Infrastructure
    IGNITE_ADDRESSES: List[str] = Field(["ignite:10800"], env="IGNITE_ADDRESSES")
    PULSAR_URL: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    
    # Connection pool settings
    MAX_CONNECTIONS_PER_CHAIN: int = 10
    CONNECTION_TIMEOUT: int = 30
    HEALTH_CHECK_INTERVAL: int = 60
    RETRY_ATTEMPTS: int = 3
    RETRY_DELAY: float = 1.0
    
    # Performance settings
    REQUEST_BATCH_SIZE: int = 50
    MAX_CONCURRENT_REQUESTS: int = 100
    
    # Monitoring
    METRICS_ENABLED: bool = True
    METRICS_PORT: int = 9091
    LOG_LEVEL: str = "INFO"
    
    # Chain configurations (loaded from Consul in production)
    CHAIN_CONFIGS: Dict[str, ChainConfig] = Field(default_factory=dict)
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings() 