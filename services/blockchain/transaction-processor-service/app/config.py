"""
Configuration for Transaction Processor Service
"""

from typing import List, Dict, Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    SERVICE_NAME: str = "transaction-processor"
    SERVICE_PORT: int = 8011
    
    # Consul & Vault
    CONSUL_HOST: str = Field("consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(8500, env="CONSUL_PORT")
    VAULT_ADDR: str = Field("http://vault:8200", env="VAULT_ADDR")
    VAULT_TOKEN: Optional[str] = Field(None, env="VAULT_TOKEN")
    
    # Infrastructure
    IGNITE_ADDRESSES: List[str] = Field(["ignite:10800"], env="IGNITE_ADDRESSES")
    PULSAR_URL: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    
    # Transaction processing
    TRANSACTION_QUEUE_TOPIC: str = "persistent://public/default/transactions"
    TRANSACTION_STATUS_TOPIC: str = "persistent://public/default/transaction-status"
    TRANSACTION_BATCH_SIZE: int = 10
    TRANSACTION_BATCH_TIMEOUT: float = 5.0  # seconds
    
    # Retry configuration
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_DELAY: float = 1.0
    RETRY_MAX_DELAY: float = 60.0
    RETRY_EXPONENTIAL_BASE: float = 2.0
    
    # Processing configuration
    MAX_CONCURRENT_TRANSACTIONS: int = 100
    TRANSACTION_TIMEOUT: int = 300  # seconds
    GAS_PRICE_REFRESH_INTERVAL: int = 30  # seconds
    NONCE_CACHE_TTL: int = 60  # seconds
    
    # Monitoring
    METRICS_ENABLED: bool = True
    METRICS_PORT: int = 9092
    LOG_LEVEL: str = "INFO"
    
    # Chain specific settings
    CHAIN_CONFIRMATION_BLOCKS: Dict[str, int] = Field(
        default_factory=lambda: {
            "ethereum": 12,
            "polygon": 32,
            "bsc": 15,
            "arbitrum": 1,
            "optimism": 1,
            "avalanche": 6
        }
    )
    
    # Security
    TRANSACTION_SIGNATURE_VERIFICATION: bool = True
    REQUIRE_TRANSACTION_APPROVAL: bool = True
    MAX_TRANSACTION_VALUE_WEI: str = "1000000000000000000000"  # 1000 ETH
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings() 