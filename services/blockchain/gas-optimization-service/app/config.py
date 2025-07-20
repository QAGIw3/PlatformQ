"""
Configuration for Gas Optimization Service
"""

from typing import List, Dict, Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    SERVICE_NAME: str = "gas-optimization"
    SERVICE_PORT: int = 8013
    
    # Consul & Vault
    CONSUL_HOST: str = Field("consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(8500, env="CONSUL_PORT")
    
    # Infrastructure
    IGNITE_ADDRESSES: List[str] = Field(["ignite:10800"], env="IGNITE_ADDRESSES")
    PULSAR_URL: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    
    # Gas optimization settings
    GAS_PRICE_UPDATE_INTERVAL: int = 15  # seconds
    PRICE_HISTORY_WINDOW: int = 3600  # seconds (1 hour)
    PREDICTION_WINDOW: int = 300  # seconds (5 minutes)
    
    # Optimization strategies
    ENABLE_META_TRANSACTIONS: bool = True
    ENABLE_BATCH_OPTIMIZATION: bool = True
    ENABLE_L2_SUGGESTIONS: bool = True
    ENABLE_TIME_BASED_OPTIMIZATION: bool = True
    
    # Batch settings
    MAX_BATCH_SIZE: int = 100
    BATCH_TIMEOUT: float = 5.0  # seconds
    BATCH_GAS_SAVINGS_THRESHOLD: float = 0.1  # 10% savings required
    
    # L2 settings
    L2_COST_MULTIPLIER: Dict[str, float] = Field(
        default_factory=lambda: {
            "arbitrum": 0.1,
            "optimism": 0.15,
            "polygon": 0.01,
            "zksync": 0.05
        }
    )
    
    # Meta-transaction settings
    RELAYER_ADDRESSES: Dict[str, List[str]] = Field(default_factory=dict)
    MAX_RELAYER_FEE_PERCENTAGE: float = 5.0  # 5% max fee
    
    # ML model settings
    MODEL_UPDATE_INTERVAL: int = 3600  # seconds (1 hour)
    MIN_TRAINING_SAMPLES: int = 1000
    FEATURE_WINDOW_SIZES: List[int] = Field(default=[5, 15, 60])  # minutes
    
    # Caching
    CACHE_TTL: int = 60  # seconds
    
    # Monitoring
    METRICS_ENABLED: bool = True
    METRICS_PORT: int = 9094
    LOG_LEVEL: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings() 