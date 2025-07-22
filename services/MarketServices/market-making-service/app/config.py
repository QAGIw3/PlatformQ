"""Configuration settings for Market Making Service"""

from typing import List, Optional
from decimal import Decimal
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings"""
    
    # Service info
    SERVICE_NAME: str = "market-making-service"
    SERVICE_ID: str = "market-making-001"  # For direct communication
    SERVICE_PORT: int = 8000
    DEBUG: bool = False
    
    # CORS
    ALLOWED_ORIGINS: List[str] = ["*"]
    
    # External services
    IGNITE_HOST: str = "ignite"
    IGNITE_PORT: int = 10800
    
    PULSAR_URL: str = "pulsar://pulsar:6650"
    PULSAR_TOPIC_PREFIX: str = "persistent://platform/market-making"
    
    REDIS_URL: str = "redis://redis:6379"
    REDIS_KEY_PREFIX: str = "mm:"
    
    # Service dependencies
    TRADING_CORE_SERVICE_URL: str = "http://trading-core-service:8000"
    RISK_ENGINE_SERVICE_URL: str = "http://risk-engine-service:8000"
    ORACLE_SERVICE_URL: str = "http://oracle-service:8000"
    ANALYTICS_SERVICE_URL: str = "http://analytics-service:8000"
    
    # Direct Communication
    ENABLE_DIRECT_COMM: bool = True
    DIRECT_COMM_BATCH_SIZE: int = 100
    DIRECT_COMM_TIMEOUT_MS: int = 50  # 50ms timeout
    
    # Market making parameters
    DEFAULT_SPREAD_BPS: int = 20  # basis points
    MAX_POSITION_SIZE: Decimal = Decimal("1000000")
    REBALANCE_INTERVAL: int = 60  # seconds
    MIN_ORDER_SIZE: Decimal = Decimal("10")
    MAX_ORDER_SIZE: Decimal = Decimal("100000")
    
    # AMM parameters
    MIN_LIQUIDITY: Decimal = Decimal("1000")
    MAX_LIQUIDITY: Decimal = Decimal("10000000")
    DEFAULT_FEE_BPS: int = 30  # 0.3%
    MIN_FEE_BPS: int = 1     # 0.01%
    MAX_FEE_BPS: int = 1000  # 10%
    
    # Risk limits
    MAX_DRAWDOWN_PERCENT: Decimal = Decimal("10")
    POSITION_LIMIT_USD: Decimal = Decimal("5000000")
    MAX_LEVERAGE: Decimal = Decimal("10")
    
    # Performance
    CACHE_TTL: int = 60  # seconds
    MAX_CONCURRENT_STRATEGIES: int = 100
    BATCH_SIZE: int = 100
    
    # Monitoring
    METRICS_ENABLED: bool = True
    METRICS_PORT: int = 9090
    LOG_LEVEL: str = "INFO"
    
    # Authentication
    JWT_SECRET_KEY: str = "your-secret-key-here"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    model_config = SettingsConfigDict(
        env_prefix="MM_",
        case_sensitive=False
    )


# Create settings instance
settings = Settings() 