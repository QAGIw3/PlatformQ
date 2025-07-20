"""Configuration for Market Intelligence Service."""

from pydantic_settings import BaseSettings
from typing import List, Dict, Any
from decimal import Decimal


class Settings(BaseSettings):
    """Market Intelligence Service configuration."""
    
    # Service info
    service_name: str = "market-intelligence-service"
    service_version: str = "1.0.0"
    
    # API Configuration
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8022
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_prefix: str = "market_intelligence"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_market_data_topic: str = "persistent://public/default/market-data"
    pulsar_analytics_topic: str = "persistent://public/default/analytics"
    
    # Apache Flink
    flink_jobmanager_url: str = "http://localhost:8081"
    flink_checkpoint_interval_ms: int = 5000
    flink_window_size_seconds: int = 60
    
    # Data sources
    external_price_feeds: List[str] = [
        "binance", "coinbase", "kraken", "ftx"
    ]
    oracle_endpoints: List[str] = []
    
    # Analytics configuration
    # Technical indicators
    indicators_enabled: List[str] = [
        "sma", "ema", "rsi", "macd", "bollinger_bands",
        "stochastic", "atr", "volume_profile"
    ]
    
    # Time windows
    analysis_windows: List[int] = [
        60,      # 1 minute
        300,     # 5 minutes
        900,     # 15 minutes
        3600,    # 1 hour
        14400,   # 4 hours
        86400    # 1 day
    ]
    
    # ML models
    ml_models_enabled: bool = True
    price_prediction_model: str = "lstm"
    anomaly_detection_model: str = "isolation_forest"
    sentiment_analysis_model: str = "transformer"
    
    # Market data aggregation
    aggregation_interval_seconds: int = 10
    max_orderbook_depth: int = 100
    
    # Oracle configuration
    oracle_update_interval_seconds: int = 30
    oracle_price_tolerance: Decimal = Decimal("0.02")  # 2%
    min_oracle_sources: int = 3
    
    # Analytics parameters
    volatility_lookback_hours: int = 24
    correlation_window_days: int = 30
    liquidity_depth_levels: int = 10
    
    # External services
    trading_core_url: str = "http://localhost:8020"
    risk_engine_url: str = "http://localhost:8021"
    
    # Caching
    market_data_cache_ttl: int = 5  # seconds
    analytics_cache_ttl: int = 60
    
    # Monitoring
    metrics_enabled: bool = True
    metrics_port: int = 9022
    
    class Config:
        env_prefix = "MARKET_INTELLIGENCE_"
        case_sensitive = False 