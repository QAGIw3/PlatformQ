from pydantic import BaseSettings, Field
from typing import Dict, List, Optional, Any
import os


class AnalyticsConfig(BaseSettings):
    """Analytics service configuration"""
    
    # Service identification
    service_name: str = Field(default="analytics-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8092)
    api_prefix: str = Field(default="/api/v1")
    
    # Database configuration
    postgres_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/analytics"
    )
    clickhouse_url: str = Field(
        default="clickhouse://default:@localhost/analytics"
    )
    mongodb_url: str = Field(
        default="mongodb://localhost:27017"
    )
    mongodb_database: str = Field(default="analytics")
    
    # Time series database
    timescale_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/timeseries"
    )
    
    # Cache configuration
    redis_url: str = Field(default="redis://localhost:6379")
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800)
    cache_ttl_seconds: int = Field(default=3600)
    
    # Analytics settings
    default_lookback_days: int = Field(default=30)
    max_lookback_days: int = Field(default=365)
    aggregation_intervals: List[str] = Field(
        default_factory=lambda: ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]
    )
    
    # Chain analytics configuration
    chains: List[str] = Field(
        default_factory=lambda: [
            "ethereum", "polygon", "bsc", "avalanche", 
            "arbitrum", "optimism", "solana", "cosmos"
        ]
    )
    
    # Metrics to track
    metrics: Dict[str, List[str]] = Field(
        default_factory=lambda: {
            "transaction": [
                "count", "volume", "gas_used", "gas_price",
                "success_rate", "avg_confirmation_time"
            ],
            "wallet": [
                "balance", "transaction_count", "unique_interactions",
                "gas_spent", "tokens_held"
            ],
            "token": [
                "price", "volume", "market_cap", "holders",
                "transfers", "liquidity"
            ],
            "defi": [
                "tvl", "volume", "users", "transactions",
                "yield", "fees"
            ],
            "nft": [
                "sales", "volume", "floor_price", "unique_buyers",
                "unique_sellers", "avg_price"
            ]
        }
    )
    
    # Data sources
    blockchain_connector_url: str = Field(
        default="http://blockchain-connector-service:8086"
    )
    event_monitoring_url: str = Field(
        default="http://event-monitoring-service:8091"
    )
    
    # External data sources
    coingecko_api_key: Optional[str] = Field(default=None)
    etherscan_api_key: Optional[str] = Field(default=None)
    dune_api_key: Optional[str] = Field(default=None)
    
    # Pulsar configuration
    pulsar_url: str = Field(default="pulsar://localhost:6650")
    analytics_events_topic: str = Field(
        default="persistent://public/default/analytics-events"
    )
    
    # Celery configuration
    celery_broker_url: str = Field(default="redis://localhost:6379/0")
    celery_result_backend: str = Field(default="redis://localhost:6379/1")
    
    # Report generation
    report_storage_path: str = Field(default="/tmp/analytics-reports")
    max_report_size_mb: int = Field(default=100)
    report_retention_days: int = Field(default=90)
    
    # ML model settings
    model_storage_path: str = Field(default="/tmp/analytics-models")
    model_update_interval_hours: int = Field(default=24)
    
    # API rate limiting
    rate_limit_per_minute: int = Field(default=60)
    rate_limit_per_hour: int = Field(default=1000)
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9097)
    log_level: str = Field(default="INFO")
    
    # Consul configuration
    consul_host: str = Field(default="localhost")
    consul_port: int = Field(default=8500)
    service_health_interval: int = Field(default=10)
    
    class Config:
        env_prefix = "ANALYTICS_"
        case_sensitive = False


# Global configuration instance
config = AnalyticsConfig() 