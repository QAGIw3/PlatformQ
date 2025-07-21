"""Configuration settings for Futures Service."""

from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Service info
    service_name: str = "futures-service"
    service_version: str = "1.0.0"
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_name: str = "futures_cache"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_futures_topic: str = "persistent://public/default/futures-events"
    pulsar_market_data_topic: str = "persistent://public/default/market-data"
    pulsar_risk_topic: str = "persistent://public/default/risk-events"
    
    # Cassandra
    cassandra_hosts: list[str] = ["localhost"]
    cassandra_keyspace: str = "futures_data"
    cassandra_port: int = 9042
    
    # Funding calculation
    funding_interval_hours: int = 8
    max_funding_rate: float = 0.01  # 1% max per interval
    funding_smoothing_window: int = 60  # minutes
    
    # Settlement
    settlement_batch_size: int = 100
    settlement_timeout_seconds: int = 30
    physical_delivery_enabled: bool = True
    
    # Risk parameters
    initial_margin_rate: float = 0.1  # 10%
    maintenance_margin_rate: float = 0.05  # 5%
    max_leverage: int = 20
    liquidation_penalty: float = 0.025  # 2.5%
    
    # Performance tuning
    order_book_depth: int = 100
    trade_history_limit: int = 1000
    candle_intervals: list[str] = ["1m", "5m", "15m", "1h", "4h", "1d"]
    
    # External services
    oracle_service_url: str = "http://localhost:8010"
    risk_service_url: str = "http://localhost:8004"
    blockchain_gateway_url: str = "http://localhost:8002"
    
    # WebSocket
    ws_heartbeat_interval: int = 30
    ws_max_connections_per_user: int = 5
    ws_rate_limit_per_second: int = 100
    
    # Cache TTL (seconds)
    position_cache_ttl: int = 300
    margin_cache_ttl: int = 60
    funding_rate_cache_ttl: int = 300
    
    class Config:
        env_prefix = "FUTURES_"
        case_sensitive = False 