from pydantic import BaseSettings
from typing import List, Dict
from decimal import Decimal


class MarketDataConfig(BaseSettings):
    # Service configuration
    SERVICE_NAME: str = "market-data"
    SERVICE_PORT: int = 8083
    WEBSOCKET_PORT: int = 8084
    
    # Consul & Vault
    CONSUL_HOST: str = "consul"
    CONSUL_PORT: int = 8500
    VAULT_ADDR: str = "http://vault:8200"
    VAULT_ROLE: str = "market-data"
    
    # Ignite cache settings
    IGNITE_ADDRESSES: List[str] = ["127.0.0.1:10800"]
    IGNITE_CACHE_MODE: str = "REPLICATED"  # Replicated for fast reads
    
    # Pulsar settings
    PULSAR_URL: str = "pulsar://pulsar:6650"
    PULSAR_TOPIC_TRADES: str = "persistent://derivatives/trading/trades"
    PULSAR_TOPIC_ORDERS: str = "persistent://derivatives/trading/orders"
    PULSAR_TOPIC_MARKET_DATA: str = "persistent://derivatives/trading/market-data"
    
    # Cassandra settings for historical data
    CASSANDRA_HOSTS: List[str] = ["cassandra"]
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "market_data"
    
    # Market data settings
    TICK_AGGREGATION_INTERVAL_MS: int = 100  # 100ms aggregation
    CANDLE_INTERVALS: List[str] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
    MAX_ORDERBOOK_DEPTH: int = 100
    SNAPSHOT_INTERVAL_SECONDS: int = 60  # Full snapshot every minute
    
    # WebSocket settings
    WS_MAX_CONNECTIONS_PER_IP: int = 10
    WS_MESSAGE_QUEUE_SIZE: int = 1000
    WS_HEARTBEAT_INTERVAL: int = 30
    
    # Cache settings
    PRICE_CACHE_TTL_SECONDS: int = 5
    ORDERBOOK_CACHE_TTL_SECONDS: int = 2
    CANDLE_CACHE_TTL_SECONDS: int = 60
    
    # Performance settings
    MAX_CONCURRENT_SUBSCRIPTIONS: int = 10000
    BATCH_PUBLISH_SIZE: int = 100
    AGGREGATION_WINDOW_MS: int = 50
    
    # Data retention
    TICK_DATA_RETENTION_DAYS: int = 7
    CANDLE_DATA_RETENTION_DAYS: int = 365
    
    # Monitoring
    METRICS_PORT: int = 9092
    TRACE_SAMPLE_RATE: float = 0.05  # 5% sampling
    
    class Config:
        env_prefix = "MDS_"
        case_sensitive = False 