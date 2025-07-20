from pydantic import BaseSettings
from decimal import Decimal
from typing import Dict, List, Optional


class OrderMatchingConfig(BaseSettings):
    # Performance settings
    MAX_ORDER_BOOK_DEPTH: int = 1000
    MATCHING_BATCH_SIZE: int = 100
    TICK_INTERVAL_MS: int = 10
    ORDER_QUEUE_SIZE: int = 100000
    USE_MEMORY_POOL: bool = True
    PREALLOCATE_ORDERS: int = 10000
    
    # Ignite cache settings
    IGNITE_ADDRESSES: List[str] = ["127.0.0.1:10800"]
    IGNITE_CACHE_MODE: str = "PARTITIONED"
    IGNITE_BACKUP_COUNT: int = 1
    IGNITE_WRITE_SYNC_MODE: str = "PRIMARY_SYNC"
    IGNITE_ATOMIC_MODE: str = "TRANSACTIONAL"
    IGNITE_PARTITION_LOSS_POLICY: str = "READ_WRITE_SAFE"
    
    # Service settings
    SERVICE_NAME: str = "order-matching"
    SERVICE_PORT: int = 8080
    GRPC_PORT: int = 50051
    WEBSOCKET_PORT: int = 8081
    
    # Consul & Vault
    CONSUL_HOST: str = "consul"
    CONSUL_PORT: int = 8500
    VAULT_ADDR: str = "http://vault:8200"
    VAULT_ROLE: str = "order-matching"
    
    # Pulsar settings
    PULSAR_URL: str = "pulsar://pulsar:6650"
    PULSAR_TOPIC_TRADES: str = "persistent://derivatives/trading/trades"
    PULSAR_TOPIC_ORDERS: str = "persistent://derivatives/trading/orders"
    PULSAR_TOPIC_MARKET_DATA: str = "persistent://derivatives/trading/market-data"
    PULSAR_BATCH_SIZE: int = 1000
    PULSAR_BATCH_TIMEOUT_MS: int = 10
    
    # Performance monitoring
    METRICS_PORT: int = 9090
    TRACE_SAMPLE_RATE: float = 0.01  # 1% sampling for ultra-low latency
    LATENCY_HISTOGRAM_BUCKETS: List[float] = [0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
    
    # Trading settings
    MIN_TICK_SIZE: Dict[str, Decimal] = {
        "default": Decimal("0.01"),
        "BTC": Decimal("0.001"),
        "ETH": Decimal("0.001")
    }
    MAX_ORDER_SIZE: Decimal = Decimal("1000000")
    PRICE_PRECISION: int = 8
    QUANTITY_PRECISION: int = 8
    
    # Circuit breaker settings
    CIRCUIT_BREAKER_ENABLED: bool = True
    PRICE_LIMIT_PERCENTAGE: Decimal = Decimal("0.10")  # 10% price limit
    VOLUME_SPIKE_THRESHOLD: Decimal = Decimal("5.0")  # 5x normal volume
    HALT_DURATION_SECONDS: int = 300  # 5 minute trading halt
    
    # Market hours (UTC)
    MARKET_OPEN_HOUR: int = 0  # 24/7 for crypto
    MARKET_CLOSE_HOUR: int = 24
    MAINTENANCE_WINDOW_START: int = 23  # 11 PM UTC
    MAINTENANCE_WINDOW_DURATION: int = 15  # 15 minutes
    
    class Config:
        env_prefix = "OMS_"
        case_sensitive = False 