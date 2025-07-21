"""Trading Core Service configuration."""

from typing import List, Dict, Optional
from decimal import Decimal
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Trading Core Service configuration."""
    
    # Service info
    service_name: str = "trading-core-service"
    service_version: str = "1.0.0"
    
    # API Configuration
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8020
    
    # Direct Communication
    enable_direct_comm: bool = True
    direct_comm_batch_size: int = 1000
    direct_comm_timeout_ms: int = 100
    
    # Apache Ignite Configuration
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_prefix: str = "trading_core"
    
    # Ignite caches
    orderbook_cache: str = "orderbook_cache"
    order_cache: str = "order_cache"
    position_cache: str = "position_cache"
    trade_cache: str = "trade_cache"
    
    # Apache Flink Configuration
    flink_jobmanager_url: str = "http://localhost:8081"
    flink_checkpoint_dir: str = "/tmp/flink-checkpoints"
    flink_state_backend: str = "rocksdb"
    flink_checkpoint_interval_ms: int = 10000
    
    # Flink state management with Ignite
    flink_ignite_state_cache: str = "flink_state_cache"
    flink_parallelism: int = 4
    
    # Apache Pulsar Configuration
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_order_events_topic: str = "persistent://public/default/order-events"
    pulsar_trade_events_topic: str = "persistent://public/default/trade-events"
    pulsar_position_events_topic: str = "persistent://public/default/position-events"
    pulsar_market_events_topic: str = "persistent://public/default/market-events"
    
    # Cassandra Configuration
    cassandra_hosts: List[str] = ["localhost"]
    cassandra_keyspace: str = "trading_core"
    cassandra_port: int = 9042
    
    # Matching Engine Configuration
    matching_engine_type: str = "price_time"  # price_time, pro_rata, time_weighted
    max_order_depth: int = 1000
    price_tick_size: Dict[str, Decimal] = {
        "default": Decimal("0.01"),
        "crypto": Decimal("0.00001"),
        "forex": Decimal("0.00001")
    }
    
    # Order Configuration
    supported_order_types: List[str] = [
        "market", "limit", "stop", "stop_limit", 
        "iceberg", "post_only", "fill_or_kill", 
        "immediate_or_cancel"
    ]
    max_order_size: Decimal = Decimal("1000000")
    min_order_size: Decimal = Decimal("0.0001")
    
    # Risk Limits
    max_position_value: Decimal = Decimal("10000000")
    max_order_value: Decimal = Decimal("1000000")
    position_limit_check: bool = True
    
    # Performance Configuration
    order_processing_batch_size: int = 100
    trade_processing_batch_size: int = 200
    position_update_batch_size: int = 50
    order_processing_threads: int = 8
    
    # Circuit Breaker Configuration
    circuit_breaker_enabled: bool = True
    price_movement_threshold: Decimal = Decimal("0.10")  # 10%
    volume_spike_threshold: Decimal = Decimal("5.0")  # 5x average
    circuit_breaker_duration_seconds: int = 300
    
    # External Services
    risk_engine_url: str = "http://localhost:8021"
    market_intelligence_url: str = "http://localhost:8022"
    
    # Monitoring
    metrics_enabled: bool = True
    metrics_port: int = 9020
    trace_enabled: bool = True
    
    # WebSocket Configuration
    ws_heartbeat_interval: int = 30
    ws_max_connections_per_user: int = 5
    
    # Data Retention
    order_retention_days: int = 30
    trade_retention_days: int = 90
    position_history_days: int = 365
    
    class Config:
        env_prefix = "TRADING_CORE_"
        case_sensitive = False
        
        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                file_secret_settings,
            )


# Create global settings instance
settings = Settings() 