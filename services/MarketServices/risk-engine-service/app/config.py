"""Configuration settings for Risk Engine Service."""

from pydantic import BaseSettings
from typing import List, Dict, Optional
from decimal import Decimal


class Settings(BaseSettings):
    """Risk Engine Service settings."""
    
    # Service configuration
    SERVICE_NAME: str = "risk-engine-service"
    SERVICE_PORT: int = 8021
    GRPC_PORT: int = 50051
    METRICS_PORT: int = 9091
    
    # Service identity
    service_id: str = "risk-engine-001"
    
    # Consul configuration
    CONSUL_ENABLED: bool = True
    CONSUL_HOST: str = "consul"
    CONSUL_PORT: int = 8500
    
    # Vault configuration
    VAULT_ADDR: str = "http://vault:8200"
    VAULT_ROLE: str = "risk-engine"
    VAULT_TOKEN: Optional[str] = None
    
    # Apache Ignite configuration
    IGNITE_ADDRESSES: List[str] = ["127.0.0.1:10800"]
    IGNITE_CACHE_MODE: str = "PARTITIONED"
    IGNITE_BACKUP_COUNT: int = 1
    
    # Apache Pulsar configuration
    PULSAR_URL: str = "pulsar://localhost:6650"
    PULSAR_TOPIC_RISK_EVENTS: str = "persistent://derivatives/risk/events"
    PULSAR_TOPIC_MARGIN_CALLS: str = "persistent://derivatives/risk/margin-calls"
    PULSAR_TOPIC_LIQUIDATIONS: str = "persistent://derivatives/risk/liquidations"
    
    # Cassandra configuration
    CASSANDRA_HOSTS: List[str] = ["cassandra"]
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "risk_engine"
    CASSANDRA_REPLICATION_FACTOR: int = 3
    
    # Elasticsearch configuration
    ELASTICSEARCH_URL: str = "http://elasticsearch:9200"
    ELASTICSEARCH_INDEX_PREFIX: str = "risk"
    
    # Risk calculation settings
    RISK_CALCULATION_INTERVAL_SECONDS: int = 5
    VAR_CONFIDENCE_LEVEL: float = 0.95
    CVAR_CONFIDENCE_LEVEL: float = 0.95
    LOOKBACK_DAYS: int = 30
    
    # Default risk limits
    DEFAULT_MAX_LEVERAGE: Decimal = Decimal("20")
    DEFAULT_MAX_POSITION_SIZE: Decimal = Decimal("1000000")
    DEFAULT_MIN_MARGIN_LEVEL: Decimal = Decimal("120")  # 120%
    DEFAULT_CONCENTRATION_LIMIT: Decimal = Decimal("30")  # 30% max in single asset
    
    # Margin requirements by market type
    INITIAL_MARGIN_RATES: Dict[str, Decimal] = {
        "spot": Decimal("0"),
        "futures": Decimal("0.05"),  # 5%
        "perpetual": Decimal("0.05"),
        "options": Decimal("0.10"),  # 10%
        "compute_futures": Decimal("0.07"),
        "compute_options": Decimal("0.12"),
        "prediction": Decimal("0.15"),
        "synthetic": Decimal("0.20")
    }
    
    MAINTENANCE_MARGIN_RATES: Dict[str, Decimal] = {
        "spot": Decimal("0"),
        "futures": Decimal("0.025"),  # 2.5%
        "perpetual": Decimal("0.025"),
        "options": Decimal("0.05"),  # 5%
        "compute_futures": Decimal("0.035"),
        "compute_options": Decimal("0.06"),
        "prediction": Decimal("0.075"),
        "synthetic": Decimal("0.10")
    }
    
    # Liquidation settings
    LIQUIDATION_THRESHOLD: Decimal = Decimal("100")  # 100% margin level
    LIQUIDATION_BUFFER: Decimal = Decimal("0.02")  # 2% buffer
    MAX_LIQUIDATION_SLIPPAGE: Decimal = Decimal("0.05")  # 5% max slippage
    
    # Alert thresholds
    MARGIN_CALL_THRESHOLD: Decimal = Decimal("130")  # 130% margin level
    WARNING_THRESHOLD: Decimal = Decimal("150")  # 150% margin level
    
    # Direct communication settings
    enable_direct_comm: bool = True
    direct_comm_timeout_ms: float = 1.0
    direct_comm_batch_size: int = 100
    
    # ML model settings
    ML_MODEL_UPDATE_INTERVAL: int = 3600  # 1 hour
    ML_PREDICTION_CACHE_TTL: int = 30  # 30 seconds
    ML_FEATURE_ENGINEERING_ENABLED: bool = True
    
    # Performance settings
    BATCH_SIZE: int = 1000
    MAX_CONCURRENT_CALCULATIONS: int = 10
    CACHE_TTL_SECONDS: int = 60
    
    # Monitoring settings
    TRACE_SAMPLE_RATE: float = 0.1  # 10% sampling
    METRICS_COLLECTION_INTERVAL: int = 10  # seconds
    
    # Flink integration
    FLINK_ENABLED: bool = True
    FLINK_JOBMANAGER_RPC_ADDRESS: str = "flink-jobmanager"
    FLINK_JOBMANAGER_RPC_PORT: int = 6123
    FLINK_CHECKPOINT_INTERVAL: int = 60000  # 1 minute
    
    # External service URLs
    MARKET_DATA_SERVICE_URL: str = "http://market-data-service:8080"
    POSITION_SERVICE_URL: str = "http://position-service:8080"
    ORDER_SERVICE_URL: str = "http://order-service:8080"
    
    class Config:
        """Pydantic config."""
        env_prefix = "RISK_ENGINE_"
        case_sensitive = False
        
        # Allow loading from .env file
        env_file = ".env"
        env_file_encoding = "utf-8" 