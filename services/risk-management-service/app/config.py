from pydantic import BaseSettings
from decimal import Decimal
from typing import Dict, List, Optional


class RiskManagementConfig(BaseSettings):
    # Service configuration
    SERVICE_NAME: str = "risk-management"
    SERVICE_PORT: int = 8082
    GRPC_PORT: int = 50052
    
    # Consul & Vault
    CONSUL_HOST: str = "consul"
    CONSUL_PORT: int = 8500
    VAULT_ADDR: str = "http://vault:8200"
    VAULT_ROLE: str = "risk-management"
    
    # Ignite cache settings
    IGNITE_ADDRESSES: List[str] = ["127.0.0.1:10800"]
    IGNITE_CACHE_MODE: str = "PARTITIONED"
    IGNITE_BACKUP_COUNT: int = 1
    
    # Pulsar settings
    PULSAR_URL: str = "pulsar://pulsar:6650"
    PULSAR_TOPIC_RISK_EVENTS: str = "persistent://derivatives/risk/events"
    PULSAR_TOPIC_MARGIN_CALLS: str = "persistent://derivatives/risk/margin-calls"
    PULSAR_TOPIC_LIQUIDATIONS: str = "persistent://derivatives/risk/liquidations"
    
    # Cassandra settings
    CASSANDRA_HOSTS: List[str] = ["cassandra"]
    CASSANDRA_PORT: int = 9042
    CASSANDRA_KEYSPACE: str = "risk_management"
    CASSANDRA_REPLICATION_FACTOR: int = 3
    
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
    
    # Performance settings
    BATCH_SIZE: int = 1000
    MAX_CONCURRENT_CALCULATIONS: int = 10
    CACHE_TTL_SECONDS: int = 60
    
    # Monitoring
    METRICS_PORT: int = 9091
    TRACE_SAMPLE_RATE: float = 0.1  # 10% sampling
    
    class Config:
        env_prefix = "RMS_"
        case_sensitive = False 