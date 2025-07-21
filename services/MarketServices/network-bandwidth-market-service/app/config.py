"""
Network Bandwidth Market Service Configuration
"""
from typing import Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service configuration settings"""
    
    # Service Info
    SERVICE_NAME: str = "network-bandwidth-market-service"
    VERSION: str = "1.0.0"
    API_PREFIX: str = "/api/v1"
    
    # Server Configuration
    HOST: str = Field(default="0.0.0.0", env="HOST")
    PORT: int = Field(default=8026, env="PORT")
    WORKERS: int = Field(default=4, env="WORKERS")
    
    # Network Path Configuration
    MAX_PATH_HOPS: int = Field(default=10, env="MAX_PATH_HOPS")
    PATH_DISCOVERY_INTERVAL: int = Field(default=300, env="PATH_DISCOVERY_INTERVAL")  # seconds
    PATH_RELIABILITY_THRESHOLD: float = Field(default=0.95, env="PATH_RELIABILITY_THRESHOLD")
    PATH_CACHE_TTL: int = Field(default=600, env="PATH_CACHE_TTL")  # seconds
    
    # Bandwidth Configuration
    MIN_BANDWIDTH_ALLOCATION: int = Field(default=10, env="MIN_BANDWIDTH_ALLOCATION")  # Mbps
    MAX_BANDWIDTH_ALLOCATION: int = Field(default=10000, env="MAX_BANDWIDTH_ALLOCATION")  # Mbps
    BURST_MULTIPLIER: float = Field(default=2.0, env="BURST_MULTIPLIER")
    BURST_DURATION_LIMIT: int = Field(default=3600, env="BURST_DURATION_LIMIT")  # seconds
    BANDWIDTH_ALLOCATION_TIMEOUT: int = Field(default=30, env="BANDWIDTH_ALLOCATION_TIMEOUT")  # seconds
    
    # Circuit Configuration
    CIRCUIT_SETUP_TIME: int = Field(default=300, env="CIRCUIT_SETUP_TIME")  # seconds
    CIRCUIT_MIN_DURATION: int = Field(default=3600, env="CIRCUIT_MIN_DURATION")  # seconds
    CIRCUIT_MAX_DURATION: int = Field(default=2592000, env="CIRCUIT_MAX_DURATION")  # 30 days
    CIRCUIT_HEALTH_CHECK_INTERVAL: int = Field(default=60, env="CIRCUIT_HEALTH_CHECK_INTERVAL")  # seconds
    
    # QoS Configuration
    QOS_ENFORCEMENT_INTERVAL: int = Field(default=10, env="QOS_ENFORCEMENT_INTERVAL")  # seconds
    QOS_VIOLATION_THRESHOLD: int = Field(default=3, env="QOS_VIOLATION_THRESHOLD")  # consecutive violations
    QOS_CLASS_MULTIPLIERS: dict = Field(
        default={
            "best_effort": 1.0,
            "bronze": 1.5,
            "silver": 2.0,
            "gold": 3.0,
            "platinum": 5.0
        },
        env="QOS_CLASS_MULTIPLIERS"
    )
    
    # Pricing Configuration
    BASE_BANDWIDTH_RATE: float = Field(default=0.001, env="BASE_BANDWIDTH_RATE")  # per Mbps per hour
    BURST_RATE_MULTIPLIER: float = Field(default=3.0, env="BURST_RATE_MULTIPLIER")
    CONGESTION_THRESHOLD: float = Field(default=0.8, env="CONGESTION_THRESHOLD")
    LATENCY_PREMIUM_FACTOR: float = Field(default=1.5, env="LATENCY_PREMIUM_FACTOR")
    TIME_OF_DAY_MULTIPLIERS: dict = Field(
        default={
            "peak": 1.5,      # 9am-5pm
            "standard": 1.0,  # 5pm-12am
            "off_peak": 0.7   # 12am-9am
        },
        env="TIME_OF_DAY_MULTIPLIERS"
    )
    
    # Congestion Management
    CONGESTION_CHECK_INTERVAL: int = Field(default=30, env="CONGESTION_CHECK_INTERVAL")  # seconds
    CONGESTION_PREDICTION_WINDOW: int = Field(default=3600, env="CONGESTION_PREDICTION_WINDOW")  # seconds
    CONGESTION_ALERT_THRESHOLD: float = Field(default=0.9, env="CONGESTION_ALERT_THRESHOLD")
    
    # Latency Configuration
    LATENCY_MEASUREMENT_INTERVAL: int = Field(default=5, env="LATENCY_MEASUREMENT_INTERVAL")  # seconds
    LATENCY_HISTORY_SIZE: int = Field(default=1000, env="LATENCY_HISTORY_SIZE")
    LATENCY_SLA_BUFFER: float = Field(default=0.9, env="LATENCY_SLA_BUFFER")  # 90% of guaranteed
    
    # Settlement Configuration
    SETTLEMENT_INTERVAL: int = Field(default=3600, env="SETTLEMENT_INTERVAL")  # seconds
    SETTLEMENT_BATCH_SIZE: int = Field(default=100, env="SETTLEMENT_BATCH_SIZE")
    SLA_CREDIT_RATE: float = Field(default=0.1, env="SLA_CREDIT_RATE")  # 10% credit per violation
    
    # Apache Ignite Configuration
    IGNITE_HOST: str = Field(default="ignite", env="IGNITE_HOST")
    IGNITE_PORT: int = Field(default=10800, env="IGNITE_PORT")
    IGNITE_CACHE_PATH_STATE: str = Field(default="network_path_state", env="IGNITE_CACHE_PATH_STATE")
    IGNITE_CACHE_BANDWIDTH: str = Field(default="bandwidth_allocations", env="IGNITE_CACHE_BANDWIDTH")
    IGNITE_CACHE_CIRCUITS: str = Field(default="dedicated_circuits", env="IGNITE_CACHE_CIRCUITS")
    IGNITE_CACHE_CONGESTION: str = Field(default="congestion_metrics", env="IGNITE_CACHE_CONGESTION")
    
    # Apache Pulsar Configuration
    PULSAR_URL: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    PULSAR_TOPIC_BANDWIDTH: str = Field(default="network-bandwidth-events", env="PULSAR_TOPIC_BANDWIDTH")
    PULSAR_TOPIC_CIRCUITS: str = Field(default="circuit-events", env="PULSAR_TOPIC_CIRCUITS")
    PULSAR_TOPIC_CONGESTION: str = Field(default="congestion-events", env="PULSAR_TOPIC_CONGESTION")
    PULSAR_TOPIC_LATENCY: str = Field(default="latency-events", env="PULSAR_TOPIC_LATENCY")
    PULSAR_SUBSCRIPTION: str = Field(default="network-bandwidth-service", env="PULSAR_SUBSCRIPTION")
    
    # Apache Flink Configuration
    FLINK_ENABLED: bool = Field(default=True, env="FLINK_ENABLED")
    FLINK_JOB_MANAGER: str = Field(default="http://flink-jobmanager:8081", env="FLINK_JOB_MANAGER")
    FLINK_CONGESTION_JOB: str = Field(default="congestion-prediction", env="FLINK_CONGESTION_JOB")
    FLINK_TRAFFIC_JOB: str = Field(default="traffic-analytics", env="FLINK_TRAFFIC_JOB")
    
    # Blockchain Configuration
    BLOCKCHAIN_RPC_URL: str = Field(default="http://localhost:8545", env="BLOCKCHAIN_RPC_URL")
    NETWORK_BANDWIDTH_CONTRACT: str = Field(default="", env="NETWORK_BANDWIDTH_CONTRACT")
    EXTENDED_RESOURCE_TOKEN_CONTRACT: str = Field(default="", env="EXTENDED_RESOURCE_TOKEN_CONTRACT")
    BLOCKCHAIN_PRIVATE_KEY: Optional[str] = Field(default=None, env="BLOCKCHAIN_PRIVATE_KEY")
    BLOCKCHAIN_GAS_LIMIT: int = Field(default=3000000, env="BLOCKCHAIN_GAS_LIMIT")
    BLOCKCHAIN_CONFIRMATION_BLOCKS: int = Field(default=3, env="BLOCKCHAIN_CONFIRMATION_BLOCKS")
    
    # Elasticsearch Configuration
    ELASTICSEARCH_URL: str = Field(default="http://elasticsearch:9200", env="ELASTICSEARCH_URL")
    ELASTICSEARCH_INDEX_PATHS: str = Field(default="network-paths", env="ELASTICSEARCH_INDEX_PATHS")
    ELASTICSEARCH_INDEX_METRICS: str = Field(default="bandwidth-metrics", env="ELASTICSEARCH_INDEX_METRICS")
    
    # MinIO Configuration
    MINIO_ENDPOINT: str = Field(default="minio:9000", env="MINIO_ENDPOINT")
    MINIO_ACCESS_KEY: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    MINIO_BUCKET_REPORTS: str = Field(default="network-reports", env="MINIO_BUCKET_REPORTS")
    MINIO_BUCKET_BACKUPS: str = Field(default="network-backups", env="MINIO_BUCKET_BACKUPS")
    
    # Vault Configuration
    VAULT_URL: str = Field(default="http://vault:8200", env="VAULT_URL")
    VAULT_TOKEN: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    VAULT_PATH: str = Field(default="secret/network-bandwidth", env="VAULT_PATH")
    
    # Consul Configuration
    CONSUL_HOST: str = Field(default="consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(default=8500, env="CONSUL_PORT")
    CONSUL_SERVICE_NAME: str = Field(default="network-bandwidth-market", env="CONSUL_SERVICE_NAME")
    CONSUL_HEALTH_CHECK_INTERVAL: str = Field(default="10s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    
    # Monitoring Configuration
    PROMETHEUS_ENABLED: bool = Field(default=True, env="PROMETHEUS_ENABLED")
    METRICS_PORT: int = Field(default=9090, env="METRICS_PORT")
    JAEGER_ENABLED: bool = Field(default=True, env="JAEGER_ENABLED")
    JAEGER_AGENT_HOST: str = Field(default="jaeger", env="JAEGER_AGENT_HOST")
    JAEGER_AGENT_PORT: int = Field(default=6831, env="JAEGER_AGENT_PORT")
    
    # Resource Limits
    MAX_PATHS_PER_REQUEST: int = Field(default=100, env="MAX_PATHS_PER_REQUEST")
    MAX_ALLOCATIONS_PER_USER: int = Field(default=50, env="MAX_ALLOCATIONS_PER_USER")
    MAX_CIRCUITS_PER_USER: int = Field(default=10, env="MAX_CIRCUITS_PER_USER")
    MAX_BURST_REQUESTS_PER_HOUR: int = Field(default=20, env="MAX_BURST_REQUESTS_PER_HOUR")
    
    # Cache Configuration
    REDIS_URL: Optional[str] = Field(default=None, env="REDIS_URL")  # Optional for local caching
    CACHE_TTL_DEFAULT: int = Field(default=300, env="CACHE_TTL_DEFAULT")  # seconds
    CACHE_TTL_PRICING: int = Field(default=60, env="CACHE_TTL_PRICING")  # seconds
    
    # Background Task Configuration
    BACKGROUND_TASK_INTERVAL: int = Field(default=60, env="BACKGROUND_TASK_INTERVAL")  # seconds
    BACKGROUND_TASK_TIMEOUT: int = Field(default=300, env="BACKGROUND_TASK_TIMEOUT")  # seconds
    
    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(default="json", env="LOG_FORMAT")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings() 