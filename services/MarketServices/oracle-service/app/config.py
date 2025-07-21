"""
Oracle Service Configuration
"""
from typing import Optional, Dict
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Oracle service configuration settings"""
    
    # Service Info
    SERVICE_NAME: str = "oracle-service"
    VERSION: str = "1.0.0"
    API_PREFIX: str = "/api/v1"
    
    # Server Configuration
    HOST: str = Field(default="0.0.0.0", env="HOST")
    PORT: int = Field(default=8027, env="PORT")
    WORKERS: int = Field(default=4, env="WORKERS")
    
    # Oracle Configuration
    MEASUREMENT_INTERVAL: int = Field(default=60, env="MEASUREMENT_INTERVAL")  # seconds
    VERIFICATION_THRESHOLD: int = Field(default=3, env="VERIFICATION_THRESHOLD")  # confirmations
    QUALITY_UPDATE_INTERVAL: int = Field(default=300, env="QUALITY_UPDATE_INTERVAL")  # seconds
    
    # Quantum Oracle Configuration
    QUANTUM_FIDELITY_THRESHOLD: float = Field(default=0.95, env="QUANTUM_FIDELITY_THRESHOLD")
    QUANTUM_ERROR_THRESHOLD: float = Field(default=0.01, env="QUANTUM_ERROR_THRESHOLD")
    COHERENCE_MEASUREMENT_SAMPLES: int = Field(default=100, env="COHERENCE_MEASUREMENT_SAMPLES")
    
    # AI Oracle Configuration
    AI_BENCHMARK_TIMEOUT: int = Field(default=300, env="AI_BENCHMARK_TIMEOUT")  # seconds
    AI_PERFORMANCE_SAMPLES: int = Field(default=10, env="AI_PERFORMANCE_SAMPLES")
    THERMAL_THRESHOLD_C: float = Field(default=85.0, env="THERMAL_THRESHOLD_C")
    POWER_MEASUREMENT_INTERVAL: int = Field(default=10, env="POWER_MEASUREMENT_INTERVAL")  # seconds
    
    # Network Oracle Configuration
    NETWORK_PING_COUNT: int = Field(default=10, env="NETWORK_PING_COUNT")
    NETWORK_BANDWIDTH_TEST_DURATION: int = Field(default=30, env="NETWORK_BANDWIDTH_TEST_DURATION")  # seconds
    PACKET_LOSS_THRESHOLD: float = Field(default=0.001, env="PACKET_LOSS_THRESHOLD")
    JITTER_MEASUREMENT_SAMPLES: int = Field(default=100, env="JITTER_MEASUREMENT_SAMPLES")
    
    # Blockchain Configuration
    BLOCKCHAIN_RPC_URL: str = Field(default="http://localhost:8545", env="BLOCKCHAIN_RPC_URL")
    ORACLE_CONTRACT_ADDRESS: str = Field(default="", env="ORACLE_CONTRACT_ADDRESS")
    BLOCKCHAIN_PRIVATE_KEY: Optional[str] = Field(default=None, env="BLOCKCHAIN_PRIVATE_KEY")
    BLOCKCHAIN_GAS_LIMIT: int = Field(default=500000, env="BLOCKCHAIN_GAS_LIMIT")
    CHAIN_ID: int = Field(default=1, env="CHAIN_ID")
    
    # DeFi Oracle Configuration
    ORACLE_PRIVATE_KEY: Optional[str] = Field(default=None, env="ORACLE_PRIVATE_KEY")
    ORACLE_SIGNING_KEY: str = Field(default="", env="ORACLE_SIGNING_KEY")
    
    # Oracle Contract Addresses
    QUANTUM_ORACLE_ADDRESS: str = Field(default="", env="QUANTUM_ORACLE_ADDRESS")
    AI_ORACLE_ADDRESS: str = Field(default="", env="AI_ORACLE_ADDRESS")
    NETWORK_ORACLE_ADDRESS: str = Field(default="", env="NETWORK_ORACLE_ADDRESS")
    QUALITY_ORACLE_ADDRESS: str = Field(default="", env="QUALITY_ORACLE_ADDRESS")
    AVAILABILITY_MONITOR_ADDRESS: str = Field(default="", env="AVAILABILITY_MONITOR_ADDRESS")
    PRICE_ORACLE_ADDRESS: str = Field(default="", env="PRICE_ORACLE_ADDRESS")
    PERFORMANCE_ORACLE_ADDRESS: str = Field(default="", env="PERFORMANCE_ORACLE_ADDRESS")
    
    # Market Addresses
    QUANTUM_MARKET_ADDRESS: str = Field(default="", env="QUANTUM_MARKET_ADDRESS")
    AI_MARKET_ADDRESS: str = Field(default="", env="AI_MARKET_ADDRESS")
    NETWORK_MARKET_ADDRESS: str = Field(default="", env="NETWORK_MARKET_ADDRESS")
    
    # AMM Addresses
    QUANTUM_AMM_ADDRESS: str = Field(default="", env="QUANTUM_AMM_ADDRESS")
    AI_AMM_ADDRESS: str = Field(default="", env="AI_AMM_ADDRESS")
    NETWORK_AMM_ADDRESS: str = Field(default="", env="NETWORK_AMM_ADDRESS")
    
    # Availability Monitor Configuration
    AVAILABILITY_CHECK_INTERVAL: int = Field(default=60, env="AVAILABILITY_CHECK_INTERVAL")  # seconds
    
    # Data Aggregation
    AGGREGATION_METHOD: str = Field(default="median", env="AGGREGATION_METHOD")  # median, mean, weighted
    OUTLIER_DETECTION_ENABLED: bool = Field(default=True, env="OUTLIER_DETECTION_ENABLED")
    OUTLIER_ZSCORE_THRESHOLD: float = Field(default=3.0, env="OUTLIER_ZSCORE_THRESHOLD")
    
    # Apache Ignite Configuration
    IGNITE_HOST: str = Field(default="ignite", env="IGNITE_HOST")
    IGNITE_PORT: int = Field(default=10800, env="IGNITE_PORT")
    IGNITE_CACHE_MEASUREMENTS: str = Field(default="oracle_measurements", env="IGNITE_CACHE_MEASUREMENTS")
    IGNITE_CACHE_QUALITY_SCORES: str = Field(default="quality_scores", env="IGNITE_CACHE_QUALITY_SCORES")
    
    # Apache Pulsar Configuration
    PULSAR_URL: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    PULSAR_TOPIC_MEASUREMENTS: str = Field(default="oracle-measurements", env="PULSAR_TOPIC_MEASUREMENTS")
    PULSAR_TOPIC_QUALITY_UPDATES: str = Field(default="quality-updates", env="PULSAR_TOPIC_QUALITY_UPDATES")
    PULSAR_SUBSCRIPTION: str = Field(default="oracle-service", env="PULSAR_SUBSCRIPTION")
    
    # Elasticsearch Configuration
    ELASTICSEARCH_URL: str = Field(default="http://elasticsearch:9200", env="ELASTICSEARCH_URL")
    ELASTICSEARCH_INDEX_MEASUREMENTS: str = Field(default="oracle-measurements", env="ELASTICSEARCH_INDEX_MEASUREMENTS")
    ELASTICSEARCH_INDEX_QUALITY: str = Field(default="resource-quality", env="ELASTICSEARCH_INDEX_QUALITY")
    
    # MinIO Configuration
    MINIO_ENDPOINT: str = Field(default="minio:9000", env="MINIO_ENDPOINT")
    MINIO_ACCESS_KEY: str = Field(default="minioadmin", env="MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = Field(default="minioadmin", env="MINIO_SECRET_KEY")
    MINIO_BUCKET_REPORTS: str = Field(default="oracle-reports", env="MINIO_BUCKET_REPORTS")
    
    # Vault Configuration
    VAULT_URL: str = Field(default="http://vault:8200", env="VAULT_URL")
    VAULT_TOKEN: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    VAULT_PATH: str = Field(default="secret/oracle", env="VAULT_PATH")
    
    # Consul Configuration
    CONSUL_HOST: str = Field(default="consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(default=8500, env="CONSUL_PORT")
    CONSUL_SERVICE_NAME: str = Field(default="oracle-service", env="CONSUL_SERVICE_NAME")
    CONSUL_HEALTH_CHECK_INTERVAL: str = Field(default="10s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    
    # Monitoring Configuration
    PROMETHEUS_ENABLED: bool = Field(default=True, env="PROMETHEUS_ENABLED")
    METRICS_PORT: int = Field(default=9090, env="METRICS_PORT")
    JAEGER_ENABLED: bool = Field(default=True, env="JAEGER_ENABLED")
    JAEGER_AGENT_HOST: str = Field(default="jaeger", env="JAEGER_AGENT_HOST")
    JAEGER_AGENT_PORT: int = Field(default=6831, env="JAEGER_AGENT_PORT")
    
    # Security Configuration
    API_KEY_HEADER: str = Field(default="X-Oracle-API-Key", env="API_KEY_HEADER")
    REQUIRE_API_KEY: bool = Field(default=True, env="REQUIRE_API_KEY")
    TRUSTED_ORACLES: list = Field(default=[], env="TRUSTED_ORACLES")
    VALID_API_KEYS: list = Field(default=[], env="VALID_API_KEYS")
    
    # Resource Limits
    MAX_MEASUREMENTS_PER_REQUEST: int = Field(default=1000, env="MAX_MEASUREMENTS_PER_REQUEST")
    MEASUREMENT_RETENTION_DAYS: int = Field(default=30, env="MEASUREMENT_RETENTION_DAYS")
    
    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(default="json", env="LOG_FORMAT")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings() 