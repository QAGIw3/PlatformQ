"""
Market Aggregator Service Configuration
"""
from typing import Optional, Dict, List
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service configuration settings"""
    
    # Service Info
    SERVICE_NAME: str = "market-aggregator-service"
    VERSION: str = "1.0.0"
    API_PREFIX: str = "/api/v1"
    
    # Server Configuration
    HOST: str = Field(default="0.0.0.0", env="HOST")
    PORT: int = Field(default=8028, env="PORT")
    WORKERS: int = Field(default=4, env="WORKERS")
    
    # Market Service URLs
    QUANTUM_MARKET_URL: str = Field(default="http://quantum-market-service:8024", env="QUANTUM_MARKET_URL")
    AI_MARKET_URL: str = Field(default="http://ai-compute-market-service:8025", env="AI_MARKET_URL")
    NETWORK_MARKET_URL: str = Field(default="http://network-bandwidth-market-service:8026", env="NETWORK_MARKET_URL")
    ORACLE_SERVICE_URL: str = Field(default="http://oracle-service:8027", env="ORACLE_SERVICE_URL")
    
    # Aggregation Configuration
    BUNDLE_OPTIMIZATION_ENABLED: bool = Field(default=True, env="BUNDLE_OPTIMIZATION_ENABLED")
    ARBITRAGE_DETECTION_ENABLED: bool = Field(default=True, env="ARBITRAGE_DETECTION_ENABLED")
    PRICE_CACHE_TTL: int = Field(default=60, env="PRICE_CACHE_TTL")  # seconds
    RESOURCE_SYNC_INTERVAL: int = Field(default=300, env="RESOURCE_SYNC_INTERVAL")  # seconds
    
    # Bundle Configuration
    MAX_BUNDLE_SIZE: int = Field(default=10, env="MAX_BUNDLE_SIZE")
    BUNDLE_DISCOUNT_RATE: float = Field(default=0.05, env="BUNDLE_DISCOUNT_RATE")  # 5% discount
    CROSS_RESOURCE_DISCOUNT: float = Field(default=0.03, env="CROSS_RESOURCE_DISCOUNT")  # Additional 3%
    
    # Arbitrage Configuration
    ARBITRAGE_MIN_PROFIT_MARGIN: float = Field(default=0.02, env="ARBITRAGE_MIN_PROFIT_MARGIN")  # 2%
    ARBITRAGE_EXECUTION_DELAY: int = Field(default=1, env="ARBITRAGE_EXECUTION_DELAY")  # seconds
    MAX_ARBITRAGE_VALUE: float = Field(default=10000.0, env="MAX_ARBITRAGE_VALUE")  # USD equivalent
    
    # Workload Templates
    WORKLOAD_TEMPLATES: Dict[str, Dict] = Field(
        default={
            "quantum_ml_hybrid": {
                "quantum": {"coherence_window_minutes": 10, "qubit_count": 20},
                "ai": {"accelerator_type": "GPU", "tflops": 100, "duration_hours": 2},
                "network": {"bandwidth_mbps": 1000, "latency_ms": 10}
            },
            "distributed_training": {
                "ai": {"accelerator_type": "TPU", "count": 4, "duration_hours": 24},
                "network": {"bandwidth_mbps": 10000, "latency_ms": 1}
            },
            "real_time_inference": {
                "ai": {"accelerator_type": "NPU", "tflops": 50},
                "network": {"bandwidth_mbps": 100, "latency_ms": 5, "qos": "platinum"}
            },
            "quantum_simulation": {
                "quantum": {"coherence_window_minutes": 30, "qubit_count": 50},
                "ai": {"accelerator_type": "GPU", "tflops": 200},
                "network": {"bandwidth_mbps": 5000, "latency_ms": 20}
            }
        },
        env="WORKLOAD_TEMPLATES"
    )
    
    # Optimization Configuration
    OPTIMIZATION_ALGORITHM: str = Field(default="genetic", env="OPTIMIZATION_ALGORITHM")  # genetic, simulated_annealing, greedy
    OPTIMIZATION_MAX_ITERATIONS: int = Field(default=1000, env="OPTIMIZATION_MAX_ITERATIONS")
    OPTIMIZATION_TIMEOUT: int = Field(default=30, env="OPTIMIZATION_TIMEOUT")  # seconds
    
    # Apache Ignite Configuration
    IGNITE_HOST: str = Field(default="ignite", env="IGNITE_HOST")
    IGNITE_PORT: int = Field(default=10800, env="IGNITE_PORT")
    IGNITE_CACHE_BUNDLES: str = Field(default="resource_bundles", env="IGNITE_CACHE_BUNDLES")
    IGNITE_CACHE_ARBITRAGE: str = Field(default="arbitrage_opportunities", env="IGNITE_CACHE_ARBITRAGE")
    IGNITE_CACHE_ALLOCATIONS: str = Field(default="cross_market_allocations", env="IGNITE_CACHE_ALLOCATIONS")
    
    # Apache Pulsar Configuration
    PULSAR_URL: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    PULSAR_TOPIC_BUNDLES: str = Field(default="resource-bundles", env="PULSAR_TOPIC_BUNDLES")
    PULSAR_TOPIC_ARBITRAGE: str = Field(default="arbitrage-opportunities", env="PULSAR_TOPIC_ARBITRAGE")
    PULSAR_TOPIC_ALLOCATIONS: str = Field(default="cross-market-allocations", env="PULSAR_TOPIC_ALLOCATIONS")
    PULSAR_SUBSCRIPTION: str = Field(default="market-aggregator", env="PULSAR_SUBSCRIPTION")
    
    # Blockchain Configuration
    BLOCKCHAIN_RPC_URL: str = Field(default="http://localhost:8545", env="BLOCKCHAIN_RPC_URL")
    AGGREGATOR_CONTRACT_ADDRESS: str = Field(default="", env="AGGREGATOR_CONTRACT_ADDRESS")
    BLOCKCHAIN_PRIVATE_KEY: Optional[str] = Field(default=None, env="BLOCKCHAIN_PRIVATE_KEY")
    BLOCKCHAIN_GAS_LIMIT: int = Field(default=3000000, env="BLOCKCHAIN_GAS_LIMIT")
    
    # Elasticsearch Configuration
    ELASTICSEARCH_URL: str = Field(default="http://elasticsearch:9200", env="ELASTICSEARCH_URL")
    ELASTICSEARCH_INDEX_BUNDLES: str = Field(default="resource-bundles", env="ELASTICSEARCH_INDEX_BUNDLES")
    ELASTICSEARCH_INDEX_ARBITRAGE: str = Field(default="arbitrage-history", env="ELASTICSEARCH_INDEX_ARBITRAGE")
    
    # Vault Configuration
    VAULT_URL: str = Field(default="http://vault:8200", env="VAULT_URL")
    VAULT_TOKEN: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    VAULT_PATH: str = Field(default="secret/market-aggregator", env="VAULT_PATH")
    
    # Consul Configuration
    CONSUL_HOST: str = Field(default="consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(default=8500, env="CONSUL_PORT")
    CONSUL_SERVICE_NAME: str = Field(default="market-aggregator", env="CONSUL_SERVICE_NAME")
    CONSUL_HEALTH_CHECK_INTERVAL: str = Field(default="10s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    
    # Monitoring Configuration
    PROMETHEUS_ENABLED: bool = Field(default=True, env="PROMETHEUS_ENABLED")
    METRICS_PORT: int = Field(default=9090, env="METRICS_PORT")
    
    # Resource Limits
    MAX_CONCURRENT_REQUESTS: int = Field(default=100, env="MAX_CONCURRENT_REQUESTS")
    REQUEST_TIMEOUT: int = Field(default=60, env="REQUEST_TIMEOUT")  # seconds
    MAX_BUNDLE_VALUE: float = Field(default=100000.0, env="MAX_BUNDLE_VALUE")  # USD equivalent
    
    # Cache Configuration
    CACHE_TTL_DEFAULT: int = Field(default=300, env="CACHE_TTL_DEFAULT")  # seconds
    CACHE_TTL_PRICING: int = Field(default=60, env="CACHE_TTL_PRICING")  # seconds
    CACHE_TTL_RESOURCES: int = Field(default=120, env="CACHE_TTL_RESOURCES")  # seconds
    
    # Logging Configuration
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(default="json", env="LOG_FORMAT")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings() 