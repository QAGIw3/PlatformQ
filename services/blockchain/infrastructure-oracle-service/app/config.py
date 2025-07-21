"""Configuration for Infrastructure Oracle Service"""

from pydantic_settings import BaseSettings
from typing import List, Optional
import os


class Settings(BaseSettings):
    # Service Configuration
    service_name: str = "infrastructure-oracle-service"
    service_port: int = 8095
    environment: str = "development"
    
    # Blockchain Configuration
    chain_id: int = 1  # Ethereum mainnet
    chain_name: str = "ethereum"
    rpc_url: str = os.getenv("RPC_URL", "http://localhost:8545")
    oracle_contract_address: str = os.getenv("ORACLE_CONTRACT_ADDRESS", "")
    oracle_private_key: str = os.getenv("ORACLE_PRIVATE_KEY", "")
    
    # Data Sources
    cloudkitty_url: str = os.getenv("CLOUDKITTY_URL", "http://cloudkitty:8889")
    prometheus_url: str = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
    market_data_url: str = os.getenv("MARKET_DATA_URL", "http://market-data-service:8080")
    
    # Spot Price Providers
    spot_price_providers: List[str] = [
        "https://api.aws.amazon.com/pricing",
        "https://api.gcp.com/pricing",
        "https://api.azure.com/pricing"
    ]
    
    # Oracle Configuration
    update_interval_seconds: int = 300  # 5 minutes
    price_aggregation_method: str = "weighted_average"  # or "median", "mean"
    minimum_data_sources: int = 2  # Minimum sources required for price update
    price_deviation_threshold: float = 0.1  # 10% max deviation
    
    # Cache Configuration
    ignite_host: str = os.getenv("IGNITE_HOST", "ignite")
    ignite_port: int = 10800
    cache_ttl_seconds: int = 300
    
    # API Configuration
    cors_origins: List[str] = ["*"]
    api_prefix: str = "/api/v1"
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    
    # Security
    require_auth: bool = True
    jwt_secret: str = os.getenv("JWT_SECRET", "dev-secret")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings() 