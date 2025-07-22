"""
SBT Service Configuration
"""

import os
from typing import List
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # Service info
    service_name: str = "sbt-service"
    service_host: str = os.getenv("SERVICE_HOST", "0.0.0.0")
    service_port: int = int(os.getenv("SERVICE_PORT", "8053"))
    instance_id: str = os.getenv("INSTANCE_ID", "sbt-1")
    
    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/sbt_db"
    )
    
    # Service URLs
    blockchain_connector_url: str = os.getenv(
        "BLOCKCHAIN_CONNECTOR_URL",
        "http://localhost:8020"
    )
    credential_service_url: str = os.getenv(
        "CREDENTIAL_SERVICE_URL",
        "http://localhost:8050"
    )
    storage_service_url: str = os.getenv(
        "STORAGE_SERVICE_URL",
        "http://localhost:8015"
    )
    
    # Vault configuration
    enable_vault: bool = os.getenv("ENABLE_VAULT", "true").lower() == "true"
    vault_addr: str = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token: str = os.getenv("VAULT_TOKEN", "")
    require_vault: bool = os.getenv("REQUIRE_VAULT", "false").lower() == "true"
    
    # Consul configuration
    enable_consul: bool = os.getenv("ENABLE_CONSUL", "true").lower() == "true"
    consul_host: str = os.getenv("CONSUL_HOST", "localhost")
    consul_port: int = int(os.getenv("CONSUL_PORT", "8500"))
    enable_consul_config: bool = os.getenv("ENABLE_CONSUL_CONFIG", "true").lower() == "true"
    
    # Event bus (Pulsar)
    enable_events: bool = os.getenv("ENABLE_EVENTS", "true").lower() == "true"
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    
    # Smart contract addresses
    ethereum_sbt_contract: str = os.getenv(
        "ETHEREUM_SBT_CONTRACT",
        "0x0000000000000000000000000000000000000000"
    )
    polygon_sbt_contract: str = os.getenv(
        "POLYGON_SBT_CONTRACT",
        "0x0000000000000000000000000000000000000000"
    )
    avalanche_sbt_contract: str = os.getenv(
        "AVALANCHE_SBT_CONTRACT",
        "0x0000000000000000000000000000000000000000"
    )
    binance_sbt_contract: str = os.getenv(
        "BINANCE_SBT_CONTRACT",
        "0x0000000000000000000000000000000000000000"
    )
    
    # CORS
    cors_origins: List[str] = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create settings instance
settings = Settings() 