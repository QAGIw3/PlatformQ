"""
Presentation Service Configuration
"""

import os
from typing import List
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # Service info
    service_name: str = "presentation-service"
    service_host: str = os.getenv("SERVICE_HOST", "0.0.0.0")
    service_port: int = int(os.getenv("SERVICE_PORT", "8054"))
    instance_id: str = os.getenv("INSTANCE_ID", "presentation-1")
    
    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/presentation_db"
    )
    
    # Service URLs
    credential_service_url: str = os.getenv(
        "CREDENTIAL_SERVICE_URL",
        "http://localhost:8050"
    )
    zkp_service_url: str = os.getenv(
        "ZKP_SERVICE_URL",
        "http://localhost:8052"
    )
    did_service_url: str = os.getenv(
        "DID_SERVICE_URL",
        "http://localhost:8051"
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
    
    # Apache Ignite (for session management)
    ignite_host: str = os.getenv("IGNITE_HOST", "localhost")
    ignite_port: int = int(os.getenv("IGNITE_PORT", "10800"))
    session_ttl_seconds: int = int(os.getenv("SESSION_TTL_SECONDS", "3600"))
    
    # Event bus (Pulsar)
    enable_events: bool = os.getenv("ENABLE_EVENTS", "true").lower() == "true"
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    
    # CORS
    cors_origins: List[str] = ["*"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create settings instance
settings = Settings() 