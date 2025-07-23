"""Configuration for Unified ML Platform Service"""

from typing import List, Optional
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    """Application settings"""
    
    # Basic settings
    SERVICE_NAME: str = "ml-platform-service"
    DEBUG: bool = Field(default=False, env="DEBUG")
    PORT: int = Field(default=8000, env="PORT")
    
    # API settings
    API_V1_STR: str = "/api/v1"
    ALLOWED_ORIGINS: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        env="ALLOWED_ORIGINS"
    )
    
    # Vault/Consul settings
    VAULT_ADDR: str = Field(default="http://vault:8200", env="VAULT_ADDR")
    VAULT_TOKEN: Optional[str] = Field(default=None, env="VAULT_TOKEN")
    CONSUL_ADDR: str = Field(default="http://consul:8500", env="CONSUL_ADDR")
    
    # Pulsar settings
    PULSAR_URL: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    
    # Database settings (example - adjust as needed)
    DATABASE_URL: Optional[str] = Field(default=None, env="DATABASE_URL")
    
    # Cache settings
    REDIS_URL: Optional[str] = Field(default=None, env="REDIS_URL")
    IGNITE_URL: Optional[str] = Field(default=None, env="IGNITE_URL")
    
    # Analytics engine settings (if applicable)
    TRINO_URL: Optional[str] = Field(default=None, env="TRINO_URL")
    DRUID_URL: Optional[str] = Field(default=None, env="DRUID_URL")
    PINOT_URL: Optional[str] = Field(default=None, env="PINOT_URL")
    CLICKHOUSE_URL: Optional[str] = Field(default=None, env="CLICKHOUSE_URL")
    DORIS_URL: Optional[str] = Field(default=None, env="DORIS_URL")
    
    # ML settings (if applicable)
    MLFLOW_TRACKING_URI: Optional[str] = Field(default=None, env="MLFLOW_TRACKING_URI")
    RAY_ADDRESS: Optional[str] = Field(default=None, env="RAY_ADDRESS")
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
