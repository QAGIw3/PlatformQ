"""
Service configuration using Pydantic settings
"""

from typing import List, Optional
from pydantic import BaseSettings, Field
from data_intelligence_common.core.config import BaseServiceConfig


class Settings(BaseServiceConfig):
    """Service settings with environment variable support"""
    
    # Service identification
    SERVICE_NAME: str = "data-governance-service"
    SERVICE_VERSION: str = "2.0.0"
    
    # API settings
    API_V1_PREFIX: str = "/api/v1"
    API_V2_PREFIX: str = "/api/v2"
    
    # Service-specific settings
    ENABLE_CACHING: bool = True
    CACHE_TTL_SECONDS: int = 300
    
    # Performance settings
    MAX_WORKERS: int = Field(default=4, ge=1, le=32)
    REQUEST_TIMEOUT: int = Field(default=30, ge=1, le=300)
    
    # Feature flags
    ENABLE_ML_FEATURES: bool = True
    ENABLE_STREAMING: bool = True
    ENABLE_BATCH_PROCESSING: bool = True
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()
