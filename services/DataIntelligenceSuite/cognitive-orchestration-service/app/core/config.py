"""Configuration settings for Cognitive Orchestration Service"""

from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service settings
    service_name: str = "cognitive-orchestration-service"
    service_version: str = "1.0.0"
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    
    # API settings
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")
    cors_origins: List[str] = Field(["*"], env="CORS_ORIGINS")
    
    # Service URLs
    data_platform_url: str = Field("http://data-platform-service:8000", env="DATA_PLATFORM_URL")
    ml_platform_url: str = Field("http://unified-ml-platform-service:8000", env="ML_PLATFORM_URL")
    graph_intelligence_url: str = Field("http://graph-intelligence-service:8000", env="GRAPH_INTELLIGENCE_URL")
    analytics_service_url: str = Field("http://analytics-service:8000", env="ANALYTICS_SERVICE_URL")
    
    # Apache Ignite settings
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field("cognitive_orchestration", env="IGNITE_CACHE_NAME")
    
    # Apache Pulsar settings
    pulsar_url: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_namespace: str = Field("platformq/orchestration", env="PULSAR_NAMESPACE")
    
    # ML Optimization settings
    optimization_interval_seconds: int = Field(300, env="OPTIMIZATION_INTERVAL")  # 5 minutes
    learning_rate: float = Field(0.001, env="LEARNING_RATE")
    exploration_rate: float = Field(0.1, env="EXPLORATION_RATE")
    model_update_threshold: float = Field(0.05, env="MODEL_UPDATE_THRESHOLD")
    
    # Resource monitoring settings
    metrics_collection_interval: int = Field(60, env="METRICS_INTERVAL")  # 1 minute
    resource_prediction_window: int = Field(3600, env="PREDICTION_WINDOW")  # 1 hour
    anomaly_detection_sensitivity: float = Field(0.95, env="ANOMALY_SENSITIVITY")
    
    # Workflow optimization settings
    max_parallel_workflows: int = Field(100, env="MAX_PARALLEL_WORKFLOWS")
    workflow_timeout_seconds: int = Field(3600, env="WORKFLOW_TIMEOUT")
    optimization_lookback_days: int = Field(30, env="OPTIMIZATION_LOOKBACK_DAYS")
    
    # Business rules
    cost_weight: float = Field(0.3, env="COST_WEIGHT")
    performance_weight: float = Field(0.5, env="PERFORMANCE_WEIGHT")
    reliability_weight: float = Field(0.2, env="RELIABILITY_WEIGHT")
    
    # Cache settings
    cache_ttl_seconds: int = Field(300, env="CACHE_TTL")
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    
    # Security
    jwt_secret_key: str = Field("your-secret-key-here", env="JWT_SECRET_KEY")
    api_key_header: str = Field("X-API-Key", env="API_KEY_HEADER")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings() 