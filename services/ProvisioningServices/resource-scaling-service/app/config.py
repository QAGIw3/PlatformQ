"""Configuration settings for Resource Scaling Service"""

from pydantic import BaseSettings, Field
from typing import List
import os


class Settings(BaseSettings):
    """Configuration settings"""
    
    # Service info
    service_name: str = "resource-scaling-service"
    service_host: str = Field(default="0.0.0.0", env="SERVICE_HOST")
    service_port: int = Field(default=8003, env="SERVICE_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # API Configuration
    api_version: str = "v1"
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # Kubernetes Configuration
    kubernetes_namespace: str = Field(default="platformq", env="K8S_NAMESPACE")
    kubernetes_api_url: str = Field(
        default="https://kubernetes.default.svc",
        env="K8S_API_URL"
    )
    
    # Caching Configuration
    ignite_host: str = Field(default="ignite", env="IGNITE_HOST")
    ignite_port: int = Field(default=10800, env="IGNITE_PORT")
    
    # Messaging Configuration
    pulsar_url: str = Field(default="pulsar://pulsar:6650", env="PULSAR_URL")
    
    # Consul Configuration
    consul_host: str = Field(default="consul", env="CONSUL_HOST")
    consul_port: int = Field(default=8500, env="CONSUL_PORT")
    
    # Resource Monitoring Service
    monitoring_service_url: str = Field(
        default="http://resource-monitoring-service:8002",
        env="MONITORING_SERVICE_URL"
    )
    
    # Scaling Configuration
    evaluation_interval: int = Field(default=60, env="EVALUATION_INTERVAL")  # seconds
    cooldown_period: int = Field(default=300, env="COOLDOWN_PERIOD")  # seconds
    dry_run_mode: bool = Field(default=False, env="DRY_RUN_MODE")
    
    # Predictive Scaling
    enable_predictive_scaling: bool = Field(default=True, env="ENABLE_PREDICTIVE_SCALING")
    prediction_horizon_minutes: int = Field(default=30, env="PREDICTION_HORIZON_MINUTES")
    model_training_interval: int = Field(default=3600, env="MODEL_TRAINING_INTERVAL")  # seconds
    
    # Cost-aware Scaling
    enable_cost_optimization: bool = Field(default=True, env="ENABLE_COST_OPTIMIZATION")
    max_monthly_cost_increase: float = Field(default=500.0, env="MAX_MONTHLY_COST_INCREASE")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton instance
settings = Settings() 