"""Configuration settings for Resource Monitoring Service"""

from pydantic import BaseSettings, Field
from typing import List
import os


class Settings(BaseSettings):
    """Configuration settings"""
    
    # Service info
    service_name: str = "resource-monitoring-service"
    service_host: str = Field(default="0.0.0.0", env="SERVICE_HOST")
    service_port: int = Field(default=8002, env="SERVICE_PORT")
    environment: str = Field(default="development", env="ENVIRONMENT")
    
    # API Configuration
    api_version: str = "v1"
    cors_origins: List[str] = Field(default=["*"], env="CORS_ORIGINS")
    
    # Prometheus Configuration
    prometheus_url: str = Field(default="http://prometheus:9090", env="PROMETHEUS_URL")
    
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
    
    # Monitoring Configuration
    collection_interval: int = Field(default=30, env="COLLECTION_INTERVAL")  # seconds
    anomaly_detection_enabled: bool = Field(default=True, env="ANOMALY_DETECTION_ENABLED")
    metrics_retention_days: int = Field(default=30, env="METRICS_RETENTION_DAYS")
    
    # Anomaly thresholds
    cpu_threshold_high: float = Field(default=80.0, env="CPU_THRESHOLD_HIGH")
    memory_threshold_high: float = Field(default=85.0, env="MEMORY_THRESHOLD_HIGH")
    error_rate_threshold: float = Field(default=0.05, env="ERROR_RATE_THRESHOLD")
    response_time_threshold_ms: float = Field(default=1000.0, env="RESPONSE_TIME_THRESHOLD_MS")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton instance
settings = Settings() 