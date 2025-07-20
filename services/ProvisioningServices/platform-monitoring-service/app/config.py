"""Configuration for Platform Monitoring Service"""

from pydantic_settings import BaseSettings
from typing import List, Dict, Optional


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service settings
    SERVICE_NAME: str = "platform-monitoring-service"
    SERVICE_PORT: int = 9090
    LOG_LEVEL: str = "INFO"
    DEBUG: bool = False
    
    # Thanos configuration
    THANOS_QUERY_URL: str = "http://thanos-query:10902"
    THANOS_STORE_URL: str = "http://thanos-store:10901"
    THANOS_COMPACT_URL: str = "http://thanos-compact:10902"
    THANOS_RULER_URL: str = "http://thanos-ruler:10902"
    
    # MinIO configuration for Thanos object storage
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_BUCKET: str = "thanos-metrics"
    MINIO_SECURE: bool = False
    
    # Prometheus federation settings
    PROMETHEUS_GLOBAL_URL: str = "http://prometheus-global:9090"
    PROMETHEUS_RETENTION_TIME: str = "15d"
    PROMETHEUS_SCRAPE_INTERVAL: str = "30s"
    PROMETHEUS_EVALUATION_INTERVAL: str = "30s"
    
    # Service discovery
    CONSUL_URL: str = "http://consul:8500"
    CONSUL_TOKEN: Optional[str] = None
    
    # Regional configuration
    REGIONS: List[str] = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
    DEFAULT_REGION: str = "us-east-1"
    
    # Alert configuration
    ALERTMANAGER_URL: str = "http://alertmanager:9093"
    
    # Grafana configuration
    GRAFANA_URL: str = "http://grafana:3000"
    GRAFANA_API_KEY: Optional[str] = None
    
    # Database for storing monitoring metadata
    DATABASE_URL: str = "postgresql://monitoring:password@postgres:5432/monitoring"
    
    # Cache settings
    REDIS_URL: str = "redis://redis:6379/0"
    CACHE_TTL: int = 300  # 5 minutes
    
    # Multi-tenancy settings
    ENABLE_TENANT_ISOLATION: bool = True
    TENANT_LABEL_KEY: str = "tenant_id"
    
    # Query limits
    MAX_QUERY_LOOKBACK: str = "90d"
    MAX_QUERY_SAMPLES: int = 50000000
    QUERY_TIMEOUT: int = 120  # seconds
    
    # Federation sync settings
    FEDERATION_SYNC_INTERVAL: int = 60  # seconds
    FEDERATION_CONFIG_PATH: str = "/etc/prometheus/federation"
    
    # Object storage configuration for metrics
    S3_ENDPOINT: str = "http://minio:9000"
    S3_ACCESS_KEY: str = "minioadmin"
    S3_SECRET_KEY: str = "minioadmin"
    S3_BUCKET: str = "platform-metrics"
    S3_REGION: str = "us-east-1"
    
    # Thanos compaction settings
    COMPACTION_RETENTION_1H: str = "72h"
    COMPACTION_RETENTION_5M: str = "14d"
    COMPACTION_RETENTION_1D: str = "90d"
    
    # Service mesh observability
    ENABLE_SERVICE_MESH_METRICS: bool = True
    CONSUL_MESH_NAMESPACE: str = "platform-q"
    
    # Tracing configuration
    ENABLE_TRACING: bool = True
    JAEGER_ENDPOINT: str = "http://jaeger:14268/api/traces"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings() 