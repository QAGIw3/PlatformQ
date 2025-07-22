"""Configuration for Unified Quality Service"""

from typing import List, Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service settings
    service_name: str = "unified-quality-service"
    service_version: str = "1.0.0"
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # API settings
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8003, env="API_PORT")
    cors_origins: List[str] = Field(["*"], env="CORS_ORIGINS")
    
    # ML Configuration
    ml_anomaly_models: List[str] = Field(
        ["isolation_forest", "prophet", "lstm"],
        env="ML_ANOMALY_MODELS"
    )
    ml_auto_retrain: bool = Field(True, env="ML_AUTO_RETRAIN")
    ml_retrain_interval: int = Field(86400, env="ML_RETRAIN_INTERVAL")  # 24 hours
    ml_model_path: str = Field("/models", env="ML_MODEL_PATH")
    
    # SeaTunnel Configuration
    seatunnel_home: str = Field("/opt/seatunnel", env="SEATUNNEL_HOME")
    seatunnel_api_url: str = Field("http://seatunnel-api:8080", env="SEATUNNEL_API_URL")
    seatunnel_quality_templates: str = Field("/config/quality-templates", env="SEATUNNEL_QUALITY_TEMPLATES")
    
    # Storage Configuration
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field("quality_cache", env="IGNITE_CACHE_NAME")
    
    elasticsearch_hosts: List[str] = Field(["elasticsearch:9200"], env="ELASTICSEARCH_HOSTS")
    elasticsearch_username: Optional[str] = Field(None, env="ELASTICSEARCH_USERNAME")
    elasticsearch_password: Optional[str] = Field(None, env="ELASTICSEARCH_PASSWORD")
    
    cassandra_hosts: List[str] = Field(["cassandra"], env="CASSANDRA_HOSTS")
    cassandra_port: int = Field(9042, env="CASSANDRA_PORT")
    cassandra_keyspace: str = Field("quality", env="CASSANDRA_KEYSPACE")
    
    minio_endpoint: str = Field("minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    minio_bucket: str = Field("quality-results", env="MINIO_BUCKET")
    
    # Pulsar Configuration
    pulsar_url: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_topic_prefix: str = Field("persistent://public/quality", env="PULSAR_TOPIC_PREFIX")
    
    # Quality Configuration
    quality_dimensions: List[str] = Field(
        ["completeness", "accuracy", "consistency", "timeliness", "validity", "uniqueness"],
        env="QUALITY_DIMENSIONS"
    )
    anomaly_detection_methods: List[str] = Field(
        ["statistical", "isolation_forest", "lof", "one_class_svm", "prophet"],
        env="ANOMALY_DETECTION_METHODS"
    )
    
    # Performance settings
    parallel_workers: int = Field(8, env="PARALLEL_WORKERS")
    batch_size: int = Field(10000, env="BATCH_SIZE")
    cache_ttl: int = Field(3600, env="CACHE_TTL")
    max_concurrent_jobs: int = Field(50, env="MAX_CONCURRENT_JOBS")
    
    # Monitoring
    prometheus_enabled: bool = Field(True, env="PROMETHEUS_ENABLED")
    metrics_port: int = Field(9090, env="METRICS_PORT")
    alert_channels: List[str] = Field(["pulsar", "email", "slack"], env="ALERT_CHANNELS")
    
    # Remediation settings
    auto_remediation_enabled: bool = Field(True, env="AUTO_REMEDIATION_ENABLED")
    remediation_confidence_threshold: float = Field(0.8, env="REMEDIATION_CONFIDENCE_THRESHOLD")
    remediation_simulation_mode: bool = Field(False, env="REMEDIATION_SIMULATION_MODE")
    
    # Rule engine settings
    rule_cache_size: int = Field(1000, env="RULE_CACHE_SIZE")
    rule_execution_timeout: int = Field(300, env="RULE_EXECUTION_TIMEOUT")  # seconds
    
    # Service discovery
    consul_enabled: bool = Field(True, env="CONSUL_ENABLED")
    consul_host: str = Field("consul", env="CONSUL_HOST")
    consul_port: int = Field(8500, env="CONSUL_PORT")
    consul_service_name: str = Field("unified-quality", env="CONSUL_SERVICE_NAME")
    
    # Vault settings
    vault_enabled: bool = Field(True, env="VAULT_ENABLED")
    vault_url: str = Field("http://vault:8200", env="VAULT_URL")
    vault_token: Optional[str] = Field(None, env="VAULT_TOKEN")
    vault_path: str = Field("secret/data/quality-service", env="VAULT_PATH")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Global settings instance
settings = get_settings() 