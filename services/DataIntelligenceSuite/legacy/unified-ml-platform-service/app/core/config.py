"""
Configuration settings for Unified ML Platform Service
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Service configuration settings"""
    
    # Service Info
    service_name: str = "unified-ml-platform-service"
    service_port: int = 8015
    environment: str = "development"
    
    # MLflow Configuration
    mlflow_tracking_uri: str = "http://mlflow:5000"
    mlflow_artifact_location: str = "s3://ml-artifacts"
    mlflow_experiment_name: str = "default"
    
    # Training Configuration
    max_training_jobs: int = 10
    default_training_timeout: int = 3600
    gpu_memory_fraction: float = 0.8
    training_queue_size: int = 100
    checkpoint_interval: int = 300  # seconds
    
    # Serving Configuration
    model_cache_size: int = 100
    inference_timeout: int = 60
    batch_size: int = 32
    max_concurrent_models: int = 20
    model_loading_timeout: int = 300
    
    # Feature Store
    feature_store_online_url: str = "redis://redis:6379"
    feature_store_offline_url: str = "s3://feature-store"
    feature_ttl_seconds: int = 86400  # 24 hours
    feature_computation_timeout: int = 600
    
    # Model Registry
    model_registry_backend: str = "mlflow"
    model_version_limit: int = 10
    model_artifact_retention_days: int = 30
    
    # Monitoring
    drift_detection_enabled: bool = True
    drift_check_interval: int = 3600  # 1 hour
    performance_threshold: float = 0.8
    alert_cooldown_minutes: int = 30
    
    # Federated Learning
    federated_rounds: int = 10
    min_clients_per_round: int = 2
    client_timeout_seconds: int = 300
    aggregation_strategy: str = "fedavg"
    differential_privacy_epsilon: float = 1.0
    
    # Neuromorphic Computing
    spike_threshold: float = 1.0
    refractory_period_ms: int = 2
    synapse_delay_ms: int = 1
    learning_rate_spike: float = 0.01
    
    # AutoML
    automl_time_limit_minutes: int = 60
    automl_max_trials: int = 100
    automl_metric: str = "accuracy"
    automl_frameworks: list = ["sklearn", "xgboost", "lightgbm"]
    
    # Storage
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    ml_bucket_name: str = "ml-artifacts"
    
    # Ignite Cache
    ignite_host: str = "ignite"
    ignite_port: int = 10800
    cache_ttl_seconds: int = 300
    
    # Event Streaming
    pulsar_url: str = "pulsar://pulsar:6650"
    event_topic_prefix: str = "ml-events-"
    event_batch_size: int = 100
    event_batch_timeout_ms: int = 1000
    
    # Integration URLs
    event_router_url: str = "http://event-router-service:8000"
    graph_intelligence_url: str = "http://graph-intelligence-service:8013"
    data_platform_url: str = "http://data-platform-service:8000"
    auth_service_url: str = "http://auth-service:8001"
    
    # Vault/Consul
    vault_addr: str = "http://vault:8200"
    vault_token: Optional[str] = None
    consul_addr: str = "http://consul:8500"
    consul_token: Optional[str] = None
    
    # Database
    database_url: str = "postgresql://mlops:mlops@postgres:5432/mlops"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    
    # Security
    jwt_secret_key: str = "your-secret-key-here"
    jwt_algorithm: str = "HS256"
    jwt_expiration_minutes: int = 30
    api_key_header: str = "X-API-Key"
    
    # Performance
    worker_threads: int = 4
    async_timeout: int = 30
    connection_pool_size: int = 100
    request_timeout: int = 60
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create settings instance
settings = Settings() 