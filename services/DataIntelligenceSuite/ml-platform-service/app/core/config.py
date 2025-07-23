"""
Configuration for ML Platform Service
"""
from pydantic_settings import BaseSettings
from typing import Optional, List


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    SERVICE_NAME: str = "ml-platform-service"
    SERVICE_VERSION: str = "2.0.0"
    ENVIRONMENT: str = "development"
    
    # Server config
    HOST: str = "0.0.0.0"
    PORT: int = 8015
    
    # Vault & Consul
    VAULT_ADDR: str = "http://vault:8200"
    VAULT_TOKEN: Optional[str] = None
    CONSUL_ADDR: str = "http://consul:8500"
    
    # MLflow Configuration
    MLFLOW_TRACKING_URI: str = "http://mlflow:5000"
    MLFLOW_ARTIFACT_LOCATION: str = "s3://ml-artifacts"
    MLFLOW_EXPERIMENT_NAME: str = "default"
    MLFLOW_BACKEND_STORE_URI: str = "postgresql://mlflow:mlflow@postgres:5432/mlflow"
    
    # MinIO Configuration
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MODEL_BUCKET: str = "ml-models"
    ARTIFACT_BUCKET: str = "ml-artifacts"
    
    # Apache Ignite
    IGNITE_HOST: str = "ignite"
    IGNITE_PORT: int = 10800
    
    # Apache Pulsar
    PULSAR_URL: str = "pulsar://pulsar:6650"
    
    # Training Configuration
    MAX_TRAINING_JOBS: int = 10
    DEFAULT_TRAINING_TIMEOUT: int = 3600
    GPU_MEMORY_FRACTION: float = 0.8
    TRAINING_QUEUE_SIZE: int = 100
    CHECKPOINT_INTERVAL: int = 300
    
    # Spark Configuration
    SPARK_MASTER: str = "spark://spark-master:7077"
    SPARK_EXECUTOR_MEMORY: str = "4g"
    SPARK_EXECUTOR_CORES: int = 4
    
    # Serving Configuration
    MODEL_CACHE_SIZE: int = 100
    INFERENCE_TIMEOUT: int = 60
    BATCH_SIZE: int = 32
    MAX_CONCURRENT_MODELS: int = 20
    MODEL_LOADING_TIMEOUT: int = 300
    
    # Triton Server
    TRITON_SERVER_URL: str = "http://triton:8001"
    TRITON_MODEL_REPOSITORY: str = "/models"
    
    # Feature Store (using Ignite)
    FEATURE_CACHE_TTL: int = 86400
    FEATURE_COMPUTATION_TIMEOUT: int = 600
    
    # Model Registry
    MODEL_REGISTRY_BACKEND: str = "mlflow"
    MODEL_VERSION_LIMIT: int = 10
    MODEL_ARTIFACT_RETENTION_DAYS: int = 30
    
    # Monitoring
    DRIFT_DETECTION_ENABLED: bool = True
    DRIFT_CHECK_INTERVAL: int = 3600
    PERFORMANCE_THRESHOLD: float = 0.8
    ALERT_COOLDOWN_MINUTES: int = 30
    
    # Federated Learning
    FEDERATED_ROUNDS: int = 10
    MIN_CLIENTS_PER_ROUND: int = 2
    CLIENT_TIMEOUT_SECONDS: int = 300
    AGGREGATION_STRATEGY: str = "fedavg"
    DIFFERENTIAL_PRIVACY_EPSILON: float = 1.0
    
    # AutoML
    AUTOML_TIME_LIMIT_MINUTES: int = 60
    AUTOML_MAX_TRIALS: int = 100
    AUTOML_METRIC: str = "accuracy"
    AUTOML_FRAMEWORKS: List[str] = ["sklearn", "xgboost", "lightgbm"]
    
    # Database
    DATABASE_URL: str = "postgresql://ml_platform:ml_platform@postgres:5432/ml_platform"
    
    # Monitoring & Metrics
    PROMETHEUS_ENABLED: bool = True
    GRAFANA_URL: str = "http://grafana:3000"
    
    # Event Integration
    EVENT_ROUTER_URL: str = "http://event-router-service:8000"
    GRAPH_INTELLIGENCE_URL: str = "http://graph-intelligence-service:8000"
    DATA_PLATFORM_URL: str = "http://data-platform-service:8000"
    
    # Security
    JWT_SECRET_KEY: str = "your-secret-key-here"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings() 