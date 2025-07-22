"""Configuration for Batch Processing Service"""

from typing import List, Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service settings
    service_name: str = "batch-processing-service"
    service_version: str = "1.0.0"
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # API settings
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8012, env="API_PORT")
    cors_origins: List[str] = Field(["*"], env="CORS_ORIGINS")
    
    # Spark settings
    spark_master: str = Field("local[*]", env="SPARK_MASTER")
    spark_app_name: str = Field("BatchProcessingService", env="SPARK_APP_NAME")
    spark_executor_memory: str = Field("4g", env="SPARK_EXECUTOR_MEMORY")
    spark_executor_cores: int = Field(4, env="SPARK_EXECUTOR_CORES")
    spark_max_executors: int = Field(10, env="SPARK_MAX_EXECUTORS")
    spark_sql_shuffle_partitions: int = Field(200, env="SPARK_SQL_SHUFFLE_PARTITIONS")
    spark_checkpoint_dir: str = Field("hdfs://namenode:9000/checkpoints", env="SPARK_CHECKPOINT_DIR")
    
    # MinIO settings
    minio_endpoint: str = Field("minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(False, env="MINIO_SECURE")
    minio_data_bucket: str = Field("data-lake", env="MINIO_DATA_BUCKET")
    minio_model_bucket: str = Field("ml-models", env="MINIO_MODEL_BUCKET")
    
    # Cassandra settings
    cassandra_hosts: List[str] = Field(["cassandra"], env="CASSANDRA_HOSTS")
    cassandra_port: int = Field(9042, env="CASSANDRA_PORT")
    cassandra_keyspace: str = Field("platformq", env="CASSANDRA_KEYSPACE")
    cassandra_username: Optional[str] = Field(None, env="CASSANDRA_USERNAME")
    cassandra_password: Optional[str] = Field(None, env="CASSANDRA_PASSWORD")
    
    # Elasticsearch settings
    elasticsearch_hosts: List[str] = Field(["elasticsearch:9200"], env="ELASTICSEARCH_HOSTS")
    elasticsearch_username: Optional[str] = Field(None, env="ELASTICSEARCH_USERNAME")
    elasticsearch_password: Optional[str] = Field(None, env="ELASTICSEARCH_PASSWORD")
    
    # Ignite settings
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field("batch_processing", env="IGNITE_CACHE_NAME")
    
    # MLflow settings
    mlflow_tracking_uri: str = Field("http://mlflow:5000", env="MLFLOW_TRACKING_URI")
    mlflow_artifact_location: str = Field("s3://mlflow-artifacts", env="MLFLOW_ARTIFACT_LOCATION")
    
    # Service discovery
    consul_enabled: bool = Field(True, env="CONSUL_ENABLED")
    consul_host: str = Field("consul", env="CONSUL_HOST")
    consul_port: int = Field(8500, env="CONSUL_PORT")
    consul_service_name: str = Field("batch-processing", env="CONSUL_SERVICE_NAME")
    consul_health_check_interval: str = Field("30s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    
    # Job management
    max_concurrent_jobs: int = Field(50, env="MAX_CONCURRENT_JOBS")
    job_timeout_seconds: int = Field(3600, env="JOB_TIMEOUT_SECONDS")
    job_log_retention_days: int = Field(7, env="JOB_LOG_RETENTION_DAYS")
    
    # Resource profiles
    resource_profiles: Dict[str, Dict[str, Any]] = Field(
        default={
            "small": {
                "executor_memory": "2g",
                "executor_cores": 2,
                "max_executors": 5
            },
            "medium": {
                "executor_memory": "4g",
                "executor_cores": 4,
                "max_executors": 10
            },
            "large": {
                "executor_memory": "8g",
                "executor_cores": 8,
                "max_executors": 20
            },
            "xlarge": {
                "executor_memory": "16g",
                "executor_cores": 16,
                "max_executors": 50,
                "gpu": True
            }
        },
        env="RESOURCE_PROFILES"
    )
    
    # Monitoring
    metrics_enabled: bool = Field(True, env="METRICS_ENABLED")
    metrics_port: int = Field(9090, env="METRICS_PORT")
    trace_enabled: bool = Field(True, env="TRACE_ENABLED")
    trace_endpoint: str = Field("http://jaeger:14268/api/traces", env="TRACE_ENDPOINT")
    
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