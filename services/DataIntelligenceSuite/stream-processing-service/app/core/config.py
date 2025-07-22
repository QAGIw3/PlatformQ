"""Configuration for Stream Processing Service"""

from typing import List, Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service settings
    service_name: str = "stream-processing-service"
    service_version: str = "1.0.0"
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # API settings
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")
    cors_origins: List[str] = Field(["*"], env="CORS_ORIGINS")
    
    # Flink settings
    flink_master: str = Field("localhost:8081", env="FLINK_MASTER")
    flink_parallelism: int = Field(4, env="FLINK_PARALLELISM")
    flink_checkpoint_interval: int = Field(30000, env="FLINK_CHECKPOINT_INTERVAL")
    flink_checkpoint_dir: str = Field("file:///tmp/flink-checkpoints", env="FLINK_CHECKPOINT_DIR")
    flink_state_backend: str = Field("rocksdb", env="FLINK_STATE_BACKEND")
    
    # Pulsar settings
    pulsar_url: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_admin_url: str = Field("http://pulsar:8080", env="PULSAR_ADMIN_URL")
    pulsar_namespace: str = Field("platformq/streaming", env="PULSAR_NAMESPACE")
    
    # Cassandra settings
    cassandra_hosts: List[str] = Field(["cassandra"], env="CASSANDRA_HOSTS")
    cassandra_port: int = Field(9042, env="CASSANDRA_PORT")
    cassandra_keyspace: str = Field("platformq", env="CASSANDRA_KEYSPACE")
    cassandra_username: Optional[str] = Field(None, env="CASSANDRA_USERNAME")
    cassandra_password: Optional[str] = Field(None, env="CASSANDRA_PASSWORD")
    
    # MinIO settings
    minio_endpoint: str = Field("minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(False, env="MINIO_SECURE")
    minio_bucket_prefix: str = Field("streaming", env="MINIO_BUCKET_PREFIX")
    
    # Ignite settings
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field("stream_processing", env="IGNITE_CACHE_NAME")
    
    # Elasticsearch settings
    elasticsearch_hosts: List[str] = Field(["elasticsearch:9200"], env="ELASTICSEARCH_HOSTS")
    elasticsearch_username: Optional[str] = Field(None, env="ELASTICSEARCH_USERNAME")
    elasticsearch_password: Optional[str] = Field(None, env="ELASTICSEARCH_PASSWORD")
    
    # Service discovery
    consul_enabled: bool = Field(True, env="CONSUL_ENABLED")
    consul_host: str = Field("consul", env="CONSUL_HOST")
    consul_port: int = Field(8500, env="CONSUL_PORT")
    consul_service_name: str = Field("stream-processing", env="CONSUL_SERVICE_NAME")
    consul_health_check_interval: str = Field("30s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    
    # Job management
    max_concurrent_jobs: int = Field(100, env="MAX_CONCURRENT_JOBS")
    job_submission_timeout: int = Field(60, env="JOB_SUBMISSION_TIMEOUT")
    job_cleanup_interval: int = Field(3600, env="JOB_CLEANUP_INTERVAL")
    
    # Pattern library
    pattern_library_path: str = Field("/app/patterns", env="PATTERN_LIBRARY_PATH")
    pattern_reload_interval: int = Field(300, env="PATTERN_RELOAD_INTERVAL")
    
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