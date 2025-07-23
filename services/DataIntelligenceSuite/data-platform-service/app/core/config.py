"""
Configuration Settings

Environment-based configuration for Data Platform Service
"""

from typing import List, Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Service configuration settings"""
    
    # Service info
    SERVICE_NAME: str = "data-platform-service"
    SERVICE_VERSION: str = "2.0.0"
    
    # API settings
    API_PREFIX: str = "/api"
    DEBUG: bool = False
    
    # External services
    VAULT_URL: str = Field("http://vault:8200", env="VAULT_URL")
    VAULT_TOKEN: str = Field("", env="VAULT_TOKEN")
    
    CONSUL_HOST: str = Field("consul", env="CONSUL_HOST")
    CONSUL_PORT: int = Field(8500, env="CONSUL_PORT")
    
    PULSAR_URL: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    
    # Infrastructure services
    SEATUNNEL_URL: str = Field("http://seatunnel:8080", env="SEATUNNEL_URL")
    SEATUNNEL_API_KEY: Optional[str] = Field(None, env="SEATUNNEL_API_KEY")
    
    SPARK_MASTER_URL: str = Field("spark://spark-master:7077", env="SPARK_MASTER_URL")
    
    FLINK_JOB_MANAGER_URL: str = Field("http://flink-jobmanager:8081", env="FLINK_JOB_MANAGER_URL")
    
    MINIO_ENDPOINT: str = Field("minio:9000", env="MINIO_ENDPOINT")
    MINIO_ACCESS_KEY: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    MINIO_USE_SSL: bool = Field(False, env="MINIO_USE_SSL")
    
    IGNITE_NODES: List[str] = Field(
        ["ignite:10800"],
        env="IGNITE_NODES"
    )
    
    # Lakehouse settings
    ICEBERG_CATALOG_NAME: str = Field("platform_catalog", env="ICEBERG_CATALOG_NAME")
    ICEBERG_WAREHOUSE_LOCATION: str = Field(
        "s3a://platform-lakehouse/warehouse/iceberg",
        env="ICEBERG_WAREHOUSE_LOCATION"
    )
    
    DELTA_WAREHOUSE_LOCATION: str = Field(
        "s3a://platform-lakehouse/warehouse/delta",
        env="DELTA_WAREHOUSE_LOCATION"
    )
    
    HUDI_WAREHOUSE_LOCATION: str = Field(
        "s3a://platform-lakehouse/warehouse/hudi",
        env="HUDI_WAREHOUSE_LOCATION"
    )
    
    # Database connections
    POSTGRES_HOST: str = Field("postgres", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(5432, env="POSTGRES_PORT")
    POSTGRES_DB: str = Field("platform_metadata", env="POSTGRES_DB")
    POSTGRES_USER: str = Field("platform", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field("", env="POSTGRES_PASSWORD")
    
    CASSANDRA_HOSTS: List[str] = Field(
        ["cassandra:9042"],
        env="CASSANDRA_HOSTS"
    )
    CASSANDRA_KEYSPACE: str = Field("platform_data", env="CASSANDRA_KEYSPACE")
    
    ELASTICSEARCH_HOSTS: List[str] = Field(
        ["http://elasticsearch:9200"],
        env="ELASTICSEARCH_HOSTS"
    )
    
    # Feature flags
    ENABLE_ML_OPTIMIZATION: bool = Field(True, env="ENABLE_ML_OPTIMIZATION")
    ENABLE_COST_TRACKING: bool = Field(True, env="ENABLE_COST_TRACKING")
    ENABLE_AUTO_PARTITIONING: bool = Field(True, env="ENABLE_AUTO_PARTITIONING")
    ENABLE_SCHEMA_EVOLUTION: bool = Field(True, env="ENABLE_SCHEMA_EVOLUTION")
    
    # Performance settings
    MAX_CONCURRENT_JOBS: int = Field(100, env="MAX_CONCURRENT_JOBS")
    DEFAULT_BATCH_SIZE: int = Field(1000, env="DEFAULT_BATCH_SIZE")
    DEFAULT_PARALLELISM: int = Field(4, env="DEFAULT_PARALLELISM")
    
    # Storage settings
    DEFAULT_STORAGE_BUCKET: str = Field("platform-data", env="DEFAULT_STORAGE_BUCKET")
    TEMP_STORAGE_BUCKET: str = Field("platform-temp", env="TEMP_STORAGE_BUCKET")
    
    # Quality settings
    QUALITY_CHECK_ENABLED: bool = Field(True, env="QUALITY_CHECK_ENABLED")
    QUALITY_THRESHOLD: float = Field(0.95, env="QUALITY_THRESHOLD")
    
    # Monitoring settings
    METRICS_ENABLED: bool = Field(True, env="METRICS_ENABLED")
    TRACING_ENABLED: bool = Field(True, env="TRACING_ENABLED")
    JAEGER_ENDPOINT: str = Field("http://jaeger:14268/api/traces", env="JAEGER_ENDPOINT")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings() 