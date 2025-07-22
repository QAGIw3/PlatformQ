"""
Configuration settings for Data Ingestion Service
"""

from pydantic_settings import BaseSettings
from typing import Optional, List, Dict, Any


class Settings(BaseSettings):
    """Service configuration settings"""
    
    # Service Info
    service_name: str = "data-ingestion-service"
    service_version: str = "1.0.0"
    service_port: int = 8010
    environment: str = "development"
    log_level: str = "INFO"
    
    # SeaTunnel Configuration
    seatunnel_home: str = "/opt/seatunnel"
    seatunnel_config_dir: str = "/opt/seatunnel/config"
    seatunnel_checkpoint_dir: str = "/tmp/seatunnel/checkpoint"
    seatunnel_job_timeout: int = 3600  # 1 hour
    seatunnel_parallelism: int = 4
    
    # CDC Configuration
    cdc_snapshot_mode: str = "initial"  # initial, latest, schema_only
    cdc_poll_interval: int = 5000  # milliseconds
    cdc_batch_size: int = 1000
    cdc_checkpoint_interval: int = 60000  # milliseconds
    
    # Supported CDC Sources
    cdc_postgres_enabled: bool = True
    cdc_mysql_enabled: bool = True
    cdc_mongodb_enabled: bool = True
    
    # Stream Ingestion Configuration
    stream_batch_size: int = 100
    stream_batch_timeout_ms: int = 1000
    stream_consumer_group: str = "data-ingestion-service"
    stream_auto_commit: bool = True
    stream_max_poll_records: int = 500
    
    # Batch Ingestion Configuration
    batch_upload_path: str = "/tmp/uploads"
    batch_max_file_size: int = 5 * 1024 * 1024 * 1024  # 5GB
    batch_supported_formats: List[str] = ["csv", "json", "parquet", "avro", "orc"]
    batch_chunk_size: int = 10000
    batch_processing_threads: int = 4
    
    # Schema Registry Configuration
    schema_compatibility: str = "BACKWARD"
    schema_cache_size: int = 1000
    schema_validation_enabled: bool = True
    schema_evolution_enabled: bool = True
    
    # Storage Configuration
    # MinIO (Data Lake)
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket_raw: str = "raw-data"
    minio_bucket_processed: str = "processed-data"
    minio_secure: bool = False
    
    # Cassandra (Hot Storage)
    cassandra_hosts: List[str] = ["cassandra:9042"]
    cassandra_keyspace: str = "ingestion"
    cassandra_replication_factor: int = 3
    cassandra_consistency_level: str = "QUORUM"
    
    # Ignite (Cache)
    ignite_host: str = "ignite"
    ignite_port: int = 10800
    cache_ttl_seconds: int = 3600
    
    # Pulsar Configuration
    pulsar_url: str = "pulsar://pulsar:6650"
    pulsar_admin_url: str = "http://pulsar:8080"
    pulsar_topic_prefix: str = "persistent://public/default/"
    pulsar_subscription_type: str = "Shared"
    
    # Data Quality Integration
    quality_check_enabled: bool = True
    quality_service_url: str = "http://unified-quality-service:8015"
    quality_check_async: bool = True
    quality_threshold: float = 0.95
    
    # Monitoring and Metrics
    metrics_enabled: bool = True
    metrics_port: int = 9090
    tracing_enabled: bool = True
    jaeger_endpoint: str = "http://jaeger:14268/api/traces"
    
    # Service Discovery
    consul_enabled: bool = True
    consul_host: str = "consul"
    consul_port: int = 8500
    consul_service_name: str = "data-ingestion-service"
    consul_health_check_interval: str = "10s"
    consul_deregister_critical_after: str = "30s"
    
    # Database
    database_url: str = "postgresql://ingestion:ingestion@postgres:5432/ingestion"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    
    # Security
    auth_enabled: bool = True
    auth_service_url: str = "http://auth-service:8001"
    jwt_secret_key: str = "your-secret-key-here"
    jwt_algorithm: str = "HS256"
    api_key_header: str = "X-API-Key"
    
    # Performance
    worker_threads: int = 4
    async_timeout: int = 30
    connection_pool_size: int = 100
    request_timeout: int = 60
    
    # SeaTunnel Job Templates
    seatunnel_template_cdc: str = "cdc-template.conf"
    seatunnel_template_stream: str = "stream-template.conf"
    seatunnel_template_batch: str = "batch-template.conf"
    
    # Connector Configuration
    connector_configs: Dict[str, Dict[str, Any]] = {}
    connector_default_destination: str = "cassandra"
    connector_webhook_enabled: bool = True
    connector_webhook_port: int = 8011
    
    # Medallion Architecture
    minio_bucket_bronze: str = "lake-bronze"
    minio_bucket_silver: str = "lake-silver"
    minio_bucket_gold: str = "lake-gold"
    bronze_retention_days: int = 90
    silver_retention_days: int = 365
    gold_retention_days: int = 1825
    halt_on_quality_failure: bool = False
    archive_silver: bool = False
    enable_delta_lake: bool = True
    enable_iceberg: bool = True
    
    # Data Lifecycle
    lifecycle_enabled: bool = True
    hot_tier_enabled: bool = True
    warm_tier_enabled: bool = True
    cold_tier_enabled: bool = True
    lifecycle_check_interval: int = 3600  # 1 hour
    cassandra_username: Optional[str] = None
    cassandra_password: Optional[str] = None
    elasticsearch_hosts: List[str] = ["elasticsearch:9200"]
    elasticsearch_username: Optional[str] = None
    elasticsearch_password: Optional[str] = None
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create settings instance
settings = Settings() 