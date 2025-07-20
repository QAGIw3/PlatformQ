"""
Data Platform Service Configuration
"""
from typing import Optional, Dict, Any
from pydantic import BaseSettings, Field, validator
from functools import lru_cache


class DataPlatformSettings(BaseSettings):
    """Data Platform Service configuration"""
    
    # Service settings
    service_name: str = "data-platform-service"
    service_version: str = "2.0.0"
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    
    # API settings
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8000, env="API_PORT")
    api_prefix: str = "/api/v1"
    cors_origins: list = Field(["*"], env="CORS_ORIGINS")
    
    # Trino (Federation) settings
    trino_host: str = Field("trino-coordinator", env="TRINO_HOST")
    trino_port: int = Field(8080, env="TRINO_PORT")
    trino_user: str = Field("platformq", env="TRINO_USER")
    trino_catalog: str = Field("hive", env="TRINO_CATALOG")
    trino_schema: str = Field("default", env="TRINO_SCHEMA")
    trino_query_timeout: int = Field(300, env="TRINO_QUERY_TIMEOUT")  # seconds
    
    # Apache Ignite (Cache) settings
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field("data_platform_cache", env="IGNITE_CACHE_NAME")
    cache_ttl: int = Field(3600, env="CACHE_TTL")  # seconds
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    
    # MinIO (Object Storage) settings
    minio_endpoint: str = Field("minio:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", env="MINIO_SECRET_KEY")
    minio_secure: bool = Field(False, env="MINIO_SECURE")
    minio_bucket_prefix: str = Field("platformq", env="MINIO_BUCKET_PREFIX")
    
    # Elasticsearch (Catalog) settings
    elasticsearch_hosts: list = Field(["elasticsearch:9200"], env="ELASTICSEARCH_HOSTS")
    elasticsearch_username: Optional[str] = Field(None, env="ELASTICSEARCH_USERNAME")
    elasticsearch_password: Optional[str] = Field(None, env="ELASTICSEARCH_PASSWORD")
    elasticsearch_index_prefix: str = Field("platformq", env="ELASTICSEARCH_INDEX_PREFIX")
    
    # Apache Pulsar (Messaging) settings
    pulsar_url: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    pulsar_namespace: str = Field("platformq/data-platform", env="PULSAR_NAMESPACE")
    pulsar_subscription_prefix: str = Field("data-platform", env="PULSAR_SUBSCRIPTION_PREFIX")
    
    # Apache Spark settings
    spark_master: str = Field("spark://spark-master:7077", env="SPARK_MASTER")
    spark_app_name: str = Field("DataPlatformService", env="SPARK_APP_NAME")
    spark_executor_memory: str = Field("2g", env="SPARK_EXECUTOR_MEMORY")
    spark_executor_cores: int = Field(2, env="SPARK_EXECUTOR_CORES")
    spark_max_executors: int = Field(10, env="SPARK_MAX_EXECUTORS")
    
    # SeaTunnel settings
    seatunnel_home: str = Field("/opt/seatunnel", env="SEATUNNEL_HOME")
    seatunnel_api_url: str = Field("http://seatunnel-api:8080", env="SEATUNNEL_API_URL")
    seatunnel_k8s_namespace: str = Field("data-pipelines", env="SEATUNNEL_K8S_NAMESPACE")
    
    # JanusGraph (Lineage) settings
    janusgraph_host: str = Field("janusgraph", env="JANUSGRAPH_HOST")
    janusgraph_port: int = Field(8182, env="JANUSGRAPH_PORT")
    janusgraph_graph_name: str = Field("platformq_lineage", env="JANUSGRAPH_GRAPH_NAME")
    
    # Cassandra settings
    cassandra_hosts: list = Field(["cassandra"], env="CASSANDRA_HOSTS")
    cassandra_port: int = Field(9042, env="CASSANDRA_PORT")
    cassandra_keyspace: str = Field("platformq", env="CASSANDRA_KEYSPACE")
    cassandra_consistency: str = Field("LOCAL_QUORUM", env="CASSANDRA_CONSISTENCY")
    
    # Data Lake settings
    lake_bronze_path: str = Field("s3a://platformq/bronze", env="LAKE_BRONZE_PATH")
    lake_silver_path: str = Field("s3a://platformq/silver", env="LAKE_SILVER_PATH")
    lake_gold_path: str = Field("s3a://platformq/gold", env="LAKE_GOLD_PATH")
    lake_checkpoint_path: str = Field("s3a://platformq/checkpoints", env="LAKE_CHECKPOINT_PATH")
    lake_format: str = Field("delta", env="LAKE_FORMAT")  # delta, iceberg, hudi
    
    # Data Quality settings
    quality_sample_size: int = Field(10000, env="QUALITY_SAMPLE_SIZE")
    quality_profiling_enabled: bool = Field(True, env="QUALITY_PROFILING_ENABLED")
    quality_ml_enabled: bool = Field(True, env="QUALITY_ML_ENABLED")
    quality_anomaly_threshold: float = Field(0.95, env="QUALITY_ANOMALY_THRESHOLD")
    
    # Governance settings
    governance_auto_discovery: bool = Field(True, env="GOVERNANCE_AUTO_DISCOVERY")
    governance_pii_detection: bool = Field(True, env="GOVERNANCE_PII_DETECTION")
    governance_compliance_frameworks: list = Field(
        ["GDPR", "CCPA", "HIPAA", "SOC2", "PCI-DSS"],
        env="GOVERNANCE_COMPLIANCE_FRAMEWORKS"
    )
    
    # Apache Atlas settings (optional)
    atlas_url: Optional[str] = Field(None, env="ATLAS_URL")
    atlas_username: Optional[str] = Field(None, env="ATLAS_USERNAME")
    atlas_password: Optional[str] = Field(None, env="ATLAS_PASSWORD")
    
    # Apache Ranger settings (optional)
    ranger_url: Optional[str] = Field(None, env="RANGER_URL")
    ranger_username: Optional[str] = Field(None, env="RANGER_USERNAME")
    ranger_password: Optional[str] = Field(None, env="RANGER_PASSWORD")
    
    # Monitoring settings
    prometheus_enabled: bool = Field(True, env="PROMETHEUS_ENABLED")
    prometheus_port: int = Field(9090, env="PROMETHEUS_PORT")
    opentelemetry_enabled: bool = Field(True, env="OPENTELEMETRY_ENABLED")
    opentelemetry_endpoint: str = Field("http://otel-collector:4317", env="OPENTELEMETRY_ENDPOINT")
    
    # Security settings
    jwt_secret_key: str = Field("your-secret-key-here", env="JWT_SECRET_KEY")
    jwt_algorithm: str = Field("HS256", env="JWT_ALGORITHM")
    jwt_expiration_minutes: int = Field(60, env="JWT_EXPIRATION_MINUTES")
    api_key_header: str = Field("X-API-Key", env="API_KEY_HEADER")
    
    # Performance settings
    query_cache_enabled: bool = Field(True, env="QUERY_CACHE_ENABLED")
    query_cache_ttl: int = Field(300, env="QUERY_CACHE_TTL")  # seconds
    connection_pool_size: int = Field(20, env="CONNECTION_POOL_SIZE")
    connection_pool_overflow: int = Field(10, env="CONNECTION_POOL_OVERFLOW")
    
    # Scheduler settings
    scheduler_enabled: bool = Field(True, env="SCHEDULER_ENABLED")
    scheduler_timezone: str = Field("UTC", env="SCHEDULER_TIMEZONE")
    
    # Feature flags
    feature_auto_lineage: bool = Field(True, env="FEATURE_AUTO_LINEAGE")
    feature_smart_caching: bool = Field(True, env="FEATURE_SMART_CACHING")
    feature_cost_optimization: bool = Field(True, env="FEATURE_COST_OPTIMIZATION")
    feature_ml_recommendations: bool = Field(True, env="FEATURE_ML_RECOMMENDATIONS")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        
    @validator("elasticsearch_hosts", "cassandra_hosts", pre=True)
    def parse_hosts(cls, v):
        """Parse comma-separated host lists"""
        if isinstance(v, str):
            return [h.strip() for h in v.split(",")]
        return v
    
    @validator("cors_origins", pre=True)
    def parse_cors_origins(cls, v):
        """Parse comma-separated CORS origins"""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    @validator("governance_compliance_frameworks", pre=True)
    def parse_frameworks(cls, v):
        """Parse comma-separated compliance frameworks"""
        if isinstance(v, str):
            return [f.strip() for f in v.split(",")]
        return v
    
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration dictionary"""
        return {
            "spark.app.name": self.spark_app_name,
            "spark.master": self.spark_master,
            "spark.executor.memory": self.spark_executor_memory,
            "spark.executor.cores": str(self.spark_executor_cores),
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.maxExecutors": str(self.spark_max_executors),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.hadoop.fs.s3a.endpoint": self.minio_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        }
    
    def get_trino_connection_params(self) -> Dict[str, Any]:
        """Get Trino connection parameters"""
        return {
            "host": self.trino_host,
            "port": self.trino_port,
            "user": self.trino_user,
            "catalog": self.trino_catalog,
            "schema": self.trino_schema,
            "http_scheme": "http",
            "verify": False
        }
    
    def get_elasticsearch_config(self) -> Dict[str, Any]:
        """Get Elasticsearch configuration"""
        config = {
            "hosts": self.elasticsearch_hosts,
            "verify_certs": False,
            "ssl_show_warn": False
        }
        
        if self.elasticsearch_username and self.elasticsearch_password:
            config["basic_auth"] = (self.elasticsearch_username, self.elasticsearch_password)
            
        return config
    
    def get_minio_config(self) -> Dict[str, Any]:
        """Get MinIO configuration"""
        return {
            "endpoint": self.minio_endpoint,
            "access_key": self.minio_access_key,
            "secret_key": self.minio_secret_key,
            "secure": self.minio_secure
        }
    
    def get_cassandra_config(self) -> Dict[str, Any]:
        """Get Cassandra configuration"""
        return {
            "contact_points": self.cassandra_hosts,
            "port": self.cassandra_port,
            "keyspace": self.cassandra_keyspace,
            "consistency_level": self.cassandra_consistency,
            "connection_pooling": True
        }
    
    def get_pulsar_config(self) -> Dict[str, Any]:
        """Get Pulsar configuration"""
        return {
            "service_url": self.pulsar_url,
            "namespace": self.pulsar_namespace,
            "subscription_prefix": self.pulsar_subscription_prefix,
            "authentication": None,  # Add authentication config if needed
            "operation_timeout_seconds": 30
        }


@lru_cache()
def get_settings() -> DataPlatformSettings:
    """Get cached settings instance"""
    return DataPlatformSettings()


# Global settings instance
settings = get_settings() 