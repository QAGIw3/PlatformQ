"""Configuration for Unified Graph Service"""

from typing import List, Optional, Dict, Any
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service settings
    service_name: str = "unified-graph-service"
    service_version: str = "1.0.0"
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # API settings
    api_host: str = Field("0.0.0.0", env="API_HOST")
    api_port: int = Field(8010, env="API_PORT")
    cors_origins: List[str] = Field(["*"], env="CORS_ORIGINS")
    
    # JanusGraph settings
    janusgraph_url: str = Field("ws://janusgraph:8182/gremlin", env="JANUSGRAPH_URL")
    janusgraph_timeout: int = Field(30000, env="JANUSGRAPH_TIMEOUT")
    janusgraph_pool_size: int = Field(8, env="JANUSGRAPH_POOL_SIZE")
    
    # Cassandra settings (for JanusGraph backend)
    cassandra_hosts: List[str] = Field(["cassandra"], env="CASSANDRA_HOSTS")
    cassandra_port: int = Field(9042, env="CASSANDRA_PORT")
    cassandra_keyspace: str = Field("janusgraph", env="CASSANDRA_KEYSPACE")
    cassandra_username: Optional[str] = Field(None, env="CASSANDRA_USERNAME")
    cassandra_password: Optional[str] = Field(None, env="CASSANDRA_PASSWORD")
    
    # Elasticsearch settings (for JanusGraph indexing)
    elasticsearch_hosts: List[str] = Field(["elasticsearch:9200"], env="ELASTICSEARCH_HOSTS")
    elasticsearch_username: Optional[str] = Field(None, env="ELASTICSEARCH_USERNAME")
    elasticsearch_password: Optional[str] = Field(None, env="ELASTICSEARCH_PASSWORD")
    
    # Spark settings (for GraphX)
    spark_master: str = Field("spark://spark-master:7077", env="SPARK_MASTER")
    spark_app_name: str = Field("UnifiedGraphService", env="SPARK_APP_NAME")
    spark_executor_memory: str = Field("4g", env="SPARK_EXECUTOR_MEMORY")
    spark_executor_cores: int = Field(4, env="SPARK_EXECUTOR_CORES")
    graphx_checkpoint_dir: str = Field("hdfs://namenode:9000/graphx-checkpoints", env="GRAPHX_CHECKPOINT_DIR")
    
    # Ignite settings (for caching)
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    ignite_cache_name: str = Field("graph_cache", env="IGNITE_CACHE_NAME")
    cache_ttl: int = Field(3600, env="CACHE_TTL")
    
    # Analytics configuration
    pagerank_iterations: int = Field(20, env="PAGERANK_ITERATIONS")
    pagerank_damping_factor: float = Field(0.85, env="PAGERANK_DAMPING_FACTOR")
    community_detection_resolution: float = Field(1.0, env="COMMUNITY_DETECTION_RESOLUTION")
    max_path_length: int = Field(6, env="MAX_PATH_LENGTH")
    
    # Trust configuration
    trust_algorithm: str = Field("eigentrust", env="TRUST_ALGORITHM")
    trust_propagation_depth: int = Field(3, env="TRUST_PROPAGATION_DEPTH")
    trust_update_interval: int = Field(300, env="TRUST_UPDATE_INTERVAL")
    trust_decay_factor: float = Field(0.9, env="TRUST_DECAY_FACTOR")
    
    # Temporal configuration
    temporal_index_enabled: bool = Field(True, env="TEMPORAL_INDEX_ENABLED")
    causal_discovery_threshold: float = Field(0.05, env="CAUSAL_DISCOVERY_THRESHOLD")
    scenario_simulation_threads: int = Field(4, env="SCENARIO_SIMULATION_THREADS")
    time_series_window: int = Field(86400, env="TIME_SERIES_WINDOW")  # 24 hours
    
    # Service discovery
    consul_enabled: bool = Field(True, env="CONSUL_ENABLED")
    consul_host: str = Field("consul", env="CONSUL_HOST")
    consul_port: int = Field(8500, env="CONSUL_PORT")
    consul_service_name: str = Field("unified-graph", env="CONSUL_SERVICE_NAME")
    consul_health_check_interval: str = Field("30s", env="CONSUL_HEALTH_CHECK_INTERVAL")
    
    # Vault settings
    vault_enabled: bool = Field(True, env="VAULT_ENABLED")
    vault_url: str = Field("http://vault:8200", env="VAULT_URL")
    vault_token: Optional[str] = Field(None, env="VAULT_TOKEN")
    vault_path: str = Field("secret/data/graph-service", env="VAULT_PATH")
    
    # Performance settings
    max_concurrent_queries: int = Field(100, env="MAX_CONCURRENT_QUERIES")
    query_timeout_seconds: int = Field(300, env="QUERY_TIMEOUT_SECONDS")
    batch_size: int = Field(1000, env="BATCH_SIZE")
    
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