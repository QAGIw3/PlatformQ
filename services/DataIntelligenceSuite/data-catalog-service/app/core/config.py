"""
Configuration settings for Data Catalog Service
"""

from typing import List, Optional
from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Service configuration"""
    
    # Service info
    service_name: str = "data-catalog-service"
    service_port: int = 8017
    environment: str = Field("development", env="ENVIRONMENT")
    
    # Apache Atlas
    atlas_url: str = Field("http://atlas:21000", env="ATLAS_URL")
    atlas_username: str = Field("admin", env="ATLAS_USERNAME")
    atlas_password: str = Field("admin", env="ATLAS_PASSWORD")
    atlas_client_timeout: int = Field(30, env="ATLAS_CLIENT_TIMEOUT")
    atlas_max_retries: int = Field(3, env="ATLAS_MAX_RETRIES")
    
    # Schema Registry
    schema_registry_enabled: bool = Field(True, env="SCHEMA_REGISTRY_ENABLED")
    schema_compatibility_default: str = Field("BACKWARD", env="SCHEMA_COMPATIBILITY_DEFAULT")
    schema_cache_size: int = Field(1000, env="SCHEMA_CACHE_SIZE")
    schema_cache_ttl: int = Field(3600, env="SCHEMA_CACHE_TTL")  # 1 hour
    
    # Search Configuration
    search_engine: str = Field("elasticsearch", env="SEARCH_ENGINE")
    elasticsearch_hosts: List[str] = Field(
        default_factory=lambda: ["elasticsearch:9200"],
        env="ELASTICSEARCH_HOSTS"
    )
    search_index_prefix: str = Field("catalog_", env="SEARCH_INDEX_PREFIX")
    search_result_limit: int = Field(100, env="SEARCH_RESULT_LIMIT")
    search_timeout: int = Field(10, env="SEARCH_TIMEOUT")
    
    # Lineage Processing
    lineage_batch_size: int = Field(100, env="LINEAGE_BATCH_SIZE")
    lineage_processing_interval: int = Field(60, env="LINEAGE_PROCESSING_INTERVAL")
    lineage_retention_days: int = Field(365, env="LINEAGE_RETENTION_DAYS")
    lineage_max_depth: int = Field(10, env="LINEAGE_MAX_DEPTH")
    
    # Classification
    auto_classification_enabled: bool = Field(True, env="AUTO_CLASSIFICATION_ENABLED")
    classification_scan_interval: int = Field(300, env="CLASSIFICATION_SCAN_INTERVAL")
    pii_detection_enabled: bool = Field(True, env="PII_DETECTION_ENABLED")
    classification_sample_size: int = Field(1000, env="CLASSIFICATION_SAMPLE_SIZE")
    classification_confidence_threshold: float = Field(0.8, env="CLASSIFICATION_CONFIDENCE_THRESHOLD")
    
    # Caching (Ignite)
    ignite_host: str = Field("ignite", env="IGNITE_HOST")
    ignite_port: int = Field(10800, env="IGNITE_PORT")
    cache_ttl: int = Field(300, env="CACHE_TTL")  # 5 minutes
    cache_max_size: int = Field(10000, env="CACHE_MAX_SIZE")
    
    # Event Streaming (Pulsar)
    pulsar_url: str = Field("pulsar://pulsar:6650", env="PULSAR_URL")
    event_topic_prefix: str = Field("catalog-events-", env="EVENT_TOPIC_PREFIX")
    event_batch_size: int = Field(100, env="EVENT_BATCH_SIZE")
    event_batch_timeout: int = Field(5, env="EVENT_BATCH_TIMEOUT")
    
    # Integration Services
    auth_service_url: str = Field("http://auth-service:8001", env="AUTH_SERVICE_URL")
    quality_service_url: str = Field("http://unified-quality-service:8015", env="QUALITY_SERVICE_URL")
    
    # Performance
    connection_pool_size: int = Field(10, env="CONNECTION_POOL_SIZE")
    request_timeout: int = Field(30, env="REQUEST_TIMEOUT")
    async_task_timeout: int = Field(300, env="ASYNC_TASK_TIMEOUT")
    
    # Security
    encryption_key: Optional[str] = Field(None, env="ENCRYPTION_KEY")
    audit_log_enabled: bool = Field(True, env="AUDIT_LOG_ENABLED")
    sensitive_data_masking: bool = Field(True, env="SENSITIVE_DATA_MASKING")
    
    # Business Glossary
    glossary_approval_required: bool = Field(True, env="GLOSSARY_APPROVAL_REQUIRED")
    glossary_default_language: str = Field("en", env="GLOSSARY_DEFAULT_LANGUAGE")
    
    # Monitoring
    metrics_enabled: bool = Field(True, env="METRICS_ENABLED")
    metrics_port: int = Field(9090, env="METRICS_PORT")
    
    # Vault/Consul (if using data-intelligence-common)
    vault_enabled: bool = Field(False, env="VAULT_ENABLED")
    vault_url: str = Field("http://vault:8200", env="VAULT_URL")
    consul_enabled: bool = Field(False, env="CONSUL_ENABLED")
    consul_url: str = Field("http://consul:8500", env="CONSUL_URL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create global settings instance
settings = Settings() 