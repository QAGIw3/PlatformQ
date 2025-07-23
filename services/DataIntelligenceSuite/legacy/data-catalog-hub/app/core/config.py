"""
Configuration settings for Data Catalog Service
"""

from typing import List, Optional, Dict
from pathlib import Path

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Enhanced Data Catalog Service configuration"""
    
    # Service Configuration
    SERVICE_NAME: str = "data-catalog-service"
    SERVICE_PORT: int = 8017
    ENVIRONMENT: str = "production"
    
    # Apache Atlas
    ATLAS_URL: str = "http://atlas:21000"
    ATLAS_USERNAME: str = "admin"
    ATLAS_PASSWORD: Optional[str] = None
    ATLAS_CLIENT_TIMEOUT: int = 30
    
    # Schema Registry
    SCHEMA_REGISTRY_ENABLED: bool = True
    SCHEMA_COMPATIBILITY_DEFAULT: str = "BACKWARD"
    SCHEMA_CACHE_SIZE: int = 1000
    
    # Search Configuration
    SEARCH_ENGINE: str = "elasticsearch"
    ELASTICSEARCH_HOSTS: List[str] = ["elasticsearch:9200"]
    SEARCH_INDEX_PREFIX: str = "catalog_"
    SEARCH_RESULT_LIMIT: int = 100
    
    # Lineage Processing
    LINEAGE_BATCH_SIZE: int = 100
    LINEAGE_PROCESSING_INTERVAL: int = 60  # seconds
    LINEAGE_RETENTION_DAYS: int = 365
    
    # Classification
    AUTO_CLASSIFICATION_ENABLED: bool = True
    CLASSIFICATION_SCAN_INTERVAL: int = 300  # 5 minutes
    PII_DETECTION_ENABLED: bool = True
    
    # Storage
    IGNITE_HOST: str = "ignite"
    IGNITE_PORT: int = 10800
    CACHE_TTL: int = 300  # 5 minutes
    
    # Events
    PULSAR_URL: str = "pulsar://pulsar:6650"
    EVENT_TOPIC_PREFIX: str = "catalog-events-"
    
    # Integration
    AUTH_SERVICE_URL: str = "http://auth-service:8001"
    QUALITY_SERVICE_URL: str = "http://unified-quality-service:8015"
    
    # New Enhancement Settings
    
    # Medallion Discovery
    minio_endpoint: str = "minio:9000"
    minio_bucket_bronze: Optional[str] = "lake-bronze"
    minio_bucket_silver: Optional[str] = "lake-silver"
    minio_bucket_gold: Optional[str] = "lake-gold"
    discovery_interval_minutes: int = 60  # Run discovery every hour
    discovery_full_scan_hours: int = 24  # Full scan once a day
    enable_data_profiling: bool = True
    profiling_sample_size: int = 10000
    batch_service_url: str = "http://batch-processing-service:8012"
    
    # Enhanced Search Integration
    search_service_url: str = "http://search-service:8031"
    search_api_key: Optional[str] = None
    enable_ai_search: bool = True
    
    # Quality Integration
    quality_service_url: str = "http://unified-quality-service:8015"
    quality_cache_ttl: int = 3600  # 1 hour
    auto_quality_rules: bool = True
    
    # Access Analytics
    analytics_backend_url: str = "http://analytics-service:8018"
    access_tracking_enabled: bool = True
    analytics_retention_days: int = 90
    
    # Business Glossary
    glossary_ai_enabled: bool = True
    glossary_auto_mapping: bool = True
    glossary_approval_required: bool = True
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Create global settings instance
settings = Settings() 