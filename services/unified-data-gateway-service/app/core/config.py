"""
Configuration for Unified Data Gateway Service
"""

import os
from pydantic_settings import BaseSettings
from typing import List, Dict, Any, Optional


class Settings(BaseSettings):
    """Application settings"""
    
    # Service info
    SERVICE_NAME: str = "unified-data-gateway-service"
    VERSION: str = "1.0.0"
    
    # API settings
    API_PREFIX: str = "/api/v1"
    DEBUG: bool = False
    
    # Database connections
    POSTGRESQL_HOST: str = os.getenv("POSTGRESQL_HOST", "postgres")
    POSTGRESQL_PORT: int = int(os.getenv("POSTGRESQL_PORT", "5432"))
    POSTGRESQL_USER: str = os.getenv("POSTGRESQL_USER", "platformq")
    POSTGRESQL_PASSWORD: str = os.getenv("POSTGRESQL_PASSWORD", "platformq")
    POSTGRESQL_DB: str = os.getenv("POSTGRESQL_DB", "platformq")
    
    CASSANDRA_HOSTS: List[str] = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
    CASSANDRA_PORT: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    CASSANDRA_USER: str = os.getenv("CASSANDRA_USER", "cassandra")
    CASSANDRA_PASSWORD: str = os.getenv("CASSANDRA_PASSWORD", "cassandra")
    CASSANDRA_KEYSPACE: str = os.getenv("CASSANDRA_KEYSPACE", "platformq")
    
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "platformq")
    
    IGNITE_HOST: str = os.getenv("IGNITE_HOST", "ignite")
    IGNITE_PORT: int = int(os.getenv("IGNITE_PORT", "10800"))
    
    JANUSGRAPH_HOST: str = os.getenv("JANUSGRAPH_HOST", "janusgraph")
    JANUSGRAPH_PORT: int = int(os.getenv("JANUSGRAPH_PORT", "8182"))
    
    ELASTICSEARCH_HOSTS: List[str] = os.getenv("ELASTICSEARCH_HOSTS", "elasticsearch:9200").split(",")
    ELASTICSEARCH_USER: Optional[str] = os.getenv("ELASTICSEARCH_USER")
    ELASTICSEARCH_PASSWORD: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD")
    
    INFLUXDB_HOST: str = os.getenv("INFLUXDB_HOST", "influxdb")
    INFLUXDB_PORT: int = int(os.getenv("INFLUXDB_PORT", "8086"))
    INFLUXDB_TOKEN: str = os.getenv("INFLUXDB_TOKEN", "")
    INFLUXDB_ORG: str = os.getenv("INFLUXDB_ORG", "platformq")
    INFLUXDB_BUCKET: str = os.getenv("INFLUXDB_BUCKET", "platformq")
    
    # Connection pool settings
    POOL_SIZE_MIN: int = 5
    POOL_SIZE_MAX: int = 50
    POOL_TIMEOUT: int = 30
    POOL_RETRY_ATTEMPTS: int = 3
    
    # Cache settings
    CACHE_TTL_DEFAULT: int = 300  # 5 minutes
    CACHE_TTL_METADATA: int = 3600  # 1 hour
    CACHE_TTL_ANALYTICS: int = 60  # 1 minute
    CACHE_MAX_SIZE: int = 100000
    
    # Query routing
    ENABLE_QUERY_ROUTING: bool = True
    ENABLE_QUERY_CACHE: bool = True
    ENABLE_QUERY_OPTIMIZATION: bool = True
    
    # Monitoring
    ENABLE_METRICS: bool = True
    METRICS_PORT: int = 9090
    
    # Security
    ENABLE_TENANT_ISOLATION: bool = True
    ENABLE_ENCRYPTION: bool = True
    
    # Performance
    MAX_CONCURRENT_QUERIES: int = 1000
    QUERY_TIMEOUT_DEFAULT: int = 30
    BATCH_SIZE_DEFAULT: int = 1000
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create settings instance
settings = Settings()


# Database configuration builder
def build_database_config() -> Dict[str, Any]:
    """Build database configuration from settings"""
    return {
        "postgresql": [
            {
                "db_type": "postgresql",
                "hosts": [settings.POSTGRESQL_HOST],
                "port": settings.POSTGRESQL_PORT,
                "database": settings.POSTGRESQL_DB,
                "username": settings.POSTGRESQL_USER,
                "password": settings.POSTGRESQL_PASSWORD,
                "pool_size": settings.POOL_SIZE_MIN,
                "max_pool_size": settings.POOL_SIZE_MAX,
                "timeout": settings.POOL_TIMEOUT
            }
        ],
        "cassandra": [
            {
                "db_type": "cassandra",
                "hosts": settings.CASSANDRA_HOSTS,
                "port": settings.CASSANDRA_PORT,
                "database": settings.CASSANDRA_KEYSPACE,
                "username": settings.CASSANDRA_USER,
                "password": settings.CASSANDRA_PASSWORD,
                "pool_size": settings.POOL_SIZE_MIN,
                "max_pool_size": settings.POOL_SIZE_MAX
            }
        ],
        "mongodb": [
            {
                "db_type": "mongodb",
                "hosts": [settings.MONGODB_URI],
                "port": 27017,
                "database": settings.MONGODB_DATABASE,
                "pool_size": settings.POOL_SIZE_MIN,
                "max_pool_size": settings.POOL_SIZE_MAX
            }
        ],
        "ignite": [
            {
                "db_type": "ignite",
                "hosts": [settings.IGNITE_HOST],
                "port": settings.IGNITE_PORT,
                "pool_size": settings.POOL_SIZE_MIN
            }
        ],
        "janusgraph": [
            {
                "db_type": "janusgraph",
                "hosts": [settings.JANUSGRAPH_HOST],
                "port": settings.JANUSGRAPH_PORT,
                "pool_size": settings.POOL_SIZE_MIN,
                "max_pool_size": settings.POOL_SIZE_MAX
            }
        ],
        "elasticsearch": [
            {
                "db_type": "elasticsearch",
                "hosts": settings.ELASTICSEARCH_HOSTS,
                "port": 9200,
                "username": settings.ELASTICSEARCH_USER,
                "password": settings.ELASTICSEARCH_PASSWORD,
                "timeout": settings.POOL_TIMEOUT,
                "retry_attempts": settings.POOL_RETRY_ATTEMPTS
            }
        ],
        "influxdb": [
            {
                "db_type": "influxdb",
                "hosts": [settings.INFLUXDB_HOST],
                "port": settings.INFLUXDB_PORT,
                "password": settings.INFLUXDB_TOKEN,
                "extra_params": {
                    "org": settings.INFLUXDB_ORG,
                    "bucket": settings.INFLUXDB_BUCKET
                }
            }
        ]
    } 