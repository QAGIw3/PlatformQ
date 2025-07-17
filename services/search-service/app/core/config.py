"""
Search Service Configuration
"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings"""
    
    # Service info
    SERVICE_NAME: str = "search-service"
    VERSION: str = "2.0.0"
    
    # Elasticsearch
    ES_HOST: str = os.getenv("ELASTICSEARCH_HOST", "elasticsearch:9200")
    ES_INDEX_NAME: str = os.getenv("ES_INDEX_NAME", "platformq_search")
    ES_TIMEOUT: int = 30
    
    # Milvus vector database
    MILVUS_HOST: str = os.getenv("MILVUS_HOST", "milvus")
    MILVUS_PORT: int = int(os.getenv("MILVUS_PORT", "19530"))
    
    # Redis cache
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    
    # Pulsar
    PULSAR_URL: str = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    
    # Model settings
    EMBEDDING_MODEL_NAME: str = "sentence-transformers/all-mpnet-base-v2"
    MULTILINGUAL_MODEL_NAME: str = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"
    CODE_MODEL_NAME: str = "microsoft/codebert-base"
    
    # Search settings
    DEFAULT_SEARCH_SIZE: int = 10
    MAX_SEARCH_SIZE: int = 100
    VECTOR_SEARCH_TOP_K: int = 20
    
    # Hybrid search weights
    DEFAULT_TEXT_WEIGHT: float = 0.6
    DEFAULT_VECTOR_WEIGHT: float = 0.4
    
    # Cache settings
    EMBEDDING_CACHE_TTL: int = 3600  # 1 hour
    QUERY_CACHE_TTL: int = 300  # 5 minutes
    
    # Performance settings
    MAX_BATCH_SIZE: int = 100
    INDEXING_BATCH_SIZE: int = 50
    
    # Multi-tenancy
    ENABLE_MULTI_TENANCY: bool = True
    DEFAULT_TENANT_ID: str = "default"
    
    # Feature flags
    ENABLE_VECTOR_SEARCH: bool = True
    ENABLE_QUERY_UNDERSTANDING: bool = True
    ENABLE_MULTI_MODAL_SEARCH: bool = True
    ENABLE_SEARCH_ANALYTICS: bool = True
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create settings instance
settings = Settings() 