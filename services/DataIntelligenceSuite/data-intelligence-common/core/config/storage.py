"""
Storage Configuration Classes

Provides configurations for various storage backends using unified approach.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from datetime import timedelta

from .unified import DatabaseConnectionConfig
from .base import DatabaseConfig


@dataclass
class IgniteConfig(DatabaseConnectionConfig):
    """Apache Ignite configuration"""
    # Ignite specific
    cache_mode: str = "PARTITIONED"
    backups: int = 1
    write_synchronization_mode: str = "FULL_SYNC"
    atomicity_mode: str = "TRANSACTIONAL"
    
    # Memory
    data_region_name: str = "default"
    initial_size_mb: int = 256
    max_size_mb: int = 1024
    persistence_enabled: bool = True
    
    # Query
    sql_schema: str = "PUBLIC"
    query_parallelism: int = 4
    
    def __post_init__(self):
        if not self.port:
            self.port = 10800
        super().__post_init__()


@dataclass
class CassandraConfig(DatabaseConnectionConfig):
    """Apache Cassandra configuration"""
    # Cassandra specific
    keyspace: str = ""
    consistency_level: str = "LOCAL_QUORUM"
    replication_factor: int = 3
    
    # Connection
    contact_points: List[str] = field(default_factory=list)
    local_datacenter: str = "datacenter1"
    
    # Performance
    fetch_size: int = 5000
    request_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=12))
    
    def __post_init__(self):
        if not self.port:
            self.port = 9042
        if not self.contact_points and self.host:
            self.contact_points = [self.host]
        super().__post_init__()


@dataclass
class ElasticsearchConfig(DatabaseConnectionConfig):
    """Elasticsearch configuration"""
    # Elasticsearch specific
    index_prefix: str = ""
    number_of_shards: int = 5
    number_of_replicas: int = 1
    
    # Connection
    use_https: bool = True
    verify_certs: bool = True
    api_key: Optional[str] = None
    
    # Performance
    bulk_size: int = 1000
    bulk_refresh: str = "wait_for"
    scroll_timeout: str = "5m"
    
    def __post_init__(self):
        if not self.port:
            self.port = 9200 if not self.use_https else 443
        super().__post_init__()


@dataclass
class JanusGraphConfig(DatabaseConnectionConfig):
    """JanusGraph configuration"""
    # JanusGraph specific
    storage_backend: str = "cassandra"
    index_backend: str = "elasticsearch"
    
    # Graph settings
    schema_default: str = "none"
    schema_constraints: bool = True
    
    # Cache
    cache_db_cache: bool = True
    cache_db_cache_size: float = 0.5
    cache_db_cache_time: int = 180000
    
    # Transactions
    max_commit_time: int = 10000
    
    def __post_init__(self):
        if not self.port:
            self.port = 8182
        super().__post_init__()


@dataclass
class MinioConfig(DatabaseConnectionConfig):
    """MinIO configuration"""
    # MinIO specific
    access_key: str = ""
    secret_key: str = ""
    bucket_name: str = ""
    region: str = "us-east-1"
    
    # Connection
    secure: bool = True
    
    # Performance
    part_size: int = 5 * 1024 * 1024  # 5MB
    multipart_threshold: int = 25 * 1024 * 1024  # 25MB
    max_pool_connections: int = 10
    
    def __post_init__(self):
        if not self.port:
            self.port = 9000
        # Map to standard auth fields
        if self.access_key and not self.username:
            self.username = self.access_key
        if self.secret_key and not self.password:
            self.password = self.secret_key
        super().__post_init__()


@dataclass
class MilvusConfig(DatabaseConnectionConfig):
    """Milvus configuration"""
    # Milvus specific
    collection_name: str = ""
    dim: int = 128
    index_type: str = "IVF_FLAT"
    metric_type: str = "L2"
    
    # Index parameters
    nlist: int = 1024
    nprobe: int = 10
    
    # Search parameters
    top_k: int = 10
    search_params: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.port:
            self.port = 19530
        super().__post_init__()


# Backward compatibility - keep old base classes
@dataclass
class LegacyIgniteConfig(DatabaseConfig):
    """Legacy Ignite config for backward compatibility"""
    cache_mode: str = "PARTITIONED"
    backups: int = 1
    
    def to_unified(self) -> IgniteConfig:
        """Convert to unified config"""
        return IgniteConfig(
            host=self.host,
            port=self.port,
            cache_mode=self.cache_mode,
            backups=self.backups
        )


# Re-export
__all__ = [
    'IgniteConfig',
    'CassandraConfig',
    'ElasticsearchConfig',
    'JanusGraphConfig',
    'MinioConfig',
    'MilvusConfig',
    'LegacyIgniteConfig'
] 