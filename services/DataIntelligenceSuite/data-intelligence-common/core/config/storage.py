"""Storage system configurations."""

from dataclasses import dataclass
from .base import DatabaseConfig


@dataclass
class IgniteConfig(DatabaseConfig):
    """Apache Ignite configuration"""
    type: str = "ignite"
    port: int = 10800


@dataclass
class CassandraConfig(DatabaseConfig):
    """Apache Cassandra configuration"""
    type: str = "cassandra"
    port: int = 9042


@dataclass
class ElasticsearchConfig(DatabaseConfig):
    """Elasticsearch configuration"""
    type: str = "elasticsearch"
    port: int = 9200


@dataclass
class JanusGraphConfig(DatabaseConfig):
    """JanusGraph configuration"""
    type: str = "janusgraph"
    port: int = 8182


@dataclass
class MinioConfig(DatabaseConfig):
    """MinIO configuration"""
    type: str = "minio"
    port: int = 9000


@dataclass
class MilvusConfig(DatabaseConfig):
    """Milvus configuration"""
    type: str = "milvus"
    port: int = 19530 