"""
Enhanced Client Framework for DataIntelligenceSuite

Provides base client implementations with built-in patterns and decorators.
"""

from .base import (
    BaseClient,
    RESTClient,
    ClientConfig,
    RetryConfig,
    CircuitBreakerConfig,
    ClientError,
    ConnectionError,
    AuthenticationError,
    RateLimitError,
    CircuitBreakerError,
    retry,
    cached,
    circuit_breaker,
    rate_limited,
    monitored,
    authenticated
)

from .base_client import ServiceClient

# Plugin architecture
from .base_plugin import (
    ClientPlugin,
    PluginMetadata,
    PluginCapability,
    PluginRegistry,
    EnhancedServiceClient,
    get_plugin_registry,
    create_client as create_plugin_client
)

# Factory functions
from .factory import (
    # Data stores
    create_ignite_client,
    create_cassandra_client,
    create_elasticsearch_client,
    create_janusgraph_client,
    # Messaging
    create_pulsar_client,
    create_flink_client,
    create_flink_sql_client,
    # Analytics
    create_spark_client,
    create_trino_client,
    create_druid_client,
    # Orchestration
    create_airflow_client,
    create_seatunnel_client,
    # Storage
    create_minio_client,
    # Governance
    create_atlas_client,
    create_datahub_client,
    create_openlineage_client,
    create_unity_catalog_client,
    # Quality
    create_great_expectations_client,
    create_deequ_client,
    create_soda_core_client,
    # Realtime
    create_clickhouse_client,
    create_doris_client,
    create_pinot_client,
    # Generic
    create_client,
    # Aliases
    ignite,
    cassandra,
    elasticsearch,
    janusgraph,
    pulsar,
    flink,
    flink_sql,
    spark,
    trino,
    druid,
    airflow,
    seatunnel,
    minio,
    atlas,
    datahub,
    openlineage,
    unity_catalog,
    great_expectations,
    deequ,
    soda_core,
    clickhouse,
    doris,
    pinot
)

# Service clients
from .analytics_client import AnalyticsClient
from .auth_client import AuthServiceClient
from .catalog_client import CatalogClient
from .ml_client import MLPlatformClient
from .processing_client import ProcessingClient

__all__ = [
    # Base classes
    "BaseClient",
    "RESTClient",
    "ServiceClient",
    
    # Configuration
    "ClientConfig",
    "RetryConfig",
    "CircuitBreakerConfig",
    
    # Errors
    "ClientError",
    "ConnectionError",
    "AuthenticationError",
    "RateLimitError",
    "CircuitBreakerError",
    
    # Decorators
    "retry",
    "cached",
    "circuit_breaker",
    "rate_limited",
    "monitored",
    "authenticated",
    
    # Plugin architecture
    "ClientPlugin",
    "PluginMetadata",
    "PluginCapability",
    "PluginRegistry",
    "EnhancedServiceClient",
    "get_plugin_registry",
    "create_plugin_client",
    
    # Factory functions - Data stores
    "create_ignite_client",
    "create_cassandra_client",
    "create_elasticsearch_client",
    "create_janusgraph_client",
    
    # Factory functions - Messaging
    "create_pulsar_client",
    "create_flink_client",
    "create_flink_sql_client",
    
    # Factory functions - Analytics
    "create_spark_client",
    "create_trino_client",
    "create_druid_client",
    
    # Factory functions - Orchestration
    "create_airflow_client",
    "create_seatunnel_client",
    
    # Factory functions - Storage
    "create_minio_client",
    
    # Factory functions - Governance
    "create_atlas_client",
    "create_datahub_client",
    "create_openlineage_client",
    "create_unity_catalog_client",
    
    # Factory functions - Quality
    "create_great_expectations_client",
    "create_deequ_client",
    "create_soda_core_client",
    
    # Factory functions - Realtime
    "create_clickhouse_client",
    "create_doris_client",
    "create_pinot_client",
    
    # Generic factory
    "create_client",
    
    # Convenience aliases
    "ignite",
    "cassandra",
    "elasticsearch",
    "janusgraph",
    "pulsar",
    "flink",
    "flink_sql",
    "spark",
    "trino",
    "druid",
    "airflow",
    "seatunnel",
    "minio",
    "atlas",
    "datahub",
    "openlineage",
    "unity_catalog",
    "great_expectations",
    "deequ",
    "soda_core",
    "clickhouse",
    "doris",
    "pinot",
    
    # Service clients
    "AnalyticsClient",
    "AuthServiceClient",
    "CatalogClient",
    "MLPlatformClient",
    "ProcessingClient"
] 