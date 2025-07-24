"""
Client Factory for Plugin-Based Architecture

Provides factory functions for creating service clients using the plugin system.
"""

from typing import Optional, Dict, Any, Union
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient

from .base_plugin import EnhancedServiceClient, ClientConfig, get_plugin_registry
from .plugins import data_stores, messaging, analytics, orchestration, storage, governance, quality, realtime

# Import plugin modules to register them
import importlib
import pkgutil


def discover_plugins():
    """Discover and register all available plugins"""
    registry = get_plugin_registry()
    
    # Plugin packages to scan
    plugin_packages = [
        ("data_stores", data_stores),
        ("messaging", messaging),
        ("analytics", analytics),
        ("orchestration", orchestration),
        ("storage", storage),
        ("governance", governance),
        ("quality", quality),
        ("realtime", realtime)
    ]
    
    for category_name, package in plugin_packages:
        # Discover all modules in the package
        if hasattr(package, '__path__'):
            for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
                if not ispkg and modname.endswith('_plugin'):
                    try:
                        # Import the module to trigger plugin registration
                        full_module_name = f"{package.__name__}.{modname}"
                        importlib.import_module(full_module_name)
                    except Exception as e:
                        print(f"Failed to load plugin {modname}: {e}")


# Auto-discover plugins on module import
discover_plugins()


# Data Store Clients
def create_ignite_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create an Ignite client using the plugin architecture"""
    client_config = ClientConfig(service_name="ignite")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="ignite",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_cassandra_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Cassandra client using the plugin architecture"""
    client_config = ClientConfig(service_name="cassandra")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="cassandra",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_elasticsearch_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create an Elasticsearch client using the plugin architecture"""
    client_config = ClientConfig(service_name="elasticsearch")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="elasticsearch",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_janusgraph_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a JanusGraph client using the plugin architecture"""
    client_config = ClientConfig(service_name="janusgraph")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="janusgraph",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Messaging/Streaming Clients
def create_pulsar_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Pulsar client using the plugin architecture"""
    client_config = ClientConfig(service_name="pulsar")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="pulsar",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_flink_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Flink client using the plugin architecture"""
    client_config = ClientConfig(service_name="flink")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="flink",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_flink_sql_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Flink SQL client using the plugin architecture"""
    client_config = ClientConfig(service_name="flink_sql")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="flink_sql",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Analytics Clients
def create_spark_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Spark client using the plugin architecture"""
    client_config = ClientConfig(service_name="spark")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="spark",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_trino_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Trino client using the plugin architecture"""
    client_config = ClientConfig(service_name="trino")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="trino",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_druid_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Druid client using the plugin architecture"""
    client_config = ClientConfig(service_name="druid")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="druid",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Orchestration Clients
def create_airflow_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create an Airflow client using the plugin architecture"""
    client_config = ClientConfig(service_name="airflow")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="airflow",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_seatunnel_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a SeaTunnel client using the plugin architecture"""
    client_config = ClientConfig(service_name="seatunnel")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="seatunnel",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Storage Clients
def create_minio_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a MinIO client using the plugin architecture"""
    client_config = ClientConfig(service_name="minio")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="minio",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Governance Clients
def create_atlas_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create an Atlas client using the plugin architecture"""
    client_config = ClientConfig(service_name="atlas")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="atlas",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_datahub_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a DataHub client using the plugin architecture"""
    client_config = ClientConfig(service_name="datahub")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="datahub",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_openlineage_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create an OpenLineage client using the plugin architecture"""
    client_config = ClientConfig(service_name="openlineage")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="openlineage",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_unity_catalog_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Unity Catalog client using the plugin architecture"""
    client_config = ClientConfig(service_name="unity_catalog")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="unity_catalog",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Data Quality Clients
def create_great_expectations_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Great Expectations client using the plugin architecture"""
    client_config = ClientConfig(service_name="great_expectations")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="great_expectations",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_deequ_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Deequ client using the plugin architecture"""
    client_config = ClientConfig(service_name="deequ")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="deequ",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_soda_core_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Soda Core client using the plugin architecture"""
    client_config = ClientConfig(service_name="soda_core")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="soda_core",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Real-time Analytics Clients
def create_clickhouse_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a ClickHouse client using the plugin architecture"""
    client_config = ClientConfig(service_name="clickhouse")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="clickhouse",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_doris_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Doris client using the plugin architecture"""
    client_config = ClientConfig(service_name="doris")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="doris",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


def create_pinot_client(
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """Create a Pinot client using the plugin architecture"""
    client_config = ClientConfig(service_name="pinot")
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name="pinot",
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Generic factory function
def create_client(
    service_name: str,
    config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """
    Generic factory function to create any client.
    
    Args:
        service_name: Name of the service/plugin
        config: Service-specific configuration
        vault_client: Vault client for secrets
        consul_client: Consul client for service discovery
        
    Returns:
        Configured service client
        
    Raises:
        ValueError: If plugin not found
    """
    registry = get_plugin_registry()
    
    # Check if plugin exists
    if not registry.get_plugin(service_name):
        available = [p.name for p in registry.list_plugins()]
        raise ValueError(
            f"Plugin '{service_name}' not found. "
            f"Available plugins: {', '.join(available)}"
        )
    
    client_config = ClientConfig(service_name=service_name)
    plugin_config = config or {}
    
    return EnhancedServiceClient(
        plugin_name=service_name,
        config=client_config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Convenience aliases
ignite = create_ignite_client
cassandra = create_cassandra_client
elasticsearch = create_elasticsearch_client
janusgraph = create_janusgraph_client
pulsar = create_pulsar_client
flink = create_flink_client
flink_sql = create_flink_sql_client
spark = create_spark_client
trino = create_trino_client
druid = create_druid_client
airflow = create_airflow_client
seatunnel = create_seatunnel_client
minio = create_minio_client
atlas = create_atlas_client
datahub = create_datahub_client
openlineage = create_openlineage_client
unity_catalog = create_unity_catalog_client
great_expectations = create_great_expectations_client
deequ = create_deequ_client
soda_core = create_soda_core_client
clickhouse = create_clickhouse_client
doris = create_doris_client
pinot = create_pinot_client 