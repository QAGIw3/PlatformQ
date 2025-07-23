"""
Dependency Injection Container

Central container for managing service dependencies
"""

from typing import Optional
from dependency_injector import containers, providers

from data_intelligence_common import (
    ProcessorConfig,
    MetricsCollector,
    EventBus,
    CacheManager as BaseCacheManager
)
from data_intelligence_common.core.events.backends import PulsarEventBackend
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient

from .config import settings
from .cdc_manager import CDCManager
from .stream_manager import StreamManager
from .batch_manager import BatchManager
from .catalog_manager import CatalogManager
from .storage_manager import StorageManager
from .lakehouse_manager import LakehouseManager
from ..infrastructure.seatunnel import SeaTunnelClient
from ..infrastructure.spark import SparkClient
from ..infrastructure.flink import FlinkClient
from ..infrastructure.minio import MinIOClient
from ..infrastructure.iceberg import IcebergCatalog
from ..infrastructure.delta import DeltaLakeClient


class Container(containers.DeclarativeContainer):
    """Main dependency injection container"""
    
    # Configuration
    config = providers.Configuration()
    
    # External clients
    vault_client = providers.Singleton(
        VaultClient,
        url=settings.VAULT_URL,
        token=settings.VAULT_TOKEN
    )
    
    consul_client = providers.Singleton(
        ConsulClient,
        host=settings.CONSUL_HOST,
        port=settings.CONSUL_PORT
    )
    
    # Event system
    pulsar_backend = providers.Singleton(
        PulsarEventBackend,
        service_url=settings.PULSAR_URL,
        topic_prefix="data-platform"
    )
    
    event_bus = providers.Singleton(
        EventBus,
        backend=pulsar_backend
    )
    
    # Metrics
    metrics_collector = providers.Singleton(
        MetricsCollector,
        service_name="data-platform-service"
    )
    
    # Cache
    cache_manager = providers.Singleton(
        BaseCacheManager,
        ignite_nodes=settings.IGNITE_NODES
    )
    
    # Infrastructure clients
    seatunnel_client = providers.Singleton(
        SeaTunnelClient,
        base_url=settings.SEATUNNEL_URL,
        api_key=settings.SEATUNNEL_API_KEY
    )
    
    spark_client = providers.Singleton(
        SparkClient,
        master_url=settings.SPARK_MASTER_URL,
        app_name="data-platform-service"
    )
    
    flink_client = providers.Singleton(
        FlinkClient,
        job_manager_url=settings.FLINK_JOB_MANAGER_URL
    )
    
    minio_client = providers.Singleton(
        MinIOClient,
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY
    )
    
    iceberg_catalog = providers.Singleton(
        IcebergCatalog,
        catalog_name=settings.ICEBERG_CATALOG_NAME,
        warehouse_location=settings.ICEBERG_WAREHOUSE_LOCATION,
        minio_client=minio_client
    )
    
    delta_client = providers.Singleton(
        DeltaLakeClient,
        spark_client=spark_client,
        warehouse_location=settings.DELTA_WAREHOUSE_LOCATION
    )
    
    # Core managers
    cdc_manager = providers.Singleton(
        CDCManager,
        seatunnel_client=seatunnel_client,
        vault_client=vault_client,
        event_bus=event_bus,
        metrics=metrics_collector,
        config=providers.Factory(
            ProcessorConfig,
            name="cdc-manager",
            ml_optimization=settings.ENABLE_ML_OPTIMIZATION,
            cost_tracking=settings.ENABLE_COST_TRACKING
        )
    )
    
    stream_manager = providers.Singleton(
        StreamManager,
        flink_client=flink_client,
        pulsar_backend=pulsar_backend,
        event_bus=event_bus,
        metrics=metrics_collector,
        config=providers.Factory(
            ProcessorConfig,
            name="stream-manager"
        )
    )
    
    batch_manager = providers.Singleton(
        BatchManager,
        spark_client=spark_client,
        seatunnel_client=seatunnel_client,
        event_bus=event_bus,
        metrics=metrics_collector,
        config=providers.Factory(
            ProcessorConfig,
            name="batch-manager"
        )
    )
    
    catalog_manager = providers.Singleton(
        CatalogManager,
        iceberg_catalog=iceberg_catalog,
        delta_client=delta_client,
        event_bus=event_bus,
        cache_manager=cache_manager,
        config=providers.Factory(
            ProcessorConfig,
            name="catalog-manager"
        )
    )
    
    storage_manager = providers.Singleton(
        StorageManager,
        minio_client=minio_client,
        vault_client=vault_client,
        event_bus=event_bus,
        metrics=metrics_collector,
        config=providers.Factory(
            ProcessorConfig,
            name="storage-manager"
        )
    )
    
    lakehouse_manager = providers.Singleton(
        LakehouseManager,
        iceberg_catalog=iceberg_catalog,
        delta_client=delta_client,
        spark_client=spark_client,
        event_bus=event_bus,
        metrics=metrics_collector,
        config=providers.Factory(
            ProcessorConfig,
            name="lakehouse-manager"
        )
    )


# Global container instance
container = Container()


async def initialize_container():
    """Initialize the container and all services"""
    # Initialize all singleton services
    await container.event_bus().initialize()
    await container.cache_manager().initialize()
    
    # Initialize managers
    await container.cdc_manager().initialize()
    await container.stream_manager().initialize()
    await container.batch_manager().initialize()
    await container.catalog_manager().initialize()
    await container.storage_manager().initialize()
    await container.lakehouse_manager().initialize()


async def shutdown_container():
    """Shutdown the container and all services"""
    # Shutdown managers
    await container.cdc_manager().shutdown()
    await container.stream_manager().shutdown()
    await container.batch_manager().shutdown()
    await container.catalog_manager().shutdown()
    await container.storage_manager().shutdown()
    await container.lakehouse_manager().shutdown()
    
    # Shutdown infrastructure
    await container.event_bus().shutdown()
    await container.cache_manager().shutdown() 