"""
Digital Integration Hub (DIH) Service

High-performance, in-memory data integration layer built on Apache Ignite.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, HTTPException
from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService
)
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.event_publisher import EventPublisher

from .core.dih import DigitalIntegrationHub, ConsistencyLevel
from .core.cache_manager import CacheManager
from .api import cache_api, region_api, sync_api, health_api
from .integrations.data_sources import DataSourceManager
from .sync.cdc_processor import CDCProcessor
from .sync.sync_orchestrator import SyncOrchestrator

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="dih-service",
    version="1.0.0",
    description="Digital Integration Hub - High-performance in-memory data integration",
    capabilities=[
        "cache-management",
        "data-aggregation",
        "cdc-sync",
        "api-acceleration",
        "transaction-support"
    ],
    dependencies=[
        "data-platform-service",
        "ignite"
    ],
    data_sources=[
        "postgres",
        "cassandra",
        "elasticsearch",
        "mongodb",
        "janusgraph"
    ],
    data_outputs=["ignite", "api"],
    min_memory_mb=2048,
    min_cpu_cores=2.0
)


class DIHService(DataIntelligenceBaseService):
    """Digital Integration Hub Service implementation."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.dih: Optional[DigitalIntegrationHub] = None
        self.cache_manager: Optional[CacheManager] = None
        self.data_source_manager: Optional[DataSourceManager] = None
        self.cdc_processor: Optional[CDCProcessor] = None
        self.sync_orchestrator: Optional[SyncOrchestrator] = None
        
    async def initialize_service(self):
        """Initialize DIH-specific components."""
        # Get Ignite configuration
        ignite_nodes = await self.get_config(
            "ignite.nodes",
            default=[("ignite", 10800)]
        )
        
        # Initialize DIH
        self.dih = DigitalIntegrationHub(
            ignite_nodes=ignite_nodes,
            default_consistency=ConsistencyLevel.STRONG
        )
        await self.dih.initialize()
        
        # Initialize cache manager
        self.cache_manager = CacheManager(self.dih)
        await self.cache_manager.initialize()
        
        # Initialize data source manager
        self.data_source_manager = DataSourceManager(
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher
        )
        await self.data_source_manager.initialize()
        
        # Initialize CDC processor
        self.cdc_processor = CDCProcessor(
            dih=self.dih,
            data_sources=self.data_source_manager,
            event_publisher=self.event_publisher
        )
        await self.cdc_processor.start()
        
        # Initialize sync orchestrator
        self.sync_orchestrator = SyncOrchestrator(
            dih=self.dih,
            data_sources=self.data_source_manager,
            cdc_processor=self.cdc_processor
        )
        await self.sync_orchestrator.start()
        
        # Store components in app state for API access
        self.app.state.dih = self.dih
        self.app.state.cache_manager = self.cache_manager
        self.app.state.data_source_manager = self.data_source_manager
        self.app.state.sync_orchestrator = self.sync_orchestrator
        
    async def cleanup_service(self):
        """Cleanup DIH-specific components."""
        if self.sync_orchestrator:
            await self.sync_orchestrator.stop()
            
        if self.cdc_processor:
            await self.cdc_processor.stop()
            
        if self.data_source_manager:
            await self.data_source_manager.cleanup()
            
        if self.cache_manager:
            await self.cache_manager.cleanup()
            
        if self.dih:
            await self.dih.cleanup()


# Create FastAPI app and service
def create_app():
    """Create the DIH service application."""
    # Get environment configuration
    vault_addr = os.getenv("VAULT_ADDR", "http://vault:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_addr = os.getenv("CONSUL_ADDR", "http://consul:8500")
    
    # Create clients if configured
    vault_client = None
    consul_client = None
    
    if vault_token:
        vault_client = VaultClient(addr=vault_addr, token=vault_token)
        consul_client = ConsulClient(addr=consul_addr)
    
    # Create event publisher
    event_publisher = EventPublisher()
    
    # Create app with common setup
    app, service = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher,
        on_startup=lambda: initialize_dih_service(service),
        on_shutdown=lambda: cleanup_dih_service(service)
    )
    
    # Include API routers
    app.include_router(cache_api.router, prefix="/api/v1/cache", tags=["cache"])
    app.include_router(region_api.router, prefix="/api/v1/regions", tags=["regions"])
    app.include_router(sync_api.router, prefix="/api/v1/sync", tags=["sync"])
    app.include_router(health_api.router, prefix="/api/v1/health", tags=["health"])
    
    return app, service


async def initialize_dih_service(service: DIHService):
    """Additional DIH initialization."""
    # Set up default cache regions
    await setup_default_regions(service.dih)
    
    
async def cleanup_dih_service(service: DIHService):
    """Additional DIH cleanup."""
    pass


async def setup_default_regions(dih: DigitalIntegrationHub):
    """Set up default cache regions."""
    # User session cache
    await dih.create_cache_region(
        name="user-sessions",
        cache_mode="PARTITIONED",
        backups=1,
        eviction_policy="LRU",
        eviction_max_size=10000,
        expiry_policy_factory="CreatedExpiryPolicy",
        expiry_duration=3600000  # 1 hour
    )
    
    # Asset metadata cache
    await dih.create_cache_region(
        name="asset-metadata",
        cache_mode="REPLICATED",
        eviction_policy="LRU",
        eviction_max_size=50000
    )
    
    # Real-time metrics cache
    await dih.create_cache_region(
        name="realtime-metrics",
        cache_mode="PARTITIONED",
        backups=0,
        eviction_policy="FIFO",
        eviction_max_size=100000,
        expiry_policy_factory="CreatedExpiryPolicy",
        expiry_duration=60000  # 1 minute
    )
    
    # Transaction cache
    await dih.create_cache_region(
        name="transactions",
        cache_mode="PARTITIONED",
        backups=2,
        atomicity_mode="TRANSACTIONAL",
        write_synchronization_mode="FULL_SYNC"
    )


# Create the app
app, service = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 