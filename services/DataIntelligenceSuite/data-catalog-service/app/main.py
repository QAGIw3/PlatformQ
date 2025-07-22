"""
Data Catalog Service

Comprehensive metadata management and data discovery platform built on Apache Atlas.
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
import uvicorn
import hvac
import consul.aio

from data_intelligence_common import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    StructuredLogger,
    create_data_intelligence_app
)

from app.core import (
    settings,
    AtlasClient,
    SchemaRegistry,
    SearchEngine,
    LineageProcessor,
    Classifier,
    GlossaryManager,
    CacheManager
)

from app.api import (
    entities_router,
    schemas_router,
    search_router,
    lineage_router,
    classifications_router,
    glossary_router,
    monitoring_router,
    health_router,
    set_entities_deps,
    set_schemas_deps,
    set_search_deps,
    set_lineage_deps,
    set_classifications_deps,
    set_glossary_deps,
    set_monitoring_deps,
    set_health_deps
)

from platformq_events import EventStream

logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-catalog-service",
    version="1.0.0",
    description="Comprehensive metadata management and data discovery platform",
    dependencies=["atlas", "elasticsearch", "ignite", "pulsar"],
    health_checks=["atlas", "elasticsearch", "cache", "schema_registry"],
    capabilities=["metadata", "lineage", "search", "governance"],
    data_sources=["atlas", "elasticsearch"],
    data_outputs=["catalog-events", "lineage-updates"]
)

# Global components
atlas_client: Optional[AtlasClient] = None
schema_registry: Optional[SchemaRegistry] = None
search_engine: Optional[SearchEngine] = None
lineage_processor: Optional[LineageProcessor] = None
classifier: Optional[Classifier] = None
glossary_manager: Optional[GlossaryManager] = None
cache_manager: Optional[CacheManager] = None
event_stream: Optional[EventStream] = None


class DataCatalogService(DataIntelligenceBaseService):
    """Data Catalog Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.atlas_client = None
        self.schema_registry = None
        self.search_engine = None
        self.lineage_processor = None
        self.classifier = None
        self.glossary_manager = None
        self.cache_manager = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global atlas_client, schema_registry, search_engine, lineage_processor
        global classifier, glossary_manager, cache_manager, event_stream
        
        logger.info("Initializing Data Catalog Service components...")
        
        # Get Atlas configuration from Vault
        atlas_config = await self.vault_consul.get_secret("atlas/config")
        if atlas_config:
            settings.atlas_url = atlas_config.get("url", settings.atlas_url)
            settings.atlas_username = atlas_config.get("username", settings.atlas_username)
            settings.atlas_password = atlas_config.get("password", settings.atlas_password)
        
        # Get Elasticsearch credentials from Vault
        es_creds = await self.vault_consul.get_database_credentials("elasticsearch")
        if es_creds:
            settings.elasticsearch_hosts = es_creds.get("hosts", settings.elasticsearch_hosts)
            settings.elasticsearch_username = es_creds.get("username")
            settings.elasticsearch_password = es_creds.get("password")
        
        # Initialize cache manager with Ignite credentials
        cache_manager = CacheManager(settings)
        await cache_manager.connect()
        self.cache_manager = cache_manager
        
        # Initialize Atlas client
        atlas_client = AtlasClient(settings)
        await atlas_client.initialize()
        self.atlas_client = atlas_client
        
        # Initialize schema registry
        schema_registry = SchemaRegistry(settings, atlas_client, cache_manager)
        await schema_registry.initialize()
        self.schema_registry = schema_registry
        
        # Initialize search engine
        search_engine = SearchEngine(settings, atlas_client, cache_manager)
        await search_engine.initialize()
        self.search_engine = search_engine
        
        # Initialize lineage processor
        lineage_processor = LineageProcessor(settings, atlas_client, cache_manager)
        await lineage_processor.start()
        self.lineage_processor = lineage_processor
        
        # Initialize classifier
        classifier = Classifier(settings, atlas_client)
        await classifier.initialize()
        self.classifier = classifier
        
        # Initialize glossary manager
        glossary_manager = GlossaryManager(settings, atlas_client, cache_manager)
        await glossary_manager.initialize()
        self.glossary_manager = glossary_manager
        
        # Initialize event stream
        event_stream = EventStream(
            service_name="data-catalog-service",
            pulsar_url=settings.pulsar_url
        )
        await event_stream.initialize()
        
        # Set dependencies for API routers
        set_entities_deps(atlas_client, schema_registry, event_stream)
        set_schemas_deps(schema_registry, cache_manager)
        set_search_deps(search_engine, atlas_client)
        set_lineage_deps(lineage_processor, atlas_client, event_stream)
        set_classifications_deps(classifier, atlas_client)
        set_glossary_deps(glossary_manager, atlas_client)
        set_monitoring_deps(cache_manager, atlas_client, schema_registry, search_engine, lineage_processor)
        set_health_deps(atlas_client, schema_registry, search_engine, lineage_processor, classifier, glossary_manager, cache_manager)
        
        # Register health checks
        self.health_manager.register_check("atlas", atlas_client.health_check, critical=True)
        self.health_manager.register_check("elasticsearch", search_engine.health_check, critical=True)
        self.health_manager.register_check("cache", cache_manager.health_check)
        self.health_manager.register_check("schema_registry", schema_registry.health_check)
        
        logger.info("Data Catalog Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Data Catalog Service...")
        
        if self.lineage_processor:
            await self.lineage_processor.stop()
        
        if self.search_engine:
            await self.search_engine.cleanup()
        
        if self.atlas_client:
            await self.atlas_client.cleanup()
        
        if self.cache_manager:
            await self.cache_manager.disconnect()
        
        if event_stream:
            await event_stream.close()
        
        logger.info("Data Catalog Service cleaned up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Get configuration from environment
    vault_addr = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_host = os.getenv("CONSUL_HOST", "localhost")
    consul_port = int(os.getenv("CONSUL_PORT", "8500"))
    consul_token = os.getenv("CONSUL_TOKEN")
    
    # Create Vault client
    vault_client = hvac.Client(url=vault_addr, token=vault_token)
    
    # Create Consul client
    consul_client = consul.aio.Consul(
        host=consul_host,
        port=consul_port,
        token=consul_token
    )
    
    # Create service instance
    service = DataCatalogService(
        vault_client=vault_client,
        consul_client=consul_client
    )
    
    # Create app with common setup
    app = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        service_instance=service,
        title="Data Catalog Service API",
        include_common_middleware=True
    )
    
    # Include API routers
    app.include_router(entities_router, prefix="/api/v1/entities", tags=["entities"])
    app.include_router(schemas_router, prefix="/api/v1/schemas", tags=["schemas"])
    app.include_router(search_router, prefix="/api/v1/search", tags=["search"])
    app.include_router(lineage_router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(classifications_router, prefix="/api/v1/classifications", tags=["classifications"])
    app.include_router(glossary_router, prefix="/api/v1/glossary", tags=["glossary"])
    app.include_router(monitoring_router, prefix="/api/v1/monitoring", tags=["monitoring"])
    app.include_router(health_router, prefix="/api/v1/health", tags=["health"])
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", "8001"))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    ) 