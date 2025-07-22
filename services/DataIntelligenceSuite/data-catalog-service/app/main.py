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

# Import new components
from app.core.medallion_discovery import MedallionDiscoveryEngine
from app.core.business_glossary_enhanced import BusinessGlossaryEnhanced
from app.core.quality_integration import QualityIntegrationEngine
from app.core.catalog_search_integration import CatalogSearchIntegration
from app.core.access_analytics import AccessAnalyticsEngine

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

# Import new API routers
from app.api.discovery import discovery_router, set_discovery_deps
from app.api.quality import quality_router, set_quality_deps
from app.api.intelligent_search import intelligent_search_router, set_intelligent_search_deps
from app.api.analytics import analytics_router, set_analytics_deps

from platformq_events import EventStream
from minio import Minio

logger = StructuredLogger.get_logger(__name__)

# Service metadata - Updated with new capabilities
SERVICE_METADATA = ServiceMetadata(
    name="data-catalog-service",
    version="2.0.0",
    description="Intelligent data discovery platform with AI-powered search and automated cataloging",
    dependencies=["atlas", "elasticsearch", "ignite", "pulsar", "minio", "quality-service", "search-service"],
    health_checks=["atlas", "elasticsearch", "cache", "schema_registry", "minio", "search_integration"],
    capabilities=[
        "metadata", "lineage", "search", "governance",
        "auto-discovery", "business-glossary", "quality-integration",
        "ai-search", "access-analytics"
    ],
    data_sources=["atlas", "elasticsearch", "minio", "quality-service"],
    data_outputs=["catalog-events", "lineage-updates", "discovery-alerts", "access-metrics"]
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

# New global components
medallion_discovery: Optional[MedallionDiscoveryEngine] = None
business_glossary_enhanced: Optional[BusinessGlossaryEnhanced] = None
quality_integration: Optional[QualityIntegrationEngine] = None
catalog_search_integration: Optional[CatalogSearchIntegration] = None
access_analytics: Optional[AccessAnalyticsEngine] = None
minio_client: Optional[Minio] = None


class DataCatalogService(DataIntelligenceBaseService):
    """Enhanced Data Catalog Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.atlas_client = None
        self.schema_registry = None
        self.search_engine = None
        self.lineage_processor = None
        self.classifier = None
        self.glossary_manager = None
        self.cache_manager = None
        
        # New components
        self.medallion_discovery = None
        self.business_glossary_enhanced = None
        self.quality_integration = None
        self.catalog_search_integration = None
        self.access_analytics = None
        self.minio_client = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global atlas_client, schema_registry, search_engine, lineage_processor
        global classifier, glossary_manager, cache_manager, event_stream
        global medallion_discovery, business_glossary_enhanced, quality_integration
        global catalog_search_integration, access_analytics, minio_client
        
        logger.info("Initializing Enhanced Data Catalog Service components...")
        
        # Get Atlas configuration from Vault
        atlas_config = await self.vault_consul.get_service_config("atlas")
        
        # Initialize core components
        atlas_client = AtlasClient(
            base_url=atlas_config.get("url", settings.ATLAS_URL),
            username=atlas_config.get("username", settings.ATLAS_USERNAME),
            password=atlas_config.get("password", settings.ATLAS_PASSWORD)
        )
        self.atlas_client = atlas_client
        await atlas_client.initialize()
        
        # Initialize schema registry
        schema_registry = SchemaRegistry(settings)
        self.schema_registry = schema_registry
        await schema_registry.initialize()
        
        # Initialize search engine
        search_engine = SearchEngine(settings)
        self.search_engine = search_engine
        await search_engine.initialize()
        
        # Initialize lineage processor
        lineage_processor = LineageProcessor(atlas_client, settings)
        self.lineage_processor = lineage_processor
        
        # Initialize classifier
        classifier = Classifier(atlas_client, settings)
        self.classifier = classifier
        
        # Initialize glossary manager
        glossary_manager = GlossaryManager(atlas_client, settings)
        self.glossary_manager = glossary_manager
        
        # Initialize cache
        cache_manager = CacheManager(settings)
        self.cache_manager = cache_manager
        await cache_manager.initialize()
        
        # Initialize event stream
        event_stream = EventStream(
            service_name=SERVICE_METADATA.name,
            pulsar_url=settings.PULSAR_URL
        )
        await event_stream.initialize()
        
        # Initialize MinIO client for medallion discovery
        minio_config = await self.vault_consul.get_service_config("minio")
        minio_client = Minio(
            endpoint=minio_config.get("endpoint", "minio:9000"),
            access_key=minio_config.get("access_key"),
            secret_key=minio_config.get("secret_key"),
            secure=minio_config.get("secure", False)
        )
        self.minio_client = minio_client
        
        # Initialize new enhancement components
        
        # Medallion Discovery Engine
        medallion_discovery = MedallionDiscoveryEngine(
            atlas_client=atlas_client,
            minio_client=minio_client,
            quality_service_url=settings.quality_service_url
        )
        self.medallion_discovery = medallion_discovery
        
        # Enhanced Business Glossary
        business_glossary_enhanced = BusinessGlossaryEnhanced(atlas_client)
        self.business_glossary_enhanced = business_glossary_enhanced
        
        # Quality Integration Engine
        quality_integration = QualityIntegrationEngine(
            atlas_client=atlas_client,
            quality_service_url=settings.quality_service_url
        )
        self.quality_integration = quality_integration
        
        # Catalog Search Integration
        catalog_search_integration = CatalogSearchIntegration(
            atlas_client=atlas_client,
            search_service_url=settings.search_service_url
        )
        self.catalog_search_integration = catalog_search_integration
        
        # Access Analytics Engine
        access_analytics = AccessAnalyticsEngine(
            atlas_client=atlas_client,
            analytics_backend_url=settings.analytics_backend_url
        )
        self.access_analytics = access_analytics
        
        # Start background tasks
        import asyncio
        
        # Start continuous medallion discovery
        asyncio.create_task(
            medallion_discovery.schedule_continuous_discovery(
                interval_minutes=settings.discovery_interval_minutes,
                full_scan_interval_hours=settings.discovery_full_scan_hours
            )
        )
        
        logger.info("All Enhanced Data Catalog Service components initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific resources"""
        logger.info("Cleaning up Enhanced Data Catalog Service components...")
        
        # Cleanup new components
        if self.medallion_discovery:
            await self.medallion_discovery.cleanup()
        
        if self.quality_integration:
            await self.quality_integration.cleanup()
        
        if self.catalog_search_integration:
            await self.catalog_search_integration.cleanup()
        
        if self.access_analytics:
            await self.access_analytics.cleanup()
        
        # Cleanup core components
        if self.atlas_client:
            await self.atlas_client.cleanup()
        
        if self.schema_registry:
            await self.schema_registry.cleanup()
        
        if self.search_engine:
            await self.search_engine.cleanup()
        
        if self.cache_manager:
            await self.cache_manager.cleanup()
        
        if event_stream:
            await event_stream.cleanup()
        
        logger.info("Enhanced Data Catalog Service cleanup completed")


# Create service instance
service = DataCatalogService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Initialize service
    await service.initialize_service()
    
    # Set dependencies for API routers
    deps = {
        "atlas_client": atlas_client,
        "schema_registry": schema_registry,
        "search_engine": search_engine,
        "lineage_processor": lineage_processor,
        "classifier": classifier,
        "glossary_manager": glossary_manager,
        "cache_manager": cache_manager,
        "event_stream": event_stream
    }
    
    # Set dependencies for existing routers
    set_entities_deps(**deps)
    set_schemas_deps(**deps)
    set_search_deps(**deps)
    set_lineage_deps(**deps)
    set_classifications_deps(**deps)
    set_glossary_deps(
        glossary_manager=glossary_manager,
        atlas_client=atlas_client,
        business_glossary_enhanced=business_glossary_enhanced,
        event_stream=event_stream
    )
    set_monitoring_deps(**deps)
    set_health_deps(**deps)
    
    # Set dependencies for new enhancement routers
    set_discovery_deps(
        medallion_discovery=medallion_discovery,
        atlas_client=atlas_client,
        event_stream=event_stream
    )
    
    set_quality_deps(
        quality_integration=quality_integration,
        atlas_client=atlas_client,
        event_stream=event_stream
    )
    
    set_intelligent_search_deps(
        catalog_search_integration=catalog_search_integration,
        access_analytics=access_analytics,
        atlas_client=atlas_client,
        event_stream=event_stream
    )
    
    set_analytics_deps(
        access_analytics=access_analytics,
        atlas_client=atlas_client,
        event_stream=event_stream
    )
    
    yield
    
    # Cleanup
    await service.cleanup_service()


# Create FastAPI app
    app = create_data_intelligence_app(
    service=service,
    title="Enhanced Data Catalog Service",
    description="Intelligent data discovery platform with AI-powered search and automated cataloging",
    version="2.0.0",
    lifespan=lifespan
    )
    
    # Include API routers
app.include_router(entities_router)
app.include_router(schemas_router)
app.include_router(search_router)
app.include_router(lineage_router)
app.include_router(classifications_router)
app.include_router(glossary_router)
app.include_router(monitoring_router)
app.include_router(health_router)

# Include new enhancement routers
app.include_router(discovery_router)
app.include_router(quality_router)
app.include_router(intelligent_search_router)
app.include_router(analytics_router)


@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "description": SERVICE_METADATA.description,
        "status": "running",
        "capabilities": SERVICE_METADATA.capabilities,
        "enhancements": {
            "auto_discovery": "Active - Continuously discovering medallion layers",
            "business_glossary": "Enhanced - AI-powered term mapping",
            "quality_integration": "Active - Real-time quality scoring",
            "intelligent_search": "Active - AI-powered catalog search",
            "access_analytics": "Active - Usage pattern analysis"
        },
        "endpoints": {
            "openapi": "/docs",
            "health": "/health",
            "metrics": "/metrics",
            "entities": "/api/v1/entities",
            "schemas": "/api/v1/schemas",
            "search": "/api/v1/search",
            "lineage": "/api/v1/lineage",
            "classifications": "/api/v1/classifications",
            "glossary": "/api/v1/glossary",
            "discovery": "/api/v1/discovery",
            "quality": "/api/v1/quality",
            "intelligent_search": "/api/v1/search/intelligent",
            "analytics": "/api/v1/analytics"
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.SERVICE_PORT,
        reload=settings.ENVIRONMENT == "development"
    ) 