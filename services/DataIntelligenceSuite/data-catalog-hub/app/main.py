"""
Data Catalog Hub - Unified Catalog and Search Service

Combines comprehensive metadata management from Apache Atlas with 
advanced search capabilities including AI-powered search, vector search,
and intelligent discovery.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_client import make_asgi_app
import uvicorn

from platformq_shared.logging import get_logger
from libs.data_intelligence_common.base_service import DataIntelligenceBaseService

from app.core.config import settings
from app.core.atlas_client import AtlasClient
from app.core.cache_manager import CacheManager
from app.core.schema_registry import SchemaRegistry
from app.core.lineage_processor import LineageProcessor
from app.core.classifier import Classifier
from app.core.glossary import GlossaryManager
from app.core.analytics import MedallionDiscoveryEngine, AccessAnalyticsEngine, QualityIntegrationEngine
from app.core.glossary import AIGlossaryEnhancements
from app.core.search import CatalogSearchIntegration

# Search components
from app.services.indexer import IndexingService
from app.services.es_vector_search import ElasticsearchVectorService
from app.services.hybrid_search import HybridSearchService
from app.services.query_understanding import QueryUnderstandingService
from app.services.ai_search_enhancement import AISearchEnhancement
from app.services.search_analytics import SearchAnalyticsService
from app.core.search import UnifiedSearchIntegration

# Event processors
from app.event_processors import IndexEventProcessor
from app.messaging.search_consumer import SearchEventConsumer

# API routers
from app.api import (
    entities, classifications, glossary, lineage, schemas, search,
    monitoring, health, discovery, intelligent_search, analytics, quality,
    unified_search, vector_endpoints
)

logger = get_logger(__name__)


class DataCatalogHub(DataIntelligenceBaseService):
    """Unified Data Catalog and Search Hub"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Core catalog components
        self.atlas_client: Optional[AtlasClient] = None
        self.cache_manager: Optional[CacheManager] = None
        self.schema_registry: Optional[SchemaRegistry] = None
        self.search_engine = None  # Will be initialized with ES
        self.lineage_processor: Optional[LineageProcessor] = None
        self.classifier: Optional[Classifier] = None
        self.glossary_manager: Optional[GlossaryManager] = None
        
        # Enhanced catalog components
        self.medallion_discovery: Optional[MedallionDiscoveryEngine] = None
        self.access_analytics: Optional[AccessAnalyticsEngine] = None
        self.business_glossary: Optional[AIGlossaryEnhancements] = None
        self.quality_integration: Optional[QualityIntegrationEngine] = None
        self.catalog_search: Optional[CatalogSearchIntegration] = None
        
        # Search components
        self.indexing_service: Optional[IndexingService] = None
        self.vector_search_service: Optional[ElasticsearchVectorService] = None
        self.hybrid_search_service: Optional[HybridSearchService] = None
        self.query_understanding: Optional[QueryUnderstandingService] = None
        self.ai_search: Optional[AISearchEnhancement] = None
        self.search_analytics: Optional[SearchAnalyticsService] = None
        self.unified_search: Optional[UnifiedSearchIntegration] = None
        
        # Event processors
        self.index_processor: Optional[IndexEventProcessor] = None
        self.search_consumer: Optional[SearchEventConsumer] = None
    
    async def initialize_service(self):
        """Initialize all service components"""
        try:
            # Initialize base service
            await super().initialize_service()
            
            # Initialize Atlas client
            self.atlas_client = AtlasClient(settings)
            await self.atlas_client.initialize()
            logger.info("Atlas client initialized")
            
            # Initialize cache
            self.cache_manager = CacheManager(settings)
            await self.cache_manager.connect()
            logger.info("Cache manager connected")
            
            # Initialize search components first (as catalog components depend on them)
            await self._initialize_search_components()
            
            # Initialize catalog components
            await self._initialize_catalog_components()
            
            # Initialize event processors
            await self._initialize_event_processors()
            
            # Setup API dependencies
            self._setup_api_dependencies()
            
            logger.info("Data Catalog Hub initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Data Catalog Hub: {e}")
            raise
    
    async def _initialize_catalog_components(self):
        """Initialize catalog-specific components"""
        # Schema Registry
        self.schema_registry = SchemaRegistry(self.atlas_client, self.cache_manager)
        await self.schema_registry.initialize()
        logger.info("Schema registry initialized")
        
        # Lineage Processor
        self.lineage_processor = LineageProcessor(self.atlas_client, self.cache_manager)
        await self.lineage_processor.start()
        logger.info("Lineage processor started")
        
        # Classifier
        self.classifier = Classifier(settings, self.atlas_client)
        await self.classifier.initialize()
        logger.info("Classifier initialized")
        
        # Glossary Manager
        self.glossary_manager = GlossaryManager(settings, self.atlas_client, self.cache_manager)
        await self.glossary_manager.initialize()
        logger.info("Glossary manager initialized")
        
        # Enhanced components
        self.medallion_discovery = MedallionDiscoveryEngine(
            self.atlas_client,
            quality_service_url=settings.quality_service_url
        )
        await self.medallion_discovery.initialize()
        logger.info("Medallion discovery engine initialized")
        
        self.access_analytics = AccessAnalyticsEngine(
            self.atlas_client,
            analytics_backend_url=settings.analytics_backend_url
        )
        logger.info("Access analytics engine initialized")
        
        self.business_glossary = AIGlossaryEnhancements(self.atlas_client, self.glossary_manager)
        logger.info("Enhanced business glossary initialized")
        
        self.quality_integration = QualityIntegrationEngine(
            self.atlas_client,
            quality_service_url=settings.quality_service_url
        )
        logger.info("Quality integration engine initialized")
        
        self.catalog_search = CatalogSearchIntegration(
            self.atlas_client,
            search_service_url=None  # Use internal search
        )
        logger.info("Catalog search integration initialized")
    
    async def _initialize_search_components(self):
        """Initialize search-specific components"""
        # Elasticsearch connection
        from elasticsearch import AsyncElasticsearch
        self.es_client = AsyncElasticsearch(
            hosts=settings.ELASTICSEARCH_HOSTS,
            verify_certs=False,
            ssl_show_warn=False
        )
        
        # Core search services
        self.indexing_service = IndexingService(
            es_client=self.es_client,
            index_prefix=settings.SEARCH_INDEX_PREFIX
        )
        await self.indexing_service.initialize()
        logger.info("Indexing service initialized")
        
        self.vector_search_service = ElasticsearchVectorService(
            es_client=self.es_client,
            embedding_service=None  # Will be configured if enabled
        )
        logger.info("Vector search service initialized")
        
        self.query_understanding = QueryUnderstandingService()
        logger.info("Query understanding service initialized")
        
        self.hybrid_search_service = HybridSearchService(
            es_client=self.es_client,
            vector_service=self.vector_search_service,
            query_understanding=self.query_understanding
        )
        logger.info("Hybrid search service initialized")
        
        # Advanced search services
        self.ai_search = AISearchEnhancement(
            es_client=self.es_client,
            query_understanding=self.query_understanding
        )
        logger.info("AI search enhancement initialized")
        
        self.search_analytics = SearchAnalyticsService(
            es_client=self.es_client,
            index_prefix=settings.SEARCH_INDEX_PREFIX
        )
        await self.search_analytics.initialize()
        logger.info("Search analytics service initialized")
        
        self.unified_search = UnifiedSearchIntegration(
            es_client=self.es_client,
            hybrid_search=self.hybrid_search_service,
            ai_enhancement=self.ai_search,
            search_analytics=self.search_analytics
        )
        logger.info("Unified search integration initialized")
        
        # Set search engine reference for catalog components
        self.search_engine = self.es_client
    
    async def _initialize_event_processors(self):
        """Initialize event processing components"""
        self.index_processor = IndexEventProcessor(
            indexing_service=self.indexing_service,
            event_stream=self.event_stream
        )
        
        self.search_consumer = SearchEventConsumer(
            consumer_id="catalog-hub-search",
            subscription_name="catalog-hub-search-sub",
            topics=["catalog-events-created", "catalog-events-updated", "catalog-events-deleted"],
            pulsar_url=settings.PULSAR_URL,
            processor=self.index_processor
        )
        await self.search_consumer.start()
        logger.info("Search event consumer started")
    
    def _setup_api_dependencies(self):
        """Setup dependencies for API routers"""
        # Catalog API dependencies
        entities.set_dependencies(
            atlas=self.atlas_client,
            schemas=self.schema_registry,
            events=self.event_stream
        )
        
        classifications.set_dependencies(
            clf=self.classifier,
            atlas=self.atlas_client
        )
        
        glossary.set_glossary_deps(
            glossary_manager=self.glossary_manager,
            atlas_client=self.atlas_client,
            cache_manager=self.cache_manager,
            business_glossary=self.business_glossary,
            event_stream=self.event_stream
        )
        
        lineage.set_dependencies(
            processor=self.lineage_processor,
            atlas=self.atlas_client,
            events=self.event_stream
        )
        
        schemas.set_dependencies(
            registry=self.schema_registry,
            cache=self.cache_manager
        )
        
        search.set_dependencies(
            search=self.search_engine,
            atlas=self.atlas_client
        )
        
        monitoring.set_dependencies(
            atlas=self.atlas_client,
            schemas=self.schema_registry,
            search=self.search_engine,
            lineage=self.lineage_processor
        )
        
        health.set_dependencies({
            'atlas': self.atlas_client,
            'cache': self.cache_manager,
            'search': self.search_engine,
            'pulsar': self.event_stream
        })
        
        # Enhanced catalog API dependencies
        discovery.set_discovery_deps(
            medallion_discovery=self.medallion_discovery,
            atlas_client=self.atlas_client,
            event_stream=self.event_stream
        )
        
        intelligent_search.set_intelligent_search_deps(
            catalog_search=self.catalog_search,
            ai_search=self.ai_search,
            search_analytics=self.search_analytics,
            event_stream=self.event_stream
        )
        
        analytics.set_analytics_deps(
            access_analytics=self.access_analytics,
            atlas_client=self.atlas_client,
            event_stream=self.event_stream
        )
        
        quality.set_quality_deps(
            quality_integration=self.quality_integration,
            atlas_client=self.atlas_client,
            event_stream=self.event_stream
        )
        
        # Search API dependencies
        unified_search.set_search_deps(
            unified_search=self.unified_search,
            indexing_service=self.indexing_service,
            search_analytics=self.search_analytics
        )
        
        vector_endpoints.set_vector_deps(
            vector_service=self.vector_search_service,
            hybrid_search=self.hybrid_search_service
        )
    
    async def cleanup_service(self):
        """Cleanup all service resources"""
        try:
            # Stop event processors
            if self.search_consumer:
                await self.search_consumer.stop()
            
            # Cleanup search components
            if self.search_analytics:
                await self.search_analytics.cleanup()
            
            if self.es_client:
                await self.es_client.close()
            
            # Cleanup catalog components
            if self.lineage_processor:
                await self.lineage_processor.stop()
            
            if self.classifier:
                await self.classifier.cleanup()
            
            if self.glossary_manager:
                await self.glossary_manager.cleanup()
            
            if self.medallion_discovery:
                await self.medallion_discovery.cleanup()
            
            if self.catalog_search:
                await self.catalog_search.cleanup()
            
            if self.access_analytics:
                await self.access_analytics.cleanup()
            
            if self.atlas_client:
                await self.atlas_client.cleanup()
            
            if self.cache_manager:
                await self.cache_manager.disconnect()
            
            # Cleanup base service
            await super().cleanup_service()
            
            logger.info("Data Catalog Hub cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise


# Create service instance
catalog_hub = DataCatalogHub(
    service_name="data-catalog-hub",
    service_port=settings.SERVICE_PORT
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    try:
        await catalog_hub.initialize_service()
        yield
    finally:
        # Shutdown
        await catalog_hub.cleanup_service()


# Create FastAPI app
app = FastAPI(
    title="Data Catalog Hub",
    description="Unified catalog and search service with AI-powered discovery",
    version="2.0.0",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Mount Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Include all routers
app.include_router(health.router)
app.include_router(entities.router)
app.include_router(classifications.router)
app.include_router(glossary.router)
app.include_router(lineage.router)
app.include_router(schemas.router)
app.include_router(search.router)
app.include_router(monitoring.router)
app.include_router(discovery.router)
app.include_router(intelligent_search.router)
app.include_router(analytics.router)
app.include_router(quality.router)
app.include_router(unified_search.router)
app.include_router(vector_endpoints.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Data Catalog Hub",
        "version": "2.0.0",
        "status": "healthy",
        "description": "Unified catalog and search service",
        "features": [
            "Apache Atlas metadata management",
            "AI-powered intelligent search",
            "Vector and hybrid search",
            "Medallion architecture discovery",
            "Data lineage and impact analysis",
            "Business glossary with AI mapping",
            "Quality score integration",
            "Access pattern analytics",
            "Real-time search analytics"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.SERVICE_PORT,
        reload=False,
        log_config={
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
            },
            "root": {
                "level": "INFO",
                "handlers": ["default"],
            },
        }
    ) 