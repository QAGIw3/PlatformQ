"""
Dependency Injection Container

Central container for managing all service dependencies.
"""

from dependency_injector import containers, providers
from elasticsearch import AsyncElasticsearch
from pyignite import AsyncClient as IgniteAsyncClient

from app.core.config import settings
from app.core.atlas_client import AtlasClient
from app.core.cache_manager import CacheManager
from app.core.schema_registry import SchemaRegistry
from app.core.lineage_processor import LineageProcessor
from app.core.classifier import Classifier
from app.core.glossary import GlossaryManager, AIGlossaryEnhancements
from app.core.analytics import (
    MedallionDiscoveryEngine,
    AccessAnalyticsEngine,
    QualityIntegrationEngine
)
from app.core.search import CatalogSearchIntegration

# Import new service interfaces
from app.services.catalog import (
    EntityService,
    SchemaService,
    LineageService,
    ClassificationService,
    GlossaryService
)
from app.services.search import UnifiedSearchService
from app.services.ai import EmbeddingManager, UnifiedQueryAnalyzer
from app.services.storage import IgniteCacheAdapter

# Import repositories
from app.infrastructure.repositories import (
    EntityRepository,
    SchemaRepository,
    LineageRepository,
    GlossaryRepository
)

# Import event bus
from app.events import EventBus


class Container(containers.DeclarativeContainer):
    """Main dependency injection container"""
    
    # Configuration
    config = providers.Configuration()
    
    # Infrastructure - External Clients
    elasticsearch_client = providers.Singleton(
        AsyncElasticsearch,
        hosts=config.elasticsearch.hosts,
        verify_certs=False,
        ssl_show_warn=False
    )
    
    ignite_client = providers.Singleton(
        IgniteAsyncClient
    )
    
    # Core Components
    atlas_client = providers.Singleton(
        AtlasClient,
        settings=config
    )
    
    cache_manager = providers.Singleton(
        CacheManager,
        settings=config
    )
    
    # Ignite Cache Adapter (new unified cache)
    ignite_cache_adapter = providers.Singleton(
        IgniteCacheAdapter,
        ignite_client=ignite_client,
        cache_config=config.cache
    )
    
    # Event Bus
    event_bus = providers.Singleton(
        EventBus
    )
    
    # Core Catalog Components
    schema_registry = providers.Singleton(
        SchemaRegistry,
        settings=config,
        atlas_client=atlas_client,
        cache_manager=ignite_cache_adapter
    )
    
    lineage_processor = providers.Singleton(
        LineageProcessor,
        atlas_client=atlas_client,
        cache_manager=ignite_cache_adapter
    )
    
    classifier = providers.Singleton(
        Classifier,
        settings=config,
        atlas_client=atlas_client
    )
    
    glossary_manager = providers.Singleton(
        GlossaryManager,
        settings=config,
        atlas_client=atlas_client,
        cache_manager=ignite_cache_adapter
    )
    
    # Enhanced Components
    medallion_discovery = providers.Singleton(
        MedallionDiscoveryEngine,
        atlas_client=atlas_client,
        quality_service_url=config.quality_service_url
    )
    
    access_analytics = providers.Singleton(
        AccessAnalyticsEngine,
        atlas_client=atlas_client,
        analytics_backend_url=config.analytics_backend_url
    )
    
    ai_glossary_enhancements = providers.Singleton(
        AIGlossaryEnhancements,
        atlas_client=atlas_client,
        glossary_manager=glossary_manager
    )
    
    quality_integration = providers.Singleton(
        QualityIntegrationEngine,
        atlas_client=atlas_client,
        quality_service_url=config.quality_service_url
    )
    
    catalog_search_integration = providers.Singleton(
        CatalogSearchIntegration,
        atlas_client=atlas_client,
        search_service_url=None
    )
    
    # AI Components
    embedding_manager = providers.Singleton(
        EmbeddingManager,
        ignite_client=ignite_cache_adapter
    )
    
    query_analyzer = providers.Singleton(
        UnifiedQueryAnalyzer,
        cache_adapter=ignite_cache_adapter
    )
    
    # Repositories
    entity_repository = providers.Singleton(
        EntityRepository,
        atlas_client=atlas_client,
        cache_manager=ignite_cache_adapter,
        event_bus=event_bus
    )
    
    schema_repository = providers.Singleton(
        SchemaRepository,
        schema_registry=schema_registry,
        cache_manager=ignite_cache_adapter
    )
    
    lineage_repository = providers.Singleton(
        LineageRepository,
        lineage_processor=lineage_processor,
        atlas_client=atlas_client,
        cache_manager=ignite_cache_adapter
    )
    
    glossary_repository = providers.Singleton(
        GlossaryRepository,
        glossary_manager=glossary_manager,
        ai_enhancements=ai_glossary_enhancements,
        cache_manager=ignite_cache_adapter
    )
    
    # Domain Services
    entity_service = providers.Factory(
        EntityService,
        repository=entity_repository,
        schema_service=providers.DependsOn(schema_service),
        event_bus=event_bus,
        classifier=classifier
    )
    
    schema_service = providers.Factory(
        SchemaService,
        repository=schema_repository,
        event_bus=event_bus
    )
    
    lineage_service = providers.Factory(
        LineageService,
        repository=lineage_repository,
        entity_repository=entity_repository,
        event_bus=event_bus
    )
    
    classification_service = providers.Factory(
        ClassificationService,
        classifier=classifier,
        entity_repository=entity_repository,
        event_bus=event_bus
    )
    
    glossary_service = providers.Factory(
        GlossaryService,
        repository=glossary_repository,
        entity_repository=entity_repository,
        event_bus=event_bus
    )
    
    # Search Service
    unified_search_service = providers.Factory(
        UnifiedSearchService,
        es_client=elasticsearch_client,
        query_analyzer=query_analyzer,
        embedding_manager=embedding_manager,
        cache_adapter=ignite_cache_adapter,
        catalog_search=catalog_search_integration,
        event_bus=event_bus
    )
    
    # Analytics Services
    access_analytics_service = providers.Factory(
        providers.Object,  # Will be replaced with actual service
        access_analytics=access_analytics,
        event_bus=event_bus
    )
    
    quality_service = providers.Factory(
        providers.Object,  # Will be replaced with actual service
        quality_integration=quality_integration,
        event_bus=event_bus
    )
    
    discovery_service = providers.Factory(
        providers.Object,  # Will be replaced with actual service
        medallion_discovery=medallion_discovery,
        event_bus=event_bus
    ) 