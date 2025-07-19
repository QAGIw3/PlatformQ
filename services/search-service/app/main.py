"""
Search Service

Unified search with Elasticsearch, Milvus, and graph integration.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Depends, HTTPException
import logging
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader
from platformq_events import (
    AssetCreatedEvent,
    AssetUpdatedEvent,
    AssetDeletedEvent,
    SimulationCompletedEvent,
    ProjectCreatedEvent,
    DocumentUpdatedEvent,
    SearchIndexRequestEvent
)

from elasticsearch import AsyncElasticsearch
from .api import endpoints
from .api.deps import get_db_session, get_api_key_crud, get_user_crud, get_password_verifier
from .repository import SearchIndexRepository, SearchHistoryRepository
from .event_processors import SearchIndexEventProcessor
from .search_engine import UnifiedSearchEngine
from .indexer import AssetIndexer, SimulationIndexer, ProjectIndexer, DocumentIndexer
from .query_parser import QueryParser
from .graph_search_integration import GraphEnrichedSearchEngine, GraphSearchOrchestrator, GraphSearchConfig
from .services.vector_search import VectorSearchService
from .services.es_vector_search import ESVectorSearchService
from .core.index_mapping import INDEX_MAPPING
from .core.config import settings

logger = logging.getLogger(__name__)

# Service components
search_index_processor = None
es_client = None
vector_service = None
unified_search_engine = None
graph_search_engine = None
service_clients = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global search_index_processor, es_client, vector_service
    global unified_search_engine, graph_search_engine, service_clients
    
    # Startup
    logger.info("Starting Search Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings_dict = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize Elasticsearch v8
    es_config = {
        "hosts": [settings_dict.get("elasticsearch_url", "http://elasticsearch:9200")],
        "verify_certs": settings.ES_VERIFY_CERTS,
        "ssl_show_warn": False
    }
    if settings.ES_USE_SSL:
        es_config["use_ssl"] = True
        
    es_client = AsyncElasticsearch(**es_config)
    app.state.es_client = es_client
    
    # Verify Elasticsearch v8
    info = await es_client.info()
    es_version = info["version"]["number"]
    logger.info(f"Connected to Elasticsearch v{es_version}")
    if not es_version.startswith("8"):
        logger.warning(f"Expected Elasticsearch v8, got v{es_version}")
    
    # Create indices if they don't exist
    indices = ["assets", "simulations", "projects", "documents", "users"]
    for index_name in indices:
        full_index_name = f"{settings.ES_INDEX_PREFIX}_{index_name}"
        if not await es_client.indices.exists(index=full_index_name):
            await es_client.indices.create(
                index=full_index_name,
                body={"mappings": INDEX_MAPPING.get(index_name, {})}
            )
            logger.info(f"Created Elasticsearch index: {full_index_name}")
    
    # Initialize repositories
    app.state.search_index_repo = SearchIndexRepository(get_db_session)
    app.state.search_history_repo = SearchHistoryRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    
    # Initialize vector search if enabled
    if settings_dict.get("enable_vector_search", False):
        vector_service = VectorSearchService(
            milvus_host=settings_dict.get("milvus_host", "milvus"),
            milvus_port=int(settings_dict.get("milvus_port", 19530)),
            collection_name=settings_dict.get("milvus_collection", "platformq_vectors")
        )
        await vector_service.initialize()
        app.state.vector_service = vector_service
    
    # Initialize native Elasticsearch v8 vector search
    es_vector_service = None
    if settings.ENABLE_ES_VECTOR_SEARCH:
        es_vector_service = ESVectorSearchService(es_client)
        await es_vector_service.initialize()
        app.state.es_vector_service = es_vector_service
        logger.info("Initialized native Elasticsearch v8 vector search")
        
    # Initialize enhanced vector search with JanusGraph
    from .services.enhanced_vector_search import EnhancedVectorSearchService
    from .api import vector_endpoints
    
    enhanced_vector_service = EnhancedVectorSearchService(
        es_client=es_client,
        janusgraph_url=settings.JANUSGRAPH_URL,
        redis_client=None,  # Would initialize Redis here
        openai_api_key=settings.OPENAI_API_KEY
    )
    await enhanced_vector_service.initialize()
    app.state.enhanced_vector_service = enhanced_vector_service
    vector_endpoints.vector_service = enhanced_vector_service
    logger.info("Initialized enhanced vector search with JanusGraph integration")
    
    # Initialize indexers
    app.state.asset_indexer = AssetIndexer(es_client, vector_service)
    app.state.simulation_indexer = SimulationIndexer(es_client, vector_service)
    app.state.project_indexer = ProjectIndexer(es_client)
    app.state.document_indexer = DocumentIndexer(es_client, vector_service)
    
    # Initialize query parser
    app.state.query_parser = QueryParser()
    
    # Initialize search engines
    unified_search_engine = UnifiedSearchEngine(
        es_client=es_client,
        vector_service=vector_service,
        query_parser=app.state.query_parser,
        indexers={
            "assets": app.state.asset_indexer,
            "simulations": app.state.simulation_indexer,
            "projects": app.state.project_indexer,
            "documents": app.state.document_indexer
        }
    )
    app.state.unified_search_engine = unified_search_engine
    
    # Initialize graph-enhanced search if enabled
    if settings_dict.get("enable_graph_search", False):
        graph_config = GraphSearchConfig(
            graph_service_url=settings_dict.get("graph_intelligence_service_url", "http://graph-intelligence-service:8000"),
            max_hops=int(settings_dict.get("graph_max_hops", 2)),
            relationship_weights={
                "created_by": 0.8,
                "modified_by": 0.6,
                "references": 0.7,
                "derived_from": 0.9,
                "used_in": 0.7
            }
        )
        
        graph_search_engine = GraphEnrichedSearchEngine(
            search_engine=unified_search_engine,
            graph_config=graph_config,
            service_clients=service_clients
        )
        app.state.graph_search_engine = graph_search_engine
    
    # Initialize event processor
    search_index_processor = SearchIndexEventProcessor(
        service_name="search-service",
        pulsar_url=settings_dict.get("pulsar_url", "pulsar://pulsar:6650"),
        search_index_repo=app.state.search_index_repo,
        indexers={
            "assets": app.state.asset_indexer,
            "simulations": app.state.simulation_indexer,
            "projects": app.state.project_indexer,
            "documents": app.state.document_indexer
        }
    )
    
    # Start event processor
    await search_index_processor.start()
    
    logger.info("Search Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Search Service...")
    
    # Stop event processor
    if search_index_processor:
        await search_index_processor.stop()
    
    # Close Elasticsearch
    if es_client:
        await es_client.close()
    
    # Close vector service
    if vector_service:
        await vector_service.close()
    
    logger.info("Search Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="search-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[search_index_processor] if search_index_processor else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["search"])
app.include_router(vector_endpoints.router, prefix="/api/v1", tags=["vector_search"])

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "search-service",
        "version": "2.0",
        "features": [
            "unified-search",
            "elasticsearch",
            "vector-search",
            "graph-integration",
            "faceted-search",
            "real-time-indexing"
        ]
    }


# Health check with service-specific checks
@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check including dependencies"""
    health = {
        "status": "healthy",
        "checks": {}
    }
    
    # Check Elasticsearch
    try:
        es_health = await app.state.es_client.cluster.health()
        health["checks"]["elasticsearch"] = {
            "status": es_health["status"],
            "number_of_nodes": es_health["number_of_nodes"]
        }
    except Exception as e:
        health["status"] = "unhealthy"
        health["checks"]["elasticsearch"] = {
            "status": "down",
            "error": str(e)
        }
    
    # Check Milvus if enabled
    if hasattr(app.state, "vector_service"):
        try:
            milvus_status = await app.state.vector_service.health_check()
            health["checks"]["milvus"] = milvus_status
        except Exception as e:
            health["status"] = "degraded"
            health["checks"]["milvus"] = {
                "status": "down",
                "error": str(e)
            }
    
    # Check Graph service if enabled
    if hasattr(app.state, "graph_search_engine"):
        try:
            graph_status = await app.state.service_clients.get(
                "graph-intelligence-service",
                "/health"
            )
            health["checks"]["graph_service"] = graph_status
        except Exception as e:
            health["status"] = "degraded"
            health["checks"]["graph_service"] = {
                "status": "down",
                "error": str(e)
            }
    
    return health 