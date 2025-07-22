"""
Search Service

Unified search with Elasticsearch, Milvus, and graph integration.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Depends, HTTPException, Header
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
from .vault_consul_integration import VaultConsulIntegration
from .api import endpoints, unified_search
from .api.deps import get_db_session, get_api_key_crud, get_user_crud, get_password_verifier
from .repository import SearchIndexRepository, SearchHistoryRepository
from .event_processors import SearchIndexEventProcessor
from .search_engine import UnifiedSearchEngine
from .indexer import AssetIndexer, SimulationIndexer, ProjectIndexer, DocumentIndexer
from .query_parser import QueryParser
from .graph_search_integration import GraphEnrichedSearchEngine, GraphSearchOrchestrator, GraphSearchConfig
from .services.vector_search import VectorSearchService
from .services.es_vector_search import ESVectorSearchService
from .services.unified_search_integration import UnifiedSearchIntegration
from .services.ai_search_enhancement import AISearchOrchestrator
from .services.search_analytics import SearchAnalyticsTracker, SearchAnalyticsAnalyzer, SearchInsightsGenerator
from .core.index_mapping import INDEX_MAPPING
from .core.config import settings

logger = logging.getLogger(__name__)

# Service components
vault_consul = None
search_index_processor = None
es_client = None
vector_service = None
unified_search_engine = None
graph_search_engine = None
service_clients = None
unified_search_integration = None
ai_search_orchestrator = None
search_analytics_tracker = None
search_analytics_analyzer = None
search_insights_generator = None


async def verify_api_key(x_api_key: str = Header(None)) -> Dict[str, Any]:
    """Verify API key using Vault integration"""
    if not vault_consul:
        raise HTTPException(status_code=500, detail="Security not initialized")
    
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    is_valid, key_info = await vault_consul.validate_api_key(x_api_key)
    if not is_valid:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return key_info


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global vault_consul, search_index_processor, es_client, vector_service
    global unified_search_engine, graph_search_engine, service_clients
    global unified_search_integration, ai_search_orchestrator
    global search_analytics_tracker, search_analytics_analyzer, search_insights_generator
    
    # Startup
    logger.info("Starting Search Service...")
    
    # Initialize Vault and Consul integration first
    vault_consul = VaultConsulIntegration()
    await vault_consul.initialize()
    
    # Get configurations from Vault and Consul
    es_config = await vault_consul.get_elasticsearch_config()
    milvus_config = await vault_consul.get_milvus_config()
    search_config = await vault_consul.get_search_config()
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings_dict = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize Elasticsearch v8 with Vault credentials
    es_client = AsyncElasticsearch(**es_config)
    app.state.es_client = es_client
    
    # Verify Elasticsearch v8
    info = await es_client.info()
    es_version = info["version"]["number"]
    logger.info(f"Connected to Elasticsearch v{es_version}")
    if not es_version.startswith("8"):
        logger.warning(f"Expected Elasticsearch v8, got v{es_version}")
    
    # Create indices with settings from Consul
    indices = ["assets", "simulations", "projects", "documents", "users"]
    for index_name in indices:
        full_index_name = f"{settings.ES_INDEX_PREFIX}_{index_name}"
        if not await es_client.indices.exists(index=full_index_name):
            index_settings = await vault_consul.get_search_index_settings(index_name)
            await es_client.indices.create(
                index=full_index_name,
                body={
                    "settings": index_settings,
                    "mappings": INDEX_MAPPING.get(index_name, {})
                }
            )
            logger.info(f"Created Elasticsearch index: {full_index_name}")
    
    # Initialize repositories
    app.state.search_index_repo = SearchIndexRepository(get_db_session)
    app.state.search_history_repo = SearchHistoryRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    
    # Initialize vector search with Milvus credentials
    if settings_dict.get("enable_vector_search", False):
        vector_service = VectorSearchService(
            **milvus_config,
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
    
    # Get OpenAI API key from Vault
    openai_api_key = await vault_consul.get_openai_api_key()
    
    enhanced_vector_service = EnhancedVectorSearchService(
        es_client=es_client,
        janusgraph_url=settings.JANUSGRAPH_URL,
        redis_client=None,  # Would initialize Redis here
        openai_api_key=openai_api_key,
        vault_integration=vault_consul
    )
    await enhanced_vector_service.initialize()
    app.state.enhanced_vector_service = enhanced_vector_service
    vector_endpoints.vector_service = enhanced_vector_service
    logger.info("Initialized enhanced vector search with JanusGraph integration")
    
    # Initialize indexers with encryption support
    app.state.asset_indexer = AssetIndexer(es_client, vector_service, vault_consul)
    app.state.simulation_indexer = SimulationIndexer(es_client, vector_service, vault_consul)
    app.state.project_indexer = ProjectIndexer(es_client, vault_consul)
    app.state.document_indexer = DocumentIndexer(es_client, vector_service, vault_consul)
    
    # Initialize query parser with search config
    app.state.query_parser = QueryParser(search_config['relevance'])
    
    # Initialize search engines with Vault integration
    unified_search_engine = UnifiedSearchEngine(
        es_client=es_client,
        vector_service=vector_service,
        query_parser=app.state.query_parser,
        indexers={
            "assets": app.state.asset_indexer,
            "simulations": app.state.simulation_indexer,
            "projects": app.state.project_indexer,
            "documents": app.state.document_indexer
        },
        vault_integration=vault_consul,
        search_config=search_config
    )
    app.state.unified_search_engine = unified_search_engine
    app.state.vault_consul = vault_consul
    
    # Initialize graph-enhanced search if enabled
    if settings_dict.get("enable_graph_search", False):
        graph_config = GraphSearchConfig(
            graph_service_url=settings_dict.get("graph_intelligence_service_url", "http://graph-intelligence-service:8000"),
            max_hops=int(settings_dict.get("graph_max_hops", 2)),
            relationship_weights=search_config.get('graph_weights', {
                "created_by": 0.8,
                "modified_by": 0.6,
                "references": 0.7,
                "derived_from": 0.9,
                "used_in": 0.7
            })
        )
        
        graph_search_engine = GraphEnrichedSearchEngine(
            search_engine=unified_search_engine,
            graph_config=graph_config,
            service_clients=service_clients,
            consul_client=vault_consul.consul_client
        )
        app.state.graph_search_engine = graph_search_engine
    
    # Initialize unified search integration
    unified_search_integration = UnifiedSearchIntegration(
        es_client=es_client,
        vault_consul=vault_consul
    )
    await unified_search_integration.initialize()
    app.state.unified_search_integration = unified_search_integration
    
    # Initialize AI search orchestrator
    redis_client = None
    if settings_dict.get("enable_redis", True):
        import redis.asyncio as redis
        redis_client = redis.from_url(
            f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
        )
    
    ai_search_orchestrator = AISearchOrchestrator(
        es_client=es_client,
        redis_client=redis_client,
        openai_api_key=openai_api_key
    )
    app.state.ai_search_orchestrator = ai_search_orchestrator
    
    # Initialize search analytics
    search_analytics_tracker = SearchAnalyticsTracker(es_client, redis_client)
    await search_analytics_tracker.initialize()
    app.state.search_analytics_tracker = search_analytics_tracker
    
    search_analytics_analyzer = SearchAnalyticsAnalyzer(es_client, redis_client)
    app.state.search_analytics_analyzer = search_analytics_analyzer
    
    search_insights_generator = SearchInsightsGenerator(search_analytics_analyzer)
    app.state.search_insights_generator = search_insights_generator
    
    logger.info("Initialized AI-powered search enhancements and analytics")
    
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
        },
        vault_integration=vault_consul
    )
    
    # Start event processor
    await search_index_processor.start()
    
    # Register service-specific health checks with Consul
    await vault_consul.consul_client.agent.check.register(
        name=f"{vault_consul.service_name}-elasticsearch",
        check=consul.Check.http(
            f"http://localhost:8000/health/elasticsearch",
            interval="30s",
            timeout="10s"
        )
    )
    
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
        
    # Close Vault/Consul integration
    if vault_consul:
        await vault_consul.close()
    
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
app.include_router(unified_search.router, tags=["unified_search"])

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "search-service",
        "version": "3.0",
        "features": [
            "unified-search",
            "cross-service-search",
            "ai-powered-search",
            "semantic-search",
            "personalized-results",
            "search-analytics",
            "elasticsearch-v8",
            "vector-search",
            "graph-integration",
            "faceted-search",
            "real-time-indexing",
            "auto-categorization",
            "query-intent-understanding",
            "vault-secured",
            "consul-configured"
        ],
        "endpoints": {
            "unified_search": "/api/v1/unified/search",
            "search_suggestions": "/api/v1/unified/suggestions",
            "search_analytics": "/api/v1/unified/analytics",
            "vector_search": "/api/v1/vector",
            "traditional_search": "/api/v1/search"
        }
    }


# Health check with service-specific checks
@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check including dependencies"""
    health = {
        "status": "healthy",
        "checks": {}
    }
    
    # Check Vault/Consul integration
    if vault_consul:
        try:
            if vault_consul.vault_client.is_authenticated():
                health["checks"]["vault"] = {"status": "healthy"}
            else:
                health["checks"]["vault"] = {"status": "unhealthy", "error": "Not authenticated"}
                health["status"] = "degraded"
                
            consul_health = await vault_consul.consul_client.health.node("consul")
            if consul_health:
                health["checks"]["consul"] = {"status": "healthy"}
            else:
                health["checks"]["consul"] = {"status": "unhealthy"}
                health["status"] = "degraded"
        except Exception as e:
            health["checks"]["security"] = {"status": "down", "error": str(e)}
            health["status"] = "unhealthy"
    
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


# Elasticsearch-specific health endpoint for Consul
@app.get("/health/elasticsearch")
async def elasticsearch_health():
    """Elasticsearch-specific health check"""
    try:
        es_health = await app.state.es_client.cluster.health()
        return {
            "status": "healthy" if es_health["status"] != "red" else "unhealthy",
            "cluster_status": es_health["status"]
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


# Search with API key validation
@app.get("/api/v1/secure/search")
async def secure_search(
    query: str = Query(..., description="Search query"),
    indices: Optional[List[str]] = Query(None, description="Indices to search"),
    key_info: Dict[str, Any] = Depends(verify_api_key)
):
    """Secure search endpoint with API key validation"""
    try:
        # Log encrypted query for audit
        encrypted_query = await vault_consul.encrypt_search_query(query)
        logger.info(f"Search request from {key_info['type']}:{key_info.get('client_id', key_info.get('role'))}")
        
        # Perform search
        results = await app.state.unified_search_engine.search(
            query=query,
            indices=indices or ["assets", "simulations", "documents"],
            user_context=key_info
        )
        
        # Encrypt sensitive results if needed
        if key_info['type'] == 'external':
            # Mask PII for external clients
            results = await _mask_pii_in_results(results, vault_consul)
        
        return results
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")


# Update search relevance configuration
@app.put("/api/v1/admin/relevance-config")
async def update_relevance_config(
    config: Dict[str, Any],
    key_info: Dict[str, Any] = Depends(verify_api_key)
):
    """Update search relevance configuration (admin only)"""
    if key_info.get('role') != 'admin':
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        await vault_consul.update_search_relevance_config(config)
        return {"status": "updated", "config": config}
    except Exception as e:
        logger.error(f"Failed to update relevance config: {e}")
        raise HTTPException(status_code=500, detail="Update failed")


async def _mask_pii_in_results(results: Dict[str, Any], vault_consul: VaultConsulIntegration) -> Dict[str, Any]:
    """Mask PII in search results for external clients"""
    # This would implement PII masking logic
    # For now, just encrypt sensitive fields
    if 'hits' in results:
        for hit in results['hits']:
            if 'creator_email' in hit:
                hit['creator_email'] = await vault_consul.encrypt_pii(hit['creator_email'])
    
    return results 