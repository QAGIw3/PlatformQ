import logging
from contextlib import asynccontextmanager
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import FastAPI, Query, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .dependencies import get_search_consumer, get_es_client
from .api.endpoints import router as api_router
from .core.index_mapping import INDEX_MAPPING
from .core.config import settings
from .services.vector_search import get_vector_search_service
from .search_engine import UnifiedSearchEngine
from .indexer import AssetIndexer, SimulationIndexer, ProjectIndexer
from .query_parser import QueryParser
from .graph_search_integration import GraphEnrichedSearchEngine, GraphSearchOrchestrator, GraphSearchConfig

from elasticsearch import Elasticsearch
from elasticsearch import AsyncElasticsearch
from platformq_shared.config import ConfigLoader
from platformq_shared.jwt import get_current_tenant_and_user

es = Elasticsearch(['elasticsearch:9200'])

# Configure logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
logger = logging.getLogger(__name__)

# Global instances
consumer_task: Optional[asyncio.Task] = None
vector_service = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global consumer_task, vector_service
    
    # Startup
    logger.info("Initializing Search Service...")
    
    # Create Elasticsearch index if it doesn't exist
    es_client = get_es_client()
    if not await es_client.indices.exists(index=settings.ES_INDEX_NAME):
        await es_client.indices.create(index=settings.ES_INDEX_NAME, body={"mappings": INDEX_MAPPING})
        logger.info(f"Created Elasticsearch index: {settings.ES_INDEX_NAME}")

    # Initialize vector search service if enabled
    if settings.ENABLE_VECTOR_SEARCH:
        try:
            vector_service = await get_vector_search_service()
            logger.info("Vector search service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize vector search: {e}")
            logger.warning("Continuing without vector search capabilities")

    # Start event consumer
    consumer = get_search_consumer()
    await consumer.start()
    consumer_task = asyncio.create_task(consumer.consume_messages())
    
    logger.info("Search Service initialized successfully")
    yield
    
    # Shutdown
    logger.info("Shutting down Search Service...")
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    
    consumer = get_search_consumer()
    if consumer:
        await consumer.close()
        
    es_client = get_es_client()
    if es_client:
        await es_client.close()
    
    # Close vector search service
    if vector_service:
        await vector_service.close()
        
    logger.info("Search Service shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="PlatformQ Search Service",
    description="Advanced search service with text, vector, and multi-modal capabilities.",
    version=settings.VERSION,
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix="/api/v1")

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    es_client = get_es_client()
    
    health_status = {
        "status": "healthy",
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "elasticsearch_connected": False,
        "vector_search_enabled": settings.ENABLE_VECTOR_SEARCH,
        "vector_search_connected": False
    }
    
    # Check Elasticsearch
    try:
        health_status["elasticsearch_connected"] = await es_client.ping()
    except:
        health_status["status"] = "degraded"
    
    # Check vector search
    if settings.ENABLE_VECTOR_SEARCH and vector_service:
        try:
            stats = await vector_service.get_collection_stats("text_embeddings")
            health_status["vector_search_connected"] = stats is not None
        except:
            health_status["status"] = "degraded"
    
    return health_status

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "description": "Advanced search service for PlatformQ",
        "features": {
            "text_search": True,
            "vector_search": settings.ENABLE_VECTOR_SEARCH,
            "multi_modal_search": settings.ENABLE_MULTI_MODAL_SEARCH,
            "query_understanding": settings.ENABLE_QUERY_UNDERSTANDING,
            "hybrid_search": True
        },
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "search": "/api/v1/search",
            "unified_search": "/api/v1/search/unified",
            "vector_search": "/api/v1/search/vector",
            "similar_search": "/api/v1/search/similar",
            "multi_modal_search": "/api/v1/search/multi_modal"
        }
    }

# Legacy endpoints for backward compatibility
@app.get('/api/v1/federated-graph-search')
async def federated_search(query: str):
    """Legacy federated search endpoint"""
    res = es.search(index='federated_graph', body={'query': {'match': {'content': query}}})
    return res['hits']['hits']

@app.get('/api/v1/trust-search')
async def trust_search(query: str, trust_min: float = 0.5):
    """Legacy trust search endpoint"""
    body = {
        'query': {
            'bool': {
                'must': {'match': {'content': query}},
                'filter': {'range': {'trust_score': {'gte': trust_min}}}
            }
        }
    }
    res = es.search(index='data', body=body)
    return res['hits']['hits']

@app.on_event("startup")
async def startup_event():
    """Initialize search service connections"""
    config_loader = ConfigLoader()
    
    # Initialize Elasticsearch
    es_host = config_loader.get_setting("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
    es_user = config_loader.get_setting("ELASTICSEARCH_USER", None)
    es_password = config_loader.get_setting("ELASTICSEARCH_PASSWORD", None)
    
    app.state.es_client = AsyncElasticsearch(
        [es_host],
        basic_auth=(es_user, es_password) if es_user else None
    )
    
    # Initialize search engine
    app.state.search_engine = UnifiedSearchEngine(app.state.es_client)
    
    # Initialize indexers
    app.state.asset_indexer = AssetIndexer(app.state.es_client)
    app.state.simulation_indexer = SimulationIndexer(app.state.es_client)
    app.state.project_indexer = ProjectIndexer(app.state.es_client)
    
    # Initialize query parser
    app.state.query_parser = QueryParser()
    
    # Initialize graph-enriched search
    app.state.graph_search_engine = GraphEnrichedSearchEngine(
        app.state.es_client,
        janusgraph_url=config_loader.get_setting("JANUSGRAPH_URL", "ws://janusgraph:8182/gremlin"),
        graph_intelligence_url=config_loader.get_setting("GRAPH_INTELLIGENCE_URL", "http://graph-intelligence-service:8000")
    )
    await app.state.graph_search_engine.connect()
    
    app.state.graph_search_orchestrator = GraphSearchOrchestrator(
        app.state.graph_search_engine,
        cache_ttl=300  # 5 minutes
    )
    
    # Create indices if they don't exist
    await app.state.search_engine.create_indices()
    
    logger.info("Search service initialized with graph enrichment")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections"""
    if hasattr(app.state, "graph_search_engine"):
        await app.state.graph_search_engine.close()
        
    if hasattr(app.state, "es_client"):
        await app.state.es_client.close()

@app.get("/api/v1/search/unified", response_model=Dict[str, Any])
async def unified_graph_search(
    q: str = Query(..., description="Search query"),
    entity_types: Optional[List[str]] = Query(None, description="Filter by entity types"),
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    date_from: Optional[datetime] = Query(None, description="Filter by date from"),
    date_to: Optional[datetime] = Query(None, description="Filter by date to"),
    include_graph: bool = Query(True, description="Include graph enrichment"),
    graph_depth: int = Query(3, ge=1, le=5, description="Graph traversal depth"),
    include_lineage: bool = Query(True, description="Include asset lineage"),
    include_collaborators: bool = Query(True, description="Include collaborators"),
    semantic_expansion: bool = Query(True, description="Enable semantic expansion"),
    context: dict = Depends(get_current_tenant_and_user),
    orchestrator: GraphSearchOrchestrator = Depends(lambda: app.state.graph_search_orchestrator)
):
    """
    Perform unified search with graph enrichment
    """
    tenant_id = context["tenant_id"]
    user = context["user"]
    
    try:
        # Build filters
        filters = {"tenant_id": tenant_id}
        
        if entity_types:
            filters["_index"] = entity_types
        if tags:
            filters["tags"] = tags
        if date_from or date_to:
            date_filter = {}
            if date_from:
                date_filter["gte"] = date_from.isoformat()
            if date_to:
                date_filter["lte"] = date_to.isoformat()
            filters["created_at"] = date_filter
            
        # Configure graph search
        config = GraphSearchConfig(
            max_graph_depth=graph_depth,
            include_lineage=include_lineage and include_graph,
            include_collaborators=include_collaborators and include_graph,
            semantic_expansion=semantic_expansion and include_graph
        )
        
        # User context for personalization
        user_context = {
            "user_id": user["id"],
            "roles": user.get("roles", []),
            "preferences": user.get("preferences", {})
        }
        
        # Perform search
        results = await orchestrator.search(
            query=q,
            filters=filters,
            config=config,
            user_context=user_context
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Error in unified graph search: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/search/graph-context", response_model=Dict[str, Any])
async def get_entity_graph_context(
    entity_id: str,
    entity_type: str,
    max_depth: int = Query(3, ge=1, le=5),
    relationship_types: Optional[List[str]] = Query(None),
    context: dict = Depends(get_current_tenant_and_user),
    graph_engine: GraphEnrichedSearchEngine = Depends(lambda: app.state.graph_search_engine)
):
    """
    Get graph context for a specific entity
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Verify entity access
        entity = await graph_engine.es.get(
            index=entity_type,
            id=entity_id
        )
        
        if entity["_source"].get("tenant_id") != tenant_id:
            raise HTTPException(status_code=403, detail="Access denied")
            
        # Get graph context
        config = GraphSearchConfig(max_graph_depth=max_depth)
        graph_context = await graph_engine._get_graph_context(
            entity_id,
            entity_type,
            config
        )
        
        # Filter by relationship types if specified
        if relationship_types and "relationships" in graph_context:
            graph_context["relationships"] = [
                rel for rel in graph_context["relationships"]
                if rel.get("type") in relationship_types
            ]
            
        return graph_context
        
    except Exception as e:
        logger.error(f"Error getting graph context: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 