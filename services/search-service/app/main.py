import logging
from contextlib import asynccontextmanager
import asyncio
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .dependencies import get_search_consumer, get_es_client
from .api.endpoints import router as api_router
from .core.index_mapping import INDEX_MAPPING
from .core.config import settings
from .services.vector_search import get_vector_search_service

from elasticsearch import Elasticsearch

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 