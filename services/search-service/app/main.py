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

from elasticsearch import Elasticsearch

es = Elasticsearch(['elasticsearch:9200'])

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
consumer_task: Optional[asyncio.Task] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global consumer_task
    
    # Startup
    logger.info("Initializing Search Service...")
    
    # Create index if it doesn't exist
    es_client = get_es_client()
    if not await es_client.indices.exists(index=settings.ES_INDEX_NAME):
        await es_client.indices.create(index=settings.ES_INDEX_NAME, body={"mappings": INDEX_MAPPING})
        logger.info(f"Created Elasticsearch index: {settings.ES_INDEX_NAME}")

    consumer = get_search_consumer()
    await consumer.start()
    consumer_task = asyncio.create_task(consumer.consume_messages())
    
    logger.info("Search Service initialized successfully")
    yield
    # Shutdown
    logger.info("Shutting down Search Service...")
    if consumer_task:
        consumer_task.cancel()
    
    consumer = get_search_consumer()
    if consumer:
        await consumer.close()
        
    es_client = get_es_client()
    if es_client:
        await es_client.close()
        
    logger.info("Search Service shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="PlatformQ Search Service",
    description="Unified search service for the PlatformQ ecosystem.",
    version="1.0.0",
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
    return {
        "status": "healthy",
        "service": "search-service",
        "elasticsearch_connected": await es_client.ping(),
    }

@app.get('/api/v1/federated-graph-search')
async def federated_search(query: str):
    res = es.search(index='federated_graph', body={'query': {'match': {'content': query}}})
    return res['hits']['hits']

@app.get('/api/v1/trust-search')
async def trust_search(query: str, trust_min: float = 0.5):
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