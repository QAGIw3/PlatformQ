"""
Transaction Processor Service

Manages blockchain transaction lifecycle including signing, broadcasting, and monitoring.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pyignite import AsyncClient as IgniteClient
import aiopulsar
from platformq_consul import ConsulClient

from .config import settings
from .core.transaction_processor import TransactionProcessor
from .api import transaction_endpoints

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Manage application lifecycle"""
    logger.info("Starting Transaction Processor Service")
    
    # Initialize Consul client
    consul_client = ConsulClient(
        host=settings.CONSUL_HOST,
        port=settings.CONSUL_PORT,
        service_name=settings.SERVICE_NAME
    )
    
    # Discover service URLs
    blockchain_connector_url = await consul_client.discover_service("blockchain-connector")
    key_management_url = await consul_client.discover_service("key-management")
    
    if not blockchain_connector_url:
        blockchain_connector_url = "http://blockchain-connector:8010"
    if not key_management_url:
        key_management_url = "http://key-management:8012"
    
    # Initialize Ignite client
    ignite_client = IgniteClient()
    await ignite_client.connect(settings.IGNITE_ADDRESSES)
    
    # Initialize Pulsar client
    pulsar_client = aiopulsar.Client(settings.PULSAR_URL)
    
    # Initialize transaction processor
    transaction_processor = TransactionProcessor(
        settings,
        ignite_client,
        pulsar_client,
        blockchain_connector_url,
        key_management_url
    )
    await transaction_processor.start()
    
    # Store in app state
    app.state.transaction_processor = transaction_processor
    app.state.consul_client = consul_client
    app.state.ignite_client = ignite_client
    app.state.pulsar_client = pulsar_client
    
    # Register with Consul
    await consul_client.register_service(
        name=settings.SERVICE_NAME,
        service_id=f"{settings.SERVICE_NAME}-{settings.SERVICE_PORT}",
        address="localhost",
        port=settings.SERVICE_PORT,
        tags=["blockchain", "transaction", "processor"],
        check={
            "http": f"http://localhost:{settings.SERVICE_PORT}/health",
            "interval": "10s",
            "timeout": "5s"
        }
    )
    
    yield
    
    # Cleanup
    logger.info("Shutting down Transaction Processor Service")
    await transaction_processor.stop()
    await ignite_client.close()
    await pulsar_client.close()
    

# Create FastAPI app
app = FastAPI(
    title="Transaction Processor Service",
    description="Manages blockchain transaction lifecycle",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app, endpoint="/metrics")

# Include routers
app.include_router(transaction_endpoints.router)


@app.get("/health")
async def health_check(request: Request):
    """Health check endpoint"""
    processor = request.app.state.transaction_processor
    
    # Check if services are initialized
    if not processor:
        return {"status": "unhealthy", "reason": "Services not initialized"}
        
    return {
        "status": "healthy",
        "services": {
            "consul": request.app.state.consul_client is not None,
            "ignite": request.app.state.ignite_client is not None,
            "pulsar": request.app.state.pulsar_client is not None,
            "processor": processor._running
        },
        "processing": {
            "active_transactions": len(processor._processing_tasks),
            "max_concurrent": processor.settings.MAX_CONCURRENT_TRANSACTIONS
        }
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Transaction Processor Service",
        "version": "1.0.0",
        "description": "Manages blockchain transaction lifecycle",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.SERVICE_PORT,
        reload=True
    ) 