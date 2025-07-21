"""
Blockchain Connector Service

Provides unified access to multiple blockchain networks.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pyignite import AsyncClient as IgniteClient
from platformq_consul import ConsulClient

from .config import settings
from .core.chain_manager import ChainManager
from .api import chain_endpoints

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Manage application lifecycle"""
    logger.info("Starting Blockchain Connector Service")
    
    # Initialize Consul client
    consul_client = ConsulClient(
        host=settings.CONSUL_HOST,
        port=settings.CONSUL_PORT,
        service_name=settings.SERVICE_NAME
    )
    
    # Initialize Ignite client
    ignite_client = IgniteClient()
    await ignite_client.connect(settings.IGNITE_ADDRESSES)
    
    # Initialize chain manager
    chain_manager = ChainManager(settings, consul_client, ignite_client)
    await chain_manager.initialize()
    
    # Store in app state
    app.state.chain_manager = chain_manager
    app.state.consul_client = consul_client
    app.state.ignite_client = ignite_client
    
    yield
    
    # Cleanup
    logger.info("Shutting down Blockchain Connector Service")
    await chain_manager.shutdown()
    await ignite_client.close()
    

# Create FastAPI app
app = FastAPI(
    title="Blockchain Connector Service",
    description="Unified API for accessing multiple blockchain networks",
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
app.include_router(chain_endpoints.router)


@app.get("/health")
async def health_check(request: Request):
    """Health check endpoint"""
    chain_manager = request.app.state.chain_manager
    
    # Check if services are initialized
    if not chain_manager:
        return {"status": "unhealthy", "reason": "Services not initialized"}
        
    # Check chain connections
    supported_chains = chain_manager.get_supported_chains()
    healthy_chains = []
    unhealthy_chains = []
    
    for chain in supported_chains:
        health_scores = chain_manager._endpoint_health.get(chain, {})
        avg_health = sum(health_scores.values()) / len(health_scores) if health_scores else 0
        
        if avg_health > 0.5:
            healthy_chains.append(chain.value)
        else:
            unhealthy_chains.append(chain.value)
            
    return {
        "status": "healthy" if len(healthy_chains) > 0 else "unhealthy",
        "services": {
            "consul": request.app.state.consul_client is not None,
            "ignite": request.app.state.ignite_client is not None,
            "chain_manager": chain_manager._running
        },
        "chains": {
            "healthy": healthy_chains,
            "unhealthy": unhealthy_chains
        }
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Blockchain Connector Service",
        "version": "1.0.0",
        "description": "Unified API for accessing multiple blockchain networks",
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