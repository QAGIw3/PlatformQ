"""
Gas Optimization Service

Provides intelligent gas optimization strategies for blockchain transactions.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pyignite import AsyncClient as IgniteClient
from platformq_consul import ConsulClient

from .config import settings
from .core.gas_optimizer import GasOptimizer
from .api import optimization_endpoints

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Manage application lifecycle"""
    logger.info("Starting Gas Optimization Service")
    
    # Initialize Consul client
    consul_client = ConsulClient(
        host=settings.CONSUL_HOST,
        port=settings.CONSUL_PORT,
        service_name=settings.SERVICE_NAME
    )
    
    # Discover blockchain connector service
    blockchain_connector_url = await consul_client.discover_service("blockchain-connector")
    if not blockchain_connector_url:
        blockchain_connector_url = "http://blockchain-connector:8010"
    
    # Initialize Ignite client
    ignite_client = IgniteClient()
    await ignite_client.connect(settings.IGNITE_ADDRESSES)
    
    # Initialize gas optimizer
    gas_optimizer = GasOptimizer(
        settings,
        ignite_client,
        blockchain_connector_url
    )
    await gas_optimizer.start()
    
    # Store in app state
    app.state.gas_optimizer = gas_optimizer
    app.state.consul_client = consul_client
    app.state.ignite_client = ignite_client
    
    # Register with Consul
    await consul_client.register_service(
        name=settings.SERVICE_NAME,
        service_id=f"{settings.SERVICE_NAME}-{settings.SERVICE_PORT}",
        address="localhost",
        port=settings.SERVICE_PORT,
        tags=["blockchain", "gas", "optimization"],
        check={
            "http": f"http://localhost:{settings.SERVICE_PORT}/health",
            "interval": "10s",
            "timeout": "5s"
        }
    )
    
    yield
    
    # Cleanup
    logger.info("Shutting down Gas Optimization Service")
    await gas_optimizer.stop()
    await ignite_client.close()
    

# Create FastAPI app
app = FastAPI(
    title="Gas Optimization Service",
    description="Intelligent gas optimization for blockchain transactions",
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
app.include_router(optimization_endpoints.router)


@app.get("/health")
async def health_check(request: Request):
    """Health check endpoint"""
    optimizer = request.app.state.gas_optimizer
    
    # Check if services are initialized
    if not optimizer:
        return {"status": "unhealthy", "reason": "Services not initialized"}
        
    return {
        "status": "healthy" if optimizer._running else "unhealthy",
        "services": {
            "consul": request.app.state.consul_client is not None,
            "ignite": request.app.state.ignite_client is not None,
            "optimizer": optimizer._running
        },
        "features": {
            "batch_optimization": settings.ENABLE_BATCH_OPTIMIZATION,
            "meta_transactions": settings.ENABLE_META_TRANSACTIONS,
            "l2_suggestions": settings.ENABLE_L2_SUGGESTIONS,
            "time_based": settings.ENABLE_TIME_BASED_OPTIMIZATION
        }
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Gas Optimization Service",
        "version": "1.0.0",
        "description": "Intelligent gas optimization for blockchain transactions",
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