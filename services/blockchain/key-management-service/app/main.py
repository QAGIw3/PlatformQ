"""
Key Management Service

Secure key storage and cryptographic operations using HashiCorp Vault.
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
from .vault.vault_manager import VaultManager
from .core.blockchain_signer import BlockchainSigner
from .api import key_endpoints

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Manage application lifecycle"""
    logger.info("Starting Key Management Service")
    
    # Initialize Consul client
    consul_client = ConsulClient(
        host=settings.CONSUL_HOST,
        port=settings.CONSUL_PORT,
        service_name=settings.SERVICE_NAME
    )
    
    # Initialize Ignite client
    ignite_client = IgniteClient()
    await ignite_client.connect(settings.IGNITE_ADDRESSES)
    
    # Initialize Vault manager
    vault_manager = VaultManager(settings)
    await vault_manager.initialize()
    
    # Initialize blockchain signer
    blockchain_signer = BlockchainSigner(vault_manager, settings)
    
    # Store in app state
    app.state.vault_manager = vault_manager
    app.state.blockchain_signer = blockchain_signer
    app.state.consul_client = consul_client
    app.state.ignite_client = ignite_client
    
    # Register with Consul
    await consul_client.register_service(
        name=settings.SERVICE_NAME,
        service_id=f"{settings.SERVICE_NAME}-{settings.SERVICE_PORT}",
        address="localhost",
        port=settings.SERVICE_PORT,
        tags=["security", "keys", "vault"],
        check={
            "http": f"http://localhost:{settings.SERVICE_PORT}/health",
            "interval": "10s",
            "timeout": "5s"
        }
    )
    
    yield
    
    # Cleanup
    logger.info("Shutting down Key Management Service")
    await ignite_client.close()
    

# Create FastAPI app
app = FastAPI(
    title="Key Management Service",
    description="Secure key storage and cryptographic operations",
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
app.include_router(key_endpoints.router)


@app.get("/health")
async def health_check(request: Request):
    """Health check endpoint"""
    vault_manager = request.app.state.vault_manager
    
    # Check if services are initialized
    if not vault_manager or not vault_manager._initialized:
        return {"status": "unhealthy", "reason": "Services not initialized"}
        
    # Check Vault connection
    vault_health = False
    try:
        if vault_manager.client.is_authenticated():
            vault_health = True
    except:
        pass
        
    return {
        "status": "healthy" if vault_health else "unhealthy",
        "services": {
            "consul": request.app.state.consul_client is not None,
            "ignite": request.app.state.ignite_client is not None,
            "vault": vault_health
        }
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Key Management Service",
        "version": "1.0.0",
        "description": "Secure key storage and cryptographic operations",
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