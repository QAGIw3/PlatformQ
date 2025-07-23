"""
Data Governance Service
Unified data governance service for quality, lineage, catalog, and compliance
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
import structlog

from data_intelligence_common.base_service import BaseService
from data_intelligence_common.monitoring import setup_monitoring
from data_intelligence_common.vault_consul import VaultConsulIntegration

from app.core.config import settings
from app.api.v1.api import api_router

# Configure structured logging
logger = structlog.get_logger()

# Initialize monitoring
setup_monitoring(service_name="data-governance-service")

# Initialize Vault/Consul integration
vault_consul = VaultConsulIntegration(
    service_name="data-governance-service",
    vault_addr=settings.VAULT_ADDR,
    consul_addr=settings.CONSUL_ADDR
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting Data Governance Service")
    
    # Initialize Vault/Consul
    await vault_consul.initialize()
    
    # Register with Consul
    await vault_consul.register_service(
        port=settings.PORT,
        health_check_path="/health"
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Governance Service")
    await vault_consul.deregister_service()

# Create FastAPI app
app = FastAPI(
    title="Data Governance Service",
    description="Unified data governance service for quality, lineage, catalog, and compliance",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix="/api/v1")

# Mount Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Health check endpoint
@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "data-governance-service",
        "version": "2.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.DEBUG,
        log_config=None  # Use structlog
    )
