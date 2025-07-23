"""
Ml Platform Service Service

Enterprise-scale service for DataIntelligenceSuite v2.0
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from platformq_shared.logging import setup_logging
from data_intelligence_common.base_service import create_app
from data_intelligence_common.monitoring import setup_monitoring
from data_intelligence_common.vault_consul import UnifiedIntegration

from .core.config import settings
from .core.container import Container
from .api.v1 import api as v1_api
from .api.v2 import api as v2_api

# Setup structured logging
logger = setup_logging(__name__)

# Dependency injection container
container = Container()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info(f"Starting {settings.SERVICE_NAME} v2.0")
    
    # Initialize Vault/Consul integration
    integration = UnifiedIntegration(settings)
    await integration.initialize()
    
    # Initialize container
    await container.init_resources()
    
    # Wire dependencies
    container.wire(modules=[v1_api, v2_api])
    
    yield
    
    # Cleanup
    logger.info(f"Shutting down {settings.SERVICE_NAME}")
    await container.shutdown_resources()
    await integration.close()


# Create FastAPI application
app = create_app(
    title=settings.SERVICE_NAME,
    description="Ml Platform Service - Part of DataIntelligenceSuite v2.0",
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

# Setup monitoring
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

# Include API routers
app.include_router(v1_api.router, prefix="/api/v1", tags=["v1"])
app.include_router(v2_api.router, prefix="/api/v2", tags=["v2"])


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": settings.SERVICE_NAME,
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    # Check dependencies
    checks = await container.health_checker().check_all()
    
    if all(check["status"] == "healthy" for check in checks):
        return {"status": "ready", "checks": checks}
    else:
        return {"status": "not_ready", "checks": checks}, 503


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.DEBUG,
        log_config=None  # Use our custom logging
    )
