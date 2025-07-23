"""
Data Platform Service - Main Application

Consolidated data platform for DataIntelligenceSuite v2.0
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    StructuredLogger
)

from .core.config import settings
from .core.container import initialize_container, shutdown_container
from .api.v1 import (
    cdc_router,
    storage_router,
    ingestion_router,
    catalog_router,
    lakehouse_router
)
from .api import feature_store as feature_store_router
from .api import storage as storage_service_router

# Setup logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-platform-service",
    version="2.0.0",
    description="Unified data platform with ingestion, storage, catalog, and lakehouse capabilities",
    dependencies=["seatunnel", "spark", "flink", "minio", "iceberg", "delta"],
    health_checks=["cdc", "stream", "batch", "catalog", "storage", "lakehouse"],
    capabilities=[
        "cdc", "streaming", "batch-processing", "data-catalog",
        "object-storage", "lakehouse", "schema-evolution", "feature-store",
        "document-conversion", "preview-generation", "content-search", "quota-management"
    ],
    data_sources=["postgres", "mysql", "mongodb", "cassandra", "files", "apis"],
    data_outputs=["lakehouse", "stream", "object-storage"]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("starting_data_platform_service", version=SERVICE_METADATA.version)
    
    # Initialize container and all services
    await initialize_container()
    
    yield
    
    # Shutdown
    logger.info("shutting_down_data_platform_service")
    await shutdown_container()


# Create FastAPI app
app = create_data_intelligence_app(
    service_metadata=SERVICE_METADATA,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(cdc_router, prefix="/api/v1", tags=["CDC"])
app.include_router(ingestion_router, prefix="/api/v1", tags=["Ingestion"])
app.include_router(storage_router, prefix="/api/v1", tags=["Storage"])
app.include_router(catalog_router, prefix="/api/v1", tags=["Catalog"])
app.include_router(lakehouse_router, prefix="/api/v1", tags=["Lakehouse"])
app.include_router(feature_store_router.router, prefix="/api/v1/feature-store", tags=["Feature Store"])
app.include_router(storage_service_router.router, prefix="/api/v1/storage-service", tags=["Storage Service"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "status": "operational",
        "description": SERVICE_METADATA.description,
        "capabilities": SERVICE_METADATA.capabilities,
        "api_docs": "/docs"
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    # TODO: Add actual health checks
    return {
        "status": "healthy",
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "checks": {
            "cdc": "healthy",
            "stream": "healthy",
            "batch": "healthy",
            "catalog": "healthy",
            "storage": "healthy",
            "lakehouse": "healthy"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
