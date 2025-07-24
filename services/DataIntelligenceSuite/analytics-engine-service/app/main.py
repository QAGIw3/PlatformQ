"""
Analytics Engine Service - Main Application

Unified analytics platform using the migrated architecture.
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    StructuredLogger
)

from .core.base import AnalyticsConfig, AnalyticsBaseService
from .api.v1 import analytics_router, query_router, streaming_router

# Setup logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="analytics-engine-service",
    version="3.0.0",
    description="Unified analytics engine with multi-engine support and intelligent query routing",
    dependencies=["trino", "druid", "clickhouse", "pinot", "ignite", "pulsar"],
    health_checks=["query_engines", "stream_processor", "cache"],
    capabilities=[
        "batch-analytics", "real-time-analytics", "stream-processing",
        "ml-predictions", "anomaly-detection", "query-optimization",
        "multi-engine-routing", "result-caching"
    ],
    data_sources=["lakehouse", "streams", "databases", "apis"],
    data_outputs=["dashboards", "reports", "alerts", "predictions"]
)

# Global service instance
analytics_service: AnalyticsBaseService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global analytics_service
    
    logger.info("Starting Analytics Engine Service", version=SERVICE_METADATA.version)
    
    # Create service configuration
    config = AnalyticsConfig(
        name=SERVICE_METADATA.name,
        version=SERVICE_METADATA.version,
        
        # Engine configuration
        enable_trino=os.getenv("ENABLE_TRINO", "true").lower() == "true",
        enable_druid=os.getenv("ENABLE_DRUID", "true").lower() == "true",
        enable_clickhouse=os.getenv("ENABLE_CLICKHOUSE", "true").lower() == "true",
        enable_pinot=os.getenv("ENABLE_PINOT", "false").lower() == "true",
        
        # Processing configuration
        enable_stream_processing=os.getenv("ENABLE_STREAM_PROCESSING", "true").lower() == "true",
        enable_ml_predictions=os.getenv("ENABLE_ML_PREDICTIONS", "true").lower() == "true",
        
        # Connection settings from environment
        trino_host=os.getenv("TRINO_HOST", "trino"),
        trino_port=int(os.getenv("TRINO_PORT", "8080")),
        druid_host=os.getenv("DRUID_HOST", "druid-broker"),
        druid_port=int(os.getenv("DRUID_PORT", "8082")),
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        
        # Cache settings
        cache_ttl=int(os.getenv("CACHE_TTL", "3600")),
        max_cache_size=int(os.getenv("MAX_CACHE_SIZE", "1000")),
        
        # Performance settings
        max_workers=int(os.getenv("MAX_WORKERS", "10")),
        parallelism=int(os.getenv("PARALLELISM", "4"))
    )
    
    # Initialize service
    analytics_service = AnalyticsBaseService(config)
    await analytics_service.start()
    
    # Set service instance in routers
    analytics_router.set_service(analytics_service)
    query_router.set_service(analytics_service)
    streaming_router.set_service(analytics_service)
    
    yield
    
    # Shutdown
    logger.info("Shutting down Analytics Engine Service")
    await analytics_service.stop()


# Create FastAPI app
app = create_data_intelligence_app(
    service_metadata=SERVICE_METADATA,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(analytics_router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(query_router, prefix="/api/v1/query", tags=["Query"])
app.include_router(streaming_router, prefix="/api/v1/streaming", tags=["Streaming"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "status": "operational",
        "description": SERVICE_METADATA.description,
        "capabilities": SERVICE_METADATA.capabilities,
        "endpoints": {
            "analytics": "/api/v1/analytics",
            "query": "/api/v1/query",
            "streaming": "/api/v1/streaming",
            "health": "/health",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("SERVICE_PORT", "8000"))
    reload = os.getenv("ENVIRONMENT", "development") == "development"
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=reload
    ) 