"""
Unified Feature Store

Centralized feature store service for managing, serving, and monitoring 
ML features across the platform.
"""

import os
from fastapi import FastAPI
import uvicorn

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService,
    StructuredLogger
)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="unified-feature-store",
    version="1.0.0",
    description="Centralized ML feature management and serving",
    dependencies=["vault", "consul", "ignite", "minio", "spark"],
    health_checks=["online_store", "offline_store", "registry"]
)

logger = StructuredLogger.get_logger(__name__)


class UnifiedFeatureStore(DataIntelligenceBaseService):
    """Unified Feature Store implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_unified_feature_store")
        logger.info("unified_feature_store_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_unified_feature_store")
        logger.info("unified_feature_store_cleaned_up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app, service = create_data_intelligence_app(
        service_class=UnifiedFeatureStore,
        service_metadata=SERVICE_METADATA,
        cors_origins=["*"],
        include_health_endpoint=True,
        include_metrics_endpoint=True,
        include_ready_endpoint=True
    )
    
    return app


app = create_app()


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "description": SERVICE_METADATA.description,
        "features": [
            "feature-registry",
            "online-serving",
            "offline-serving",
            "feature-versioning",
            "drift-monitoring"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8033")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 