"""
Data Lineage Service

Specialized service for tracking data lineage, transformations, 
and impact analysis across the data platform.
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
    name="data-lineage-service",
    version="1.0.0",
    description="Data lineage tracking and impact analysis",
    dependencies=["vault", "consul", "janusgraph", "elasticsearch"],
    health_checks=["graph", "search", "tracker"]
)

logger = StructuredLogger.get_logger(__name__)


class DataLineageService(DataIntelligenceBaseService):
    """Data Lineage Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_data_lineage_service")
        logger.info("data_lineage_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_data_lineage_service")
        logger.info("data_lineage_service_cleaned_up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app, service = create_data_intelligence_app(
        service_class=DataLineageService,
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
            "lineage-tracking",
            "impact-analysis",
            "transformation-documentation",
            "compliance-support",
            "visual-lineage"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8032")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 