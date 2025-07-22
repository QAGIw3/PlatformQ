"""
Data Lake Service

Dedicated service for managing the data lake with medallion architecture, 
ingestion, transformation, and lifecycle management.
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
    name="data-lake-service",
    version="1.0.0",
    description="Data lake management with medallion architecture",
    dependencies=["vault", "consul", "minio", "spark", "delta"],
    health_checks=["storage", "ingestion", "transformation"]
)

logger = StructuredLogger.get_logger(__name__)


class DataLakeService(DataIntelligenceBaseService):
    """Data Lake Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_data_lake_service")
        logger.info("data_lake_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_data_lake_service")
        logger.info("data_lake_service_cleaned_up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app, service = create_data_intelligence_app(
        service_class=DataLakeService,
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
            "medallion-architecture",
            "data-ingestion",
            "transformation-engine",
            "lifecycle-management",
            "delta-lake"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8031")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 