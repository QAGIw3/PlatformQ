"""
Data Query Service

Focused service for federated query execution across multiple data sources 
with caching, optimization, and access control.
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
    name="data-query-service",
    version="1.0.0",
    description="Federated query execution with caching and optimization",
    dependencies=["vault", "consul", "trino", "ignite"],
    health_checks=["query_engine", "cache", "federation"]
)

logger = StructuredLogger.get_logger(__name__)


class DataQueryService(DataIntelligenceBaseService):
    """Data Query Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_data_query_service")
        logger.info("data_query_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_data_query_service")
        logger.info("data_query_service_cleaned_up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app, service = create_data_intelligence_app(
        service_class=DataQueryService,
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
            "federated-query",
            "query-optimization",
            "result-caching",
            "access-control",
            "data-masking"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8030")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 