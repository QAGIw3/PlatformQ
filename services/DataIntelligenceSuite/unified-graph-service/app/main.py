"""
Unified Graph Service

Consolidates graph intelligence, processing, analytics, and temporal knowledge 
capabilities with JanusGraph, GraphX, and advanced ML algorithms.
"""

import os
from typing import Optional
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
    name="unified-graph-service",
    version="1.0.0",
    description="Comprehensive graph platform with JanusGraph and GraphX",
    dependencies=["vault", "consul", "janusgraph", "spark", "elasticsearch"],
    health_checks=["janusgraph", "graphx", "temporal"]
)

logger = StructuredLogger.get_logger(__name__)


class UnifiedGraphService(DataIntelligenceBaseService):
    """Unified Graph Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_unified_graph_service")
        logger.info("unified_graph_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_unified_graph_service")
        logger.info("unified_graph_service_cleaned_up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app, service = create_data_intelligence_app(
        service_class=UnifiedGraphService,
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
            "knowledge-graphs",
            "graph-analytics",
            "temporal-reasoning",
            "trust-networks",
            "market-intelligence"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8010")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 