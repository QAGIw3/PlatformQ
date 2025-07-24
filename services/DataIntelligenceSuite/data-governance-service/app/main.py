"""
Data Governance Service - Main Application

Comprehensive data quality and governance platform using the migrated architecture.
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

from .core.quality_service import DataQualityConfig, DataQualityService
from .api.v1 import quality_router, profiling_router, remediation_router

# Setup logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-governance-service",
    version="3.0.0",
    description="Comprehensive data quality validation, profiling, anomaly detection, and automated remediation",
    dependencies=["ignite", "cassandra", "pulsar", "vault", "consul"],
    health_checks=["quality_validator", "anomaly_detector", "remediation_engine"],
    capabilities=[
        "quality-validation", "data-profiling", "anomaly-detection",
        "automated-remediation", "quality-scoring", "rule-management",
        "lineage-tracking", "compliance-monitoring"
    ],
    data_sources=["databases", "streams", "files", "apis"],
    data_outputs=["quality-reports", "alerts", "remediation-actions"]
)

# Global service instance
quality_service: DataQualityService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global quality_service
    
    logger.info("Starting Data Governance Service", version=SERVICE_METADATA.version)
    
    # Create service configuration
    config = DataQualityConfig(
        name=SERVICE_METADATA.name,
        version=SERVICE_METADATA.version,
        
        # Quality settings
        enable_auto_profiling=os.getenv("ENABLE_AUTO_PROFILING", "true").lower() == "true",
        enable_anomaly_detection=os.getenv("ENABLE_ANOMALY_DETECTION", "true").lower() == "true",
        enable_auto_remediation=os.getenv("ENABLE_AUTO_REMEDIATION", "false").lower() == "true",
        
        # Thresholds
        quality_threshold=float(os.getenv("QUALITY_THRESHOLD", "0.95")),
        anomaly_sensitivity=float(os.getenv("ANOMALY_SENSITIVITY", "0.95")),
        
        # Processing settings
        batch_size=int(os.getenv("BATCH_SIZE", "1000")),
        profiling_sample_size=int(os.getenv("PROFILING_SAMPLE_SIZE", "10000")),
        
        # Monitoring
        monitoring_interval=int(os.getenv("MONITORING_INTERVAL", "300")),
        alert_cooldown=int(os.getenv("ALERT_COOLDOWN", "3600"))
    )
    
    # Initialize service
    quality_service = DataQualityService(config)
    await quality_service.start()
    
    # Set service instance in routers
    quality_router.set_service(quality_service)
    profiling_router.set_service(quality_service)
    remediation_router.set_service(quality_service)
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Governance Service")
    await quality_service.stop()


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
app.include_router(quality_router, prefix="/api/v1/quality", tags=["Quality"])
app.include_router(profiling_router, prefix="/api/v1/profiling", tags=["Profiling"])
app.include_router(remediation_router, prefix="/api/v1/remediation", tags=["Remediation"])


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
            "quality": "/api/v1/quality",
            "profiling": "/api/v1/profiling",
            "remediation": "/api/v1/remediation",
            "health": "/health",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("SERVICE_PORT", "8020"))
    reload = os.getenv("ENVIRONMENT", "development") == "development"
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=reload
    ) 