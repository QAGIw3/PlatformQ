"""
Data Platform Service API v2 Router - Enhanced with v2.0 Features
"""

from fastapi import APIRouter
from .endpoints import (
    batch_processing,
    stream_processing,
    quality_management,
    lakehouse_operations,
    unified_catalog,
    ml_integration
)

router = APIRouter()

# Include all v2 endpoint routers
router.include_router(batch_processing.router, prefix="/batch", tags=["batch-processing"])
router.include_router(stream_processing.router, prefix="/stream", tags=["stream-processing"])
router.include_router(quality_management.router, prefix="/quality", tags=["quality"])
router.include_router(lakehouse_operations.router, prefix="/lakehouse", tags=["lakehouse"])
router.include_router(unified_catalog.router, prefix="/catalog", tags=["catalog"])
router.include_router(ml_integration.router, prefix="/ml", tags=["ml-integration"])

# Root v2 endpoint
@router.get("/")
async def root():
    return {
        "message": "Data Platform Service API v2",
        "version": "2.0.0",
        "features": [
            "multi-engine-batch-processing",
            "multi-engine-stream-processing",
            "integrated-quality-management",
            "unified-lakehouse-operations",
            "ml-pipeline-integration",
            "advanced-catalog-management"
        ]
    }
