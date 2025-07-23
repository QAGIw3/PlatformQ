"""Health check endpoints"""

from typing import Any, Dict
from fastapi import APIRouter
import structlog

from app.core.config import settings

router = APIRouter()
logger = structlog.get_logger()

@router.get("/")
async def health_check() -> Dict[str, Any]:
    """Basic health check"""
    return {
        "status": "healthy",
        "service": settings.SERVICE_NAME
    }

@router.get("/ready")
async def readiness_check() -> Dict[str, Any]:
    """Readiness check"""
    # Add checks for dependencies (DB, Pulsar, etc.)
    return {
        "status": "ready",
        "service": settings.SERVICE_NAME
    }

@router.get("/live")
async def liveness_check() -> Dict[str, Any]:
    """Liveness check"""
    return {
        "status": "alive",
        "service": settings.SERVICE_NAME
    }
