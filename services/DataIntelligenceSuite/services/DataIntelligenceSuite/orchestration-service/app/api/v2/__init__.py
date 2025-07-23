"""
API v2 routes with enhanced features
"""

from fastapi import APIRouter
from .endpoints import health, status

router = APIRouter()

# Include endpoint routers
router.include_router(health.router, prefix="/health", tags=["health"])
router.include_router(status.router, prefix="/status", tags=["status"])

# Add service-specific endpoints here
