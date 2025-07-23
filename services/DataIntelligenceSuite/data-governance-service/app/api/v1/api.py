"""
API v1 Router
"""

from fastapi import APIRouter

from app.api.v1.endpoints import data_quality

api_router = APIRouter()

# Include quality endpoints
api_router.include_router(
    data_quality.router,
    prefix="/quality",
    tags=["data-quality"]
)
