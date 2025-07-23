"""API router for v1 endpoints"""

from fastapi import APIRouter

from app.api.v1.endpoints import health, example

api_router = APIRouter()

# Include endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(example.router, prefix="/example", tags=["example"])
