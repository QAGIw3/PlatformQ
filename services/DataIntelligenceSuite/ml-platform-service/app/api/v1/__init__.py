"""
API v1 routers
"""
from fastapi import APIRouter
from .training import router as training_router
from .models import router as models_router
from .serving import router as serving_router
from .monitoring import router as monitoring_router
from .experiments import router as experiments_router

api = APIRouter(prefix="/api/v1")

# Include all routers
api.include_router(training_router)
api.include_router(models_router)
api.include_router(serving_router)
api.include_router(monitoring_router)
api.include_router(experiments_router)

__all__ = ["api"]
