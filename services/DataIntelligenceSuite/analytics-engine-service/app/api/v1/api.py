"""API router for v1 endpoints"""

from fastapi import APIRouter

from app.api.v1.endpoints import health, example, quantum_optimization, neuromorphic, stream_processing

api_router = APIRouter()

# Include endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(example.router, prefix="/example", tags=["example"])
api_router.include_router(quantum_optimization.router, prefix="/quantum", tags=["quantum-optimization"])
api_router.include_router(neuromorphic.router, prefix="/neuromorphic", tags=["neuromorphic-computing"])
api_router.include_router(stream_processing.router, prefix="/stream", tags=["stream-processing"])
