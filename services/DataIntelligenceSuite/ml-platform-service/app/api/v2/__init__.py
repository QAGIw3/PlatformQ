"""
API v2 routers (placeholder for future expansion)
"""
from fastapi import APIRouter

api = APIRouter(prefix="/api/v2")

# Placeholder for v2 API
@api.get("/")
async def v2_info():
    return {"message": "API v2 - Coming soon"}

__all__ = ["api"]
