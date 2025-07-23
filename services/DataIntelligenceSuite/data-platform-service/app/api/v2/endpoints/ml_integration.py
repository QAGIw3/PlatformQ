"""ML Integration API v2"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def ml_integration_info():
    return {"message": "ML Integration API v2", "status": "active"}
