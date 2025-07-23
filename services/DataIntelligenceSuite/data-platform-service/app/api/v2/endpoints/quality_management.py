"""Quality Management API v2"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def quality_management_info():
    return {"message": "Quality Management API v2", "status": "active"}
