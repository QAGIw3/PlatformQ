"""Stream Processing API v2"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def stream_processing_info():
    return {"message": "Stream Processing API v2", "status": "active"}
