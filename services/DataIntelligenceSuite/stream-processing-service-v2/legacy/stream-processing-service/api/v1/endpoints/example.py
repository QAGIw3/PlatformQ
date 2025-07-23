"""Example endpoint for Stream Processing Service"""

from typing import Any, Dict
from fastapi import APIRouter, Depends, HTTPException
import structlog

from app.core.config import settings

router = APIRouter()
logger = structlog.get_logger()

@router.get("/")
async def get_example() -> Dict[str, Any]:
    """Example GET endpoint"""
    logger.info("Example endpoint called")
    
    return {
        "message": "Hello from Stream Processing Service",
        "service": settings.SERVICE_NAME,
        "version": "2.0.0"
    }

@router.post("/")
async def create_example(data: Dict[str, Any]) -> Dict[str, Any]:
    """Example POST endpoint"""
    logger.info("Creating example", data=data)
    
    # Add your business logic here
    
    return {
        "status": "created",
        "data": data
    }
