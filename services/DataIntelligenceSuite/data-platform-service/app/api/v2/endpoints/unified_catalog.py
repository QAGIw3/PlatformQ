"""Unified Catalog API v2"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def unified_catalog_info():
    return {"message": "Unified Catalog API v2", "status": "active"}
