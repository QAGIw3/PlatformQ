"""Lakehouse Operations API v2"""
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def lakehouse_operations_info():
    return {"message": "Lakehouse Operations API v2", "status": "active"}
