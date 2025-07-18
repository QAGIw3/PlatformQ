"""
Yield Farming API endpoints for DeFi protocol service.
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/pools")
async def list_yield_pools():
    """List available yield farming pools"""
    return {"pools": [], "total": 0} 