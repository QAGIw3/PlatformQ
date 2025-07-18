"""
Liquidity API endpoints for DeFi protocol service.
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/pools")
async def list_liquidity_pools():
    """List available liquidity pools"""
    return {"pools": [], "total": 0} 