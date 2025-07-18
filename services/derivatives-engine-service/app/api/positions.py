from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional, Dict, Any
from decimal import Decimal
from pydantic import BaseModel

from app.models.position import Position, PositionSide, PositionStatus
from app.auth import get_current_user

router = APIRouter()


class PositionResponse(BaseModel):
    id: str
    market_id: str
    side: str
    status: str
    size: str
    entry_price: str
    mark_price: str
    liquidation_price: Optional[str]
    realized_pnl: str
    unrealized_pnl: str
    margin_ratio: str
    leverage: str


class PositionUpdateRequest(BaseModel):
    add_collateral: Optional[Decimal] = None
    remove_collateral: Optional[Decimal] = None
    take_profit_price: Optional[Decimal] = None
    stop_loss_price: Optional[Decimal] = None


@router.get("/", response_model=List[PositionResponse])
async def list_positions(
    market_id: Optional[str] = None,
    status: Optional[PositionStatus] = None,
    current_user: dict = Depends(get_current_user)
):
    """List user's positions"""
    return []


@router.get("/{position_id}", response_model=PositionResponse)
async def get_position(
    position_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get position details"""
    raise HTTPException(status_code=404, detail="Position not found")


@router.patch("/{position_id}")
async def update_position(
    position_id: str,
    request: PositionUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    """Update position (add/remove collateral, set TP/SL)"""
    return {"message": "Position updated"}


@router.post("/{position_id}/close")
async def close_position(
    position_id: str,
    size: Optional[Decimal] = None,
    current_user: dict = Depends(get_current_user)
):
    """Close position (fully or partially)"""
    return {"message": "Position closed"}


@router.get("/margin-info")
async def get_margin_info(
    current_user: dict = Depends(get_current_user)
):
    """Get user's margin information"""
    return {
        "total_collateral": "0",
        "free_collateral": "0",
        "used_margin": "0",
        "margin_ratio": "0",
        "positions_value": "0"
    }


@router.get("/funding-payments")
async def get_funding_payments(
    market_id: Optional[str] = None,
    limit: int = 100,
    current_user: dict = Depends(get_current_user)
):
    """Get funding payment history"""
    return {
        "payments": []
    } 