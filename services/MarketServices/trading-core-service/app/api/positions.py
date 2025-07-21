"""Position management API endpoints."""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from decimal import Decimal

from ..models.position import Position, PositionSide
from ..core import PositionManager
from ..dependencies import get_position_manager, get_current_user


router = APIRouter(prefix="/positions", tags=["positions"])


@router.get("/", response_model=List[Position])
async def list_positions(
    market_id: Optional[str] = Query(None, description="Filter by market"),
    open_only: bool = Query(True, description="Show only open positions"),
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """List user positions."""
    positions = await position_manager.get_user_positions(
        user_id=user_id,
        market_id=market_id,
        open_only=open_only
    )
    return positions


@router.get("/{market_id}", response_model=Position)
async def get_position(
    market_id: str,
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get position for a specific market."""
    position = await position_manager.get_position(user_id, market_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    return position


@router.get("/summary")
async def get_positions_summary(
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get summary of all positions."""
    positions = await position_manager.get_user_positions(
        user_id=user_id,
        open_only=True
    )
    
    # Calculate summary metrics
    total_value = Decimal("0")
    total_unrealized_pnl = Decimal("0")
    total_realized_pnl = Decimal("0")
    total_margin_used = Decimal("0")
    
    positions_by_side = {
        PositionSide.LONG: 0,
        PositionSide.SHORT: 0,
        PositionSide.NEUTRAL: 0
    }
    
    for position in positions:
        total_value += position.notional_value
        total_unrealized_pnl += position.unrealized_pnl
        total_realized_pnl += position.realized_pnl
        total_margin_used += position.maintenance_margin
        positions_by_side[position.side] += 1
    
    return {
        "total_positions": len(positions),
        "open_positions": len([p for p in positions if p.is_open]),
        "positions_by_side": {
            "long": positions_by_side[PositionSide.LONG],
            "short": positions_by_side[PositionSide.SHORT],
            "neutral": positions_by_side[PositionSide.NEUTRAL]
        },
        "total_value": str(total_value),
        "total_unrealized_pnl": str(total_unrealized_pnl),
        "total_realized_pnl": str(total_realized_pnl),
        "total_margin_used": str(total_margin_used),
        "metrics": position_manager.get_metrics()
    }


@router.get("/risk/summary")
async def get_risk_summary(
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get risk summary for all positions."""
    positions = await position_manager.get_user_positions(
        user_id=user_id,
        open_only=True
    )
    
    # Risk metrics
    at_risk_positions = []
    total_exposure = Decimal("0")
    max_leverage = Decimal("0")
    min_margin_ratio = Decimal("999")
    
    for position in positions:
        total_exposure += position.notional_value
        
        if position.leverage > max_leverage:
            max_leverage = position.leverage
        
        if position.margin_ratio < min_margin_ratio:
            min_margin_ratio = position.margin_ratio
        
        # Flag positions at risk
        if position.margin_ratio < Decimal("1.5") or position.liquidation_risk > Decimal("0.5"):
            at_risk_positions.append({
                "market_id": position.market_id,
                "side": position.side.value,
                "margin_ratio": str(position.margin_ratio),
                "liquidation_risk": str(position.liquidation_risk),
                "liquidation_price": str(position.liquidation_price) if position.liquidation_price else None
            })
    
    return {
        "total_exposure": str(total_exposure),
        "max_leverage": str(max_leverage),
        "min_margin_ratio": str(min_margin_ratio),
        "at_risk_count": len(at_risk_positions),
        "at_risk_positions": at_risk_positions,
        "liquidation_warning": min_margin_ratio < Decimal("1.2")
    }


@router.get("/history")
async def get_position_history(
    market_id: Optional[str] = Query(None, description="Filter by market"),
    days: int = Query(7, ge=1, le=90, description="Number of days of history"),
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get historical positions."""
    # This would query historical data from database
    # For now, return current positions as example
    positions = await position_manager.get_user_positions(
        user_id=user_id,
        market_id=market_id,
        open_only=False
    )
    
    return {
        "positions": [p.dict() for p in positions],
        "period_days": days
    }


@router.post("/{market_id}/add-collateral")
async def add_collateral(
    market_id: str,
    amount: Decimal = Query(..., gt=0, description="Amount of collateral to add"),
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Add collateral to a position."""
    position = await position_manager.get_position(user_id, market_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    
    if not position.is_open:
        raise HTTPException(status_code=400, detail="Position is closed")
    
    # Update collateral
    position.collateral += amount
    position.calculate_margin_ratio()
    
    # Store updated position
    # await position_manager.update_position(position)
    
    return {
        "message": "Collateral added successfully",
        "new_collateral": str(position.collateral),
        "new_margin_ratio": str(position.margin_ratio)
    }


@router.get("/pnl/breakdown")
async def get_pnl_breakdown(
    user_id: str = Depends(get_current_user),
    position_manager: PositionManager = Depends(get_position_manager)
):
    """Get detailed P&L breakdown."""
    positions = await position_manager.get_user_positions(
        user_id=user_id,
        open_only=False
    )
    
    # Calculate P&L by market
    pnl_by_market: Dict[str, Dict[str, Decimal]] = {}
    
    for position in positions:
        if position.market_id not in pnl_by_market:
            pnl_by_market[position.market_id] = {
                "unrealized": Decimal("0"),
                "realized": Decimal("0"),
                "total": Decimal("0"),
                "fees": Decimal("0")
            }
        
        pnl_by_market[position.market_id]["unrealized"] += position.unrealized_pnl
        pnl_by_market[position.market_id]["realized"] += position.realized_pnl
        pnl_by_market[position.market_id]["total"] += position.total_pnl
        pnl_by_market[position.market_id]["fees"] += position.total_fees_paid
    
    # Convert to response format
    breakdown = []
    for market_id, pnl in pnl_by_market.items():
        breakdown.append({
            "market_id": market_id,
            "unrealized_pnl": str(pnl["unrealized"]),
            "realized_pnl": str(pnl["realized"]),
            "total_pnl": str(pnl["total"]),
            "fees_paid": str(pnl["fees"])
        })
    
    # Sort by total P&L
    breakdown.sort(key=lambda x: Decimal(x["total_pnl"]), reverse=True)
    
    return {
        "breakdown": breakdown,
        "total_unrealized": str(sum(Decimal(b["unrealized_pnl"]) for b in breakdown)),
        "total_realized": str(sum(Decimal(b["realized_pnl"]) for b in breakdown)),
        "total_pnl": str(sum(Decimal(b["total_pnl"]) for b in breakdown)),
        "total_fees": str(sum(Decimal(b["fees_paid"]) for b in breakdown))
    } 