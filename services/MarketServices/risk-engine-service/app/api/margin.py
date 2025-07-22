"""Margin management API endpoints."""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..core import MarginCalculator
from ..models import MarginRequirement, MarginCall
from ..dependencies import get_margin_calculator, get_state_manager
from ..auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/margin", tags=["Margin Management"])


class MarginResponse(BaseModel):
    """Margin requirements response."""
    user_id: str
    initial_margin: str
    maintenance_margin: str
    available_margin: str
    margin_usage: str
    margin_ratio: str
    liquidation_threshold: str
    positions_count: int
    timestamp: str


class MarginCallRequest(BaseModel):
    """Request to issue a margin call."""
    amount_required: Decimal = Field(..., gt=0, description="Amount required")
    deadline_hours: int = Field(24, ge=1, le=72, description="Hours to meet call")
    reason: str = Field(..., description="Reason for margin call")


class MarginUpdateRequest(BaseModel):
    """Request to update margin parameters."""
    initial_margin_multiplier: Optional[Decimal] = Field(None, ge=0.5, le=2.0)
    maintenance_margin_multiplier: Optional[Decimal] = Field(None, ge=0.25, le=1.0)
    liquidation_threshold: Optional[Decimal] = Field(None, ge=1.0, le=2.0)


@router.get("/{user_id}", response_model=MarginResponse)
async def get_margin_requirements(
    user_id: str,
    current_user: Dict = Depends(get_current_user),
    margin_calculator: MarginCalculator = Depends(get_margin_calculator),
    state_manager = Depends(get_state_manager)
) -> MarginResponse:
    """Get margin requirements for a user."""
    # Check authorization
    if current_user["user_id"] != user_id and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get user positions
    positions = await state_manager.get_user_positions(user_id)
    if not positions:
        return MarginResponse(
            user_id=user_id,
            initial_margin="0",
            maintenance_margin="0",
            available_margin=str(await state_manager.get_user_balance(user_id)),
            margin_usage="0",
            margin_ratio="999",
            liquidation_threshold="0",
            positions_count=0,
            timestamp=datetime.utcnow().isoformat()
        )
    
    # Get market data
    market_ids = list(set(p.get("market_id") for p in positions))
    market_data = await state_manager.get_market_data_batch(market_ids)
    
    # Calculate margin for each position
    total_initial = Decimal("0")
    total_maintenance = Decimal("0")
    
    for position in positions:
        market_id = position.get("market_id")
        if market_id in market_data:
            margin_req = await margin_calculator.calculate_margin(
                position=position,
                market_data=market_data[market_id]
            )
            total_initial += margin_req["initial_margin"]
            total_maintenance += margin_req["maintenance_margin"]
    
    # Get user balance
    balance = await state_manager.get_user_balance(user_id)
    available_margin = balance - total_initial
    margin_ratio = balance / total_maintenance if total_maintenance > 0 else Decimal("999")
    margin_usage = total_initial / balance if balance > 0 else Decimal("0")
    
    # Calculate liquidation threshold
    liquidation_threshold = total_maintenance * margin_calculator.config.get("liquidation_margin_ratio", Decimal("1.1"))
    
    return MarginResponse(
        user_id=user_id,
        initial_margin=str(total_initial),
        maintenance_margin=str(total_maintenance),
        available_margin=str(available_margin),
        margin_usage=str(margin_usage),
        margin_ratio=str(margin_ratio),
        liquidation_threshold=str(liquidation_threshold),
        positions_count=len(positions),
        timestamp=datetime.utcnow().isoformat()
    )


@router.get("/available/{user_id}")
async def get_available_margin(
    user_id: str,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Get available margin for trading."""
    # Check authorization
    if current_user["user_id"] != user_id and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get margin requirements
    margin_req = await get_margin_requirements(
        user_id=user_id,
        current_user=current_user,
        margin_calculator=Depends(get_margin_calculator),
        state_manager=state_manager
    )
    
    return {
        "user_id": user_id,
        "available_margin": margin_req.available_margin,
        "available_for_withdrawal": str(max(Decimal(margin_req.available_margin) - Decimal(margin_req.initial_margin), 0)),
        "buying_power": str(Decimal(margin_req.available_margin) * 10),  # 10x leverage
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/call/{user_id}")
async def issue_margin_call(
    user_id: str,
    request: MarginCallRequest,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Issue a margin call to a user."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Create margin call
    margin_call = MarginCall(
        call_id=f"mc_{datetime.utcnow().timestamp()}",
        user_id=user_id,
        amount_required=request.amount_required,
        deadline=datetime.utcnow() + timedelta(hours=request.deadline_hours),
        reason=request.reason,
        issued_by=current_user["user_id"],
        status="active"
    )
    
    # Store margin call
    await state_manager.create_margin_call(margin_call)
    
    # Send notification
    await state_manager.notify_margin_call(user_id, margin_call)
    
    return {
        "call_id": margin_call.call_id,
        "user_id": user_id,
        "amount_required": str(margin_call.amount_required),
        "deadline": margin_call.deadline.isoformat(),
        "status": "issued",
        "message": f"Margin call issued successfully"
    }


@router.get("/calls")
async def get_margin_calls(
    user_id: Optional[str] = Query(None, description="Filter by user"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """Get margin calls."""
    # Check authorization
    if user_id and user_id != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # If not risk manager, only show own calls
    if "risk_manager" not in current_user.get("roles", []):
        user_id = current_user["user_id"]
    
    # Get margin calls
    calls = await state_manager.get_margin_calls(
        user_id=user_id,
        status=status,
        limit=limit
    )
    
    return [
        {
            "call_id": call.call_id,
            "user_id": call.user_id,
            "amount_required": str(call.amount_required),
            "amount_deposited": str(call.amount_deposited),
            "deadline": call.deadline.isoformat(),
            "status": call.status,
            "reason": call.reason,
            "issued_at": call.issued_at.isoformat()
        }
        for call in calls
    ]


@router.post("/deposit/{call_id}")
async def deposit_margin(
    call_id: str,
    amount: Decimal = Query(..., gt=0, description="Amount to deposit"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Deposit funds to meet a margin call."""
    # Get margin call
    margin_call = await state_manager.get_margin_call(call_id)
    if not margin_call:
        raise HTTPException(status_code=404, detail="Margin call not found")
    
    # Check authorization
    if margin_call.user_id != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Check if already met
    if margin_call.status == "met":
        raise HTTPException(status_code=400, detail="Margin call already met")
    
    # Update margin call
    margin_call.amount_deposited += amount
    if margin_call.amount_deposited >= margin_call.amount_required:
        margin_call.status = "met"
        margin_call.met_at = datetime.utcnow()
    
    await state_manager.update_margin_call(margin_call)
    
    return {
        "call_id": call_id,
        "amount_deposited": str(amount),
        "total_deposited": str(margin_call.amount_deposited),
        "amount_required": str(margin_call.amount_required),
        "status": margin_call.status,
        "remaining": str(max(margin_call.amount_required - margin_call.amount_deposited, 0))
    }


@router.put("/parameters")
async def update_margin_parameters(
    request: MarginUpdateRequest,
    current_user: Dict = Depends(get_current_user),
    margin_calculator: MarginCalculator = Depends(get_margin_calculator),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Update margin parameters (admin only)."""
    # Admin only
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Update parameters
    updates = {}
    if request.initial_margin_multiplier is not None:
        margin_calculator.initial_margin_multiplier = request.initial_margin_multiplier
        updates["initial_margin_multiplier"] = str(request.initial_margin_multiplier)
    
    if request.maintenance_margin_multiplier is not None:
        margin_calculator.maintenance_margin_multiplier = request.maintenance_margin_multiplier
        updates["maintenance_margin_multiplier"] = str(request.maintenance_margin_multiplier)
    
    if request.liquidation_threshold is not None:
        margin_calculator.config["liquidation_margin_ratio"] = request.liquidation_threshold
        updates["liquidation_threshold"] = str(request.liquidation_threshold)
    
    # Store updated config
    await state_manager.update_margin_config(updates)
    
    # Trigger recalculation for all users
    await state_manager.trigger_margin_recalculation()
    
    return {
        "status": "updated",
        "parameters": updates,
        "timestamp": datetime.utcnow().isoformat()
    } 