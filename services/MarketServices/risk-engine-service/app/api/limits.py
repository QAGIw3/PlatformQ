"""Risk limits API endpoints."""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..dependencies import get_state_manager
from ..auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/limits", tags=["Risk Limits"])


class RiskLimitRequest(BaseModel):
    """Request to set risk limits."""
    max_position_value: Optional[Decimal] = Field(None, gt=0, description="Max position value")
    max_leverage: Optional[Decimal] = Field(None, gt=0, le=100, description="Max leverage")
    max_open_positions: Optional[int] = Field(None, gt=0, le=1000, description="Max open positions")
    max_concentration: Optional[Decimal] = Field(None, gt=0, le=1, description="Max market concentration")
    max_var: Optional[Decimal] = Field(None, gt=0, description="Max Value at Risk")
    max_loss_daily: Optional[Decimal] = Field(None, gt=0, description="Max daily loss")
    max_loss_weekly: Optional[Decimal] = Field(None, gt=0, description="Max weekly loss")
    max_loss_monthly: Optional[Decimal] = Field(None, gt=0, description="Max monthly loss")


class RiskLimitResponse(BaseModel):
    """Risk limits response."""
    user_id: str
    limits: Dict[str, str]
    current_usage: Dict[str, str]
    utilization: Dict[str, float]
    breaches: List[Dict[str, Any]]
    status: str
    last_updated: str


class BreachResponse(BaseModel):
    """Limit breach information."""
    breach_id: str
    user_id: str
    limit_type: str
    limit_value: str
    actual_value: str
    breach_percentage: float
    timestamp: str
    status: str
    resolution: Optional[str]


@router.get("/{user_id}", response_model=RiskLimitResponse)
async def get_risk_limits(
    user_id: str,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> RiskLimitResponse:
    """Get risk limits for a user."""
    # Check authorization
    if current_user["user_id"] != user_id and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get limits
    limits = await state_manager.get_risk_limits(user_id)
    if not limits:
        # Return default limits
        limits = {
            "max_position_value": Decimal("1000000"),
            "max_leverage": Decimal("10"),
            "max_open_positions": 50,
            "max_concentration": Decimal("0.3"),
            "max_var": Decimal("50000"),
            "max_loss_daily": Decimal("10000"),
            "max_loss_weekly": Decimal("50000"),
            "max_loss_monthly": Decimal("100000")
        }
    
    # Get current usage
    usage = await state_manager.get_risk_usage(user_id)
    
    # Calculate utilization
    utilization = {}
    for key, limit_value in limits.items():
        if key in usage and limit_value > 0:
            utilization[key] = float(usage[key] / limit_value)
        else:
            utilization[key] = 0.0
    
    # Get active breaches
    breaches = await state_manager.get_limit_breaches(user_id, status="active")
    
    # Determine overall status
    if breaches:
        status = "breached"
    elif any(u > 0.9 for u in utilization.values()):
        status = "warning"
    else:
        status = "normal"
    
    return RiskLimitResponse(
        user_id=user_id,
        limits={k: str(v) for k, v in limits.items()},
        current_usage={k: str(v) for k, v in usage.items()},
        utilization=utilization,
        breaches=[_format_breach(b) for b in breaches],
        status=status,
        last_updated=datetime.utcnow().isoformat()
    )


@router.put("/{user_id}")
async def update_risk_limits(
    user_id: str,
    request: RiskLimitRequest,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Update risk limits for a user."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Build updates
    updates = {}
    if request.max_position_value is not None:
        updates["max_position_value"] = request.max_position_value
    if request.max_leverage is not None:
        updates["max_leverage"] = request.max_leverage
    if request.max_open_positions is not None:
        updates["max_open_positions"] = request.max_open_positions
    if request.max_concentration is not None:
        updates["max_concentration"] = request.max_concentration
    if request.max_var is not None:
        updates["max_var"] = request.max_var
    if request.max_loss_daily is not None:
        updates["max_loss_daily"] = request.max_loss_daily
    if request.max_loss_weekly is not None:
        updates["max_loss_weekly"] = request.max_loss_weekly
    if request.max_loss_monthly is not None:
        updates["max_loss_monthly"] = request.max_loss_monthly
    
    if not updates:
        raise HTTPException(status_code=400, detail="No updates provided")
    
    # Update limits
    await state_manager.update_risk_limits(user_id, updates)
    
    # Log the change
    await state_manager.log_limit_change(
        user_id=user_id,
        changes=updates,
        changed_by=current_user["user_id"]
    )
    
    return {
        "user_id": user_id,
        "updated_limits": {k: str(v) for k, v in updates.items()},
        "updated_by": current_user["user_id"],
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/breaches/active")
async def get_active_breaches(
    user_id: Optional[str] = Query(None, description="Filter by user"),
    limit_type: Optional[str] = Query(None, description="Filter by limit type"),
    severity: Optional[str] = Query(None, pattern="^(minor|major|critical)$"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[BreachResponse]:
    """Get active limit breaches."""
    # Check authorization
    if user_id and user_id != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # If not risk manager, only show own breaches
    if "risk_manager" not in current_user.get("roles", []):
        user_id = current_user["user_id"]
    
    # Get breaches
    breaches = await state_manager.get_limit_breaches(
        user_id=user_id,
        limit_type=limit_type,
        severity=severity,
        status="active",
        limit=limit
    )
    
    return [_format_breach(breach) for breach in breaches]


@router.post("/breaches/{breach_id}/resolve")
async def resolve_breach(
    breach_id: str,
    resolution: str = Query(..., description="Resolution description"),
    force_close_positions: bool = Query(False, description="Force close positions"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Resolve a limit breach."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Get breach
    breach = await state_manager.get_breach(breach_id)
    if not breach:
        raise HTTPException(status_code=404, detail="Breach not found")
    
    # Update breach status
    breach["status"] = "resolved"
    breach["resolution"] = resolution
    breach["resolved_by"] = current_user["user_id"]
    breach["resolved_at"] = datetime.utcnow()
    
    await state_manager.update_breach(breach)
    
    # Force close positions if requested
    if force_close_positions:
        await state_manager.trigger_position_liquidation(
            user_id=breach["user_id"],
            reason=f"Limit breach resolution: {breach['limit_type']}"
        )
    
    return {
        "breach_id": breach_id,
        "status": "resolved",
        "resolution": resolution,
        "force_closed": force_close_positions,
        "resolved_by": current_user["user_id"],
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/history/{user_id}")
async def get_limit_history(
    user_id: str,
    days: int = Query(30, ge=1, le=365),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """Get historical limit changes and breaches."""
    # Check authorization
    if current_user["user_id"] != user_id and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get history
    history = await state_manager.get_limit_history(
        user_id=user_id,
        days=days
    )
    
    return [
        {
            "timestamp": record["timestamp"].isoformat(),
            "event_type": record["type"],  # "limit_change" or "breach"
            "details": record["details"],
            "actor": record.get("actor", "system")
        }
        for record in history
    ]


@router.get("/templates")
async def get_limit_templates(
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """Get risk limit templates."""
    templates = await state_manager.get_limit_templates()
    
    return [
        {
            "template_id": template["id"],
            "name": template["name"],
            "description": template["description"],
            "user_type": template["user_type"],
            "limits": {k: str(v) for k, v in template["limits"].items()}
        }
        for template in templates
    ]


@router.post("/apply-template/{user_id}")
async def apply_limit_template(
    user_id: str,
    template_id: str = Query(..., description="Template ID to apply"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Apply a limit template to a user."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Get template
    template = await state_manager.get_limit_template(template_id)
    if not template:
        raise HTTPException(status_code=404, detail="Template not found")
    
    # Apply template
    await state_manager.update_risk_limits(user_id, template["limits"])
    
    # Log the change
    await state_manager.log_limit_change(
        user_id=user_id,
        changes=template["limits"],
        changed_by=current_user["user_id"],
        template_id=template_id
    )
    
    return {
        "user_id": user_id,
        "template_id": template_id,
        "template_name": template["name"],
        "applied_limits": {k: str(v) for k, v in template["limits"].items()},
        "applied_by": current_user["user_id"],
        "timestamp": datetime.utcnow().isoformat()
    }


def _format_breach(breach: Dict[str, Any]) -> Dict[str, Any]:
    """Format breach for response."""
    return {
        "breach_id": breach["id"],
        "user_id": breach["user_id"],
        "limit_type": breach["limit_type"],
        "limit_value": str(breach["limit_value"]),
        "actual_value": str(breach["actual_value"]),
        "breach_percentage": float((breach["actual_value"] - breach["limit_value"]) / breach["limit_value"] * 100),
        "timestamp": breach["timestamp"].isoformat(),
        "status": breach["status"],
        "resolution": breach.get("resolution")
    } 