"""
Risk Limits API

Endpoints for managing and monitoring risk limits.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.risk.risk_limits import (
    RiskLimitsEngine,
    LimitType,
    LimitAction,
    UserRiskLimits
)

router = APIRouter(
    prefix="/api/v1/risk-limits",
    tags=["risk_limits"]
)

# Initialize engine (would be injected in practice)
risk_limits_engine = None


class SetUserLimitsRequest(BaseModel):
    """Request to set user risk limits"""
    tier: str = Field(..., pattern="^(retail|professional|institutional)$")
    custom_limits: Optional[Dict[str, Any]] = Field(default=None, description="Custom limit overrides")


class OverrideLimitRequest(BaseModel):
    """Request to override a specific limit"""
    limit_type: str = Field(..., description="Type of limit to override")
    new_value: Decimal = Field(..., gt=0, description="New limit value")
    reason: str = Field(..., min_length=10, description="Reason for override")
    duration_hours: Optional[int] = Field(None, ge=1, le=168, description="Override duration (max 7 days)")


class UpdateLimitActionRequest(BaseModel):
    """Request to update limit breach action"""
    limit_type: str
    warning_action: str = Field(..., pattern="^(warn|block|reduce_only|liquidate)$")
    breach_action: str = Field(..., pattern="^(warn|block|reduce_only|liquidate)$")


@router.get("/user/{user_id}/limits")
async def get_user_limits(
    user_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get risk limits for a user
    """
    # Check authorization - users can view their own limits, admins can view any
    if current_user["sub"] != user_id and current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Not authorized to view these limits")
        
    utilization = await risk_limits_engine.get_limit_utilization(user_id)
    
    return {
        "user_id": user_id,
        "tier": utilization["tier"],
        "limits": utilization["limits"],
        "active_breaches": utilization["active_breaches"],
        "last_updated": utilization["last_updated"]
    }


@router.post("/user/{user_id}/limits")
async def set_user_limits(
    user_id: str,
    request: SetUserLimitsRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Set risk limits for a user (admin only)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
        
    try:
        user_limits = await risk_limits_engine.set_user_limits(
            user_id=user_id,
            tier=request.tier,
            custom_limits=request.custom_limits
        )
        
        return {
            "success": True,
            "user_id": user_id,
            "tier": user_limits.tier,
            "limits": {
                "max_notional_exposure": str(user_limits.max_notional_exposure),
                "max_leverage": str(user_limits.max_leverage),
                "max_positions": user_limits.max_positions,
                "daily_loss_limit": str(user_limits.daily_loss_limit),
                "max_delta": str(user_limits.max_delta),
                "max_gamma": str(user_limits.max_gamma),
                "max_vega": str(user_limits.max_vega)
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to set user limits: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/user/{user_id}/override")
async def override_limit(
    user_id: str,
    request: OverrideLimitRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Override a specific limit temporarily (admin only)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
        
    try:
        # Validate limit type
        try:
            limit_type = LimitType(request.limit_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid limit type: {request.limit_type}")
            
        await risk_limits_engine.override_limit(
            user_id=user_id,
            limit_type=limit_type,
            new_value=request.new_value,
            reason=request.reason,
            approved_by=current_user["sub"]
        )
        
        # Set expiration if specified
        if request.duration_hours:
            # Would implement automatic expiration
            pass
            
        return {
            "success": True,
            "user_id": user_id,
            "limit_type": request.limit_type,
            "new_value": str(request.new_value),
            "reason": request.reason,
            "approved_by": current_user["sub"],
            "duration_hours": request.duration_hours
        }
        
    except Exception as e:
        logger.error(f"Failed to override limit: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/breaches")
async def get_active_breaches(
    user_id: Optional[str] = None,
    limit_type: Optional[str] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get active limit breaches
    """
    # Filter breaches based on parameters
    all_breaches = list(risk_limits_engine.active_breaches)
    
    filtered_breaches = []
    for breach in all_breaches:
        breach_user_id, breach_limit_type = breach.split(":")
        
        # Check authorization
        if breach_user_id != current_user["sub"] and current_user.get("role") != "admin":
            continue
            
        # Apply filters
        if user_id and breach_user_id != user_id:
            continue
        if limit_type and breach_limit_type != limit_type:
            continue
            
        filtered_breaches.append({
            "user_id": breach_user_id,
            "limit_type": breach_limit_type,
            "breach_key": breach
        })
        
    return {
        "active_breaches": filtered_breaches,
        "total_count": len(filtered_breaches),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/breach-history")
async def get_breach_history(
    user_id: Optional[str] = None,
    limit: int = Query(default=100, le=1000),
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get historical limit breaches
    """
    # Filter history based on authorization
    history = []
    for record in risk_limits_engine.breach_history[-limit:]:
        if record["user_id"] == current_user["sub"] or current_user.get("role") == "admin":
            if not user_id or record["user_id"] == user_id:
                history.append(record)
                
    return {
        "breach_history": history,
        "count": len(history),
        "limit": limit
    }


@router.get("/tiers")
async def get_tier_defaults(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get default limits for each tier
    """
    return {
        "tiers": risk_limits_engine.tier_defaults,
        "available_tiers": ["retail", "professional", "institutional"],
        "limit_types": [lt.value for lt in LimitType]
    }


@router.post("/user/{user_id}/action")
async def update_limit_action(
    user_id: str,
    request: UpdateLimitActionRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Update the action taken when a limit is breached (admin only)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
        
    try:
        user_limits = await risk_limits_engine._get_user_limits(user_id)
        
        if request.limit_type not in user_limits.active_limits:
            raise HTTPException(status_code=404, detail=f"Limit type not found: {request.limit_type}")
            
        limit = user_limits.active_limits[request.limit_type]
        limit.warning_action = LimitAction(request.warning_action)
        limit.breach_action = LimitAction(request.breach_action)
        
        return {
            "success": True,
            "user_id": user_id,
            "limit_type": request.limit_type,
            "warning_action": request.warning_action,
            "breach_action": request.breach_action
        }
        
    except Exception as e:
        logger.error(f"Failed to update limit action: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/utilization-summary")
async def get_utilization_summary(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get risk limit utilization summary for current user
    """
    user_id = current_user["sub"]
    utilization = await risk_limits_engine.get_limit_utilization(user_id)
    
    # Calculate summary statistics
    high_utilization = []
    breached = []
    
    for limit_type, limit_data in utilization["limits"].items():
        util_percent = float(limit_data["utilization_percent"].rstrip("%"))
        
        if limit_data["is_breached"]:
            breached.append({
                "limit_type": limit_type,
                "utilization": limit_data["utilization_percent"]
            })
        elif util_percent > 80:
            high_utilization.append({
                "limit_type": limit_type,
                "utilization": limit_data["utilization_percent"]
            })
            
    return {
        "user_id": user_id,
        "tier": utilization["tier"],
        "summary": {
            "total_limits": len(utilization["limits"]),
            "breached_limits": len(breached),
            "high_utilization_limits": len(high_utilization),
            "average_utilization": f"{sum(float(l['utilization_percent'].rstrip('%')) for l in utilization['limits'].values()) / len(utilization['limits']):.1f}%"
        },
        "high_utilization": high_utilization,
        "breached": breached,
        "recommendations": await _generate_limit_recommendations(utilization)
    }


@router.get("/monitoring/status")
async def get_monitoring_status(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get risk monitoring system status (admin only)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
        
    return {
        "monitoring_active": True,
        "users_monitored": len(risk_limits_engine.user_limits),
        "active_breaches": len(risk_limits_engine.active_breaches),
        "total_breach_history": len(risk_limits_engine.breach_history),
        "limit_types_monitored": [lt.value for lt in LimitType],
        "last_reset": {
            "daily": "00:00 UTC",
            "weekly": "Monday 00:00 UTC",
            "monthly": "1st of month 00:00 UTC"
        }
    }


async def _generate_limit_recommendations(utilization: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on limit utilization"""
    recommendations = []
    
    for limit_type, limit_data in utilization["limits"].items():
        util_percent = float(limit_data["utilization_percent"].rstrip("%"))
        
        if limit_data["is_breached"]:
            recommendations.append(f"URGENT: {limit_type} limit breached. Reduce exposure immediately.")
        elif util_percent > 90:
            recommendations.append(f"Critical: {limit_type} at {util_percent:.0f}% utilization.")
        elif util_percent > 80:
            recommendations.append(f"Warning: {limit_type} approaching limit ({util_percent:.0f}%).")
            
    if not recommendations:
        recommendations.append("All risk limits within safe levels.")
        
    return recommendations


import logging

logger = logging.getLogger(__name__) 