"""API endpoints for Resource Scaling Service"""

from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query

from platformq_shared.security import get_current_user_from_trusted_header as get_current_user
from platformq_resource_common import (
    ScalingDecision,
    ScalingPolicy,
    ScalingAction
)

from .main import scaling_engine

router = APIRouter()


@router.get("/policies/{service_name}", response_model=ScalingPolicy)
async def get_scaling_policy(
    service_name: str,
    current_user=Depends(get_current_user)
):
    """Get scaling policy for a service"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    policy = await scaling_engine.get_scaling_policy(service_name)
    if not policy:
        raise HTTPException(status_code=404, detail=f"No policy found for {service_name}")
    
    return policy


@router.put("/policies/{service_name}", response_model=ScalingPolicy)
async def update_scaling_policy(
    service_name: str,
    policy: ScalingPolicy,
    current_user=Depends(get_current_user)
):
    """Update scaling policy for a service"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Check authorization
    if current_user.get("role") not in ["admin", "platform_admin"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Ensure service name matches
    policy.service_name = service_name
    
    success = await scaling_engine.update_scaling_policy(policy)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update policy")
    
    return policy


@router.get("/decisions", response_model=List[ScalingDecision])
async def get_scaling_decisions(
    service_name: Optional[str] = Query(default=None, description="Filter by service name"),
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history"),
    current_user=Depends(get_current_user)
):
    """Get recent scaling decisions"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    decisions = await scaling_engine.get_recent_decisions(service_name, hours)
    return decisions


@router.post("/decisions/{service_name}/trigger")
async def trigger_scaling_evaluation(
    service_name: str,
    namespace: str = Query(default="platformq", description="Kubernetes namespace"),
    current_user=Depends(get_current_user)
):
    """Manually trigger scaling evaluation for a service"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Check authorization
    if current_user.get("role") not in ["admin", "platform_admin"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Trigger evaluation
    await scaling_engine._evaluate_service_scaling(service_name, namespace)
    
    return {"message": f"Scaling evaluation triggered for {service_name}"}


@router.post("/decisions/{decision_id}/apply")
async def apply_scaling_decision(
    decision_id: str,
    current_user=Depends(get_current_user)
):
    """Manually apply a scaling decision"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Check authorization
    if current_user.get("role") not in ["admin", "platform_admin"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Find decision
    decision = None
    for key in scaling_engine.decisions_cache.keys():
        if decision_id in key:
            decision_dict = scaling_engine.decisions_cache.get(key)
            decision = ScalingDecision(**decision_dict)
            break
    
    if not decision:
        raise HTTPException(status_code=404, detail="Decision not found")
    
    if decision.applied:
        raise HTTPException(status_code=400, detail="Decision already applied")
    
    # Apply decision
    success = await scaling_engine.apply_scaling_decision(decision)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to apply decision")
    
    return {"message": "Scaling decision applied successfully"}


@router.get("/predictions/{service_name}")
async def get_load_prediction(
    service_name: str,
    horizon_minutes: int = Query(default=30, ge=5, le=120, description="Prediction horizon in minutes"),
    current_user=Depends(get_current_user)
):
    """Get load prediction for a service"""
    if not scaling_engine or not scaling_engine.predictive_scaler:
        raise HTTPException(status_code=503, detail="Predictive scaling not available")
    
    predicted_load = await scaling_engine.predictive_scaler.predict_load(
        service_name,
        horizon_minutes
    )
    
    if predicted_load is None:
        raise HTTPException(status_code=404, detail=f"No prediction available for {service_name}")
    
    return {
        "service_name": service_name,
        "predicted_load": predicted_load,
        "horizon_minutes": horizon_minutes,
        "timestamp": datetime.utcnow()
    }


@router.get("/config")
async def get_scaling_config(current_user=Depends(get_current_user)):
    """Get current scaling configuration"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    return {
        "evaluation_interval": scaling_engine.settings.evaluation_interval,
        "cooldown_period": scaling_engine.settings.cooldown_period,
        "dry_run_mode": scaling_engine.settings.dry_run_mode,
        "enable_predictive_scaling": scaling_engine.settings.enable_predictive_scaling,
        "enable_cost_optimization": scaling_engine.settings.enable_cost_optimization,
        "max_monthly_cost_increase": scaling_engine.settings.max_monthly_cost_increase
    }


@router.get("/cooldown")
async def get_cooldown_status(current_user=Depends(get_current_user)):
    """Get cooldown status for all services"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    cooldown_status = {}
    now = datetime.utcnow()
    
    for service_name, last_scaling in scaling_engine._cooldown_tracker.items():
        elapsed = (now - last_scaling).total_seconds()
        policy = await scaling_engine.get_scaling_policy(service_name)
        remaining = max(0, policy.cooldown_seconds - elapsed) if policy else 0
        
        cooldown_status[service_name] = {
            "last_scaling": last_scaling,
            "elapsed_seconds": elapsed,
            "remaining_seconds": remaining,
            "in_cooldown": remaining > 0
        }
    
    return cooldown_status 