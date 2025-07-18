"""
Burst Compute Derivatives API endpoints

Specialized derivatives for handling compute demand spikes and surge capacity
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.engines.burst_compute_derivatives import (
    BurstComputeEngine,
    BurstDerivativeType,
    BurstTriggerType,
    BurstTrigger
)

router = APIRouter(
    prefix="/api/v1/burst-compute",
    tags=["burst_compute"]
)

# Global burst engine instance (initialized in main.py)
burst_engine: Optional[BurstComputeEngine] = None


def set_burst_engine(engine: BurstComputeEngine):
    """Set the burst engine instance from main.py"""
    global burst_engine
    burst_engine = engine


class CreateSurgeSwapRequest(BaseModel):
    """Request to create a surge capacity swap"""
    underlying: str = Field(..., description="Compute resource type")
    notional_capacity: str = Field(..., description="Capacity units")
    fixed_surge_rate: str = Field(..., description="Fixed rate to pay")
    floating_benchmark: str = Field(..., description="Benchmark for floating rate")
    tenor_days: int = Field(..., ge=7, le=365, description="Swap tenor in days")


class CreateSpikeOptionRequest(BaseModel):
    """Request to create a spike-triggered option"""
    underlying: str = Field(..., description="Compute resource type")
    capacity_units: str = Field(..., description="Capacity to access on trigger")
    spike_threshold: str = Field(..., description="Price level that triggers option")
    strike_multiplier: str = Field(default="1.5", description="Strike as multiplier of spot")
    expiry_days: int = Field(..., ge=1, le=90, description="Days until expiry")


class CreateDemandCollarRequest(BaseModel):
    """Request to create a demand collar strategy"""
    underlying: str = Field(..., description="Compute resource type")
    base_capacity: str = Field(..., description="Base capacity to protect")
    min_capacity_utilization: str = Field(..., description="Utilization floor (0-1)")
    max_price_spike: str = Field(..., description="Price cap")
    tenor_days: int = Field(..., ge=7, le=365, description="Protection period")


class TriggerBurstRequest(BaseModel):
    """Request to manually trigger a burst derivative"""
    derivative_id: str = Field(..., description="Derivative to trigger")
    trigger_value: str = Field(..., description="Current trigger metric value")


@router.post("/surge-swap")
async def create_surge_swap(
    request: CreateSurgeSwapRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a surge capacity swap
    
    - Pay fixed premium for surge capacity rights
    - Receive surge capacity when triggers are met
    - Floating leg based on actual surge events
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    result = await burst_engine.create_surge_swap(
        underlying=request.underlying,
        notional_capacity=Decimal(request.notional_capacity),
        fixed_surge_rate=Decimal(request.fixed_surge_rate),
        floating_benchmark=request.floating_benchmark,
        tenor_days=request.tenor_days,
        creator=current_user["user_id"]
    )
    
    return result


@router.post("/spike-option")
async def create_spike_option(
    request: CreateSpikeOptionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a spike-triggered option
    
    - Option activated when price exceeds threshold
    - Right to access capacity at pre-defined strike
    - Protection against extreme price spikes
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    result = await burst_engine.create_spike_option(
        underlying=request.underlying,
        capacity_units=Decimal(request.capacity_units),
        spike_threshold=Decimal(request.spike_threshold),
        strike_multiplier=Decimal(request.strike_multiplier),
        expiry_days=request.expiry_days,
        creator=current_user["user_id"]
    )
    
    return result


@router.post("/demand-collar")
async def create_demand_collar(
    request: CreateDemandCollarRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a demand collar strategy
    
    - Protection against low utilization (put)
    - Protection against price spikes (call)
    - Cost-effective hedging strategy
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    result = await burst_engine.create_demand_collar(
        underlying=request.underlying,
        base_capacity=Decimal(request.base_capacity),
        min_capacity_utilization=Decimal(request.min_capacity_utilization),
        max_price_spike=Decimal(request.max_price_spike),
        tenor_days=request.tenor_days,
        creator=current_user["user_id"]
    )
    
    return result


@router.post("/trigger")
async def trigger_burst(
    request: TriggerBurstRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Manually trigger a burst derivative
    
    For derivatives that allow manual triggering
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    # Verify ownership
    derivative = burst_engine.derivatives.get(request.derivative_id)
    if not derivative:
        raise HTTPException(status_code=404, detail="Derivative not found")
    
    if derivative.creator != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Trigger burst
    activation = await burst_engine.trigger_burst(
        derivative_id=request.derivative_id,
        trigger_value=Decimal(request.trigger_value)
    )
    
    return {
        "activation_id": activation.activation_id,
        "derivative_id": activation.derivative_id,
        "trigger_time": activation.trigger_time.isoformat(),
        "surge_capacity_allocated": str(activation.surge_capacity_allocated),
        "status": "active"
    }


@router.get("/derivatives")
async def list_burst_derivatives(
    derivative_type: Optional[str] = Query(None, description="Filter by type"),
    underlying: Optional[str] = Query(None, description="Filter by underlying"),
    active_only: bool = Query(default=True, description="Show only active derivatives"),
    current_user: dict = Depends(get_current_user)
):
    """List user's burst derivatives"""
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    derivatives = []
    
    for derivative in burst_engine.derivatives.values():
        if derivative.creator != current_user["user_id"]:
            continue
            
        if active_only and derivative.is_expired:
            continue
            
        if derivative_type and derivative.derivative_type.value != derivative_type:
            continue
            
        if underlying and derivative.underlying != underlying:
            continue
            
        # Check if currently active
        is_active = derivative.derivative_id in burst_engine.active_bursts
        
        derivatives.append({
            "derivative_id": derivative.derivative_id,
            "type": derivative.derivative_type.value,
            "underlying": derivative.underlying,
            "notional_capacity": str(derivative.notional_capacity),
            "surge_multiplier": str(derivative.surge_multiplier),
            "max_duration": str(derivative.max_duration),
            "premium": str(derivative.premium),
            "strike_price": str(derivative.strike_price) if derivative.strike_price else None,
            "expiry": derivative.expiry.isoformat() if derivative.expiry else None,
            "is_expired": derivative.is_expired,
            "is_active": is_active,
            "trigger": {
                "type": derivative.trigger.trigger_type.value,
                "threshold": str(derivative.trigger.threshold),
                "window": str(derivative.trigger.measurement_window)
            }
        })
    
    return {"derivatives": derivatives}


@router.get("/surge-pools")
async def get_surge_pool_status(
    resource_type: Optional[str] = Query(None, description="Filter by resource type")
):
    """Get surge capacity pool status"""
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    if resource_type:
        status = await burst_engine.get_surge_pool_status(resource_type)
        return {"pools": [status] if "error" not in status else []}
    
    # Get all pools
    pools = []
    for rt in burst_engine.surge_pools.keys():
        status = await burst_engine.get_surge_pool_status(rt)
        if "error" not in status:
            pools.append(status)
    
    return {"pools": pools}


@router.get("/activations")
async def get_burst_activations(
    derivative_id: Optional[str] = Query(None, description="Filter by derivative"),
    active_only: bool = Query(default=True, description="Show only active bursts"),
    current_user: dict = Depends(get_current_user)
):
    """Get burst activation history"""
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    activations = []
    
    for deriv_id, activation in burst_engine.active_bursts.items():
        derivative = burst_engine.derivatives.get(deriv_id)
        if not derivative or derivative.creator != current_user["user_id"]:
            continue
            
        if derivative_id and deriv_id != derivative_id:
            continue
            
        activations.append({
            "activation_id": activation.activation_id,
            "derivative_id": activation.derivative_id,
            "trigger_time": activation.trigger_time.isoformat(),
            "trigger_value": str(activation.trigger_value),
            "surge_capacity_allocated": str(activation.surge_capacity_allocated),
            "actual_duration": str(activation.actual_duration) if activation.actual_duration else None,
            "total_cost": str(activation.total_cost) if activation.total_cost else None,
            "status": "active" if deriv_id in burst_engine.active_bursts else "completed",
            "performance_metrics": activation.performance_metrics
        })
    
    return {"activations": activations}


@router.get("/analytics")
async def get_burst_analytics():
    """Get burst compute market analytics"""
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    analytics = await burst_engine.get_analytics()
    
    return analytics


@router.get("/pricing/{derivative_type}")
async def get_derivative_pricing(
    derivative_type: str,
    underlying: str,
    notional_capacity: str,
    surge_multiplier: str = Query(default="2", description="Surge capacity multiplier"),
    max_duration_hours: int = Query(default=4, description="Max burst duration"),
    spike_threshold: Optional[str] = Query(None, description="For spike options")
):
    """
    Get indicative pricing for burst derivatives
    
    Calculate premium for different derivative types
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    # Create sample trigger
    if derivative_type == "spike_option" and spike_threshold:
        trigger = BurstTrigger(
            trigger_type=BurstTriggerType.PRICE_SPIKE,
            threshold=Decimal(spike_threshold),
            measurement_window=timedelta(minutes=5)
        )
    else:
        # Default demand spike trigger
        current_demand = await burst_engine._get_current_demand(underlying)
        trigger = BurstTrigger(
            trigger_type=BurstTriggerType.DEMAND_SPIKE,
            threshold=current_demand * Decimal("1.5"),
            measurement_window=timedelta(minutes=15)
        )
    
    # Calculate premium
    premium = await burst_engine._calculate_premium(
        derivative_type=BurstDerivativeType(derivative_type),
        underlying=underlying,
        trigger=trigger,
        notional_capacity=Decimal(notional_capacity),
        surge_multiplier=Decimal(surge_multiplier),
        max_duration=timedelta(hours=max_duration_hours),
        strike_price=None
    )
    
    # Get current metrics
    current_price = await burst_engine._get_current_price(underlying)
    surge_probability = await burst_engine._calculate_surge_probability(underlying)
    
    return {
        "derivative_type": derivative_type,
        "underlying": underlying,
        "notional_capacity": notional_capacity,
        "surge_multiplier": surge_multiplier,
        "max_duration_hours": max_duration_hours,
        "estimated_premium": str(premium),
        "current_spot_price": str(current_price),
        "surge_probability": str(surge_probability),
        "surge_capacity": str(Decimal(notional_capacity) * Decimal(surge_multiplier)),
        "pricing_timestamp": datetime.utcnow().isoformat()
    }


@router.post("/custom-derivative")
async def create_custom_burst_derivative(
    underlying: str,
    trigger_type: str = Query(..., description="Trigger type"),
    trigger_threshold: str = Query(..., description="Trigger threshold value"),
    measurement_window_minutes: int = Query(5, description="Measurement window"),
    notional_capacity: str = Query(..., description="Base capacity"),
    surge_multiplier: str = Query("2", description="Surge multiplier"),
    max_duration_hours: int = Query(4, description="Max burst duration"),
    expiry_days: Optional[int] = Query(None, description="Days to expiry"),
    current_user: dict = Depends(get_current_user)
):
    """
    Create a custom burst derivative with flexible parameters
    
    Design your own trigger conditions and surge parameters
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    # Create trigger
    trigger = BurstTrigger(
        trigger_type=BurstTriggerType(trigger_type),
        threshold=Decimal(trigger_threshold),
        measurement_window=timedelta(minutes=measurement_window_minutes),
        cooldown_period=timedelta(hours=1)
    )
    
    # Create derivative
    derivative = await burst_engine.create_burst_derivative(
        derivative_type=BurstDerivativeType.BURST_FORWARD,  # Generic type
        underlying=underlying,
        trigger=trigger,
        notional_capacity=Decimal(notional_capacity),
        surge_multiplier=Decimal(surge_multiplier),
        max_duration=timedelta(hours=max_duration_hours),
        expiry=datetime.utcnow() + timedelta(days=expiry_days) if expiry_days else None,
        creator=current_user["user_id"]
    )
    
    return {
        "derivative_id": derivative.derivative_id,
        "derivative_type": derivative.derivative_type.value,
        "underlying": derivative.underlying,
        "trigger": {
            "type": trigger.trigger_type.value,
            "threshold": str(trigger.threshold),
            "window": str(trigger.measurement_window)
        },
        "surge_specs": {
            "notional_capacity": str(derivative.notional_capacity),
            "surge_multiplier": str(derivative.surge_multiplier),
            "surge_capacity": str(derivative.surge_capacity),
            "max_duration": str(derivative.max_duration)
        },
        "premium": str(derivative.premium),
        "expiry": derivative.expiry.isoformat() if derivative.expiry else None,
        "created_at": derivative.created_at.isoformat()
    }


@router.get("/market-conditions/{resource_type}")
async def get_market_conditions(
    resource_type: str,
    lookback_hours: int = Query(default=24, ge=1, le=168)
):
    """
    Get current market conditions for burst derivatives
    
    Includes demand, price, and capacity metrics
    """
    if not burst_engine:
        raise HTTPException(status_code=503, detail="Burst engine not available")
    
    # Get current metrics
    current_demand = await burst_engine._get_current_demand(resource_type)
    current_price = await burst_engine._get_current_price(resource_type)
    current_capacity = await burst_engine._get_available_capacity(resource_type)
    surge_probability = await burst_engine._calculate_surge_probability(resource_type)
    
    # Get historical data
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    
    demand_history = [
        {"timestamp": t.isoformat(), "value": str(v)}
        for t, v in burst_engine.demand_history.get(resource_type, [])
        if t > cutoff
    ]
    
    price_history = [
        {"timestamp": t.isoformat(), "value": str(v)}
        for t, v in burst_engine.price_history.get(resource_type, [])
        if t > cutoff
    ]
    
    # Calculate statistics
    if demand_history:
        demands = [float(h["value"]) for h in demand_history]
        demand_volatility = Decimal(str(np.std(demands) / np.mean(demands))) if len(demands) > 1 else Decimal("0")
    else:
        demand_volatility = Decimal("0")
    
    return {
        "resource_type": resource_type,
        "current_conditions": {
            "demand": str(current_demand),
            "price": str(current_price),
            "available_capacity": str(current_capacity),
            "surge_probability": str(surge_probability),
            "demand_volatility": str(demand_volatility)
        },
        "surge_indicators": {
            "demand_spike_likely": surge_probability > Decimal("0.3"),
            "capacity_constrained": current_capacity < current_demand * Decimal("1.2"),
            "high_volatility": demand_volatility > Decimal("0.3")
        },
        "history": {
            "demand": demand_history[-20:] if demand_history else [],  # Last 20 points
            "price": price_history[-20:] if price_history else []
        },
        "timestamp": datetime.utcnow().isoformat()
    }


# Import for numpy
import numpy as np 