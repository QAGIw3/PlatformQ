"""
Cross-Service Capacity Coordinator API endpoints
"""

from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.main import cross_service_coordinator


router = APIRouter(prefix="/api/v1/capacity", tags=["capacity-coordination"])


class CapacityRequestModel(BaseModel):
    """Request for capacity allocation"""
    service_type: str
    tenant_id: str
    resource_type: str
    quantity: str
    duration_hours: int
    start_time: datetime
    priority: int = Field(default=5, ge=0, le=10)
    flexibility_hours: int = Field(default=0, ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CapacityForecastResponse(BaseModel):
    """Capacity forecast response"""
    service_type: str
    resource_type: str
    date: str
    expected_demand: str
    confidence: float
    peak_demand: str


@router.post("/request")
async def request_capacity(
    request: CapacityRequestModel,
    current_user: dict = Depends(get_current_user)
):
    """Request capacity allocation for a service"""
    if not cross_service_coordinator:
        raise HTTPException(status_code=503, detail="Capacity coordinator not initialized")
    
    try:
        # Create request object
        from app.engines.cross_service_capacity_coordinator import (
            ServiceCapacityRequest,
            ServiceType
        )
        
        capacity_request = ServiceCapacityRequest(
            request_id=f"REQ_{datetime.utcnow().timestamp()}",
            service_type=ServiceType(request.service_type),
            tenant_id=request.tenant_id,
            resource_type=request.resource_type,
            quantity=Decimal(request.quantity),
            duration=timedelta(hours=request.duration_hours),
            start_time=request.start_time,
            priority=request.priority,
            flexibility_hours=request.flexibility_hours,
            metadata=request.metadata
        )
        
        # Request capacity
        allocation_id = await cross_service_coordinator.request_capacity(capacity_request)
        
        if allocation_id:
            # Check if immediately allocated
            allocation = cross_service_coordinator.allocations.get(allocation_id)
            if allocation:
                return {
                    "status": "allocated",
                    "allocation_id": allocation_id,
                    "provider": allocation.provider,
                    "cost": str(allocation.cost),
                    "start_time": allocation.start_time.isoformat()
                }
            else:
                return {
                    "status": "pending",
                    "request_id": capacity_request.request_id,
                    "message": "Capacity request queued for allocation"
                }
        else:
            raise HTTPException(status_code=400, detail="Failed to process capacity request")
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/allocation/{allocation_id}")
async def get_allocation_status(
    allocation_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get status of capacity allocation"""
    if not cross_service_coordinator:
        raise HTTPException(status_code=503, detail="Capacity coordinator not initialized")
    
    allocation = cross_service_coordinator.allocations.get(allocation_id)
    
    if not allocation:
        # Check if it's a pending request
        for request in cross_service_coordinator.pending_requests:
            if request.request_id == allocation_id:
                return {
                    "status": "pending",
                    "request_id": allocation_id,
                    "service_type": request.service_type.value,
                    "resource_type": request.resource_type,
                    "quantity": str(request.quantity)
                }
        
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    return {
        "status": allocation.status,
        "allocation_id": allocation.allocation_id,
        "service_type": allocation.service_type.value,
        "resource_type": allocation.resource_type,
        "quantity": str(allocation.quantity),
        "tier": allocation.tier.value,
        "provider": allocation.provider,
        "start_time": allocation.start_time.isoformat(),
        "duration_hours": allocation.duration.total_seconds() / 3600,
        "cost": str(allocation.cost)
    }


@router.get("/forecast")
async def get_capacity_forecast(
    service_type: Optional[str] = Query(None, description="Filter by service type"),
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    days_ahead: int = Query(7, description="Number of days to forecast"),
    current_user: dict = Depends(get_current_user)
):
    """Get capacity demand forecast"""
    if not cross_service_coordinator:
        raise HTTPException(status_code=503, detail="Capacity coordinator not initialized")
    
    try:
        # Convert service type if provided
        service_type_enum = None
        if service_type:
            from app.engines.cross_service_capacity_coordinator import ServiceType
            service_type_enum = ServiceType(service_type)
        
        # Get forecasts
        forecasts = await cross_service_coordinator.get_capacity_forecast(
            service_type=service_type_enum,
            resource_type=resource_type,
            days_ahead=days_ahead
        )
        
        return {
            "total_forecasts": len(forecasts),
            "forecasts": [
                CapacityForecastResponse(
                    service_type=f.service_type.value,
                    resource_type=f.resource_type,
                    date=f.time_window.date().isoformat(),
                    expected_demand=str(f.expected_demand),
                    confidence=f.confidence,
                    peak_demand=str(f.peak_demand)
                )
                for f in forecasts
            ]
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize")
async def optimize_capacity_allocation(
    current_user: dict = Depends(get_current_user)
):
    """Trigger capacity allocation optimization"""
    if not cross_service_coordinator:
        raise HTTPException(status_code=503, detail="Capacity coordinator not initialized")
    
    # Check admin role
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
    
    try:
        await cross_service_coordinator.optimize_capacity_allocation()
        
        return {
            "status": "optimization_completed",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_capacity_metrics(
    current_user: dict = Depends(get_current_user)
):
    """Get capacity utilization metrics"""
    if not cross_service_coordinator:
        raise HTTPException(status_code=503, detail="Capacity coordinator not initialized")
    
    try:
        # Calculate metrics
        total_allocations = len(cross_service_coordinator.allocations)
        active_allocations = sum(
            1 for a in cross_service_coordinator.allocations.values()
            if a.status in ["reserved", "active"]
        )
        pending_requests = len(cross_service_coordinator.pending_requests)
        
        # Get capacity by tier
        capacity_by_tier = {}
        for allocation in cross_service_coordinator.allocations.values():
            if allocation.status in ["reserved", "active"]:
                tier = allocation.tier.value
                if tier not in capacity_by_tier:
                    capacity_by_tier[tier] = {
                        "count": 0,
                        "total_quantity": Decimal("0"),
                        "total_cost": Decimal("0")
                    }
                capacity_by_tier[tier]["count"] += 1
                capacity_by_tier[tier]["total_quantity"] += allocation.quantity
                capacity_by_tier[tier]["total_cost"] += allocation.cost
        
        # Convert to serializable format
        for tier_data in capacity_by_tier.values():
            tier_data["total_quantity"] = str(tier_data["total_quantity"])
            tier_data["total_cost"] = str(tier_data["total_cost"])
        
        return {
            "total_allocations": total_allocations,
            "active_allocations": active_allocations,
            "pending_requests": pending_requests,
            "capacity_by_tier": capacity_by_tier
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 