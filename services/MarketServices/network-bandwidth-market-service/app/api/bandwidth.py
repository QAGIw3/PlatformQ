"""
Bandwidth Allocation API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Header
from typing import Optional

from ..models import (
    BandwidthAllocationRequest, BurstCapacityRequest,
    AllocationResponse, BurstResponse, BandwidthAllocation
)
from ..services import BandwidthManagerService, PathRegistryService
from ..core.dependencies import get_bandwidth_manager, get_path_registry


router = APIRouter(prefix="/bandwidth", tags=["Bandwidth Allocation"])


@router.post("/allocate", response_model=AllocationResponse)
async def allocate_bandwidth(
    request: BandwidthAllocationRequest,
    x_user_address: str = Header(..., description="User wallet address"),
    bandwidth_manager: BandwidthManagerService = Depends(get_bandwidth_manager),
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Allocate bandwidth on a network path"""
    # Allocate bandwidth
    allocation, error = await bandwidth_manager.allocate_bandwidth(
        request,
        x_user_address
    )
    
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    # Get path details
    path = await path_registry.get_path(request.path_id)
    
    return AllocationResponse(
        allocation=allocation,
        path_details=path,
        estimated_performance=allocation.qos_parameters,
        blockchain_tx_hash=None  # Would be set after blockchain tx
    )


@router.post("/release/{allocation_id}")
async def release_bandwidth(
    allocation_id: str,
    x_user_address: str = Header(..., description="User wallet address"),
    bandwidth_manager: BandwidthManagerService = Depends(get_bandwidth_manager)
):
    """Release an active bandwidth allocation"""
    success = await bandwidth_manager.release_bandwidth(
        allocation_id,
        x_user_address
    )
    
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Failed to release allocation - not found or unauthorized"
        )
    
    return {"status": "released", "allocation_id": allocation_id}


@router.get("/allocation/{allocation_id}", response_model=BandwidthAllocation)
async def get_allocation(
    allocation_id: str,
    bandwidth_manager: BandwidthManagerService = Depends(get_bandwidth_manager)
):
    """Get allocation details"""
    allocation = await bandwidth_manager.get_allocation(allocation_id)
    
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    return allocation


@router.get("/available/{path_id}")
async def get_available_bandwidth(
    path_id: str,
    bandwidth_manager: BandwidthManagerService = Depends(get_bandwidth_manager)
):
    """Get available bandwidth for a path"""
    available = await bandwidth_manager.get_available_bandwidth(path_id)
    
    if available is None:
        raise HTTPException(status_code=404, detail="Path not found")
    
    return {
        "path_id": path_id,
        "available_bandwidth_mbps": available
    }


@router.post("/burst", response_model=BurstResponse)
async def request_burst_capacity(
    request: BurstCapacityRequest,
    x_user_address: str = Header(..., description="User wallet address"),
    bandwidth_manager: BandwidthManagerService = Depends(get_bandwidth_manager)
):
    """Request burst bandwidth capacity"""
    burst_request, approved, error = await bandwidth_manager.request_burst(
        request,
        x_user_address
    )
    
    if error:
        # Still return the request but with rejection reason
        if burst_request:
            return BurstResponse(
                burst_request=burst_request,
                approved=False,
                reason=error
            )
        else:
            raise HTTPException(status_code=400, detail=error)
    
    # If not approved, suggest alternatives
    alternative_options = []
    if not approved:
        # Could calculate alternative burst options here
        alternative_options = [
            {
                "bandwidth_mbps": request.additional_bandwidth_mbps // 2,
                "duration_seconds": request.duration_seconds,
                "estimated_approval": "high"
            },
            {
                "bandwidth_mbps": request.additional_bandwidth_mbps,
                "duration_seconds": request.duration_seconds // 2,
                "estimated_approval": "medium"
            }
        ]
    
    return BurstResponse(
        burst_request=burst_request,
        approved=approved,
        reason=None if approved else "Insufficient available bandwidth",
        alternative_options=alternative_options if not approved else None
    ) 