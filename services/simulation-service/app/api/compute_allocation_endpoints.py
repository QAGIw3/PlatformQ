"""
Compute Allocation API Endpoints for Simulation Service
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from ..api.deps import get_current_tenant_and_user
from ..compute_allocation import (
    SimulationComputeAllocator,
    SimulationResourceRequirements,
    SimulationType,
    ResourceIntensity
)

router = APIRouter()


class SimulationResourceRequest(BaseModel):
    """Request for simulation compute resources"""
    simulation_id: str = Field(..., description="Unique simulation ID")
    simulation_type: str = Field(..., description="Type of simulation")
    agent_count: int = Field(1000, description="Number of agents")
    timesteps: int = Field(1000, description="Number of timesteps")
    physics_engines: List[str] = Field(default_factory=list, description="Physics engines required")
    ml_models: List[str] = Field(default_factory=list, description="ML models to run")
    participant_count: int = Field(1, description="Number of federated participants")
    data_size_gb: float = Field(1.0, description="Data size in GB")
    expected_duration_hours: float = Field(1.0, description="Expected duration")
    deadline: Optional[str] = Field(None, description="Deadline for completion")
    priority: str = Field("normal", description="Priority level")


class FederatedAllocationRequest(BaseModel):
    """Request for federated simulation allocation"""
    simulation_id: str = Field(..., description="Simulation ID")
    simulation_type: str = Field("federated_ml", description="Simulation type")
    participant_locations: List[str] = Field(..., description="Participant locations")
    agent_count: int = Field(1000, description="Total agents across participants")
    timesteps: int = Field(1000, description="Number of timesteps")
    ml_models: List[str] = Field(..., description="ML models to train")
    data_size_gb: float = Field(1.0, description="Total data size")
    expected_duration_hours: float = Field(1.0, description="Expected duration")
    deadline: Optional[str] = Field(None, description="Deadline")
    priority: str = Field("normal", description="Priority level")


@router.post("/simulations/{simulation_id}/allocate-compute", response_model=Dict[str, Any])
async def allocate_simulation_compute(
    simulation_id: str,
    request: SimulationResourceRequest,
    compute_allocator: SimulationComputeAllocator = Depends(lambda: router.app.state.compute_allocator),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Allocate compute resources for a simulation"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Create resource requirements
        requirements = SimulationResourceRequirements(
            simulation_id=simulation_id,
            simulation_type=SimulationType(request.simulation_type),
            agent_count=request.agent_count,
            timesteps=request.timesteps,
            physics_engines=request.physics_engines,
            ml_models=request.ml_models,
            participant_count=request.participant_count,
            data_size_gb=request.data_size_gb,
            expected_duration_hours=request.expected_duration_hours,
            deadline=datetime.fromisoformat(request.deadline) if request.deadline else None,
            priority=request.priority
        )
        
        # Allocate resources
        result = await compute_allocator.allocate_simulation_resources(
            requirements=requirements,
            tenant_id=tenant_id,
            user_id=user_id
        )
        
        if not result.success:
            raise HTTPException(
                status_code=400,
                detail="Failed to allocate compute resources"
            )
            
        return {
            "success": result.success,
            "simulation_id": result.simulation_id,
            "allocated_resources": result.allocated_resources,
            "total_cost": float(result.total_cost),
            "allocation_strategy": result.allocation_strategy,
            "contracts": result.contracts,
            "burst_capacity_available": result.burst_capacity_available,
            "estimated_start_time": result.estimated_start_time.isoformat() if result.estimated_start_time else None,
            "estimated_completion_time": result.estimated_completion_time.isoformat() if result.estimated_completion_time else None,
            "optimization_applied": result.optimization_applied
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/simulations/{simulation_id}/allocate-federated", response_model=Dict[str, Any])
async def allocate_federated_compute(
    simulation_id: str,
    request: FederatedAllocationRequest,
    compute_allocator: SimulationComputeAllocator = Depends(lambda: router.app.state.compute_allocator),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Allocate compute resources for federated simulation across regions"""
    tenant_id = context["tenant_id"]
    
    try:
        # Create base requirements
        requirements = SimulationResourceRequirements(
            simulation_id=simulation_id,
            simulation_type=SimulationType.FEDERATED_ML,
            agent_count=request.agent_count,
            timesteps=request.timesteps,
            ml_models=request.ml_models,
            participant_count=len(request.participant_locations),
            data_size_gb=request.data_size_gb,
            expected_duration_hours=request.expected_duration_hours,
            deadline=datetime.fromisoformat(request.deadline) if request.deadline else None,
            priority=request.priority
        )
        
        # Optimize federated allocation
        allocations = await compute_allocator.optimize_federated_allocation(
            requirements=requirements,
            participant_locations=request.participant_locations,
            tenant_id=tenant_id
        )
        
        # Format response
        response = {
            "simulation_id": simulation_id,
            "total_participants": len(request.participant_locations),
            "allocations": {}
        }
        
        total_cost = Decimal("0")
        for location, allocation in allocations.items():
            response["allocations"][location] = {
                "success": allocation.success,
                "allocated_resources": allocation.allocated_resources,
                "cost": float(allocation.total_cost),
                "strategy": allocation.allocation_strategy,
                "contracts": allocation.contracts
            }
            total_cost += allocation.total_cost
            
        response["total_cost"] = float(total_cost)
        response["cost_per_participant"] = float(total_cost / len(request.participant_locations))
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/simulations/{simulation_id}/estimate-cost", response_model=Dict[str, Any])
async def estimate_simulation_cost(
    simulation_id: str,
    simulation_type: str = "agent_based",
    agent_count: int = 1000,
    timesteps: int = 1000,
    physics_engines: Optional[str] = None,
    ml_models: Optional[str] = None,
    participant_count: int = 1,
    data_size_gb: float = 1.0,
    expected_duration_hours: float = 1.0,
    compute_allocator: SimulationComputeAllocator = Depends(lambda: router.app.state.compute_allocator)
):
    """Estimate compute costs for a simulation"""
    try:
        # Parse comma-separated lists
        physics_list = physics_engines.split(",") if physics_engines else []
        ml_list = ml_models.split(",") if ml_models else []
        
        # Create requirements
        requirements = SimulationResourceRequirements(
            simulation_id=simulation_id,
            simulation_type=SimulationType(simulation_type),
            agent_count=agent_count,
            timesteps=timesteps,
            physics_engines=physics_list,
            ml_models=ml_list,
            participant_count=participant_count,
            data_size_gb=data_size_gb,
            expected_duration_hours=expected_duration_hours
        )
        
        # Get cost estimate
        estimate = await compute_allocator.estimate_simulation_cost(requirements)
        
        return estimate
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compute/market-conditions", response_model=Dict[str, Any])
async def get_compute_market_conditions():
    """Get current compute market conditions for simulations"""
    try:
        # This would call the derivatives engine to get market data
        # For now, return sample data
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "spot_prices": {
                "GPU_V100": 35.0,
                "GPU_A100": 45.0,
                "GPU_A100_80GB": 65.0
            },
            "futures_prices": {
                "GPU_V100": 38.0,
                "GPU_A100": 48.0,
                "GPU_A100_80GB": 68.0
            },
            "availability": {
                "spot": "high",
                "futures": "medium",
                "burst": "low"
            },
            "recommendations": {
                "small_simulations": "Use spot market for cost efficiency",
                "medium_simulations": "Mix spot and futures for balance",
                "large_simulations": "Lock in futures contracts",
                "urgent_simulations": "Consider burst derivatives"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 