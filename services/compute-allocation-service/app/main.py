"""
Compute Allocation Service

Centralized compute resource allocation service with multi-provider support.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Depends, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from platformq_shared.metrics import MetricsCollector
from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .core.allocation_engine import (
    AllocationEngine, ResourceRequirements, AllocationStrategy,
    ResourceAllocation
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pydantic models
class AllocateRequest(BaseModel):
    workload_type: str
    workload_id: str
    requirements: Dict[str, Any]
    strategy: str = "BALANCED"
    duration_hours: float = 1.0


class ModifyAllocationRequest(BaseModel):
    extend_hours: Optional[float] = None
    scale_to: Optional[Dict[str, Any]] = None


class CostForecastRequest(BaseModel):
    workload_type: str
    requirements: Dict[str, Any]
    duration_hours: float = 1.0


class FuturesContractRequest(BaseModel):
    resource_type: str
    quantity: int
    duration_days: int
    max_price_per_unit: float
    start_date: Optional[str] = None


class SLADerivativeRequest(BaseModel):
    workload_id: str
    sla_metrics: Dict[str, float]
    penalty_structure: Dict[str, Any]
    duration_days: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Compute Allocation Service")
    
    # Initialize allocation engine
    app.state.allocation_engine = AllocationEngine()
    await app.state.allocation_engine.start()
    
    # Initialize metrics
    app.state.metrics = MetricsCollector("compute_allocation")
    
    # Track contracts
    app.state.futures_contracts = {}
    app.state.sla_derivatives = {}
    
    yield
    
    # Cleanup
    logger.info("Shutting down Compute Allocation Service")
    await app.state.allocation_engine.stop()


# Create FastAPI app
app = FastAPI(
    title="Compute Allocation Service",
    description="Centralized compute resource allocation with multi-provider support",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "compute-allocation",
        "version": "1.0.0"
    }


# Allocation endpoints
@app.post("/api/v1/allocations")
async def allocate_resources(
    request: AllocateRequest,
    current_user=Depends(get_current_user)
):
    """Allocate compute resources"""
    try:
        # Parse requirements
        requirements = ResourceRequirements.from_dict(request.requirements)
        
        # Parse strategy
        try:
            strategy = AllocationStrategy(request.strategy)
        except ValueError:
            strategy = AllocationStrategy.BALANCED
        
        # Allocate resources
        allocation = await app.state.allocation_engine.allocate_resources(
            workload_type=request.workload_type,
            workload_id=request.workload_id,
            requirements=requirements,
            strategy=strategy,
            duration_hours=request.duration_hours
        )
        
        if not allocation:
            raise HTTPException(status_code=503, detail="No resources available")
        
        # Track metrics
        app.state.metrics.increment("allocations_created", 
                                   tags={"provider": allocation.provider.value,
                                         "workload_type": request.workload_type})
        
        return {
            "success": True,
            "allocation": allocation.to_dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to allocate resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/allocations/{allocation_id}")
async def get_allocation(
    allocation_id: str,
    current_user=Depends(get_current_user)
):
    """Get allocation details"""
    allocation = await app.state.allocation_engine.get_allocation(allocation_id)
    
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    return allocation.to_dict()


@app.put("/api/v1/allocations/{allocation_id}")
async def modify_allocation(
    allocation_id: str,
    request: ModifyAllocationRequest,
    current_user=Depends(get_current_user)
):
    """Modify an existing allocation"""
    try:
        modifications = request.dict(exclude_none=True)
        
        success = await app.state.allocation_engine.modify_allocation(
            allocation_id, modifications
        )
        
        if not success:
            raise HTTPException(status_code=404, detail="Allocation not found")
        
        # Track metrics
        app.state.metrics.increment("allocations_modified")
        
        return {"success": True}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to modify allocation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/allocations/{allocation_id}")
async def release_allocation(
    allocation_id: str,
    current_user=Depends(get_current_user)
):
    """Release allocated resources"""
    try:
        success = await app.state.allocation_engine.deallocate_resources(allocation_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Allocation not found")
        
        # Track metrics
        app.state.metrics.increment("allocations_released")
        
        return {"success": True}
        
    except Exception as e:
        logger.error(f"Failed to release allocation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/allocations")
async def list_allocations(
    workload_type: Optional[str] = Query(None),
    workload_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    current_user=Depends(get_current_user)
):
    """List allocations"""
    allocations = []
    
    for allocation in app.state.allocation_engine.allocations.values():
        # Filter by criteria
        if workload_type and allocation.workload_type != workload_type:
            continue
        if workload_id and allocation.workload_id != workload_id:
            continue
        if status and allocation.status != status:
            continue
        
        allocations.append(allocation.to_dict())
    
    return {
        "allocations": allocations,
        "total": len(allocations)
    }


# Pricing endpoints
@app.get("/api/v1/pricing/current")
async def get_current_pricing():
    """Get current spot pricing across providers"""
    pricing = await app.state.allocation_engine.get_current_pricing()
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "pricing": pricing
    }


@app.get("/api/v1/costs/forecast")
async def get_cost_forecast(request: CostForecastRequest):
    """Get cost forecast for a workload"""
    try:
        requirements = ResourceRequirements.from_dict(request.requirements)
        
        # Get pricing for each strategy
        forecasts = {}
        for strategy in AllocationStrategy:
            # Simulate allocation to get cost
            provider, region, instance_type, cost = await app.state.allocation_engine._find_best_allocation(
                requirements, strategy
            )
            
            if provider:
                forecasts[strategy.value] = {
                    "provider": provider.provider_type.value,
                    "region": region,
                    "instance_type": instance_type,
                    "cost_per_hour": cost,
                    "total_cost": cost * request.duration_hours
                }
        
        return {
            "workload_type": request.workload_type,
            "duration_hours": request.duration_hours,
            "forecasts": forecasts
        }
        
    except Exception as e:
        logger.error(f"Failed to forecast costs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Resource availability
@app.get("/api/v1/resources/available")
async def get_available_resources(
    resource_type: Optional[str] = Query(None)
):
    """Get available resources across providers"""
    resources = await app.state.allocation_engine.get_available_resources(resource_type)
    
    return resources


# Futures contracts (simplified mock implementation)
@app.post("/api/v1/contracts/futures")
async def create_futures_contract(
    request: FuturesContractRequest,
    current_user=Depends(get_current_user)
):
    """Create a futures contract for compute capacity"""
    import uuid
    from datetime import datetime, timedelta
    
    contract_id = str(uuid.uuid4())
    
    contract = {
        "contract_id": contract_id,
        "resource_type": request.resource_type,
        "quantity": request.quantity,
        "duration_days": request.duration_days,
        "max_price_per_unit": request.max_price_per_unit,
        "created_at": datetime.utcnow().isoformat(),
        "start_date": request.start_date or datetime.utcnow().isoformat(),
        "end_date": (datetime.utcnow() + timedelta(days=request.duration_days)).isoformat(),
        "status": "ACTIVE",
        "holder": current_user["user_id"]
    }
    
    app.state.futures_contracts[contract_id] = contract
    
    # Track metric
    app.state.metrics.increment("futures_contracts_created")
    
    return contract


@app.get("/api/v1/contracts/futures/{contract_id}")
async def get_futures_contract(
    contract_id: str,
    current_user=Depends(get_current_user)
):
    """Get futures contract details"""
    contract = app.state.futures_contracts.get(contract_id)
    
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    return contract


# SLA derivatives (simplified mock implementation)
@app.post("/api/v1/derivatives/sla")
async def create_sla_derivative(
    request: SLADerivativeRequest,
    current_user=Depends(get_current_user)
):
    """Create an SLA performance derivative"""
    import uuid
    from datetime import datetime, timedelta
    
    derivative_id = str(uuid.uuid4())
    
    derivative = {
        "derivative_id": derivative_id,
        "workload_id": request.workload_id,
        "sla_metrics": request.sla_metrics,
        "penalty_structure": request.penalty_structure,
        "duration_days": request.duration_days,
        "created_at": datetime.utcnow().isoformat(),
        "expires_at": (datetime.utcnow() + timedelta(days=request.duration_days)).isoformat(),
        "status": "ACTIVE",
        "holder": current_user["user_id"],
        "current_performance": {
            "uptime": 0.999,
            "latency_ms": 25,
            "throughput_mbps": 950
        }
    }
    
    app.state.sla_derivatives[derivative_id] = derivative
    
    # Track metric
    app.state.metrics.increment("sla_derivatives_created")
    
    return derivative


@app.get("/api/v1/derivatives/sla/{derivative_id}")
async def get_sla_derivative(
    derivative_id: str,
    current_user=Depends(get_current_user)
):
    """Get SLA derivative details"""
    derivative = app.state.sla_derivatives.get(derivative_id)
    
    if not derivative:
        raise HTTPException(status_code=404, detail="Derivative not found")
    
    return derivative


# Metrics endpoints
@app.get("/api/v1/metrics/allocations")
async def get_allocation_metrics():
    """Get allocation metrics"""
    metrics = app.state.allocation_engine.get_allocation_metrics()
    return metrics


@app.get("/metrics")
async def get_metrics():
    """Get Prometheus metrics"""
    return app.state.metrics.generate_metrics()


if __name__ == "__main__":
    import uvicorn
    from datetime import datetime
    uvicorn.run(app, host="0.0.0.0", port=8000) 