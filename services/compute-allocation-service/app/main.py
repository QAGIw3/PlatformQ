"""Compute Allocation Service API

Provides REST API for compute resource allocation using the shared framework.
"""

from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
import os
import logging

from fastapi import FastAPI, HTTPException, Depends, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from pydantic import BaseModel, Field

from platformq_compute_common.models import (
    ResourceRequirements,
    AllocationRequest,
    AllocationResponse,
    AllocationStrategy,
    PricingModel
)
from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .allocation_service import AllocationService
from .config_manager import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
registry = CollectorRegistry()
allocation_counter = Counter(
    'compute_allocations_total',
    'Total number of allocation requests',
    ['status', 'provider'],
    registry=registry
)
allocation_duration = Histogram(
    'compute_allocation_duration_seconds',
    'Time spent allocating resources',
    registry=registry
)
active_allocations = Gauge(
    'compute_allocations_active',
    'Number of active allocations',
    ['provider'],
    registry=registry
)

# Global instances
config_manager = ConfigManager()
allocation_service = None


class ModifyAllocationRequest(BaseModel):
    extend_hours: Optional[float] = None
    scale_to: Optional[Dict[str, Any]] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global allocation_service
    
    # Initialize configuration manager
    consul_config = {
        "host": os.getenv("CONSUL_HOST", "consul"),
        "port": int(os.getenv("CONSUL_PORT", "8500")),
        "token": os.getenv("CONSUL_TOKEN", "")
    }
    
    vault_config = {
        "enabled": os.getenv("VAULT_ENABLED", "true").lower() == "true",
        "address": os.getenv("VAULT_ADDR", "http://vault:8200"),
        "token": os.getenv("VAULT_TOKEN", "")
    }
    
    await config_manager.initialize(consul_config, vault_config)
    
    # Register service with Consul
    service_host = os.getenv("SERVICE_HOST", "0.0.0.0")
    service_port = int(os.getenv("SERVICE_PORT", "8000"))
    await config_manager.register_service(service_host, service_port)
    
    # Initialize allocation service
    allocation_service = AllocationService(config_manager)
    await allocation_service.initialize()
    
    logger.info("Compute Allocation Service started")
    
    yield
    
    # Cleanup
    await allocation_service.shutdown()
    await config_manager.deregister_service(service_host, service_port)
    await config_manager.close()
    
    logger.info("Compute Allocation Service stopped")


app = FastAPI(
    title="Compute Allocation Service",
    description="Manages compute resource allocation across multiple providers",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "compute-allocation-service",
        "version": "1.0.0"
    }


@app.post("/api/v1/allocations", response_model=AllocationResponse)
async def allocate_resources(
    requirements: ResourceRequirements,
    workload_type: str,
    workload_id: str,
    strategy: AllocationStrategy = AllocationStrategy.BALANCED,
    duration_hours: float = 1.0,
    pricing_preferences: Optional[List[PricingModel]] = None,
    tags: Optional[Dict[str, str]] = None,
    current_user=Depends(get_current_user)
):
    """Allocate compute resources"""
    with allocation_duration.time():
        try:
            # Create allocation request
            request = AllocationRequest(
                tenant_id=current_user["tenant_id"],
                workload_id=workload_id,
                workload_type=workload_type,
                requirements=requirements,
                strategy=strategy,
                duration_hours=duration_hours,
                pricing_preferences=pricing_preferences or [PricingModel.ON_DEMAND],
                tags=tags or {}
            )
            
            # Allocate resources
            response = await allocation_service.allocate_resources(request)
            
            # Update metrics
            if response.success:
                allocation_counter.labels(
                    status="success",
                    provider=response.allocation.provider.value if response.allocation else "unknown"
                ).inc()
            else:
                allocation_counter.labels(status="failure", provider="unknown").inc()
            
            if not response.success:
                raise HTTPException(status_code=400, detail=response.message)
            
            return response
            
        except Exception as e:
            allocation_counter.labels(status="error", provider="unknown").inc()
            logger.error(f"Error allocating resources: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/allocations/{allocation_id}")
async def get_allocation(
    allocation_id: str,
    current_user=Depends(get_current_user)
):
    """Get allocation details"""
    allocation = await allocation_service.get_allocation(allocation_id)
    
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    # Check tenant authorization
    if allocation.tenant_id != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return allocation.to_dict()


@app.put("/api/v1/allocations/{allocation_id}")
async def modify_allocation(
    allocation_id: str,
    request: ModifyAllocationRequest,
    current_user=Depends(get_current_user)
):
    """Modify an existing allocation"""
    # Get allocation to check authorization
    allocation = await allocation_service.get_allocation(allocation_id)
    
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    if allocation.tenant_id != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Modify allocation
    success = await allocation_service.modify_allocation(
        allocation_id,
        request.dict(exclude_unset=True)
    )
    
    if not success:
        raise HTTPException(status_code=400, detail="Failed to modify allocation")
    
    return {"status": "modified", "allocation_id": allocation_id}


@app.delete("/api/v1/allocations/{allocation_id}")
async def release_allocation(
    allocation_id: str,
    current_user=Depends(get_current_user)
):
    """Release allocated resources"""
    # Get allocation to check authorization
    allocation = await allocation_service.get_allocation(allocation_id)
    
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    if allocation.tenant_id != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Deallocate resources
    success = await allocation_service.deallocate_resources(allocation_id)
    
    if not success:
        raise HTTPException(status_code=400, detail="Failed to deallocate resources")
    
    return {"status": "deallocated", "allocation_id": allocation_id}


@app.get("/api/v1/allocations")
async def list_allocations(
    workload_type: Optional[str] = Query(None),
    workload_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    current_user=Depends(get_current_user)
):
    """List allocations for the current tenant"""
    # Get all allocations for the tenant
    all_allocations = []
    
    for allocation in allocation_service.allocations.values():
        if allocation.tenant_id == current_user["tenant_id"]:
            # Apply filters
            if workload_type and allocation.workload_type != workload_type:
                continue
            if workload_id and allocation.workload_id != workload_id:
                continue
            if status and allocation.status.value != status:
                continue
                
            all_allocations.append(allocation.to_dict())
    
    return {
        "allocations": all_allocations,
        "total": len(all_allocations)
    }


@app.get("/api/v1/metrics/allocations")
async def get_allocation_metrics():
    """Get allocation metrics"""
    metrics = await allocation_service.get_allocation_metrics()
    
    # Update Prometheus gauges
    for provider, stats in metrics.get("by_provider", {}).items():
        active_allocations.labels(provider=provider).set(stats["count"])
    
    return metrics


@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(registry)


# Additional endpoints for cost analysis
@app.get("/api/v1/costs/estimate")
async def estimate_costs(
    requirements: ResourceRequirements,
    duration_hours: float = 1.0,
    pricing_model: PricingModel = PricingModel.ON_DEMAND
):
    """Estimate costs for given requirements"""
    from platformq_compute_common.cost import CostCalculator
    from platformq_compute_common.models import ProviderType
    
    calculator = CostCalculator()
    
    # Calculate costs for different providers
    estimates = {}
    
    for provider in [ProviderType.AWS, ProviderType.CLOUDSTACK, ProviderType.KUBERNETES]:
        cost_analysis = calculator.calculate_requirements_cost(
            requirements,
            provider,
            "us-east-1",  # Default region
            pricing_model,
            duration_hours
        )
        
        estimates[provider.value] = cost_analysis.to_dict()
    
    return {
        "estimates": estimates,
        "duration_hours": duration_hours,
        "pricing_model": pricing_model.value
    }


@app.get("/api/v1/providers/capabilities")
async def get_provider_capabilities():
    """Get capabilities of all registered providers"""
    capabilities = {}
    
    for name, provider in allocation_service.provider_registry.get_all_providers().items():
        try:
            caps = await provider.get_capabilities()
            capabilities[name] = {
                "provider_type": caps.provider_type.value,
                "regions": caps.supported_regions,
                "instance_types": list(caps.supported_instance_types.keys()),
                "gpu_types": caps.supported_gpu_types,
                "pricing_models": [p.value for p in caps.supported_pricing_models],
                "features": caps.features,
                "sla_guarantees": caps.sla_guarantees
            }
        except Exception as e:
            logger.error(f"Failed to get capabilities for {name}: {e}")
            capabilities[name] = {"error": str(e)}
    
    return capabilities


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 