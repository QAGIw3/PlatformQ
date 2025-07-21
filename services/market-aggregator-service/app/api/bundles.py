"""
Resource Bundle API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
import uuid
from datetime import datetime, timedelta

from ..models.aggregation import (
    BundleCreateRequest, ResourceBundle, BundleResponse,
    BundleAllocationRequest, AllocationResponse,
    ResourceRequirement, QuantumRequirement, AIRequirement, NetworkRequirement,
    BundleAllocation, BundleStatus, OptimizationRequest, ResourceType
)
from ..aggregators.bundle_optimizer import BundleOptimizer
from ..core.dependencies import get_bundle_optimizer, get_market_client
from ..core.market_client import MarketClient
from ..config import settings
from pyignite import Client


router = APIRouter(prefix="/bundles", tags=["Resource Bundles"])


@router.post("/", response_model=BundleResponse)
async def create_bundle(
    request: BundleCreateRequest,
    user_address: str = Query(..., description="User wallet address"),
    market_client: MarketClient = Depends(get_market_client)
):
    """Create a new resource bundle"""
    try:
        # Parse requirements
        requirements = []
        for req_data in request.requirements:
            resource_type = req_data.get('resource_type')
            
            if resource_type == 'quantum':
                requirement = QuantumRequirement(
                    specifications=req_data,
                    min_qubit_count=req_data.get('min_qubit_count', 1),
                    min_coherence_minutes=req_data.get('min_coherence_minutes', 1),
                    max_error_rate=req_data.get('max_error_rate'),
                    priority=req_data.get('priority', 1)
                )
            elif resource_type == 'ai':
                requirement = AIRequirement(
                    specifications=req_data,
                    accelerator_type=req_data.get('accelerator_type', 'GPU'),
                    min_tflops=req_data.get('min_tflops', 1),
                    duration_hours=req_data.get('duration_hours', 1),
                    priority=req_data.get('priority', 1)
                )
            elif resource_type == 'network':
                requirement = NetworkRequirement(
                    specifications=req_data,
                    source_node=req_data.get('source_node'),
                    destination_node=req_data.get('destination_node'),
                    min_bandwidth_mbps=req_data.get('min_bandwidth_mbps', 100),
                    duration_hours=req_data.get('duration_hours', 1),
                    priority=req_data.get('priority', 1)
                )
            else:
                raise ValueError(f"Unknown resource type: {resource_type}")
            
            requirements.append(requirement)
        
        # Create bundle
        bundle = ResourceBundle(
            bundle_id=f"bundle_{uuid.uuid4().hex[:8]}",
            name=request.name,
            description=request.description,
            requirements=requirements,
            optimization_objective=request.optimization_objective,
            constraints=request.constraints,
            created_at=datetime.utcnow(),
            user_address=user_address
        )
        
        # Check availability
        availability = await market_client.get_resource_availability(
            [req.dict() for req in requirements]
        )
        
        # Estimate cost
        estimated_cost = await _estimate_bundle_cost(bundle, market_client)
        
        # Store bundle in cache
        ignite_client = Client()
        ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        bundle_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_BUNDLES)
        bundle_cache.put(bundle.bundle_id, bundle.dict())
        ignite_client.close()
        
        # Generate optimization suggestions
        suggestions = []
        if estimated_cost > 1000:
            suggestions.append("Consider using reserved instances for AI resources to reduce costs")
        if any(req.resource_type == ResourceType.QUANTUM for req in requirements):
            suggestions.append("Quantum resources have limited availability - consider flexible scheduling")
        
        return BundleResponse(
            bundle=bundle,
            estimated_cost=estimated_cost,
            availability_status=availability,
            optimization_suggestions=suggestions
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{bundle_id}", response_model=BundleResponse)
async def get_bundle(
    bundle_id: str,
    market_client: MarketClient = Depends(get_market_client)
):
    """Get bundle details"""
    try:
        # Get bundle from cache
        ignite_client = Client()
        ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        bundle_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_BUNDLES)
        
        bundle_data = bundle_cache.get(bundle_id)
        if not bundle_data:
            raise HTTPException(status_code=404, detail="Bundle not found")
        
        ignite_client.close()
        
        # Reconstruct bundle
        bundle = ResourceBundle(**bundle_data)
        
        # Check current availability
        availability = await market_client.get_resource_availability(
            [req.dict() for req in bundle.requirements]
        )
        
        # Estimate current cost
        estimated_cost = await _estimate_bundle_cost(bundle, market_client)
        
        return BundleResponse(
            bundle=bundle,
            estimated_cost=estimated_cost,
            availability_status=availability,
            optimization_suggestions=[]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{bundle_id}/allocate", response_model=AllocationResponse)
async def allocate_bundle(
    bundle_id: str,
    request: BundleAllocationRequest,
    bundle_optimizer: BundleOptimizer = Depends(get_bundle_optimizer),
    market_client: MarketClient = Depends(get_market_client)
):
    """Allocate resources for a bundle"""
    try:
        # Get bundle
        ignite_client = Client()
        ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        bundle_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_BUNDLES)
        
        bundle_data = bundle_cache.get(bundle_id)
        if not bundle_data:
            raise HTTPException(status_code=404, detail="Bundle not found")
        
        bundle = ResourceBundle(**bundle_data)
        
        # Create optimization request
        opt_request = OptimizationRequest(
            bundle=bundle,
            budget_limit=request.budget_limit,
            quality_thresholds=request.quality_thresholds
        )
        
        # Optimize allocation
        optimization_result = await bundle_optimizer.optimize_bundle(opt_request)
        
        # Apply bundle discount
        total_cost = optimization_result.total_cost
        bundle_discount = settings.BUNDLE_DISCOUNT_RATE
        
        # Additional discount for cross-resource bundles
        resource_types = set(alloc.resource_type for alloc in optimization_result.optimal_allocations)
        if len(resource_types) > 1:
            bundle_discount += settings.CROSS_RESOURCE_DISCOUNT
        
        final_cost = total_cost * (1 - bundle_discount)
        
        # Create allocation record
        allocation = BundleAllocation(
            bundle_id=bundle_id,
            allocation_id=f"alloc_{uuid.uuid4().hex[:8]}",
            status=BundleStatus.PENDING,
            allocations=optimization_result.optimal_allocations,
            total_cost=total_cost,
            bundle_discount=bundle_discount,
            final_cost=final_cost,
            optimization_score=optimization_result.performance_score,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=request.duration_hours)
        )
        
        # Store allocation
        allocation_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_ALLOCATIONS)
        allocation_cache.put(allocation.allocation_id, allocation.dict())
        ignite_client.close()
        
        # Create execution plan
        execution_plan = {
            "steps": [
                {
                    "order": i + 1,
                    "resource_type": alloc.resource_type.value,
                    "resource_id": alloc.resource_id,
                    "action": "allocate",
                    "estimated_time": "30s"
                }
                for i, alloc in enumerate(optimization_result.optimal_allocations)
            ],
            "total_steps": len(optimization_result.optimal_allocations),
            "estimated_total_time": f"{len(optimization_result.optimal_allocations) * 30}s"
        }
        
        return AllocationResponse(
            allocation=allocation,
            resource_details=optimization_result.optimal_allocations,
            optimization_report=optimization_result,
            execution_plan=execution_plan
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{bundle_id}/allocations", response_model=List[BundleAllocation])
async def get_bundle_allocations(bundle_id: str):
    """Get all allocations for a bundle"""
    try:
        ignite_client = Client()
        ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        allocation_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_ALLOCATIONS)
        
        # Get all allocations (simplified - in production would use proper querying)
        allocations = []
        # Would implement proper filtering by bundle_id
        
        ignite_client.close()
        
        return allocations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{bundle_id}/execute/{allocation_id}")
async def execute_allocation(
    bundle_id: str,
    allocation_id: str,
    market_client: MarketClient = Depends(get_market_client)
):
    """Execute a bundle allocation"""
    try:
        # Get allocation
        ignite_client = Client()
        ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        allocation_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_ALLOCATIONS)
        
        allocation_data = allocation_cache.get(allocation_id)
        if not allocation_data:
            raise HTTPException(status_code=404, detail="Allocation not found")
        
        allocation = BundleAllocation(**allocation_data)
        
        if allocation.bundle_id != bundle_id:
            raise HTTPException(status_code=400, detail="Bundle ID mismatch")
        
        if allocation.status != BundleStatus.PENDING:
            raise HTTPException(status_code=400, detail=f"Cannot execute allocation in {allocation.status} status")
        
        # Execute each resource allocation
        executed_allocations = []
        failed_allocations = []
        
        for resource_alloc in allocation.allocations:
            try:
                if resource_alloc.resource_type == ResourceType.QUANTUM:
                    result = await market_client.allocate_quantum_resource(
                        qpu_id=resource_alloc.resource_id,
                        duration_minutes=int((resource_alloc.end_time - resource_alloc.start_time).total_seconds() / 60),
                        user_address=allocation_data.get('user_address', 'system')
                    )
                elif resource_alloc.resource_type == ResourceType.AI:
                    result = await market_client.allocate_ai_accelerator(
                        accelerator_id=resource_alloc.resource_id,
                        duration_hours=(resource_alloc.end_time - resource_alloc.start_time).total_seconds() / 3600,
                        user_address=allocation_data.get('user_address', 'system')
                    )
                elif resource_alloc.resource_type == ResourceType.NETWORK:
                    result = await market_client.allocate_network_bandwidth(
                        path_id=resource_alloc.resource_id,
                        bandwidth_mbps=resource_alloc.specifications.get('bandwidth_mbps', 100),
                        duration_hours=(resource_alloc.end_time - resource_alloc.start_time).total_seconds() / 3600,
                        qos_class=resource_alloc.specifications.get('qos_class', 'best_effort'),
                        user_address=allocation_data.get('user_address', 'system')
                    )
                
                executed_allocations.append({
                    "resource_type": resource_alloc.resource_type.value,
                    "resource_id": resource_alloc.resource_id,
                    "result": result
                })
                
            except Exception as e:
                failed_allocations.append({
                    "resource_type": resource_alloc.resource_type.value,
                    "resource_id": resource_alloc.resource_id,
                    "error": str(e)
                })
        
        # Update allocation status
        if failed_allocations:
            allocation.status = BundleStatus.PARTIALLY_FULFILLED
        else:
            allocation.status = BundleStatus.ACTIVE
        
        allocation_cache.put(allocation_id, allocation.dict())
        ignite_client.close()
        
        return {
            "allocation_id": allocation_id,
            "status": allocation.status.value,
            "executed": executed_allocations,
            "failed": failed_allocations,
            "success_rate": len(executed_allocations) / len(allocation.allocations)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{bundle_id}/allocations/{allocation_id}")
async def cancel_allocation(
    bundle_id: str,
    allocation_id: str
):
    """Cancel a bundle allocation"""
    try:
        ignite_client = Client()
        ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        allocation_cache = ignite_client.get_or_create_cache(settings.IGNITE_CACHE_ALLOCATIONS)
        
        allocation_data = allocation_cache.get(allocation_id)
        if not allocation_data:
            raise HTTPException(status_code=404, detail="Allocation not found")
        
        allocation = BundleAllocation(**allocation_data)
        
        if allocation.bundle_id != bundle_id:
            raise HTTPException(status_code=400, detail="Bundle ID mismatch")
        
        if allocation.status in [BundleStatus.EXPIRED, BundleStatus.CANCELLED]:
            raise HTTPException(status_code=400, detail=f"Allocation already {allocation.status}")
        
        # Update status
        allocation.status = BundleStatus.CANCELLED
        allocation_cache.put(allocation_id, allocation.dict())
        ignite_client.close()
        
        # In production, would also cancel actual resource allocations
        
        return {"status": "cancelled", "allocation_id": allocation_id}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions
async def _estimate_bundle_cost(
    bundle: ResourceBundle,
    market_client: MarketClient
) -> float:
    """Estimate the cost of a resource bundle"""
    total_cost = 0
    
    for req in bundle.requirements:
        if req.resource_type == ResourceType.QUANTUM:
            resources = await market_client.search_quantum_resources(
                min_qubit_count=req.specifications.get('min_qubit_count', 1)
            )
            if resources:
                avg_price = sum(r.get('price_per_minute', 10) for r in resources) / len(resources)
                duration_minutes = req.specifications.get('min_coherence_minutes', 10)
                total_cost += avg_price * duration_minutes
                
        elif req.resource_type == ResourceType.AI:
            resources = await market_client.search_ai_accelerators(
                accelerator_type=req.specifications.get('accelerator_type', 'GPU')
            )
            if resources:
                avg_price = sum(r.get('price_per_hour', 10) for r in resources) / len(resources)
                duration_hours = req.specifications.get('duration_hours', 1)
                total_cost += avg_price * duration_hours
                
        elif req.resource_type == ResourceType.NETWORK:
            paths = await market_client.search_network_paths(
                source=req.specifications.get('source_node', 'node_a'),
                destination=req.specifications.get('destination_node', 'node_b')
            )
            if paths:
                avg_price = sum(p.get('price_per_mbps_hour', 0.01) for p in paths) / len(paths)
                bandwidth = req.specifications.get('min_bandwidth_mbps', 100)
                duration_hours = req.specifications.get('duration_hours', 1)
                total_cost += avg_price * bandwidth * duration_hours
    
    # Apply bundle discount
    discount = settings.BUNDLE_DISCOUNT_RATE
    if len(set(req.resource_type for req in bundle.requirements)) > 1:
        discount += settings.CROSS_RESOURCE_DISCOUNT
    
    return total_cost * (1 - discount) 