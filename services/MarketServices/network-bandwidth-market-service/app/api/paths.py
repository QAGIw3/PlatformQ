"""
Network Path API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional

from ..models import (
    PathRegistrationRequest, PathSearchRequest,
    PathResponse, NetworkPath, PathStatus
)
from ..services import PathRegistryService
from ..services.pricing_engine import PricingEngineService
from ..core.dependencies import get_path_registry, get_pricing_engine


router = APIRouter(prefix="/paths", tags=["Network Paths"])


@router.post("/", response_model=NetworkPath)
async def register_path(
    request: PathRegistrationRequest,
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Register a new network path"""
    try:
        path = await path_registry.register_path(request)
        return path
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{path_id}", response_model=PathResponse)
async def get_path(
    path_id: str,
    path_registry: PathRegistryService = Depends(get_path_registry),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Get path details with current pricing"""
    path = await path_registry.get_path(path_id)
    if not path:
        raise HTTPException(status_code=404, detail="Path not found")
    
    # Get current pricing
    pricing = await pricing_engine.calculate_path_pricing(path)
    
    # Get congestion metrics from pricing engine cache
    congestion_metrics = await pricing_engine._get_congestion_metrics(path_id)
    
    # Determine available QoS classes based on path capacity
    available_qos = ["best_effort", "bronze", "silver"]
    if path.max_bandwidth_mbps >= 1000:
        available_qos.append("gold")
    if path.max_bandwidth_mbps >= 5000 and path.latency_ms <= 10:
        available_qos.append("platinum")
    
    return PathResponse(
        path=path,
        current_pricing=pricing,
        congestion_metrics=congestion_metrics,
        available_qos_classes=available_qos
    )


@router.put("/{path_id}/availability")
async def update_path_availability(
    path_id: str,
    status: PathStatus,
    available_bandwidth_mbps: Optional[int] = None,
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Update path availability status"""
    success = await path_registry.update_path_status(
        path_id,
        status,
        available_bandwidth_mbps
    )
    
    if not success:
        raise HTTPException(status_code=404, detail="Path not found")
    
    return {"status": "updated", "path_id": path_id}


@router.post("/search", response_model=List[PathResponse])
async def search_paths(
    request: PathSearchRequest,
    path_registry: PathRegistryService = Depends(get_path_registry),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Search for available network paths"""
    try:
        paths = await path_registry.search_paths(request)
        
        # Enrich with pricing information
        path_responses = []
        for path in paths[:20]:  # Limit to 20 results
            pricing = await pricing_engine.calculate_path_pricing(path)
            congestion_metrics = await pricing_engine._get_congestion_metrics(path.path_id)
            
            available_qos = ["best_effort", "bronze", "silver"]
            if path.max_bandwidth_mbps >= 1000:
                available_qos.append("gold")
            if path.max_bandwidth_mbps >= 5000 and path.latency_ms <= 10:
                available_qos.append("platinum")
            
            path_responses.append(PathResponse(
                path=path,
                current_pricing=pricing,
                congestion_metrics=congestion_metrics,
                available_qos_classes=available_qos
            ))
        
        return path_responses
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{path_id}/alternatives", response_model=List[NetworkPath])
async def get_alternative_paths(
    path_id: str,
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Get alternative paths for a given path"""
    # Get the original path
    original_path = await path_registry.get_path(path_id)
    if not original_path:
        raise HTTPException(status_code=404, detail="Path not found")
    
    # Find alternatives
    alternatives = await path_registry.find_alternative_paths(
        original_path.source.node_id,
        original_path.destination.node_id,
        [path_id]
    )
    
    return alternatives


@router.get("/status/{status}", response_model=List[NetworkPath])
async def get_paths_by_status(
    status: PathStatus,
    limit: int = Query(100, ge=1, le=1000),
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Get all paths with a specific status"""
    paths = await path_registry.get_paths_by_status(status, limit)
    return paths 