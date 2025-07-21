"""
Pricing API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional

from ..models import (
    BandwidthClass, PricingResponse, PathPricing
)
from ..services import PricingEngineService, PathRegistryService
from ..core.dependencies import get_pricing_engine, get_path_registry


router = APIRouter(prefix="/pricing", tags=["Pricing"])


@router.get("/bandwidth")
async def get_bandwidth_pricing(
    path_id: str,
    bandwidth_mbps: int = Query(..., ge=10, le=10000),
    qos_class: BandwidthClass = Query(...),
    duration_hours: int = Query(..., ge=1, le=720),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Get bandwidth allocation pricing estimate"""
    estimate = await pricing_engine.get_bandwidth_price_estimate(
        path_id,
        bandwidth_mbps,
        qos_class,
        duration_hours
    )
    
    if "error" in estimate:
        raise HTTPException(status_code=500, detail=estimate["error"])
    
    return estimate


@router.get("/burst")
async def get_burst_pricing(
    path_id: str,
    burst_bandwidth_mbps: int = Query(..., ge=10),
    duration_seconds: int = Query(..., ge=60, le=3600),
    urgency_factor: float = Query(1.0, ge=1.0, le=5.0),
    qos_class: BandwidthClass = Query(...),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Get burst bandwidth pricing estimate"""
    price = await pricing_engine.get_burst_price_estimate(
        path_id,
        burst_bandwidth_mbps,
        duration_seconds,
        urgency_factor,
        qos_class
    )
    
    return {
        "burst_bandwidth_mbps": burst_bandwidth_mbps,
        "duration_seconds": duration_seconds,
        "urgency_factor": urgency_factor,
        "qos_class": qos_class.value,
        "estimated_price": price,
        "price_per_gb": price / (burst_bandwidth_mbps * duration_seconds / 8000)
    }


@router.get("/circuits")
async def get_circuit_pricing(
    bandwidth_mbps: int = Query(..., ge=100),
    redundancy: bool = Query(False),
    duration_days: int = Query(..., ge=1, le=365),
    source_node: str = Query(...),
    destination_node: str = Query(...),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine),
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Get dedicated circuit pricing estimate"""
    # Find suitable paths
    paths = await path_registry.search_paths({
        "source": source_node,
        "destination": destination_node,
        "min_bandwidth_mbps": bandwidth_mbps
    })
    
    if not paths:
        raise HTTPException(
            status_code=404,
            detail="No suitable paths found for circuit"
        )
    
    # Calculate pricing
    estimate = await pricing_engine.get_circuit_price_estimate(
        paths[:2] if redundancy else paths[:1],  # Use 2 paths if redundant
        bandwidth_mbps,
        redundancy,
        duration_days
    )
    
    if "error" in estimate:
        raise HTTPException(status_code=500, detail=estimate["error"])
    
    return estimate


@router.get("/congestion/{path_id}")
async def get_congestion_pricing(
    path_id: str,
    pricing_engine: PricingEngineService = Depends(get_pricing_engine),
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Get current congestion-based pricing for a path"""
    path = await path_registry.get_path(path_id)
    if not path:
        raise HTTPException(status_code=404, detail="Path not found")
    
    pricing = await pricing_engine.calculate_path_pricing(path)
    congestion_metrics = await pricing_engine._get_congestion_metrics(path_id)
    
    return {
        "path_id": path_id,
        "current_pricing": pricing,
        "congestion_metrics": congestion_metrics,
        "congestion_multiplier": pricing.congestion_multiplier,
        "spot_price": pricing.spot_price_per_mbps_hour
    }


@router.get("/trends/{path_id}")
async def get_pricing_trends(
    path_id: str,
    hours: int = Query(24, ge=1, le=168),  # Max 7 days
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Get historical pricing trends for a path"""
    trends = await pricing_engine.get_pricing_trends(path_id, hours)
    
    if "error" in trends:
        raise HTTPException(status_code=500, detail=trends["error"])
    
    return trends


@router.get("/latency-futures")
async def get_latency_future_pricing(
    guaranteed_latency_ms: float = Query(..., ge=1),
    current_latency_ms: float = Query(..., ge=1),
    duration_hours: int = Query(..., ge=1, le=720),
    penalty_rate: float = Query(0.1, ge=0.01, le=1.0),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Get latency future contract pricing"""
    estimate = await pricing_engine.get_latency_future_price(
        guaranteed_latency_ms,
        current_latency_ms,
        duration_hours,
        penalty_rate
    )
    
    if "error" in estimate:
        raise HTTPException(status_code=500, detail=estimate["error"])
    
    return estimate 