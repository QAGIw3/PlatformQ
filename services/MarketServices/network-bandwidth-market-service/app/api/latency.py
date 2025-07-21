"""
Latency Futures API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Header, Query
from typing import List
import uuid
from datetime import datetime, timedelta
from pyignite import Client

from ..models import (
    LatencyFutureRequest, LatencyFuture, AllocationStatus
)
from ..services import PathRegistryService, PricingEngineService
from ..core.dependencies import get_path_registry, get_pricing_engine
from ..config import settings


router = APIRouter(prefix="/latency", tags=["Latency Futures"])


# Temporary storage - in production would be a proper service
latency_futures_cache = None


async def get_futures_cache():
    """Get or create futures cache"""
    global latency_futures_cache
    if not latency_futures_cache:
        client = Client()
        client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
        latency_futures_cache = client.get_or_create_cache("latency_futures")
    return latency_futures_cache


@router.post("/futures", response_model=LatencyFuture)
async def create_latency_future(
    request: LatencyFutureRequest,
    x_user_address: str = Header(..., description="User wallet address"),
    path_registry: PathRegistryService = Depends(get_path_registry),
    pricing_engine: PricingEngineService = Depends(get_pricing_engine)
):
    """Create a latency guarantee future contract"""
    # Find paths between source and destination
    paths = await path_registry.search_paths({
        "source": request.source,
        "destination": request.destination
    })
    
    if not paths:
        raise HTTPException(
            status_code=404,
            detail="No paths found between source and destination"
        )
    
    # Get best path latency
    best_path = min(paths, key=lambda p: p.latency_ms)
    current_latency = best_path.latency_ms
    
    # Validate guaranteed latency is achievable
    if request.guaranteed_latency_ms < current_latency * 0.8:
        raise HTTPException(
            status_code=400,
            detail=f"Guaranteed latency too aggressive. Current best: {current_latency}ms"
        )
    
    # Calculate premium
    pricing = await pricing_engine.get_latency_future_price(
        request.guaranteed_latency_ms,
        current_latency,
        request.duration_hours,
        request.penalty_rate
    )
    
    # Create future contract
    contract_id = f"latency_{uuid.uuid4().hex[:8]}"
    start_time = datetime.utcnow()
    end_time = start_time + timedelta(hours=request.duration_hours)
    
    future = LatencyFuture(
        contract_id=contract_id,
        user_address=x_user_address,
        source=request.source,
        destination=request.destination,
        guaranteed_latency_ms=request.guaranteed_latency_ms,
        measurement_interval_seconds=60,
        contract_duration_hours=request.duration_hours,
        penalty_rate=request.penalty_rate,
        premium_paid=pricing["final_premium"],
        status=AllocationStatus.ACTIVE,
        measurements=[],
        violations_count=0,
        total_penalties=0,
        start_time=start_time,
        end_time=end_time,
        created_at=datetime.utcnow()
    )
    
    # Store future
    cache = await get_futures_cache()
    cache.put(contract_id, future.dict())
    
    return future


@router.get("/futures/{contract_id}", response_model=LatencyFuture)
async def get_latency_future(
    contract_id: str
):
    """Get latency future contract details"""
    cache = await get_futures_cache()
    future_data = cache.get(contract_id)
    
    if not future_data:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    return LatencyFuture(**future_data)


@router.post("/futures/{contract_id}/exercise")
async def exercise_latency_future(
    contract_id: str,
    x_user_address: str = Header(..., description="User wallet address")
):
    """Exercise latency future contract (claim penalties)"""
    cache = await get_futures_cache()
    future_data = cache.get(contract_id)
    
    if not future_data:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    future = LatencyFuture(**future_data)
    
    # Verify ownership
    if future.user_address != x_user_address:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    # Check if contract is still active
    if future.status != AllocationStatus.ACTIVE:
        raise HTTPException(
            status_code=400,
            detail=f"Contract is {future.status}"
        )
    
    # Check if there are penalties to claim
    if future.total_penalties == 0:
        raise HTTPException(
            status_code=400,
            detail="No penalties to claim"
        )
    
    # Process penalty payment (in production would interact with blockchain)
    payout = future.total_penalties
    
    # Update contract status
    future.status = AllocationStatus.TERMINATED
    cache.put(contract_id, future.dict())
    
    return {
        "contract_id": contract_id,
        "penalties_claimed": payout,
        "violations_count": future.violations_count,
        "status": "exercised"
    }


@router.get("/current")
async def get_current_latency(
    source: str = Query(...),
    destination: str = Query(...),
    path_registry: PathRegistryService = Depends(get_path_registry)
):
    """Get current latency metrics between endpoints"""
    # Find paths
    paths = await path_registry.search_paths({
        "source": source,
        "destination": destination
    })
    
    if not paths:
        raise HTTPException(
            status_code=404,
            detail="No paths found between endpoints"
        )
    
    # Get latency stats
    latencies = [p.latency_ms for p in paths]
    
    return {
        "source": source,
        "destination": destination,
        "current_best_latency_ms": min(latencies),
        "average_latency_ms": sum(latencies) / len(latencies),
        "worst_latency_ms": max(latencies),
        "path_count": len(paths),
        "timestamp": datetime.utcnow()
    }


@router.post("/measurements/{contract_id}")
async def record_latency_measurement(
    contract_id: str,
    measured_latency_ms: float = Query(..., ge=0),
    x_service_key: str = Header(..., description="Service API key")
):
    """Record a latency measurement for a contract (Oracle service)"""
    # In production, verify service key
    if x_service_key != "oracle-service-key":
        raise HTTPException(status_code=403, detail="Invalid service key")
    
    cache = await get_futures_cache()
    future_data = cache.get(contract_id)
    
    if not future_data:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    future = LatencyFuture(**future_data)
    
    # Check if contract is active
    if future.status != AllocationStatus.ACTIVE:
        raise HTTPException(
            status_code=400,
            detail=f"Contract is {future.status}"
        )
    
    # Record measurement
    future.measurements.append(measured_latency_ms)
    
    # Check for violation
    if measured_latency_ms > future.guaranteed_latency_ms:
        future.violations_count += 1
        
        # Calculate penalty
        excess_ms = measured_latency_ms - future.guaranteed_latency_ms
        penalty = (excess_ms * future.penalty_rate * future.premium_paid / 
                  future.contract_duration_hours)
        future.total_penalties += penalty
    
    # Keep only last 1000 measurements
    if len(future.measurements) > 1000:
        future.measurements = future.measurements[-1000:]
    
    # Update contract
    cache.put(contract_id, future.dict())
    
    return {
        "contract_id": contract_id,
        "measurement_recorded": measured_latency_ms,
        "violation": measured_latency_ms > future.guaranteed_latency_ms,
        "total_violations": future.violations_count,
        "total_penalties": future.total_penalties
    } 