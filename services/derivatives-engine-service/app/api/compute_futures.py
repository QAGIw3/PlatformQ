"""
Compute Futures Trading API

Implements electricity market-style compute resource trading.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from enum import Enum

from app.auth import get_current_user
from app.models.market import Market, MarketType
from app.engines.compute_futures_engine import (
    ComputeFuturesEngine,
    DayAheadMarket,
    CapacityAuction,
    AncillaryServices,
    SLARequirement
)

router = APIRouter(
    prefix="/api/v1/compute-futures",
    tags=["compute_futures"]
)


class ComputeResourceType(Enum):
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    QUANTUM = "quantum"
    NEUROMORPHIC = "neuromorphic"
    STORAGE = "storage"
    BANDWIDTH = "bandwidth"


class ComputeContractSpec(BaseModel):
    """Specification for compute futures contract"""
    resource_type: ComputeResourceType
    quantity: Decimal = Field(..., gt=0, description="Amount of resource")
    location_zone: Optional[str] = Field(None, description="Preferred data center zone")
    quality_specs: Dict[str, Any] = Field(default_factory=dict)
    duration_hours: int = Field(..., ge=1, le=8760)
    start_time: datetime


class DayAheadBid(BaseModel):
    """Bid for day-ahead compute market"""
    hour: int = Field(..., ge=0, le=23)
    resource_type: ComputeResourceType
    quantity: Decimal
    max_price: Decimal = Field(..., description="Maximum price willing to pay")
    flexible: bool = Field(default=False, description="Can shift ±2 hours")


@router.post("/day-ahead/submit-bid")
async def submit_day_ahead_bid(
    bid: DayAheadBid,
    delivery_date: datetime = Query(..., description="Date for delivery"),
    current_user=Depends(get_current_user),
    compute_futures_engine: ComputeFuturesEngine = Depends(get_compute_futures_engine)
) -> Dict[str, Any]:
    """
    Submit bid for day-ahead compute market
    """
    try:
        market = await compute_futures_engine.get_day_ahead_market(
            delivery_date,
            bid.resource_type
        )
        
        result = await market.submit_bid(
            user_id=current_user["sub"],
            hour=bid.hour,
            quantity=bid.quantity,
            max_price=bid.max_price,
            flexible=bid.flexible
        )
        
        return {
            "success": True,
            "bid_id": result["bid_id"],
            "estimated_clearing_price": result["estimated_price"],
            "delivery_window": result["delivery_window"]
        }
        
    except Exception as e:
        logger.error(f"Failed to submit day-ahead bid: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/day-ahead/market-clearing")
async def get_market_clearing_results(
    delivery_date: datetime,
    resource_type: ComputeResourceType,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get day-ahead market clearing results
    """
    results = await compute_futures_engine.get_clearing_results(
        delivery_date,
        resource_type
    )
    
    return {
        "clearing_prices": results["hourly_prices"],
        "total_cleared_volume": results["total_volume"],
        "supply_demand_curve": results["curves"],
        "congestion_zones": results["congestion"]
    }


@router.post("/capacity-auction/submit-offer")
async def submit_capacity_offer(
    capacity_mw: Decimal,
    delivery_year: int,
    resource_type: ComputeResourceType,
    minimum_price: Optional[Decimal] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Submit capacity commitment for annual auction
    """
    offer = await capacity_auction.submit_offer(
        provider_id=current_user["sub"],
        capacity_mw=capacity_mw,
        delivery_year=delivery_year,
        resource_type=resource_type,
        minimum_price=minimum_price
    )
    
    return {
        "offer_id": offer["id"],
        "status": "pending_auction",
        "auction_date": offer["auction_date"],
        "estimated_clearing_price": offer["price_estimate"]
    }


@router.post("/ancillary/provide-service")
async def provide_ancillary_service(
    service_type: str,  # "latency_regulation", "burst_capacity", etc.
    capacity: Decimal,
    response_time_ms: int,
    duration_hours: int = 24,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Offer ancillary services for compute grid stability
    """
    result = await ancillary_services.register_provider(
        provider_id=current_user["sub"],
        service_type=service_type,
        capacity=capacity,
        response_time_ms=response_time_ms,
        duration_hours=duration_hours
    )
    
    return {
        "registration_id": result["id"],
        "qualification_status": result["qualified"],
        "expected_compensation": result["compensation_estimate"],
        "performance_requirements": result["requirements"]
    }


@router.get("/real-time/imbalance-price")
async def get_realtime_imbalance_price(
    resource_type: ComputeResourceType
) -> Dict[str, Any]:
    """
    Get real-time imbalance pricing for compute resources
    """
    imbalance = await compute_futures_engine.get_current_imbalance(resource_type)
    
    return {
        "current_imbalance_mw": imbalance["quantity"],
        "imbalance_direction": imbalance["direction"],  # "shortage" or "surplus"
        "realtime_price": imbalance["price"],
        "day_ahead_price": imbalance["da_price"],
        "price_spread": imbalance["price"] - imbalance["da_price"]
    }


@router.post("/futures/create-contract")
async def create_compute_futures_contract(
    spec: ComputeContractSpec,
    contract_months: int = 3,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create standardized compute futures contract
    """
    contract = await compute_futures_engine.create_futures_contract(
        creator_id=current_user["sub"],
        resource_type=spec.resource_type,
        quantity=spec.quantity,
        delivery_start=spec.start_time,
        duration_hours=spec.duration_hours,
        contract_months=contract_months,
        location_zone=spec.location_zone
    )
    
    return {
        "contract_id": contract["id"],
        "symbol": contract["symbol"],  # e.g., "GPU-JUN24"
        "specifications": contract["specs"],
        "margin_requirement": contract["margin_requirement"],
        "created_at": contract["created_at"].isoformat()
    }


# Physical Settlement Endpoints
@router.post("/settlement/initiate")
async def initiate_settlement(
    contract_id: str,
    provider_id: str,
    sla_strict: bool = True,
    current_user=Depends(get_current_user),
    compute_futures_engine: ComputeFuturesEngine = Depends(get_compute_futures_engine)
) -> Dict[str, Any]:
    """
    Initiate physical settlement of a compute futures contract
    """
    # Get contract details (in practice from database)
    # For now, using example values
    sla_req = SLARequirement() if sla_strict else None
    
    settlement = await compute_futures_engine.initiate_physical_settlement(
        contract_id=contract_id,
        buyer_id=current_user["sub"],
        provider_id=provider_id,
        resource_type="gpu",
        quantity=Decimal("10"),
        delivery_start=datetime.utcnow() + timedelta(hours=1),
        duration_hours=24,
        sla_requirements=sla_req
    )
    
    return {
        "settlement_id": settlement.settlement_id,
        "status": settlement.provisioning_status,
        "delivery_start": settlement.delivery_start.isoformat(),
        "duration_hours": settlement.duration_hours,
        "sla_monitoring": sla_strict
    }


@router.get("/settlement/{settlement_id}/status")
async def get_settlement_status(
    settlement_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get status of a compute settlement including SLA violations
    """
    settlement = compute_futures_engine.settlements.get(settlement_id)
    
    if not settlement:
        raise HTTPException(status_code=404, detail="Settlement not found")
        
    return {
        "settlement_id": settlement.settlement_id,
        "status": settlement.provisioning_status,
        "sla_violations": settlement.sla_violations,
        "penalty_amount": str(settlement.penalty_amount),
        "failover_used": settlement.failover_used,
        "failover_provider": settlement.failover_provider
    }


# Failover Provider Management
@router.post("/failover/register-provider")
async def register_failover_provider(
    resource_type: ComputeResourceType,
    provider_id: str,
    priority: int = 100,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Register a failover provider for automatic fallback
    """
    await compute_futures_engine.register_failover_provider(
        resource_type=resource_type.value,
        provider_id=provider_id,
        priority=priority
    )
    
    return {
        "status": "registered",
        "resource_type": resource_type.value,
        "provider_id": provider_id,
        "priority": priority
    }


# Compute Quality Derivatives
@router.post("/quality/latency-future")
async def create_latency_future(
    source_region: str,
    dest_region: str,
    strike_latency_ms: int,
    notional: Decimal,
    expiry_days: int = 30,
    side: str = "buy",  # "buy" or "sell"
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a latency future contract for network performance
    """
    # In practice, match buyers and sellers
    buyer_id = current_user["sub"] if side == "buy" else "market_maker"
    seller_id = "market_maker" if side == "buy" else current_user["sub"]
    
    future = await compute_futures_engine.create_latency_future(
        buyer_id=buyer_id,
        seller_id=seller_id,
        source_region=source_region,
        dest_region=dest_region,
        strike_latency_ms=strike_latency_ms,
        notional=notional,
        expiry_days=expiry_days
    )
    
    return {
        "contract_id": future.contract_id,
        "region_pair": f"{source_region}-{dest_region}",
        "strike_latency_ms": future.strike_latency_ms,
        "notional": str(future.notional),
        "expiry": future.expiry.isoformat(),
        "your_side": side
    }


@router.post("/quality/uptime-swap")
async def create_uptime_swap(
    service_id: str,
    fixed_uptime_percent: Decimal,
    notional_per_hour: Decimal,
    duration_days: int = 30,
    side: str = "fixed_payer",  # "fixed_payer" or "floating_payer"
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create an uptime swap for service reliability hedging
    """
    buyer_id = current_user["sub"] if side == "fixed_payer" else "market_maker"
    seller_id = "market_maker" if side == "fixed_payer" else current_user["sub"]
    
    swap = await compute_futures_engine.create_uptime_swap(
        buyer_id=buyer_id,
        seller_id=seller_id,
        service_id=service_id,
        fixed_uptime_rate=fixed_uptime_percent / Decimal("100"),
        notional_per_hour=notional_per_hour,
        duration_days=duration_days
    )
    
    return {
        "swap_id": swap.swap_id,
        "service_id": swap.service_id,
        "fixed_rate": f"{fixed_uptime_percent}%",
        "notional_per_hour": str(swap.notional_per_hour),
        "start_date": swap.start_date.isoformat(),
        "end_date": swap.end_date.isoformat(),
        "your_side": side
    }


@router.post("/quality/performance-bond")
async def create_performance_bond(
    gpu_model: str,
    guaranteed_performance_percent: Decimal,
    bond_amount: Decimal,
    expiry_days: int = 90,
    as_issuer: bool = False,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a performance bond for compute hardware guarantees
    """
    issuer_id = current_user["sub"] if as_issuer else "provider"
    buyer_id = "investor" if as_issuer else current_user["sub"]
    
    bond = await compute_futures_engine.create_performance_bond(
        issuer_id=issuer_id,
        buyer_id=buyer_id,
        hardware_spec={"gpu_model": gpu_model},
        guaranteed_performance=guaranteed_performance_percent / Decimal("100"),
        bond_amount=bond_amount,
        expiry_days=expiry_days
    )
    
    return {
        "bond_id": bond.bond_id,
        "hardware": bond.hardware_spec,
        "guaranteed_performance": f"{guaranteed_performance_percent}%",
        "bond_amount": str(bond.bond_amount),
        "expiry": bond.expiry.isoformat(),
        "role": "issuer" if as_issuer else "buyer"
    }


@router.get("/quality/positions")
async def get_quality_positions(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get all quality derivative positions for user
    """
    user_id = current_user["sub"]
    
    # Find user's positions
    latency_futures = [
        f for f in compute_futures_engine.latency_futures.values()
        if f.buyer_id == user_id or f.seller_id == user_id
    ]
    
    uptime_swaps = [
        s for s in compute_futures_engine.uptime_swaps.values()
        if s.buyer_id == user_id or s.seller_id == user_id
    ]
    
    performance_bonds = [
        b for b in compute_futures_engine.performance_bonds.values()
        if b.issuer_id == user_id or b.buyer_id == user_id
    ]
    
    return {
        "latency_futures": [
            {
                "contract_id": f.contract_id,
                "regions": f"{f.region_pair[0]}-{f.region_pair[1]}",
                "strike": f.strike_latency_ms,
                "notional": str(f.notional),
                "expiry": f.expiry.isoformat(),
                "side": "buyer" if f.buyer_id == user_id else "seller"
            }
            for f in latency_futures
        ],
        "uptime_swaps": [
            {
                "swap_id": s.swap_id,
                "service": s.service_id,
                "fixed_rate": str(s.fixed_uptime_rate * 100) + "%",
                "notional_per_hour": str(s.notional_per_hour),
                "end_date": s.end_date.isoformat(),
                "side": "fixed_payer" if s.buyer_id == user_id else "floating_payer"
            }
            for s in uptime_swaps
        ],
        "performance_bonds": [
            {
                "bond_id": b.bond_id,
                "hardware": b.hardware_spec,
                "guaranteed": str(b.guaranteed_performance * 100) + "%",
                "amount": str(b.bond_amount),
                "expiry": b.expiry.isoformat(),
                "role": "issuer" if b.issuer_id == user_id else "buyer"
            }
            for b in performance_bonds
        ]
    }


@router.get("/sla/metrics/{service_id}")
async def get_sla_metrics(
    service_id: str,
    lookback_hours: int = 24,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get SLA metrics for a service
    """
    # In practice, query monitoring system
    # Mock data for now
    return {
        "service_id": service_id,
        "period": f"last_{lookback_hours}h",
        "metrics": {
            "uptime_percent": 99.95,
            "avg_latency_ms": 42,
            "p99_latency_ms": 89,
            "performance_score": 0.97,
            "error_rate": 0.001
        },
        "violations": [
            {
                "timestamp": "2024-01-20T14:30:00Z",
                "type": "latency",
                "severity": "minor",
                "duration_minutes": 5
            }
        ]
    }


# Get engine instance from main app
def get_compute_futures_engine():
    """Get the compute futures engine instance"""
    from app.main import compute_futures_engine
    if compute_futures_engine is None:
        raise HTTPException(status_code=503, detail="Compute futures engine not initialized")
    return compute_futures_engine 