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
    AncillaryServices
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
    current_user=Depends(get_current_user)
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
        "initial_margin": contract["margin_requirement"],
        "tick_size": contract["tick_size"]
    }


# Initialize engines
compute_futures_engine = ComputeFuturesEngine()
capacity_auction = CapacityAuction()
ancillary_services = AncillaryServices() 