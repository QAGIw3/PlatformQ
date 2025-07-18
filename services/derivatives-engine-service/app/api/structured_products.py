"""
Structured Products API

Endpoints for creating and managing structured products.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any, Union
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.engines.structured_products import (
    StructuredProductEngine,
    ProductType,
    AutocallableNote,
    ReverseConvertible,
    RangeAccrual
)

router = APIRouter(
    prefix="/api/v1/structured-products",
    tags=["structured_products"]
)

# Initialize engine (would be injected in practice)
structured_engine = None


class CreateAutocallableRequest(BaseModel):
    """Request to create an autocallable note"""
    underlying: str = Field(..., description="Underlying asset")
    notional: Decimal = Field(..., gt=0, description="Notional amount")
    maturity_months: int = Field(..., ge=3, le=36, description="Months to maturity")
    autocall_levels: List[Decimal] = Field(
        default=[1.0, 0.95, 0.90],
        description="Autocall trigger levels as % of initial"
    )
    barrier_level: Decimal = Field(default=0.7, gt=0, le=1, description="Barrier level")
    coupon_rate: Decimal = Field(..., ge=0, le=0.5, description="Annual coupon rate")
    barrier_type: str = Field(default="european", pattern="^(european|american)$")


class CreateReverseConvertibleRequest(BaseModel):
    """Request to create a reverse convertible"""
    underlying: Union[str, List[str]] = Field(..., description="Single asset or basket")
    notional: Decimal = Field(..., gt=0)
    maturity_months: int = Field(..., ge=1, le=12)
    strike_percent: Decimal = Field(default=0.9, gt=0, le=1, description="Strike as % of spot")
    coupon_rate: Decimal = Field(..., ge=0, le=0.5, description="Annual coupon")
    barrier_level: Optional[Decimal] = Field(None, gt=0, le=1, description="Optional knock-in barrier")


class CreateRangeAccrualRequest(BaseModel):
    """Request to create a range accrual note"""
    underlying: str
    notional: Decimal = Field(..., gt=0)
    maturity_months: int = Field(..., ge=1, le=24)
    lower_bound_percent: Decimal = Field(default=0.9, gt=0, le=1)
    upper_bound_percent: Decimal = Field(default=1.1, gt=1, le=2)
    daily_accrual_rate: Decimal = Field(..., ge=0, le=0.001, description="Daily accrual when in range")


@router.post("/autocallable/create")
async def create_autocallable(
    request: CreateAutocallableRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create an autocallable structured note
    """
    try:
        product = await structured_engine.create_autocallable(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            autocall_levels=request.autocall_levels,
            barrier_level=request.barrier_level,
            coupon_rate=request.coupon_rate,
            issuer_id=current_user["sub"]
        )
        
        # Set barrier type
        product.barrier_type = request.barrier_type
        
        # Calculate pricing
        fair_value = await structured_engine._price_autocallable(product)
        discount = (product.notional - fair_value) / product.notional
        
        return {
            "success": True,
            "product_id": product.product_id,
            "details": {
                "underlying": product.underlying,
                "notional": str(product.notional),
                "fair_value": str(fair_value),
                "discount_to_par": f"{discount:.2%}",
                "maturity": product.maturity_date.isoformat(),
                "observation_dates": [d.isoformat() for d in product.observation_dates],
                "autocall_levels": [f"{level:.0%}" for level in product.autocall_levels],
                "barrier_level": f"{product.barrier_level:.0%}",
                "coupon_rate": f"{product.coupon_rate:.2%}"
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to create autocallable: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reverse-convertible/create")
async def create_reverse_convertible(
    request: CreateReverseConvertibleRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a reverse convertible note
    """
    try:
        product = await structured_engine.create_reverse_convertible(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            strike_percent=request.strike_percent,
            coupon_rate=request.coupon_rate,
            barrier_level=request.barrier_level,
            issuer_id=current_user["sub"]
        )
        
        fair_value = await structured_engine._price_reverse_convertible(product)
        
        return {
            "success": True,
            "product_id": product.product_id,
            "details": {
                "underlying": product.underlying,
                "notional": str(product.notional),
                "fair_value": str(fair_value),
                "maturity": product.maturity_date.isoformat(),
                "strike_price": str(product.strike_price),
                "coupon_rate": f"{product.coupon_rate:.2%}",
                "barrier_level": f"{product.barrier_level:.0%}" if product.barrier_level else "None",
                "worst_of_basket": product.worst_of_basket
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to create reverse convertible: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/range-accrual/create")
async def create_range_accrual(
    request: CreateRangeAccrualRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a range accrual note
    """
    try:
        product = await structured_engine.create_range_accrual(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            lower_bound_percent=request.lower_bound_percent,
            upper_bound_percent=request.upper_bound_percent,
            daily_accrual_rate=request.daily_accrual_rate,
            issuer_id=current_user["sub"]
        )
        
        # Calculate maximum possible payout
        max_days = len(product.observation_dates)
        max_accrual = product.accrual_rate * max_days * 365  # Annualized
        
        return {
            "success": True,
            "product_id": product.product_id,
            "details": {
                "underlying": product.underlying,
                "notional": str(product.notional),
                "maturity": product.maturity_date.isoformat(),
                "range": {
                    "lower_bound": str(product.lower_bound),
                    "upper_bound": str(product.upper_bound)
                },
                "daily_accrual_rate": f"{product.accrual_rate:.4%}",
                "max_annual_return": f"{max_accrual:.2%}",
                "observation_days": max_days
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to create range accrual: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products")
async def list_products(
    product_type: Optional[ProductType] = None,
    is_active: Optional[bool] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    List structured products
    """
    products = []
    
    for product_id, product in structured_engine.products.items():
        # Filter by type
        if product_type and product.product_type != product_type:
            continue
            
        # Filter by status
        if is_active is not None and product.is_active != is_active:
            continue
            
        # Get current valuation
        valuation = await structured_engine.get_product_valuation(product_id)
        
        products.append({
            "product_id": product_id,
            "type": product.product_type.value,
            "underlying": product.underlying,
            "notional": str(product.notional),
            "current_value": valuation["current_value"],
            "pnl": valuation["pnl"],
            "maturity": product.maturity_date.isoformat(),
            "is_active": product.is_active,
            "is_knocked_out": product.is_knocked_out
        })
        
    # Calculate summary statistics
    total_notional = sum(p["notional"] for p in products)
    active_products = [p for p in products if p["is_active"]]
    
    return {
        "products": products,
        "summary": {
            "total_products": len(products),
            "active_products": len(active_products),
            "total_notional": str(total_notional),
            "product_types": {
                "autocallables": len([p for p in products if p["type"] == "autocallable"]),
                "reverse_convertibles": len([p for p in products if p["type"] == "reverse_convertible"]),
                "range_accruals": len([p for p in products if p["type"] == "range_accrual"])
            }
        }
    }


@router.get("/product/{product_id}")
async def get_product_details(
    product_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get detailed information about a structured product
    """
    product = structured_engine.products.get(product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
        
    valuation = await structured_engine.get_product_valuation(product_id)
    
    details = {
        "product_id": product_id,
        "type": product.product_type.value,
        "underlying": product.underlying,
        "notional": str(product.notional),
        "issue_date": product.issue_date.isoformat(),
        "maturity_date": product.maturity_date.isoformat(),
        "current_value": valuation["current_value"],
        "pnl": valuation["pnl"],
        "is_active": product.is_active,
        "is_knocked_out": product.is_knocked_out,
        "observations": []
    }
    
    # Add type-specific details
    if isinstance(product, AutocallableNote):
        details["autocallable_details"] = {
            "autocall_levels": [f"{level:.0%}" for level in product.autocall_levels],
            "barrier_level": f"{product.barrier_level:.0%}",
            "barrier_type": product.barrier_type,
            "coupon_rate": f"{product.coupon_rate:.2%}",
            "next_observation": product.observation_dates[len(product.observations)].isoformat() if len(product.observations) < len(product.observation_dates) else None
        }
        
    elif isinstance(product, ReverseConvertible):
        details["reverse_convertible_details"] = {
            "strike_price": str(product.strike_price),
            "coupon_rate": f"{product.coupon_rate:.2%}",
            "barrier_level": f"{product.barrier_level:.0%}" if product.barrier_level else None,
            "worst_of_basket": product.worst_of_basket
        }
        
    elif isinstance(product, RangeAccrual):
        details["range_accrual_details"] = {
            "lower_bound": str(product.lower_bound),
            "upper_bound": str(product.upper_bound),
            "daily_accrual_rate": f"{product.accrual_rate:.4%}",
            "days_in_range": product.days_in_range,
            "total_observation_days": product.total_observation_days,
            "current_accrual": f"{(product.days_in_range / max(1, product.total_observation_days)):.2%}"
        }
        
    # Add observations
    for obs in product.observations[:10]:  # Last 10 observations
        details["observations"].append({
            "date": obs.date.isoformat(),
            "underlying_price": str(obs.underlying_price),
            "barrier_breached": obs.barrier_breached,
            "autocall_triggered": obs.autocall_triggered
        })
        
    return details


@router.get("/pricing-models")
async def get_pricing_models(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get information about available pricing models
    """
    return {
        "models": {
            "autocallable": {
                "method": "Monte Carlo simulation",
                "parameters": ["spot", "volatility", "rates", "correlation"],
                "accuracy": "High for path-dependent features"
            },
            "reverse_convertible": {
                "method": "Black-Scholes for embedded put",
                "parameters": ["spot", "strike", "volatility", "rates"],
                "accuracy": "Exact for European options"
            },
            "range_accrual": {
                "method": "Probability-based accrual",
                "parameters": ["spot", "volatility", "mean reversion"],
                "accuracy": "Good for daily observations"
            }
        },
        "risk_factors": [
            "spot_price",
            "implied_volatility",
            "interest_rates",
            "dividend_yield",
            "correlation",
            "credit_risk"
        ]
    }


@router.get("/market-overview")
async def get_structured_products_market_overview(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get overview of structured products market
    """
    active_products = [p for p in structured_engine.products.values() if p.is_active]
    
    # Calculate statistics
    total_notional = sum(p.notional for p in active_products)
    
    # Group by type
    by_type = {}
    for product in active_products:
        ptype = product.product_type.value
        if ptype not in by_type:
            by_type[ptype] = {
                "count": 0,
                "notional": Decimal("0"),
                "avg_days_to_maturity": 0
            }
        by_type[ptype]["count"] += 1
        by_type[ptype]["notional"] += product.notional
        by_type[ptype]["avg_days_to_maturity"] += (product.maturity_date - datetime.utcnow()).days
        
    # Calculate averages
    for ptype in by_type:
        if by_type[ptype]["count"] > 0:
            by_type[ptype]["avg_days_to_maturity"] /= by_type[ptype]["count"]
            by_type[ptype]["notional"] = str(by_type[ptype]["notional"])
            
    return {
        "market_overview": {
            "total_active_products": len(active_products),
            "total_notional": str(total_notional),
            "products_by_type": by_type
        },
        "popular_structures": [
            {
                "type": "autocallable",
                "description": "Capital protection with upside participation",
                "typical_return": "8-15% p.a.",
                "risk_level": "Medium"
            },
            {
                "type": "reverse_convertible",
                "description": "High coupon with downside risk",
                "typical_return": "10-20% p.a.",
                "risk_level": "High"
            },
            {
                "type": "range_accrual",
                "description": "Enhanced yield when underlying stays in range",
                "typical_return": "5-12% p.a.",
                "risk_level": "Low-Medium"
            }
        ]
    }


import logging

logger = logging.getLogger(__name__) 