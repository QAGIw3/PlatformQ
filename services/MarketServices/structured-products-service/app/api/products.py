"""Structured products API endpoints."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.engine import StructuredProductEngine
from app.models.products import (
    ProductType,
    StructuredProduct,
    AutocallableNote,
    ReverseConvertible,
    RangeAccrual,
    AccumulatorProduct,
    VolatilityTargetNote
)
from app.core.pricing import StructuredProductPricer
from app.core.risk import RiskAnalyzer

router = APIRouter(prefix="/api/v1/products", tags=["products"])

# Dependencies
async def get_product_engine():
    """Get structured product engine instance."""
    # In production, would be properly initialized
    return StructuredProductEngine()

async def get_pricer():
    """Get product pricer instance."""
    return StructuredProductPricer()

async def get_risk_analyzer():
    """Get risk analyzer instance."""
    return RiskAnalyzer()


# Request Models

class CreateAutocallableRequest(BaseModel):
    """Request to create an autocallable note."""
    underlying: str = Field(..., description="Underlying asset")
    notional: Decimal = Field(..., gt=0, description="Notional amount")
    maturity_months: int = Field(..., ge=3, le=36, description="Months to maturity")
    
    # Autocall features
    autocall_levels: List[Decimal] = Field(
        default=[1.0, 0.95, 0.90],
        description="Autocall trigger levels as % of initial"
    )
    autocall_coupon: Decimal = Field(
        ..., ge=0, le=0.5,
        description="Annual coupon rate on autocall"
    )
    
    # Barrier features
    barrier_level: Decimal = Field(
        default=0.7, gt=0, le=1,
        description="Knock-in barrier level"
    )
    barrier_type: str = Field(
        default="european",
        pattern="^(european|american)$"
    )
    
    # Coupon features
    coupon_rate: Decimal = Field(
        default=0, ge=0, le=0.5,
        description="Regular coupon if not autocalled"
    )
    memory_coupon: bool = Field(
        default=True,
        description="Accumulate missed coupons"
    )
    
    # Observation schedule
    observation_frequency: str = Field(
        default="monthly",
        pattern="^(monthly|quarterly|semiannual)$"
    )


class CreateReverseConvertibleRequest(BaseModel):
    """Request to create a reverse convertible."""
    underlying: Union[str, List[str]] = Field(
        ...,
        description="Single asset or basket"
    )
    notional: Decimal = Field(..., gt=0)
    maturity_months: int = Field(..., ge=1, le=12)
    
    # Strike and conversion
    strike_percent: Decimal = Field(
        default=0.9, gt=0, le=1,
        description="Strike as % of spot"
    )
    conversion_ratio: Decimal = Field(
        default=1, gt=0,
        description="Shares per note if converted"
    )
    
    # Coupon
    coupon_rate: Decimal = Field(
        ..., ge=0, le=0.5,
        description="Annual guaranteed coupon"
    )
    
    # Optional barrier
    barrier_level: Optional[Decimal] = Field(
        None, gt=0, le=1,
        description="Optional knock-in barrier"
    )
    
    # Basket options
    worst_of_basket: bool = Field(
        default=False,
        description="Use worst performer for basket"
    )


class CreateRangeAccrualRequest(BaseModel):
    """Request to create a range accrual note."""
    underlying: str
    notional: Decimal = Field(..., gt=0)
    maturity_months: int = Field(..., ge=1, le=24)
    
    # Range parameters
    lower_bound_percent: Decimal = Field(
        default=0.9, gt=0, le=1
    )
    upper_bound_percent: Decimal = Field(
        default=1.1, gt=1, le=2
    )
    
    # Accrual
    daily_accrual_rate: Decimal = Field(
        ..., ge=0, le=0.001,
        description="Daily accrual when in range"
    )
    
    # Capital protection
    protection_level: Decimal = Field(
        default=1.0, ge=0, le=1,
        description="Capital protection level"
    )


class CreateAccumulatorRequest(BaseModel):
    """Request to create an accumulator product."""
    underlying: str
    notional: Decimal = Field(..., gt=0)
    maturity_months: int = Field(..., ge=1, le=12)
    
    # Accumulation parameters
    strike_percent: Decimal = Field(
        default=0.95, gt=0, le=1,
        description="Accumulation strike as % of spot"
    )
    leverage: Decimal = Field(
        default=2, ge=1, le=5,
        description="Leverage when below strike"
    )
    
    # Knock-out
    knock_out_level: Decimal = Field(
        default=1.05, gt=1, le=2,
        description="Knock-out level as % of initial"
    )
    
    # Limits
    max_accumulation: Decimal = Field(
        ..., gt=0,
        description="Maximum units to accumulate"
    )


# API Endpoints

@router.post("/autocallable")
async def create_autocallable(
    request: CreateAutocallableRequest,
    user_id: str = Depends(lambda: "mock_user"),
    engine: StructuredProductEngine = Depends(get_product_engine),
    pricer: StructuredProductPricer = Depends(get_pricer)
) -> Dict[str, Any]:
    """Create an autocallable structured note."""
    try:
        # Calculate observation dates
        observation_dates = []
        current_date = datetime.utcnow()
        
        if request.observation_frequency == "monthly":
            interval = 1
        elif request.observation_frequency == "quarterly":
            interval = 3
        else:  # semiannual
            interval = 6
        
        for i in range(1, request.maturity_months + 1, interval):
            observation_dates.append(current_date + timedelta(days=30 * i))
        
        # Create product
        product = await engine.create_autocallable(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            autocall_levels=request.autocall_levels,
            autocall_coupon=request.autocall_coupon,
            barrier_level=request.barrier_level,
            barrier_type=request.barrier_type,
            coupon_rate=request.coupon_rate,
            memory_coupon=request.memory_coupon,
            observation_dates=observation_dates,
            issuer_id=user_id
        )
        
        # Price the product
        pricing = await pricer.price_autocallable(product)
        
        return {
            "product_id": product.product_id,
            "product_type": "autocallable",
            "underlying": product.underlying,
            "notional": str(product.notional),
            "maturity_date": product.maturity_date.isoformat(),
            "pricing": {
                "fair_value": str(pricing.fair_value),
                "discount_to_par": str(pricing.discount_to_par),
                "probability_autocall": str(pricing.probability_autocall),
                "probability_knock_in": str(pricing.probability_knock_in),
                "expected_return": str(pricing.expected_return)
            },
            "terms": {
                "autocall_levels": [str(l) for l in product.autocall_levels],
                "autocall_coupon": str(product.autocall_coupon),
                "barrier_level": str(product.barrier_level),
                "barrier_type": product.barrier_type,
                "coupon_rate": str(product.coupon_rate),
                "memory_coupon": product.memory_coupon
            },
            "observation_dates": [d.isoformat() for d in product.observation_dates],
            "created_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reverse-convertible")
async def create_reverse_convertible(
    request: CreateReverseConvertibleRequest,
    user_id: str = Depends(lambda: "mock_user"),
    engine: StructuredProductEngine = Depends(get_product_engine),
    pricer: StructuredProductPricer = Depends(get_pricer)
) -> Dict[str, Any]:
    """Create a reverse convertible note."""
    try:
        product = await engine.create_reverse_convertible(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            strike_percent=request.strike_percent,
            conversion_ratio=request.conversion_ratio,
            coupon_rate=request.coupon_rate,
            barrier_level=request.barrier_level,
            worst_of_basket=request.worst_of_basket,
            issuer_id=user_id
        )
        
        # Price the product
        pricing = await pricer.price_reverse_convertible(product)
        
        return {
            "product_id": product.product_id,
            "product_type": "reverse_convertible",
            "underlying": product.underlying,
            "notional": str(product.notional),
            "maturity_date": product.maturity_date.isoformat(),
            "pricing": {
                "fair_value": str(pricing.fair_value),
                "discount_to_par": str(pricing.discount_to_par),
                "probability_conversion": str(pricing.probability_conversion),
                "expected_return": str(pricing.expected_return)
            },
            "terms": {
                "strike_price": str(product.strike_price),
                "conversion_ratio": str(product.conversion_ratio),
                "coupon_rate": str(product.coupon_rate),
                "barrier_level": str(product.barrier_level) if product.barrier_level else None,
                "worst_of_basket": product.worst_of_basket
            },
            "created_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/range-accrual")
async def create_range_accrual(
    request: CreateRangeAccrualRequest,
    user_id: str = Depends(lambda: "mock_user"),
    engine: StructuredProductEngine = Depends(get_product_engine),
    pricer: StructuredProductPricer = Depends(get_pricer)
) -> Dict[str, Any]:
    """Create a range accrual note."""
    try:
        # Get current spot price
        spot_price = Decimal("100")  # Mock - would get from oracle
        
        product = await engine.create_range_accrual(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            lower_bound=spot_price * request.lower_bound_percent,
            upper_bound=spot_price * request.upper_bound_percent,
            daily_accrual_rate=request.daily_accrual_rate,
            protection_level=request.protection_level,
            issuer_id=user_id
        )
        
        # Price the product
        pricing = await pricer.price_range_accrual(product)
        
        return {
            "product_id": product.product_id,
            "product_type": "range_accrual",
            "underlying": product.underlying,
            "notional": str(product.notional),
            "maturity_date": product.maturity_date.isoformat(),
            "pricing": {
                "fair_value": str(pricing.fair_value),
                "expected_days_in_range": str(pricing.expected_days_in_range),
                "expected_accrual": str(pricing.expected_accrual),
                "probability_in_range": str(pricing.probability_in_range)
            },
            "terms": {
                "lower_bound": str(product.lower_bound),
                "upper_bound": str(product.upper_bound),
                "daily_accrual_rate": str(product.daily_accrual_rate),
                "protection_level": str(product.protection_level),
                "total_observation_days": product.total_observation_days
            },
            "created_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/accumulator")
async def create_accumulator(
    request: CreateAccumulatorRequest,
    user_id: str = Depends(lambda: "mock_user"),
    engine: StructuredProductEngine = Depends(get_product_engine),
    pricer: StructuredProductPricer = Depends(get_pricer)
) -> Dict[str, Any]:
    """Create an accumulator product."""
    try:
        # Get current spot price
        spot_price = Decimal("100")  # Mock
        
        product = await engine.create_accumulator(
            underlying=request.underlying,
            notional=request.notional,
            maturity_months=request.maturity_months,
            strike_price=spot_price * request.strike_percent,
            leverage=request.leverage,
            knock_out_level=spot_price * request.knock_out_level,
            max_accumulation=request.max_accumulation,
            issuer_id=user_id
        )
        
        # Price the product
        pricing = await pricer.price_accumulator(product)
        
        return {
            "product_id": product.product_id,
            "product_type": "accumulator",
            "underlying": product.underlying,
            "notional": str(product.notional),
            "maturity_date": product.maturity_date.isoformat(),
            "pricing": {
                "fair_value": str(pricing.fair_value),
                "expected_accumulation": str(pricing.expected_accumulation),
                "probability_knock_out": str(pricing.probability_knock_out),
                "max_loss": str(pricing.max_loss)
            },
            "terms": {
                "strike_price": str(product.strike_price),
                "leverage": str(product.leverage),
                "knock_out_level": str(product.knock_out_level),
                "max_accumulation": str(product.max_accumulation),
                "accumulation_frequency": product.accumulation_frequency
            },
            "created_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{product_id}")
async def get_product(
    product_id: str,
    engine: StructuredProductEngine = Depends(get_product_engine)
) -> Dict[str, Any]:
    """Get details of a structured product."""
    try:
        product = await engine.get_product(product_id)
        
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Get current valuation
        valuation = await engine.get_current_valuation(product_id)
        
        return {
            "product_id": product.product_id,
            "product_type": product.product_type.value,
            "underlying": product.underlying,
            "notional": str(product.notional),
            "currency": product.currency,
            "issue_date": product.issue_date.isoformat(),
            "maturity_date": product.maturity_date.isoformat(),
            "status": {
                "is_active": product.is_active,
                "is_knocked_out": product.is_knocked_out,
                "final_payoff": str(product.final_payoff) if product.final_payoff else None
            },
            "valuation": {
                "current_value": str(valuation.current_value),
                "mark_to_market": str(valuation.mark_to_market),
                "unrealized_pnl": str(valuation.unrealized_pnl),
                "accrued_coupon": str(valuation.accrued_coupon)
            },
            "observations": [
                {
                    "date": obs.date.isoformat(),
                    "underlying_price": str(obs.underlying_price),
                    "barrier_breached": obs.barrier_breached,
                    "autocall_triggered": obs.autocall_triggered,
                    "coupon_paid": obs.coupon_paid
                }
                for obs in product.observations
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def list_products(
    product_type: Optional[ProductType] = None,
    underlying: Optional[str] = None,
    active_only: bool = True,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user_id: str = Depends(lambda: "mock_user"),
    engine: StructuredProductEngine = Depends(get_product_engine)
) -> List[Dict[str, Any]]:
    """List structured products with filters."""
    try:
        products = await engine.list_products(
            issuer_id=user_id,
            product_type=product_type,
            underlying=underlying,
            active_only=active_only,
            limit=limit,
            offset=offset
        )
        
        return [
            {
                "product_id": p.product_id,
                "product_type": p.product_type.value,
                "underlying": p.underlying,
                "notional": str(p.notional),
                "maturity_date": p.maturity_date.isoformat(),
                "is_active": p.is_active,
                "current_value": str(p.current_value) if hasattr(p, 'current_value') else None
            }
            for p in products
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{product_id}/observe")
async def record_observation(
    product_id: str,
    underlying_price: Decimal = Query(..., gt=0),
    observation_date: Optional[datetime] = None,
    engine: StructuredProductEngine = Depends(get_product_engine)
) -> Dict[str, Any]:
    """Record an observation for a structured product."""
    try:
        if observation_date is None:
            observation_date = datetime.utcnow()
        
        result = await engine.record_observation(
            product_id=product_id,
            observation_date=observation_date,
            underlying_price=underlying_price
        )
        
        return {
            "product_id": product_id,
            "observation_date": observation_date.isoformat(),
            "underlying_price": str(underlying_price),
            "barrier_breached": result.barrier_breached,
            "autocall_triggered": result.autocall_triggered,
            "coupon_paid": result.coupon_paid,
            "product_status": result.product_status
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{product_id}/risk")
async def get_product_risk(
    product_id: str,
    engine: StructuredProductEngine = Depends(get_product_engine),
    risk_analyzer: RiskAnalyzer = Depends(get_risk_analyzer)
) -> Dict[str, Any]:
    """Get risk metrics for a structured product."""
    try:
        product = await engine.get_product(product_id)
        
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        risk_metrics = await risk_analyzer.analyze_product(product)
        
        return {
            "product_id": product_id,
            "risk_metrics": {
                "delta": str(risk_metrics.delta),
                "gamma": str(risk_metrics.gamma),
                "vega": str(risk_metrics.vega),
                "theta": str(risk_metrics.theta),
                "rho": str(risk_metrics.rho)
            },
            "scenario_analysis": {
                "spot_up_10": str(risk_metrics.spot_up_10),
                "spot_down_10": str(risk_metrics.spot_down_10),
                "vol_up_5": str(risk_metrics.vol_up_5),
                "time_decay_30d": str(risk_metrics.time_decay_30d)
            },
            "risk_limits": {
                "max_loss": str(risk_metrics.max_loss),
                "value_at_risk_95": str(risk_metrics.value_at_risk_95),
                "expected_shortfall_95": str(risk_metrics.expected_shortfall_95)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 