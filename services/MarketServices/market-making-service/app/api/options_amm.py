"""Options AMM API endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.amm.options_amm import OptionsAMM, OptionQuote
from app.core.dependencies import get_ignite_client, get_pulsar_client
from platformq_derivatives_common import (
    BlackScholesEngine,
    GreeksCalculator,
    VolatilitySurfaceEngine,
    OptionType
)

router = APIRouter()

# Dependency to get options AMM instance
_options_amm_instance = None

async def get_options_amm() -> OptionsAMM:
    """Get or create Options AMM instance."""
    global _options_amm_instance
    
    if _options_amm_instance is None:
        ignite = await get_ignite_client()
        pulsar = await get_pulsar_client()
        
        pricing_engine = BlackScholesEngine()
        greeks_calculator = GreeksCalculator()
        vol_surface_engine = VolatilitySurfaceEngine()
        
        _options_amm_instance = OptionsAMM(
            pricing_engine=pricing_engine,
            greeks_calculator=greeks_calculator,
            vol_surface_engine=vol_surface_engine,
            ignite_cache=ignite,
            pulsar_publisher=pulsar
        )
        
        await _options_amm_instance.start()
        
    return _options_amm_instance


# Request/Response Models

class CreateOptionsPoolRequest(BaseModel):
    """Request to create an options AMM pool."""
    underlying_asset: str = Field(..., description="Underlying asset (e.g., BTC, ETH)")
    base_currency: str = Field(..., description="Base currency")
    quote_currency: str = Field(default="USD", description="Quote currency")
    initial_liquidity: Decimal = Field(..., gt=0, description="Initial liquidity in quote currency")
    max_gamma: Decimal = Field(default=Decimal("1000"), gt=0, description="Maximum gamma exposure")
    max_vega: Decimal = Field(default=Decimal("10000"), gt=0, description="Maximum vega exposure")


class GetQuoteRequest(BaseModel):
    """Request for option quote."""
    pool_id: str = Field(..., description="Pool ID")
    option_id: str = Field(..., description="Option identifier")
    option_type: str = Field(..., pattern="^(call|put)$", description="Option type")
    strike: Decimal = Field(..., gt=0, description="Strike price")
    expiry: datetime = Field(..., description="Expiration date")
    side: str = Field(..., pattern="^(buy|sell)$", description="Trade side")
    quantity: Decimal = Field(..., gt=0, description="Number of contracts")


class ExecuteTradeRequest(BaseModel):
    """Request to execute option trade."""
    pool_id: str = Field(..., description="Pool ID")
    quote: Dict[str, Any] = Field(..., description="Quote to execute")


class AddLiquidityRequest(BaseModel):
    """Request to add liquidity."""
    pool_id: str = Field(..., description="Pool ID")
    amount: Decimal = Field(..., gt=0, description="Amount in quote currency")


class RemoveLiquidityRequest(BaseModel):
    """Request to remove liquidity."""
    pool_id: str = Field(..., description="Pool ID")
    amount: Decimal = Field(..., gt=0, description="Amount to remove")


# API Endpoints

@router.post("/pools", response_model=Dict[str, str])
async def create_options_pool(
    request: CreateOptionsPoolRequest,
    user_id: str = Depends(lambda: "mock_user"),
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Create a new options AMM pool."""
    try:
        pool_id = await options_amm.create_pool(
            underlying_asset=request.underlying_asset,
            base_currency=request.base_currency,
            quote_currency=request.quote_currency,
            initial_liquidity=request.initial_liquidity,
            max_gamma=request.max_gamma,
            max_vega=request.max_vega
        )
        
        return {
            "pool_id": pool_id,
            "status": "created",
            "underlying_asset": request.underlying_asset,
            "initial_liquidity": str(request.initial_liquidity)
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/quote")
async def get_option_quote(
    request: GetQuoteRequest,
    user_id: str = Depends(lambda: "mock_user"),
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Get a quote for an option trade."""
    try:
        option_type = OptionType.CALL if request.option_type == "call" else OptionType.PUT
        
        quote = await options_amm.get_quote(
            pool_id=request.pool_id,
            option_id=request.option_id,
            option_type=option_type,
            strike=request.strike,
            expiry=request.expiry,
            side=request.side,
            quantity=request.quantity
        )
        
        if not quote:
            raise HTTPException(status_code=404, detail="Unable to provide quote")
        
        return {
            "option_id": quote.option_id,
            "side": quote.side,
            "quantity": str(quote.quantity),
            "price": str(quote.price),
            "implied_volatility": str(quote.implied_volatility),
            "greeks": {k: str(v) for k, v in quote.greeks.items()},
            "fee": str(quote.fee),
            "slippage": str(quote.slippage),
            "expires_at": quote.expires_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/trade")
async def execute_option_trade(
    request: ExecuteTradeRequest,
    user_id: str = Depends(lambda: "mock_user"),
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Execute an option trade."""
    try:
        # Reconstruct quote from dict
        quote_data = request.quote
        quote = OptionQuote(
            option_id=quote_data["option_id"],
            side=quote_data["side"],
            quantity=Decimal(quote_data["quantity"]),
            price=Decimal(quote_data["price"]),
            implied_volatility=Decimal(quote_data["implied_volatility"]),
            greeks={k: Decimal(v) for k, v in quote_data["greeks"].items()},
            fee=Decimal(quote_data["fee"]),
            slippage=Decimal(quote_data["slippage"]),
            expires_at=datetime.fromisoformat(quote_data["expires_at"])
        )
        
        success = await options_amm.execute_trade(
            pool_id=request.pool_id,
            quote=quote
        )
        
        if not success:
            raise HTTPException(status_code=400, detail="Trade execution failed")
        
        return {
            "status": "executed",
            "pool_id": request.pool_id,
            "option_id": quote.option_id,
            "side": quote.side,
            "quantity": str(quote.quantity),
            "price": str(quote.price),
            "fee": str(quote.fee)
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/liquidity/add")
async def add_liquidity(
    request: AddLiquidityRequest,
    user_id: str = Depends(lambda: "mock_user"),
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Add liquidity to an options pool."""
    try:
        success = await options_amm.add_liquidity(
            pool_id=request.pool_id,
            amount=request.amount,
            provider=user_id
        )
        
        if not success:
            raise HTTPException(status_code=400, detail="Failed to add liquidity")
        
        return {
            "status": "added",
            "pool_id": request.pool_id,
            "amount": str(request.amount),
            "provider": user_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/liquidity/remove")
async def remove_liquidity(
    request: RemoveLiquidityRequest,
    user_id: str = Depends(lambda: "mock_user"),
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Remove liquidity from an options pool."""
    try:
        amount_removed = await options_amm.remove_liquidity(
            pool_id=request.pool_id,
            amount=request.amount,
            provider=user_id
        )
        
        if amount_removed is None:
            raise HTTPException(status_code=400, detail="Failed to remove liquidity")
        
        return {
            "status": "removed",
            "pool_id": request.pool_id,
            "amount_removed": str(amount_removed),
            "provider": user_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pools/{pool_id}/stats")
async def get_pool_stats(
    pool_id: str,
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Get statistics for an options pool."""
    try:
        stats = await options_amm.get_pool_stats(pool_id)
        
        if not stats:
            raise HTTPException(status_code=404, detail="Pool not found")
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/volatility/{asset}")
async def get_volatility_surface(
    asset: str,
    options_amm: OptionsAMM = Depends(get_options_amm)
):
    """Get volatility surface for an asset."""
    try:
        surface = options_amm.vol_surface_engine.get_surface(asset)
        
        if not surface:
            raise HTTPException(status_code=404, detail="No volatility surface available")
        
        return {
            "underlying_asset": surface.underlying_asset,
            "at_the_money_vol": str(surface.at_the_money_vol),
            "skew": surface.skew,
            "term_structure": surface.term_structure,
            "spot_price": str(surface.spot_price),
            "updated_at": surface.updated_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 