"""
Options Trading API

Endpoints for options trading including pricing, Greeks, AMM liquidity, and volatility surfaces.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.engines.pricing import (
    BlackScholesEngine,
    VolatilitySurfaceEngine,
    OptionsChainManager,
    OptionParameters
)
from app.engines.options_amm import (
    OptionsAMM,
    AMMConfig,
    OptionsLiquidity
)
from app.models.order import OrderSide
from app.auth import get_current_user
from app.integrations import IgniteCache as IgniteClient, PulsarEventPublisher as PulsarClient

router = APIRouter(
    prefix="/api/v1/options",
    tags=["options"]
)

# Initialize engines
pricing_engine = BlackScholesEngine()
vol_surface_engine = VolatilitySurfaceEngine()
chain_manager = OptionsChainManager()

# Initialize Options AMM
amm_config = AMMConfig()
ignite_client = IgniteClient()
pulsar_client = PulsarClient()

options_amm = OptionsAMM(
    config=amm_config,
    pricing_engine=pricing_engine,
    vol_surface=vol_surface_engine,
    ignite_client=ignite_client,
    pulsar_client=pulsar_client
)


class OptionPricingRequest(BaseModel):
    """Request for option pricing"""
    spot: Decimal = Field(..., gt=0, description="Spot price")
    strike: Decimal = Field(..., gt=0, description="Strike price")
    time_to_expiry: Decimal = Field(..., gt=0, le=5, description="Time to expiry in years")
    volatility: Optional[Decimal] = Field(None, gt=0, le=5, description="Implied volatility (optional)")
    risk_free_rate: Decimal = Field(default=Decimal("0.05"), description="Risk-free rate")
    dividend_yield: Decimal = Field(default=Decimal("0"), description="Dividend yield")
    is_call: bool = Field(default=True, description="True for call, False for put")


class ImpliedVolRequest(BaseModel):
    """Request for implied volatility calculation"""
    option_price: Decimal = Field(..., gt=0, description="Observed option price")
    spot: Decimal = Field(..., gt=0, description="Spot price")
    strike: Decimal = Field(..., gt=0, description="Strike price")
    time_to_expiry: Decimal = Field(..., gt=0, description="Time to expiry in years")
    risk_free_rate: Decimal = Field(default=Decimal("0.05"))
    is_call: bool = Field(default=True)


class OptionChainRequest(BaseModel):
    """Request to create option chain"""
    underlying: str = Field(..., description="Underlying asset symbol")
    spot: Decimal = Field(..., gt=0, description="Current spot price")
    expiry_date: datetime = Field(..., description="Expiration date")
    strike_interval: Decimal = Field(..., gt=0, description="Strike price interval")
    num_strikes: int = Field(default=21, ge=5, le=50, description="Number of strikes")


class AMMQuoteRequest(BaseModel):
    """Request for AMM quote"""
    underlying: str
    strike: Decimal
    expiry: datetime
    is_call: bool
    size: Decimal = Field(..., gt=0)
    side: OrderSide


class AMMTradeRequest(BaseModel):
    """Request to execute AMM trade"""
    quote_id: str = Field(..., description="Quote ID from get_quote")
    slippage_tolerance_bps: Decimal = Field(default=Decimal("50"), description="Max slippage in bps")


@router.post("/price")
async def calculate_option_price(
    request: OptionPricingRequest
) -> Dict[str, Any]:
    """Calculate option price using Black-Scholes model"""
    try:
        # Use provided volatility or get from surface
        if request.volatility is None:
            request.volatility = vol_surface_engine.get_implied_volatility(
                request.strike,
                request.time_to_expiry,
                request.spot
            )
        
        params = OptionParameters(
            spot=request.spot,
            strike=request.strike,
            time_to_expiry=request.time_to_expiry,
            volatility=request.volatility,
            risk_free_rate=request.risk_free_rate,
            dividend_yield=request.dividend_yield,
            is_call=request.is_call
        )
        
        price = pricing_engine.calculate_option_price(params)
        greeks = pricing_engine.calculate_greeks(params)
        
        return {
            "price": str(price),
            "volatility_used": str(request.volatility),
            "greeks": {
                "delta": str(greeks.delta),
                "gamma": str(greeks.gamma),
                "theta": str(greeks.theta),
                "vega": str(greeks.vega),
                "rho": str(greeks.rho),
                "lambda": str(greeks.lambda_) if greeks.lambda_ else None,
                "vanna": str(greeks.vanna) if greeks.vanna else None,
                "charm": str(greeks.charm) if greeks.charm else None,
                "vomma": str(greeks.vomma) if greeks.vomma else None,
                "speed": str(greeks.speed) if greeks.speed else None
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/implied-volatility")
async def calculate_implied_volatility(
    request: ImpliedVolRequest
) -> Dict[str, Any]:
    """Calculate implied volatility from option price"""
    try:
        params = OptionParameters(
            spot=request.spot,
            strike=request.strike,
            time_to_expiry=request.time_to_expiry,
            volatility=Decimal("0.3"),  # Initial guess
            risk_free_rate=request.risk_free_rate,
            is_call=request.is_call
        )
        
        iv = pricing_engine.calculate_implied_volatility(
            request.option_price,
            params
        )
        
        if iv is None:
            raise HTTPException(
                status_code=400,
                detail="Could not calculate implied volatility"
            )
        
        return {
            "implied_volatility": str(iv),
            "implied_volatility_percent": str(iv * 100)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chain/create")
async def create_option_chain(
    request: OptionChainRequest,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Create a new option chain for an underlying"""
    try:
        chain = chain_manager.create_option_chain(
            underlying=request.underlying,
            spot=request.spot,
            expiry=request.expiry_date,
            strike_interval=request.strike_interval,
            num_strikes=request.num_strikes,
            risk_free_rate=Decimal("0.05")  # Could be dynamic
        )
        
        # Add AMM liquidity for each option
        for strike in chain["strikes"]:
            # Add call liquidity
            options_amm.add_liquidity_pool(
                underlying=request.underlying,
                strike=strike,
                expiry=request.expiry_date,
                is_call=True
            )
            
            # Add put liquidity
            options_amm.add_liquidity_pool(
                underlying=request.underlying,
                strike=strike,
                expiry=request.expiry_date,
                is_call=False
            )
        
        return {
            "underlying": request.underlying,
            "expiry": request.expiry_date.isoformat(),
            "spot": str(chain["spot"]),
            "strikes": len(chain["strikes"]),
            "chain_created": True
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chain/{underlying}")
async def get_option_chain(
    underlying: str,
    expiry: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get option chain for underlying"""
    try:
        chain_data = chain_manager.get_option_chain(underlying, expiry)
        
        if not chain_data:
            raise HTTPException(
                status_code=404,
                detail=f"No option chain found for {underlying}"
            )
        
        # Format response
        if expiry:
            # Single expiry
            formatted_strikes = {}
            for strike, data in chain_data["strikes"].items():
                formatted_strikes[str(strike)] = {
                    "call": {
                        "price": str(data["call"]["price"]),
                        "iv": str(data["call"]["iv"]),
                        "delta": str(data["call"]["greeks"].delta),
                        "gamma": str(data["call"]["greeks"].gamma),
                        "volume": str(data["call"]["volume"]),
                        "open_interest": str(data["call"]["open_interest"])
                    },
                    "put": {
                        "price": str(data["put"]["price"]),
                        "iv": str(data["put"]["iv"]),
                        "delta": str(data["put"]["greeks"].delta),
                        "gamma": str(data["put"]["greeks"].gamma),
                        "volume": str(data["put"]["volume"]),
                        "open_interest": str(data["put"]["open_interest"])
                    }
                }
            
            return {
                "underlying": underlying,
                "expiry": chain_data["expiry"].isoformat(),
                "spot": str(chain_data["spot"]),
                "time_to_expiry": str(chain_data["time_to_expiry"]),
                "strikes": formatted_strikes
            }
        else:
            # All expiries
            return {
                "underlying": underlying,
                "expiries": [exp.isoformat() for exp in chain_data.keys()]
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/volatility/surface/{underlying}")
async def get_volatility_surface(
    underlying: str
) -> Dict[str, Any]:
    """Get volatility surface for underlying"""
    try:
        if not vol_surface_engine.surface_data:
            raise HTTPException(
                status_code=404,
                detail="No volatility surface available"
            )
        
        return {
            "underlying": underlying,
            "last_update": vol_surface_engine.last_update.isoformat(),
            "strikes": [str(s) for s in vol_surface_engine.surface_data["strikes"]],
            "maturities": [str(m) for m in vol_surface_engine.surface_data["maturities"]],
            "implied_vols": [
                [str(iv) for iv in row] 
                for row in vol_surface_engine.surface_data["ivs"]
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/volatility/smile")
async def get_volatility_smile(
    spot: Decimal = Query(..., gt=0),
    time_to_expiry: Decimal = Query(..., gt=0, le=5),
    num_strikes: int = Query(default=21, ge=5, le=50)
) -> Dict[str, Any]:
    """Get volatility smile for given maturity"""
    try:
        smile = vol_surface_engine.calculate_volatility_smile(
            spot=spot,
            time_to_expiry=time_to_expiry,
            num_strikes=num_strikes
        )
        
        formatted_smile = {
            str(strike): str(iv) 
            for strike, iv in smile.items()
        }
        
        return {
            "spot": str(spot),
            "time_to_expiry": str(time_to_expiry),
            "smile": formatted_smile
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/amm/quote")
async def get_amm_quote(
    request: AMMQuoteRequest,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Get quote from options AMM"""
    try:
        # Update spot price if available
        spot_price = chain_manager.chains.get(request.underlying, {}).get(
            request.expiry, {}
        ).get("spot")
        
        quote = options_amm.get_quote(
            underlying=request.underlying,
            strike=request.strike,
            expiry=request.expiry,
            is_call=request.is_call,
            size=request.size,
            side=request.side,
            spot_price=spot_price
        )
        
        if not quote:
            raise HTTPException(
                status_code=400,
                detail="Unable to provide quote - insufficient liquidity"
            )
        
        # Add quote ID for execution
        quote["quote_id"] = f"Q-{datetime.now().timestamp()}"
        
        # Cache quote
        await ignite_client.put(
            f"option_quote_{quote['quote_id']}",
            quote,
            ttl=60  # 60 second TTL
        )
        
        return {
            "quote_id": quote["quote_id"],
            "price": str(quote["price"]),
            "size": str(quote["size"]),
            "side": quote["side"].value,
            "theo_price": str(quote["theo_price"]),
            "spread_bps": str(quote["spread_bps"]),
            "impact_bps": str(quote["impact_bps"]),
            "fee": str(quote["fee"]),
            "iv": str(quote["iv"]),
            "available_liquidity": str(quote["available_liquidity"]),
            "expires_at": (datetime.now() + timedelta(seconds=60)).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/amm/trade")
async def execute_amm_trade(
    request: AMMTradeRequest,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Execute trade with options AMM"""
    try:
        # Retrieve quote
        quote = await ignite_client.get(f"option_quote_{request.quote_id}")
        
        if not quote:
            raise HTTPException(
                status_code=404,
                detail="Quote not found or expired"
            )
        
        # Execute trade
        trade_result = await options_amm.execute_trade(
            quote=quote,
            slippage_tolerance_bps=request.slippage_tolerance_bps
        )
        
        if not trade_result:
            raise HTTPException(
                status_code=400,
                detail="Trade execution failed"
            )
        
        return {
            "success": True,
            "trade": {
                "pool_id": trade_result["pool_id"],
                "price": str(trade_result["price"]),
                "size": str(trade_result["size"]),
                "side": trade_result["side"].value,
                "fee": str(trade_result["fee"]),
                "timestamp": trade_result["timestamp"].isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/amm/liquidity/{underlying}")
async def get_amm_liquidity(
    underlying: str,
    strike: Optional[Decimal] = None,
    expiry: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get AMM liquidity for options"""
    try:
        liquidity_info = []
        
        # Filter liquidity pools
        for pool_id, liquidity in options_amm.liquidity_pools.items():
            if not pool_id.startswith(underlying):
                continue
            
            if strike and liquidity.strike != strike:
                continue
            
            if expiry and liquidity.expiry.date() != expiry.date():
                continue
            
            liquidity_info.append({
                "pool_id": pool_id,
                "strike": str(liquidity.strike),
                "expiry": liquidity.expiry.isoformat(),
                "is_call": liquidity.is_call,
                "base_size": str(liquidity.base_size),
                "current_position": str(liquidity.current_position),
                "max_position": str(liquidity.max_position),
                "spread_bps": str(liquidity.spread_bps),
                "volume_24h": str(liquidity.volume_24h)
            })
        
        return {
            "underlying": underlying,
            "liquidity_pools": liquidity_info,
            "total_pools": len(liquidity_info)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/amm/portfolio")
async def get_amm_portfolio() -> Dict[str, Any]:
    """Get AMM portfolio summary"""
    try:
        summary = options_amm.get_portfolio_summary()
        
        return {
            "active_positions": summary["active_positions"],
            "total_notional": str(summary["total_notional"]),
            "portfolio_greeks": {
                k: str(v) for k, v in summary["portfolio_greeks"].items()
            },
            "hedge_positions": {
                k: str(v) for k, v in summary["hedge_positions"].items()
            },
            "realized_pnl": str(summary["realized_pnl"]),
            "collected_fees": str(summary["collected_fees"]),
            "liquidity_pools": summary["liquidity_pools"],
            "last_update": summary["last_update"].isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/put-call-ratio/{underlying}")
async def get_put_call_ratio(
    underlying: str,
    expiry: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get put/call ratio for sentiment analysis"""
    try:
        pc_ratio = chain_manager.calculate_put_call_ratio(underlying, expiry)
        
        if pc_ratio is None:
            raise HTTPException(
                status_code=404,
                detail="Insufficient data for put/call ratio"
            )
        
        return {
            "underlying": underlying,
            "expiry": expiry.isoformat() if expiry else "all",
            "put_call_ratio": str(pc_ratio),
            "sentiment": (
                "bearish" if pc_ratio > Decimal("1.2") else
                "bullish" if pc_ratio < Decimal("0.8") else
                "neutral"
            )
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/max-pain/{underlying}")
async def get_max_pain(
    underlying: str,
    expiry: datetime
) -> Dict[str, Any]:
    """Get max pain analysis for options expiry"""
    try:
        result = chain_manager.find_max_pain(underlying, expiry)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="Cannot calculate max pain - insufficient open interest"
            )
        
        max_pain_strike, pain_value = result
        
        return {
            "underlying": underlying,
            "expiry": expiry.isoformat(),
            "max_pain_strike": str(max_pain_strike),
            "total_pain_value": str(pain_value),
            "current_spot": str(
                chain_manager.chains.get(underlying, {})
                .get(expiry, {}).get("spot", "0")
            )
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/amm/start")
async def start_options_amm(
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Start options AMM (admin only)"""
    try:
        # Check admin privileges
        if not user.is_admin:
            raise HTTPException(status_code=403, detail="Admin access required")
        
        await options_amm.start()
        
        return {
            "status": "started",
            "config": {
                "base_spread_bps": str(amm_config.base_spread_bps),
                "max_net_delta": str(amm_config.max_net_delta),
                "hedge_enabled": amm_config.hedge_enabled
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 