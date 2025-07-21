"""Options trading API endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from platformq_derivatives_common import (
    BlackScholesEngine,
    BinomialEngine,
    MonteCarloEngine,
    GreeksCalculator,
    VolatilitySurfaceEngine,
    OptionType,
    OptionStyle
)
from app.core.cache import OptionsCacheManager
from app.core.events import OptionsEventPublisher
from app.core.volatility_surface import VolatilitySurfaceEngine as LocalVolEngine
from app.models.options import (
    OptionContract,
    OptionOrder,
    OptionPosition,
    OptionPricing,
    Greeks as GreeksModel
)
from app.config import settings

router = APIRouter(prefix="/api/v1/options", tags=["options"])

# Initialize engines
black_scholes_engine = BlackScholesEngine()
binomial_engine = BinomialEngine()
monte_carlo_engine = MonteCarloEngine()
greeks_calculator = GreeksCalculator()
vol_surface_engine = VolatilitySurfaceEngine()

# Dependencies
async def get_cache_manager():
    """Get cache manager instance."""
    # In production, would be properly initialized
    return OptionsCacheManager(settings)

async def get_event_publisher():
    """Get event publisher instance."""
    # In production, would be properly initialized
    return OptionsEventPublisher(settings)


# Request Models

class PriceOptionRequest(BaseModel):
    """Request to price an option."""
    underlying_asset: str = Field(..., description="Underlying asset symbol")
    strike_price: Decimal = Field(..., gt=0, description="Strike price")
    expiry_date: datetime = Field(..., description="Expiration date")
    option_type: str = Field(..., pattern="^(call|put)$", description="Option type")
    option_style: str = Field(default="european", pattern="^(european|american|bermudan)$")
    pricing_model: str = Field(default="black_scholes", pattern="^(black_scholes|binomial|monte_carlo)$")
    
    # Market data (optional - will fetch if not provided)
    spot_price: Optional[Decimal] = Field(None, gt=0, description="Current spot price")
    volatility: Optional[Decimal] = Field(None, gt=0, le=5, description="Implied volatility")
    risk_free_rate: Optional[Decimal] = Field(default=Decimal("0.05"), description="Risk-free rate")
    dividend_yield: Optional[Decimal] = Field(default=Decimal("0"), description="Dividend yield")


class CalculateGreeksRequest(BaseModel):
    """Request to calculate Greeks."""
    underlying_asset: str
    strike_price: Decimal = Field(..., gt=0)
    expiry_date: datetime
    option_type: str = Field(..., pattern="^(call|put)$")
    
    # Market data
    spot_price: Decimal = Field(..., gt=0)
    volatility: Decimal = Field(..., gt=0, le=5)
    risk_free_rate: Decimal = Field(default=Decimal("0.05"))
    dividend_yield: Decimal = Field(default=Decimal("0"))
    
    # Options
    calculate_second_order: bool = Field(default=False, description="Calculate second-order Greeks")


class ImpliedVolatilityRequest(BaseModel):
    """Request to calculate implied volatility."""
    option_price: Decimal = Field(..., gt=0, description="Observed option price")
    underlying_asset: str
    strike_price: Decimal = Field(..., gt=0)
    expiry_date: datetime
    option_type: str = Field(..., pattern="^(call|put)$")
    
    # Market data
    spot_price: Decimal = Field(..., gt=0)
    risk_free_rate: Decimal = Field(default=Decimal("0.05"))
    dividend_yield: Decimal = Field(default=Decimal("0"))


class CreateOptionChainRequest(BaseModel):
    """Request to create an option chain."""
    underlying_asset: str
    expiry_date: datetime
    strikes: Optional[List[Decimal]] = Field(None, description="Specific strikes, or auto-generate")
    strike_interval: Optional[Decimal] = Field(None, gt=0, description="Strike interval for auto-generation")
    num_strikes_above: int = Field(default=10, ge=1, le=50)
    num_strikes_below: int = Field(default=10, ge=1, le=50)


class PlaceOptionOrderRequest(BaseModel):
    """Request to place an option order."""
    symbol: str = Field(..., description="Option symbol")
    side: str = Field(..., pattern="^(buy|sell)$")
    size: Decimal = Field(..., gt=0)
    order_type: str = Field(default="limit", pattern="^(market|limit)$")
    price: Optional[Decimal] = Field(None, gt=0, description="Limit price")
    time_in_force: str = Field(default="GTC", pattern="^(GTC|IOC|FOK|GTD)$")
    reduce_only: bool = Field(default=False)


# API Endpoints

@router.post("/price")
async def price_option(
    request: PriceOptionRequest,
    cache_manager: OptionsCacheManager = Depends(get_cache_manager)
) -> Dict[str, Any]:
    """Calculate option price using specified model."""
    try:
        # Convert types
        option_type = OptionType.CALL if request.option_type == "call" else OptionType.PUT
        option_style = OptionStyle[request.option_style.upper()]
        
        # Get market data if not provided
        if request.spot_price is None:
            # In production, would fetch from oracle service
            request.spot_price = Decimal("100")  # Mock
            
        if request.volatility is None:
            # Get from volatility surface
            request.volatility = vol_surface_engine.interpolate_volatility(
                underlying_asset=request.underlying_asset,
                strike=request.strike_price,
                expiry=request.expiry_date,
                spot_price=request.spot_price
            )
            if request.volatility is None:
                request.volatility = Decimal("0.3")  # Default 30% vol
        
        # Calculate time to expiry
        time_to_expiry = (request.expiry_date - datetime.utcnow()).total_seconds() / (365 * 24 * 3600)
        if time_to_expiry <= 0:
            raise HTTPException(status_code=400, detail="Option has expired")
        
        time_to_expiry_decimal = Decimal(str(time_to_expiry))
        
        # Price based on model
        if request.pricing_model == "black_scholes":
            if option_style != OptionStyle.EUROPEAN:
                raise HTTPException(
                    status_code=400,
                    detail="Black-Scholes only supports European options"
                )
            
            price = black_scholes_engine.calculate_price(
                spot=request.spot_price,
                strike=request.strike_price,
                time_to_expiry=time_to_expiry_decimal,
                volatility=request.volatility,
                risk_free_rate=request.risk_free_rate,
                dividend_yield=request.dividend_yield,
                option_type=option_type
            )
            
        elif request.pricing_model == "binomial":
            price = binomial_engine.calculate_price(
                spot=request.spot_price,
                strike=request.strike_price,
                time_to_expiry=time_to_expiry_decimal,
                volatility=request.volatility,
                risk_free_rate=request.risk_free_rate,
                dividend_yield=request.dividend_yield,
                option_type=option_type,
                option_style=option_style
            )
            
        elif request.pricing_model == "monte_carlo":
            price, std_error = monte_carlo_engine.calculate_price(
                spot=request.spot_price,
                strike=request.strike_price,
                time_to_expiry=time_to_expiry_decimal,
                volatility=request.volatility,
                risk_free_rate=request.risk_free_rate,
                dividend_yield=request.dividend_yield,
                option_type=option_type,
                option_style=option_style
            )
        else:
            raise HTTPException(status_code=400, detail="Invalid pricing model")
        
        # Calculate Greeks
        greeks = greeks_calculator.calculate_black_scholes_greeks(
            spot=request.spot_price,
            strike=request.strike_price,
            time_to_expiry=time_to_expiry_decimal,
            volatility=request.volatility,
            risk_free_rate=request.risk_free_rate,
            dividend_yield=request.dividend_yield,
            option_type=option_type
        )
        
        response = {
            "underlying_asset": request.underlying_asset,
            "strike_price": str(request.strike_price),
            "expiry_date": request.expiry_date.isoformat(),
            "option_type": request.option_type,
            "option_style": request.option_style,
            "pricing_model": request.pricing_model,
            "price": str(price),
            "spot_price": str(request.spot_price),
            "volatility": str(request.volatility),
            "time_to_expiry": str(time_to_expiry_decimal),
            "greeks": {
                "delta": str(greeks.delta),
                "gamma": str(greeks.gamma),
                "theta": str(greeks.theta),
                "vega": str(greeks.vega),
                "rho": str(greeks.rho)
            }
        }
        
        if request.pricing_model == "monte_carlo":
            response["standard_error"] = str(std_error)
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/greeks")
async def calculate_greeks(
    request: CalculateGreeksRequest
) -> Dict[str, Any]:
    """Calculate option Greeks."""
    try:
        # Convert types
        option_type = OptionType.CALL if request.option_type == "call" else OptionType.PUT
        
        # Calculate time to expiry
        time_to_expiry = (request.expiry_date - datetime.utcnow()).total_seconds() / (365 * 24 * 3600)
        if time_to_expiry <= 0:
            raise HTTPException(status_code=400, detail="Option has expired")
        
        time_to_expiry_decimal = Decimal(str(time_to_expiry))
        
        # Calculate Greeks
        greeks = greeks_calculator.calculate_black_scholes_greeks(
            spot=request.spot_price,
            strike=request.strike_price,
            time_to_expiry=time_to_expiry_decimal,
            volatility=request.volatility,
            risk_free_rate=request.risk_free_rate,
            dividend_yield=request.dividend_yield,
            option_type=option_type,
            calculate_second_order=request.calculate_second_order
        )
        
        result = {
            "underlying_asset": request.underlying_asset,
            "strike_price": str(request.strike_price),
            "expiry_date": request.expiry_date.isoformat(),
            "option_type": request.option_type,
            "spot_price": str(request.spot_price),
            "volatility": str(request.volatility),
            "greeks": {
                "delta": str(greeks.delta),
                "gamma": str(greeks.gamma),
                "theta": str(greeks.theta),
                "vega": str(greeks.vega),
                "rho": str(greeks.rho),
                "lambda": str(greeks.lambda_) if greeks.lambda_ else None
            }
        }
        
        if request.calculate_second_order:
            result["greeks"].update({
                "vanna": str(greeks.vanna) if greeks.vanna else None,
                "charm": str(greeks.charm) if greeks.charm else None,
                "vomma": str(greeks.vomma) if greeks.vomma else None,
                "speed": str(greeks.speed) if greeks.speed else None
            })
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/implied-volatility")
async def calculate_implied_volatility(
    request: ImpliedVolatilityRequest
) -> Dict[str, Any]:
    """Calculate implied volatility from option price."""
    try:
        # Convert types
        option_type = OptionType.CALL if request.option_type == "call" else OptionType.PUT
        
        # Calculate time to expiry
        time_to_expiry = (request.expiry_date - datetime.utcnow()).total_seconds() / (365 * 24 * 3600)
        if time_to_expiry <= 0:
            raise HTTPException(status_code=400, detail="Option has expired")
        
        time_to_expiry_decimal = Decimal(str(time_to_expiry))
        
        # Calculate implied volatility
        iv = black_scholes_engine.calculate_implied_volatility(
            option_price=request.option_price,
            spot=request.spot_price,
            strike=request.strike_price,
            time_to_expiry=time_to_expiry_decimal,
            risk_free_rate=request.risk_free_rate,
            dividend_yield=request.dividend_yield,
            option_type=option_type
        )
        
        if iv is None:
            raise HTTPException(
                status_code=400,
                detail="Could not calculate implied volatility"
            )
        
        return {
            "underlying_asset": request.underlying_asset,
            "strike_price": str(request.strike_price),
            "expiry_date": request.expiry_date.isoformat(),
            "option_type": request.option_type,
            "option_price": str(request.option_price),
            "spot_price": str(request.spot_price),
            "implied_volatility": str(iv),
            "implied_volatility_percent": str(iv * 100)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chain")
async def create_option_chain(
    request: CreateOptionChainRequest,
    event_publisher: OptionsEventPublisher = Depends(get_event_publisher)
) -> Dict[str, Any]:
    """Create or get an option chain."""
    try:
        # Get current spot price
        # In production, would fetch from oracle
        spot_price = Decimal("100")  # Mock
        
        # Generate strikes if not provided
        if request.strikes is None:
            strikes = []
            
            if request.strike_interval:
                # Generate strikes around spot
                for i in range(request.num_strikes_below):
                    strike = spot_price - (i + 1) * request.strike_interval
                    if strike > 0:
                        strikes.insert(0, strike)
                
                strikes.append(spot_price)  # ATM strike
                
                for i in range(request.num_strikes_above):
                    strike = spot_price + (i + 1) * request.strike_interval
                    strikes.append(strike)
            else:
                # Default strike generation (5% intervals)
                interval = spot_price * Decimal("0.05")
                for i in range(request.num_strikes_below):
                    strike = spot_price - (i + 1) * interval
                    if strike > 0:
                        strikes.insert(0, strike)
                
                strikes.append(spot_price)
                
                for i in range(request.num_strikes_above):
                    strike = spot_price + (i + 1) * interval
                    strikes.append(strike)
        else:
            strikes = sorted(request.strikes)
        
        # Create option contracts for each strike
        calls = []
        puts = []
        
        for strike in strikes:
            # Call option
            call_symbol = f"{request.underlying_asset}-{strike}-{request.expiry_date.strftime('%Y%m%d')}-C"
            call_contract = {
                "symbol": call_symbol,
                "underlying_asset": request.underlying_asset,
                "strike_price": str(strike),
                "expiry_date": request.expiry_date.isoformat(),
                "option_type": "call",
                "contract_size": "1"
            }
            calls.append(call_contract)
            
            # Put option
            put_symbol = f"{request.underlying_asset}-{strike}-{request.expiry_date.strftime('%Y%m%d')}-P"
            put_contract = {
                "symbol": put_symbol,
                "underlying_asset": request.underlying_asset,
                "strike_price": str(strike),
                "expiry_date": request.expiry_date.isoformat(),
                "option_type": "put",
                "contract_size": "1"
            }
            puts.append(put_contract)
        
        # Publish chain creation event
        await event_publisher.publish_option_chain_created(
            underlying_asset=request.underlying_asset,
            expiry_date=request.expiry_date,
            strikes=strikes
        )
        
        return {
            "underlying_asset": request.underlying_asset,
            "spot_price": str(spot_price),
            "expiry_date": request.expiry_date.isoformat(),
            "strikes": [str(s) for s in strikes],
            "calls": calls,
            "puts": puts,
            "total_contracts": len(calls) + len(puts)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/order")
async def place_option_order(
    request: PlaceOptionOrderRequest,
    user_id: str = Depends(lambda: "mock_user"),
    event_publisher: OptionsEventPublisher = Depends(get_event_publisher)
) -> Dict[str, Any]:
    """Place an option order."""
    try:
        # Create order
        order = OptionOrder(
            order_id=f"OPT-{datetime.utcnow().timestamp()}",
            user_id=user_id,
            symbol=request.symbol,
            side=request.side,
            size=request.size,
            price=request.price,
            order_type=request.order_type,
            time_in_force=request.time_in_force,
            status="pending"
        )
        
        # In production, would:
        # 1. Validate order
        # 2. Check risk limits
        # 3. Submit to matching engine
        # 4. Handle margin requirements
        
        # Mock execution for market orders
        if request.order_type == "market":
            order.status = "filled"
            order.filled_size = order.size
            order.average_price = request.price or Decimal("1.0")  # Mock price
        
        # Publish order event
        await event_publisher.publish_order_placed(order)
        
        return {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side,
            "size": str(order.size),
            "order_type": order.order_type,
            "status": order.status,
            "filled_size": str(order.filled_size),
            "average_price": str(order.average_price) if order.average_price else None,
            "created_at": order.created_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions")
async def get_option_positions(
    user_id: str = Depends(lambda: "mock_user"),
    cache_manager: OptionsCacheManager = Depends(get_cache_manager)
) -> List[Dict[str, Any]]:
    """Get user's option positions."""
    try:
        # In production, would fetch from database
        # Mock response
        positions = [
            {
                "position_id": "POS-001",
                "symbol": "BTC-50000-20240331-C",
                "option_type": "call",
                "size": "10",
                "entry_price": "2500",
                "mark_price": "2800",
                "unrealized_pnl": "3000",
                "realized_pnl": "0",
                "created_at": datetime.utcnow().isoformat()
            }
        ]
        
        return positions
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/volatility/surface/{underlying}")
async def get_volatility_surface(
    underlying: str,
    cache_manager: OptionsCacheManager = Depends(get_cache_manager)
) -> Dict[str, Any]:
    """Get volatility surface for an underlying asset."""
    try:
        surface = vol_surface_engine.get_surface(underlying)
        
        if not surface:
            # Build a mock surface
            spot_price = Decimal("100")
            options_data = []
            
            # Generate mock option data
            expiries = ["2024-01-31", "2024-02-29", "2024-03-31"]
            strikes = [80, 90, 100, 110, 120]
            
            for expiry in expiries:
                for strike in strikes:
                    # Simple vol smile
                    moneyness = strike / 100
                    base_vol = 0.3
                    skew = 0.1 * (1 - moneyness)
                    iv = base_vol + skew
                    
                    options_data.append({
                        "strike": strike,
                        "expiry": expiry,
                        "implied_vol": iv,
                        "option_type": "call"
                    })
            
            surface = vol_surface_engine.build_surface(
                underlying_asset=underlying,
                spot_price=spot_price,
                options_data=options_data
            )
        
        return {
            "underlying_asset": surface.underlying_asset,
            "spot_price": str(surface.spot_price),
            "at_the_money_vol": str(surface.at_the_money_vol),
            "skew": surface.skew,
            "term_structure": surface.term_structure,
            "updated_at": surface.updated_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/put-call-ratio/{underlying}")
async def get_put_call_ratio(
    underlying: str,
    period: str = Query(default="24h", pattern="^(1h|24h|7d|30d)$")
) -> Dict[str, Any]:
    """Get put/call ratio analytics."""
    try:
        # In production, would calculate from actual trading data
        # Mock response
        return {
            "underlying_asset": underlying,
            "period": period,
            "put_call_ratio": "0.85",
            "put_volume": "125000",
            "call_volume": "147000",
            "put_open_interest": "450000",
            "call_open_interest": "520000",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 