"""
Compute Options API endpoints

Options on compute resources with specialized pricing and strategies
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.engines.compute_options_engine import (
    ComputeOptionsEngine,
    ComputeOptionType,
    ExerciseStyle,
    ComputeOption
)

router = APIRouter(
    prefix="/api/v1/compute-options",
    tags=["compute_options"]
)

# Global options engine instance (initialized in main.py)
options_engine: Optional[ComputeOptionsEngine] = None


def set_options_engine(engine: ComputeOptionsEngine):
    """Set the options engine instance from main.py"""
    global options_engine
    options_engine = engine


class CreateOptionRequest(BaseModel):
    """Request to create a compute option"""
    underlying: str = Field(..., description="Underlying compute resource (e.g., GPU_A100)")
    option_type: str = Field(..., pattern="^(call|put|burst|throttle)$")
    exercise_style: str = Field(default="european", pattern="^(european|american|bermudan|asian)$")
    strike_price: str = Field(..., description="Strike price per unit")
    expiry_days: int = Field(..., ge=1, le=365, description="Days until expiry")
    contract_size: str = Field(..., description="Units per contract")
    location: Optional[str] = Field(None, description="Specific location requirement")
    quality_tier: Optional[str] = Field(default="standard", pattern="^(standard|premium|guaranteed)$")


class TradeOptionRequest(BaseModel):
    """Request to trade an option"""
    option_id: str = Field(..., description="Option ID to trade")
    quantity: str = Field(..., description="Number of contracts")
    side: str = Field(..., pattern="^(buy|sell)$")
    order_type: str = Field(default="market", pattern="^(market|limit)$")
    limit_price: Optional[str] = Field(None, description="Limit price for limit orders")


class ExerciseOptionRequest(BaseModel):
    """Request to exercise an option"""
    option_id: str = Field(..., description="Option ID to exercise")
    quantity: str = Field(..., description="Number of contracts to exercise")


class OptionPriceResponse(BaseModel):
    """Option pricing information"""
    option_id: str
    price: str
    spot_price: str
    volatility: str
    time_to_expiry: float
    greeks: Dict[str, str]
    intrinsic_value: str
    time_value: str


class VolatilitySurfaceResponse(BaseModel):
    """Volatility surface data"""
    underlying: str
    timestamp: str
    spot_price: str
    strikes: List[str]
    expiries: List[float]
    implied_vols: List[List[str]]


@router.post("/create")
async def create_option(
    request: CreateOptionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new compute option
    
    Creates standardized or custom options on compute resources
    """
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    # Calculate expiry
    expiry = datetime.utcnow() + timedelta(days=request.expiry_days)
    
    # Create option
    option = await options_engine.create_option(
        underlying=request.underlying,
        option_type=ComputeOptionType(request.option_type),
        exercise_style=ExerciseStyle(request.exercise_style),
        strike_price=Decimal(request.strike_price),
        expiry=expiry,
        contract_size=Decimal(request.contract_size),
        location=request.location,
        quality_tier=request.quality_tier,
        creator=current_user["user_id"]
    )
    
    # Get initial pricing
    price_data = await options_engine.price_option(option)
    
    return {
        "option_id": option.option_id,
        "underlying": option.underlying,
        "option_type": option.option_type.value,
        "strike_price": str(option.strike_price),
        "expiry": option.expiry.isoformat(),
        "contract_size": str(option.contract_size),
        "location": option.location,
        "quality_tier": option.quality_tier,
        "initial_price": price_data["price"],
        "greeks": price_data["greeks"]
    }


@router.get("/option/{option_id}", response_model=OptionPriceResponse)
async def get_option_price(
    option_id: str,
    volatility: Optional[str] = Query(None, description="Override implied volatility")
):
    """Get current pricing for an option"""
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    # Get option
    option = options_engine.options.get(option_id)
    if not option:
        raise HTTPException(status_code=404, detail="Option not found")
    
    # Price option
    vol_override = Decimal(volatility) if volatility else None
    price_data = await options_engine.price_option(option, volatility=vol_override)
    
    return OptionPriceResponse(**price_data)


@router.post("/trade")
async def trade_option(
    request: TradeOptionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Trade a compute option
    
    Buy or sell option contracts at market or limit price
    """
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    # Execute trade
    result = await options_engine.trade_option(
        user_id=current_user["user_id"],
        option_id=request.option_id,
        quantity=Decimal(request.quantity),
        side=request.side,
        order_type=request.order_type,
        limit_price=Decimal(request.limit_price) if request.limit_price else None
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("reason"))
    
    return result


@router.post("/exercise")
async def exercise_option(
    request: ExerciseOptionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Exercise a compute option
    
    Exercise American options early or European options at expiry
    """
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    # Exercise option
    result = await options_engine.exercise_option(
        user_id=current_user["user_id"],
        option_id=request.option_id,
        quantity=Decimal(request.quantity)
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("reason"))
    
    return result


@router.get("/portfolio/risk")
async def get_portfolio_risk(
    current_user: dict = Depends(get_current_user)
):
    """Get portfolio risk metrics including Greeks"""
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    risk_metrics = await options_engine.get_portfolio_risk(current_user["user_id"])
    
    return risk_metrics


@router.get("/positions")
async def get_option_positions(
    current_user: dict = Depends(get_current_user)
):
    """Get user's option positions"""
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    positions = []
    for position_id in options_engine.user_positions.get(current_user["user_id"], []):
        position = options_engine.positions.get(position_id)
        if position and position.quantity != 0:
            # Get current price
            try:
                price_data = await options_engine.price_option(position.option)
                current_price = price_data["price"]
            except:
                current_price = position.entry_price
                
            positions.append({
                "position_id": position.position_id,
                "option": {
                    "option_id": position.option.option_id,
                    "underlying": position.option.underlying,
                    "option_type": position.option.option_type.value,
                    "strike_price": str(position.option.strike_price),
                    "expiry": position.option.expiry.isoformat(),
                    "time_to_expiry": position.option.time_to_expiry
                },
                "quantity": str(position.quantity),
                "entry_price": str(position.entry_price),
                "current_price": str(current_price),
                "unrealized_pnl": str(position.quantity * (Decimal(current_price) - position.entry_price)),
                "realized_pnl": str(position.realized_pnl),
                "notional_value": str(position.notional_value)
            })
    
    return {"positions": positions}


@router.get("/chain/{underlying}")
async def get_option_chain(
    underlying: str,
    expiry_days: Optional[int] = Query(None, description="Filter by days to expiry"),
    option_type: Optional[str] = Query(None, pattern="^(call|put)$")
):
    """
    Get option chain for an underlying
    
    Returns all available options for a compute resource
    """
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    chain = []
    
    for option in options_engine.options.values():
        if option.underlying != underlying:
            continue
            
        if option.is_expired:
            continue
            
        if expiry_days and option.time_to_expiry * 365.25 > expiry_days + 1:
            continue
            
        if option_type and option.option_type.value != option_type:
            continue
            
        # Get pricing
        try:
            price_data = await options_engine.price_option(option)
            
            chain.append({
                "option_id": option.option_id,
                "option_type": option.option_type.value,
                "strike_price": str(option.strike_price),
                "expiry": option.expiry.isoformat(),
                "time_to_expiry": option.time_to_expiry,
                "contract_size": str(option.contract_size),
                "price": price_data["price"],
                "volume": "0",  # TODO: Track volume
                "open_interest": str(options_engine.mm_inventory.get(option.option_id, 0)),
                "greeks": price_data["greeks"]
            })
        except Exception as e:
            logger.error(f"Error pricing option {option.option_id}: {e}")
            
    # Sort by strike and expiry
    chain.sort(key=lambda x: (x["expiry"], float(x["strike_price"])))
    
    return {
        "underlying": underlying,
        "chain": chain,
        "spot_price": (await options_engine.spot_market.get_spot_price(underlying)).get("last_trade_price")
    }


@router.get("/volatility/surface/{underlying}", response_model=VolatilitySurfaceResponse)
async def get_volatility_surface(
    underlying: str
):
    """Get implied volatility surface for an underlying"""
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    # Get or create surface
    surface = options_engine.vol_surfaces.get(underlying)
    if not surface:
        surface = await options_engine.create_volatility_surface(underlying)
    
    # Convert to response format
    iv_matrix = []
    for expiry in surface.expiries:
        row = []
        for strike in surface.strikes:
            iv = surface.ivs.get((strike, expiry), Decimal("0.5"))
            row.append(str(iv))
        iv_matrix.append(row)
    
    return VolatilitySurfaceResponse(
        underlying=underlying,
        timestamp=surface.timestamp.isoformat(),
        spot_price=str(surface.spot_price),
        strikes=[str(s) for s in surface.strikes],
        expiries=surface.expiries,
        implied_vols=iv_matrix
    )


@router.post("/strategies/spread")
async def create_option_spread(
    underlying: str,
    strategy: str = Query(..., pattern="^(bull_call|bear_put|straddle|strangle|iron_condor|butterfly)$"),
    expiry_days: int = Query(..., ge=1, le=365),
    width: Optional[str] = Query(None, description="Strike width for spreads"),
    current_user: dict = Depends(get_current_user)
):
    """
    Create common option spread strategies
    
    Automatically creates the required options and executes trades
    """
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    # Get spot price
    spot_data = await options_engine.spot_market.get_spot_price(underlying)
    spot_price = Decimal(spot_data.get("last_trade_price", "0"))
    
    if spot_price == 0:
        raise HTTPException(status_code=400, detail="No spot price available")
    
    expiry = datetime.utcnow() + timedelta(days=expiry_days)
    trades = []
    
    if strategy == "bull_call":
        # Buy call at lower strike, sell call at higher strike
        strike_width = Decimal(width) if width else spot_price * Decimal("0.1")
        lower_strike = spot_price
        upper_strike = spot_price + strike_width
        
        # Create options if needed
        lower_call = await options_engine.create_option(
            underlying, ComputeOptionType.CALL, ExerciseStyle.EUROPEAN,
            lower_strike, expiry, Decimal("1")
        )
        upper_call = await options_engine.create_option(
            underlying, ComputeOptionType.CALL, ExerciseStyle.EUROPEAN,
            upper_strike, expiry, Decimal("1")
        )
        
        # Execute trades
        buy_trade = await options_engine.trade_option(
            current_user["user_id"], lower_call.option_id, Decimal("1"), "buy"
        )
        sell_trade = await options_engine.trade_option(
            current_user["user_id"], upper_call.option_id, Decimal("1"), "sell"
        )
        
        trades = [buy_trade, sell_trade]
        
    elif strategy == "straddle":
        # Buy call and put at same strike (ATM)
        strike = spot_price
        
        # Create options
        call_option = await options_engine.create_option(
            underlying, ComputeOptionType.CALL, ExerciseStyle.EUROPEAN,
            strike, expiry, Decimal("1")
        )
        put_option = await options_engine.create_option(
            underlying, ComputeOptionType.PUT, ExerciseStyle.EUROPEAN,
            strike, expiry, Decimal("1")
        )
        
        # Buy both
        call_trade = await options_engine.trade_option(
            current_user["user_id"], call_option.option_id, Decimal("1"), "buy"
        )
        put_trade = await options_engine.trade_option(
            current_user["user_id"], put_option.option_id, Decimal("1"), "buy"
        )
        
        trades = [call_trade, put_trade]
        
    # Add more strategies as needed
    
    return {
        "strategy": strategy,
        "underlying": underlying,
        "spot_price": str(spot_price),
        "expiry": expiry.isoformat(),
        "trades": trades,
        "max_profit": "Unlimited" if strategy in ["straddle", "strangle"] else "Limited",
        "max_loss": "Limited",
        "breakeven": "Calculated based on premiums"
    }


@router.get("/analytics/put-call-ratio/{underlying}")
async def get_put_call_ratio(
    underlying: str,
    expiry_days: Optional[int] = Query(None, description="Filter by days to expiry")
):
    """Get put/call ratio for sentiment analysis"""
    if not options_engine:
        raise HTTPException(status_code=503, detail="Options engine not available")
    
    put_volume = 0
    call_volume = 0
    put_oi = Decimal("0")
    call_oi = Decimal("0")
    
    for option in options_engine.options.values():
        if option.underlying != underlying:
            continue
            
        if option.is_expired:
            continue
            
        if expiry_days and option.time_to_expiry * 365.25 > expiry_days + 1:
            continue
            
        # Get open interest from inventory
        oi = abs(options_engine.mm_inventory.get(option.option_id, Decimal("0")))
        
        if option.option_type == ComputeOptionType.PUT:
            put_oi += oi
            # TODO: Track actual volume
        else:
            call_oi += oi
            
    pcr = put_oi / call_oi if call_oi > 0 else Decimal("0")
    
    return {
        "underlying": underlying,
        "put_call_ratio": str(pcr),
        "put_open_interest": str(put_oi),
        "call_open_interest": str(call_oi),
        "sentiment": "bearish" if pcr > 1.2 else "bullish" if pcr < 0.8 else "neutral",
        "timestamp": datetime.utcnow().isoformat()
    }


# Import logger
import logging
logger = logging.getLogger(__name__) 