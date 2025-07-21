"""Options strategies API endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.models.options import OptionStrategy
from app.core.events import OptionsEventPublisher

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


# Request Models

class CreateStrategyRequest(BaseModel):
    """Request to create an option strategy."""
    strategy_type: str = Field(
        ...,
        pattern="^(bull_call_spread|bear_put_spread|straddle|strangle|iron_condor|butterfly|collar|calendar_spread)$",
        description="Type of option strategy"
    )
    underlying_asset: str = Field(..., description="Underlying asset")
    
    # Common parameters
    expiry_date: datetime = Field(..., description="Expiration date for the strategy")
    quantity: Decimal = Field(default=Decimal("1"), gt=0, description="Number of strategy units")
    
    # Strategy-specific parameters
    strikes: List[Decimal] = Field(..., description="Strike prices (number depends on strategy)")
    expiries: Optional[List[datetime]] = Field(None, description="Multiple expiries for calendar spreads")
    
    # Risk parameters
    max_loss: Optional[Decimal] = Field(None, description="Maximum acceptable loss")
    target_profit: Optional[Decimal] = Field(None, description="Target profit")


class AnalyzeStrategyRequest(BaseModel):
    """Request to analyze a strategy."""
    strategy_id: str = Field(..., description="Strategy ID to analyze")
    spot_price: Decimal = Field(..., gt=0, description="Current spot price")
    volatility: Decimal = Field(..., gt=0, le=5, description="Implied volatility")
    days_to_expiry: Optional[int] = Field(None, ge=0, description="Override days to expiry")


# API Endpoints

@router.post("/create")
async def create_strategy(
    request: CreateStrategyRequest,
    user_id: str = Depends(lambda: "mock_user"),
    event_publisher: OptionsEventPublisher = Depends(lambda: None)
) -> Dict[str, Any]:
    """Create a predefined option strategy."""
    try:
        strategy_id = f"STRAT-{datetime.utcnow().timestamp()}"
        
        # Validate strikes based on strategy type
        legs = []
        
        if request.strategy_type == "bull_call_spread":
            if len(request.strikes) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="Bull call spread requires exactly 2 strikes"
                )
            
            # Buy lower strike call, sell higher strike call
            legs = [
                {
                    "option_type": "call",
                    "strike": str(request.strikes[0]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                },
                {
                    "option_type": "call",
                    "strike": str(request.strikes[1]),
                    "side": "sell",
                    "quantity": str(request.quantity)
                }
            ]
            
            max_profit = (request.strikes[1] - request.strikes[0]) * request.quantity
            max_loss = Decimal("0")  # Will calculate based on premiums
            breakeven = [request.strikes[0]]  # Plus net premium paid
            
        elif request.strategy_type == "bear_put_spread":
            if len(request.strikes) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="Bear put spread requires exactly 2 strikes"
                )
            
            # Buy higher strike put, sell lower strike put
            legs = [
                {
                    "option_type": "put",
                    "strike": str(request.strikes[1]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                },
                {
                    "option_type": "put",
                    "strike": str(request.strikes[0]),
                    "side": "sell",
                    "quantity": str(request.quantity)
                }
            ]
            
            max_profit = (request.strikes[1] - request.strikes[0]) * request.quantity
            max_loss = Decimal("0")  # Will calculate based on premiums
            breakeven = [request.strikes[1]]  # Minus net premium paid
            
        elif request.strategy_type == "straddle":
            if len(request.strikes) != 1:
                raise HTTPException(
                    status_code=400,
                    detail="Straddle requires exactly 1 strike"
                )
            
            # Buy call and put at same strike
            legs = [
                {
                    "option_type": "call",
                    "strike": str(request.strikes[0]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                },
                {
                    "option_type": "put",
                    "strike": str(request.strikes[0]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                }
            ]
            
            max_profit = None  # Unlimited
            max_loss = Decimal("0")  # Total premium paid
            breakeven = [request.strikes[0], request.strikes[0]]  # Two breakevens
            
        elif request.strategy_type == "strangle":
            if len(request.strikes) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="Strangle requires exactly 2 strikes"
                )
            
            # Buy OTM call and OTM put
            legs = [
                {
                    "option_type": "put",
                    "strike": str(request.strikes[0]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                },
                {
                    "option_type": "call",
                    "strike": str(request.strikes[1]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                }
            ]
            
            max_profit = None  # Unlimited
            max_loss = Decimal("0")  # Total premium paid
            breakeven = [request.strikes[0], request.strikes[1]]
            
        elif request.strategy_type == "iron_condor":
            if len(request.strikes) != 4:
                raise HTTPException(
                    status_code=400,
                    detail="Iron condor requires exactly 4 strikes"
                )
            
            # Sell OTM put spread and OTM call spread
            strikes_sorted = sorted(request.strikes)
            legs = [
                # Put spread (bull put spread)
                {
                    "option_type": "put",
                    "strike": str(strikes_sorted[0]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                },
                {
                    "option_type": "put",
                    "strike": str(strikes_sorted[1]),
                    "side": "sell",
                    "quantity": str(request.quantity)
                },
                # Call spread (bear call spread)
                {
                    "option_type": "call",
                    "strike": str(strikes_sorted[2]),
                    "side": "sell",
                    "quantity": str(request.quantity)
                },
                {
                    "option_type": "call",
                    "strike": str(strikes_sorted[3]),
                    "side": "buy",
                    "quantity": str(request.quantity)
                }
            ]
            
            max_profit = Decimal("0")  # Net credit received
            max_loss = min(
                strikes_sorted[1] - strikes_sorted[0],
                strikes_sorted[3] - strikes_sorted[2]
            ) * request.quantity
            breakeven = [strikes_sorted[1], strikes_sorted[2]]
            
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Strategy type {request.strategy_type} not implemented"
            )
        
        # Create strategy object
        strategy = OptionStrategy(
            strategy_id=strategy_id,
            name=request.strategy_type.replace("_", " ").title(),
            description=f"{request.strategy_type} strategy on {request.underlying_asset}",
            legs=legs,
            max_profit=max_profit,
            max_loss=max_loss,
            breakeven_points=breakeven,
            required_margin=Decimal("0")  # Would calculate based on broker rules
        )
        
        # In production, would:
        # 1. Calculate exact premiums for each leg
        # 2. Determine net cost/credit
        # 3. Calculate exact breakeven points
        # 4. Store strategy in database
        # 5. Execute orders if requested
        
        return {
            "strategy_id": strategy.strategy_id,
            "strategy_type": request.strategy_type,
            "underlying_asset": request.underlying_asset,
            "expiry_date": request.expiry_date.isoformat(),
            "legs": strategy.legs,
            "max_profit": str(strategy.max_profit) if strategy.max_profit else "unlimited",
            "max_loss": str(strategy.max_loss) if strategy.max_loss else "unlimited",
            "breakeven_points": [str(bp) for bp in strategy.breakeven_points],
            "required_margin": str(strategy.required_margin),
            "created_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates")
async def get_strategy_templates() -> List[Dict[str, Any]]:
    """Get available strategy templates."""
    templates = [
        {
            "strategy_type": "bull_call_spread",
            "name": "Bull Call Spread",
            "description": "Profit from moderate upward movement",
            "market_outlook": "Moderately bullish",
            "legs": 2,
            "risk": "Limited",
            "reward": "Limited",
            "required_strikes": 2
        },
        {
            "strategy_type": "bear_put_spread",
            "name": "Bear Put Spread",
            "description": "Profit from moderate downward movement",
            "market_outlook": "Moderately bearish",
            "legs": 2,
            "risk": "Limited",
            "reward": "Limited",
            "required_strikes": 2
        },
        {
            "strategy_type": "straddle",
            "name": "Long Straddle",
            "description": "Profit from large movement in either direction",
            "market_outlook": "High volatility expected",
            "legs": 2,
            "risk": "Limited",
            "reward": "Unlimited",
            "required_strikes": 1
        },
        {
            "strategy_type": "strangle",
            "name": "Long Strangle",
            "description": "Profit from large movement with lower cost than straddle",
            "market_outlook": "High volatility expected",
            "legs": 2,
            "risk": "Limited",
            "reward": "Unlimited",
            "required_strikes": 2
        },
        {
            "strategy_type": "iron_condor",
            "name": "Iron Condor",
            "description": "Profit from low volatility and range-bound movement",
            "market_outlook": "Neutral with low volatility",
            "legs": 4,
            "risk": "Limited",
            "reward": "Limited",
            "required_strikes": 4
        },
        {
            "strategy_type": "butterfly",
            "name": "Long Butterfly",
            "description": "Profit from minimal movement around a target price",
            "market_outlook": "Neutral with very low volatility",
            "legs": 4,
            "risk": "Limited",
            "reward": "Limited",
            "required_strikes": 3
        }
    ]
    
    return templates


@router.post("/analyze")
async def analyze_strategy(
    request: AnalyzeStrategyRequest
) -> Dict[str, Any]:
    """Analyze profit/loss for a strategy at different prices."""
    try:
        # In production, would:
        # 1. Load strategy from database
        # 2. Calculate current option prices
        # 3. Generate P&L chart data
        # 4. Calculate Greeks for the strategy
        
        # Mock analysis
        spot_prices = []
        pnl_values = []
        
        # Generate P&L curve
        base_spot = float(request.spot_price)
        for i in range(-20, 21):
            price = base_spot * (1 + i * 0.02)  # ±40% range
            spot_prices.append(price)
            
            # Mock P&L calculation (would be based on actual strategy)
            if i < -10:
                pnl = -1000  # Max loss
            elif i > 10:
                pnl = 2000  # Max profit
            else:
                pnl = i * 100  # Linear for simplicity
                
            pnl_values.append(pnl)
        
        return {
            "strategy_id": request.strategy_id,
            "current_spot": str(request.spot_price),
            "analysis": {
                "current_pnl": "150",
                "pnl_at_expiry": {
                    "spot_prices": spot_prices,
                    "pnl_values": pnl_values
                },
                "greeks": {
                    "delta": "0.35",
                    "gamma": "0.02",
                    "theta": "-15.50",
                    "vega": "25.30"
                },
                "probability_of_profit": "0.65",
                "expected_value": "250"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{strategy_id}")
async def get_strategy(
    strategy_id: str,
    user_id: str = Depends(lambda: "mock_user")
) -> Dict[str, Any]:
    """Get details of a specific strategy."""
    try:
        # In production, would fetch from database
        # Mock response
        return {
            "strategy_id": strategy_id,
            "strategy_type": "bull_call_spread",
            "underlying_asset": "BTC",
            "created_at": datetime.utcnow().isoformat(),
            "legs": [
                {
                    "option_type": "call",
                    "strike": "45000",
                    "side": "buy",
                    "quantity": "1",
                    "premium": "2500"
                },
                {
                    "option_type": "call",
                    "strike": "50000",
                    "side": "sell",
                    "quantity": "1",
                    "premium": "1000"
                }
            ],
            "net_debit": "1500",
            "max_profit": "3500",
            "max_loss": "1500",
            "breakeven_points": ["46500"],
            "current_pnl": "-200",
            "status": "active"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/")
async def list_strategies(
    user_id: str = Depends(lambda: "mock_user"),
    active_only: bool = True
) -> List[Dict[str, Any]]:
    """List user's option strategies."""
    try:
        # In production, would fetch from database
        # Mock response
        strategies = [
            {
                "strategy_id": "STRAT-001",
                "strategy_type": "iron_condor",
                "underlying_asset": "SPX",
                "created_at": datetime.utcnow().isoformat(),
                "expiry_date": "2024-02-16",
                "net_credit": "250",
                "current_pnl": "150",
                "status": "active"
            },
            {
                "strategy_id": "STRAT-002",
                "strategy_type": "straddle",
                "underlying_asset": "TSLA",
                "created_at": datetime.utcnow().isoformat(),
                "expiry_date": "2024-01-19",
                "net_debit": "5000",
                "current_pnl": "-500",
                "status": "active"
            }
        ]
        
        if active_only:
            strategies = [s for s in strategies if s["status"] == "active"]
        
        return strategies
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 