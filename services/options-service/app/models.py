"""Options service models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List
from pydantic import BaseModel, Field


class OptionContract(BaseModel):
    """Option contract specification."""
    underlying: str
    strike: Decimal
    expiry: datetime
    option_type: str = "call"  # call or put
    exercise_style: str = "european"  # european or american
    quote_currency: str = "USD"
    contract_size: Decimal = Decimal("1")
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class OptionOrder(BaseModel):
    """Option order request."""
    user_id: str
    market_id: str
    order_type: str  # market, limit
    side: str  # buy, sell
    contracts: Decimal = Field(..., gt=0)
    premium: Optional[Decimal] = None
    option_side: str  # buy_to_open, sell_to_open, buy_to_close, sell_to_close
    time_in_force: str = "good_till_cancelled"
    
    class Config:
        json_encoders = {
            Decimal: str
        }


class Greeks(BaseModel):
    """Option Greeks."""
    delta: Decimal
    gamma: Decimal
    theta: Decimal
    vega: Decimal
    rho: Decimal
    implied_volatility: Decimal
    
    class Config:
        json_encoders = {
            Decimal: str
        }


class StrategyLeg(BaseModel):
    """Single leg of an option strategy."""
    market_id: str
    side: str  # buy, sell
    contracts: Decimal
    option_side: str
    premium: Optional[Decimal] = None
    
    class Config:
        json_encoders = {
            Decimal: str
        }


class OptionStrategy(BaseModel):
    """Multi-leg option strategy."""
    legs: List[StrategyLeg]
    max_profit: Optional[Decimal] = None
    max_loss: Optional[Decimal] = None
    breakeven_points: List[Decimal] = Field(default_factory=list)
    
    class Config:
        json_encoders = {
            Decimal: str
        } 