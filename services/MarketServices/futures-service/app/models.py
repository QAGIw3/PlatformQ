"""Futures service models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


class FuturesContract(BaseModel):
    """Futures contract specification."""
    symbol: str
    underlying: str
    quote_currency: str = "USD"
    contract_size: Decimal
    tick_size: Decimal
    expiry: datetime
    settlement_type: str = "cash"  # cash or physical
    initial_margin_rate: Decimal = Field(default=Decimal("0.1"), ge=0, le=1)
    maintenance_margin_rate: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class FuturesOrder(BaseModel):
    """Futures order request."""
    user_id: str
    market_id: str
    order_type: str  # market, limit, stop, stop_limit
    side: str  # buy, sell
    contracts: Decimal = Field(..., gt=0)
    price: Optional[Decimal] = None
    leverage: Decimal = Field(default=Decimal("1"), ge=Decimal("1"), le=Decimal("20"))
    time_in_force: str = "good_till_cancelled"
    reduce_only: bool = False
    
    class Config:
        json_encoders = {
            Decimal: str
        }


class FundingRate(BaseModel):
    """Funding rate for perpetual futures."""
    market_id: str
    funding_rate: Decimal
    next_funding_time: datetime
    interval_hours: int = 8
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        } 