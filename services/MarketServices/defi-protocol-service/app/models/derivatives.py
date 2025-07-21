"""
Derivatives Models

Data models for options and perpetual futures.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from decimal import Decimal


class OptionType(str, Enum):
    """Option type"""
    CALL = "call"
    PUT = "put"


class OptionStyle(str, Enum):
    """Option exercise style"""
    EUROPEAN = "european"
    AMERICAN = "american"


class PositionSide(str, Enum):
    """Position side for perpetuals"""
    LONG = "long"
    SHORT = "short"


# Option Models

class Option(BaseModel):
    """Option contract details"""
    option_id: int
    resource_token_id: int
    strike_price: int
    expiry: datetime
    amount: int
    option_type: OptionType
    style: OptionStyle
    writer: str
    holder: str
    exercised: bool = False
    expired: bool = False
    premium: int = 0
    collateral: Optional[int] = None
    created_at: datetime


class OptionGreeks(BaseModel):
    """Option Greeks"""
    delta: float  # Rate of change of option price with respect to underlying
    gamma: float  # Rate of change of delta
    theta: float  # Time decay (per day)
    vega: float   # Sensitivity to volatility
    rho: float    # Sensitivity to interest rate


class OptionOrder(BaseModel):
    """Option order details"""
    option_id: Optional[int] = None
    resource_token_id: int
    strike_price: int
    expiry: datetime
    amount: int
    option_type: OptionType
    style: OptionStyle = OptionStyle.EUROPEAN
    side: str  # "buy" or "write"
    premium: Optional[int] = None


# Perpetual Models

class PerpetualPosition(BaseModel):
    """Perpetual futures position"""
    user: str
    resource_token_id: int
    size: int
    entry_price: int
    margin: int
    is_long: bool
    funding_index: Optional[int] = None
    last_update_time: Optional[datetime] = None
    opened_at: datetime
    unrealized_pnl: Optional[int] = None
    margin_ratio: Optional[float] = None


class PerpetualMarket(BaseModel):
    """Perpetual market info"""
    resource_token_id: int
    open_interest: int
    long_open_interest: int
    short_open_interest: int
    funding_rate: int  # Current funding rate
    cumulative_funding: int
    last_funding_time: datetime
    max_open_interest: int
    is_active: bool
    index_price: Optional[int] = None
    mark_price: Optional[int] = None


class FundingRate(BaseModel):
    """Funding rate history"""
    resource_token_id: int
    funding_rate: int
    timestamp: datetime
    long_pays: bool  # True if longs pay shorts


class PerpetualOrder(BaseModel):
    """Perpetual order details"""
    resource_token_id: int
    size: int
    margin: int
    is_long: bool
    leverage: float
    limit_price: Optional[int] = None
    stop_loss: Optional[int] = None
    take_profit: Optional[int] = None


# Options AMM Models

class OptionsPool(BaseModel):
    """Options AMM pool"""
    resource_token_id: int
    total_liquidity: int
    resource_reserve: int
    stablecoin_reserve: int
    utilization: int = 0
    base_iv: int  # Base implied volatility in basis points
    is_active: bool = True
    created_at: datetime


class LiquidityPosition(BaseModel):
    """LP position in options AMM"""
    user: str
    pool_id: int
    liquidity: int
    resource_deposited: int
    stablecoin_deposited: int
    deposit_time: datetime
    fees_earned: int = 0


# Request/Response Models

class WriteOptionRequest(BaseModel):
    """Request to write an option"""
    resource_token_id: int = Field(..., ge=0)
    strike_price: int = Field(..., gt=0)
    expiry: datetime
    option_type: OptionType
    style: OptionStyle = Field(default=OptionStyle.EUROPEAN)
    amount: int = Field(..., gt=0)


class BuyOptionRequest(BaseModel):
    """Request to buy an option"""
    option_id: int = Field(..., ge=1)


class ExerciseOptionRequest(BaseModel):
    """Request to exercise an option"""
    option_id: int = Field(..., ge=1)


class OpenPerpetualRequest(BaseModel):
    """Request to open perpetual position"""
    resource_token_id: int = Field(..., ge=0)
    size: int = Field(..., gt=0)
    margin: int = Field(..., gt=0)
    is_long: bool
    limit_price: Optional[int] = None


class ClosePerpetualRequest(BaseModel):
    """Request to close perpetual position"""
    resource_token_id: int = Field(..., ge=0)
    size: int = Field(default=0, ge=0, description="0 for full close")


class AddMarginRequest(BaseModel):
    """Request to add margin"""
    resource_token_id: int = Field(..., ge=0)
    amount: int = Field(..., gt=0)


class CreateOptionsPoolRequest(BaseModel):
    """Request to create options AMM pool"""
    resource_token_id: int = Field(..., ge=0)
    resource_amount: int = Field(..., gt=0)
    stablecoin_amount: int = Field(..., gt=0)
    base_iv: int = Field(..., gt=0, le=50000, description="Base IV in basis points")


class AddOptionsLiquidityRequest(BaseModel):
    """Request to add liquidity to options AMM"""
    resource_token_id: int = Field(..., ge=0)
    resource_amount: int = Field(..., gt=0)
    stablecoin_amount: int = Field(..., gt=0)


class RemoveOptionsLiquidityRequest(BaseModel):
    """Request to remove liquidity from options AMM"""
    resource_token_id: int = Field(..., ge=0)
    liquidity: int = Field(..., gt=0)
    min_resource_amount: int = Field(default=0, ge=0)
    min_stablecoin_amount: int = Field(default=0, ge=0)


# Response Models

class OptionResponse(BaseModel):
    """Option creation/purchase response"""
    option_id: int
    tx_hash: str
    premium: Optional[int] = None
    collateral_locked: Optional[int] = None


class ExerciseResponse(BaseModel):
    """Option exercise response"""
    tx_hash: str
    payout: int
    profit: int


class PerpetualPositionResponse(BaseModel):
    """Perpetual position response"""
    tx_hash: str
    position_size: int
    entry_price: int
    leverage: float
    margin: int
    fee: int


class PositionInfoResponse(BaseModel):
    """Detailed position information"""
    size: int
    entry_price: int
    margin: int
    is_long: bool
    unrealized_pnl: int
    margin_ratio: float
    liquidation_price: int
    funding_payment: Optional[int] = None


class GreeksResponse(BaseModel):
    """Option Greeks response"""
    option_id: int
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    iv: Optional[float] = None


class OptionsPoolResponse(BaseModel):
    """Options pool creation response"""
    tx_hash: str
    pool_id: int
    liquidity: int


class OptionPremiumQuote(BaseModel):
    """Option premium quote from AMM"""
    premium: int
    strike_price: int
    expiry: str
    option_type: str
    amount: int
    iv: float
    pool_utilization: float


class MarketDataResponse(BaseModel):
    """Market data for a resource"""
    resource_token_id: int
    spot_price: int
    mark_price: int
    index_price: int
    funding_rate: int
    open_interest: int
    long_open_interest: int
    short_open_interest: int
    volume_24h: int


class DerivativesStats(BaseModel):
    """Overall derivatives statistics"""
    total_options_volume: int
    total_perpetuals_volume: int
    open_interest: int
    active_options: int
    active_positions: int
    total_pools: int
    total_fees_collected: Optional[int] = None


class LiquidationEvent(BaseModel):
    """Liquidation event details"""
    trader: str
    resource_token_id: int
    size: int
    margin: int
    liquidation_price: int
    liquidator: str
    liquidation_fee: int
    timestamp: datetime 