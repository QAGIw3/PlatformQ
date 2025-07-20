"""AMM models for liquidity pools and positions."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field


class PoolType(str, Enum):
    """Types of AMM pools."""
    CONSTANT_PRODUCT = "constant_product"  # x*y=k (Uniswap V2 style)
    CONCENTRATED = "concentrated"           # Concentrated liquidity (Uniswap V3 style)
    STABLESWAP = "stableswap"              # For correlated assets
    OPTIONS = "options"                     # Options-specific AMM


class FeeType(str, Enum):
    """Fee calculation types."""
    FIXED = "fixed"
    DYNAMIC = "dynamic"
    TIERED = "tiered"


class SwapDirection(str, Enum):
    """Swap direction."""
    BASE_TO_QUOTE = "base_to_quote"
    QUOTE_TO_BASE = "quote_to_base"


class LiquidityPool(BaseModel):
    """AMM liquidity pool."""
    pool_id: str
    pool_type: PoolType
    base_asset: str
    quote_asset: str
    
    # Pool configuration
    fee_type: FeeType = FeeType.DYNAMIC
    base_fee_bps: int = 30  # 0.3%
    tick_spacing: Optional[int] = None  # For concentrated liquidity
    amplification: Optional[int] = None  # For stableswap
    
    # Current state
    base_reserve: Decimal = Decimal("0")
    quote_reserve: Decimal = Decimal("0")
    total_liquidity: Decimal = Decimal("0")
    current_price: Decimal = Decimal("0")
    current_tick: Optional[int] = None
    
    # Virtual reserves for pricing
    virtual_base_reserve: Optional[Decimal] = None
    virtual_quote_reserve: Optional[Decimal] = None
    
    # Metrics
    volume_24h: Decimal = Decimal("0")
    fees_collected_24h: Decimal = Decimal("0")
    trades_24h: int = 0
    unique_traders_24h: int = 0
    
    # Risk parameters
    max_price_impact: Decimal = Decimal("0.05")
    max_slippage: Decimal = Decimal("0.02")
    imbalance_ratio: Decimal = Decimal("0.5")  # base_value / total_value
    
    # Status
    is_active: bool = True
    is_paused: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class TickData(BaseModel):
    """Data for a single tick in concentrated liquidity."""
    tick_index: int
    liquidity_gross: Decimal
    liquidity_net: Decimal
    fee_growth_outside_0: Decimal = Decimal("0")
    fee_growth_outside_1: Decimal = Decimal("0")
    initialized: bool = False


class LiquidityPosition(BaseModel):
    """Individual liquidity position in a pool."""
    position_id: str
    pool_id: str
    provider: str  # User address/ID
    
    # Position range (for concentrated liquidity)
    tick_lower: Optional[int] = None
    tick_upper: Optional[int] = None
    
    # Amounts
    liquidity: Decimal
    base_amount: Decimal
    quote_amount: Decimal
    
    # Collected fees
    uncollected_fees_base: Decimal = Decimal("0")
    uncollected_fees_quote: Decimal = Decimal("0")
    total_fees_collected_base: Decimal = Decimal("0")
    total_fees_collected_quote: Decimal = Decimal("0")
    
    # Tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class SwapRequest(BaseModel):
    """Request to execute a swap."""
    pool_id: str
    trader: str
    direction: SwapDirection
    amount_in: Decimal
    min_amount_out: Optional[Decimal] = None
    max_price_impact: Optional[Decimal] = None
    deadline: Optional[datetime] = None
    
    class Config:
        use_enum_values = True


class SwapResult(BaseModel):
    """Result of a swap execution."""
    swap_id: str
    pool_id: str
    trader: str
    direction: SwapDirection
    
    # Amounts
    amount_in: Decimal
    amount_out: Decimal
    fee_paid: Decimal
    
    # Pricing
    execution_price: Decimal
    price_impact: Decimal
    slippage: Decimal
    
    # Pool state after swap
    new_base_reserve: Decimal
    new_quote_reserve: Decimal
    new_price: Decimal
    
    # Timestamp
    executed_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class OptionsAMMPool(LiquidityPool):
    """Specialized AMM pool for options."""
    underlying_asset: str
    
    # Options-specific parameters
    max_net_delta: Decimal = Decimal("1000")
    max_net_gamma: Decimal = Decimal("100")
    max_net_vega: Decimal = Decimal("500")
    
    # Current Greeks exposure
    net_delta: Decimal = Decimal("0")
    net_gamma: Decimal = Decimal("0")
    net_vega: Decimal = Decimal("0")
    net_theta: Decimal = Decimal("0")
    
    # Hedging
    hedge_enabled: bool = True
    last_hedge_time: Optional[datetime] = None
    hedge_positions: Dict[str, Decimal] = {}


class PoolMetrics(BaseModel):
    """Detailed pool metrics and analytics."""
    pool_id: str
    period: str  # hourly, daily, weekly
    
    # Volume metrics
    volume_base: Decimal
    volume_quote: Decimal
    volume_usd: Decimal
    
    # Fee metrics
    fees_base: Decimal
    fees_quote: Decimal
    fees_usd: Decimal
    avg_fee_rate: Decimal
    
    # Liquidity metrics
    avg_liquidity: Decimal
    liquidity_utilization: Decimal
    
    # Price metrics
    open_price: Decimal
    close_price: Decimal
    high_price: Decimal
    low_price: Decimal
    price_volatility: Decimal
    
    # Trading metrics
    total_trades: int
    unique_traders: int
    avg_trade_size: Decimal
    
    # Efficiency metrics
    price_impact_avg: Decimal
    slippage_avg: Decimal
    
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class FeeUpdate(BaseModel):
    """Dynamic fee update for a pool."""
    pool_id: str
    old_fee_bps: int
    new_fee_bps: int
    
    # Factors that influenced the update
    volatility_factor: Decimal
    volume_factor: Decimal
    liquidity_factor: Decimal
    imbalance_factor: Decimal
    
    # Reasons
    reasons: List[str]
    
    timestamp: datetime = Field(default_factory=datetime.utcnow) 