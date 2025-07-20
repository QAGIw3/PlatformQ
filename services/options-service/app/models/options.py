"""Options contract models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, List
from pydantic import BaseModel, Field


class OptionType(str, Enum):
    """Option types."""
    CALL = "call"
    PUT = "put"


class OptionStyle(str, Enum):
    """Option exercise styles."""
    EUROPEAN = "european"
    AMERICAN = "american"


class OptionStatus(str, Enum):
    """Option contract status."""
    ACTIVE = "active"
    EXPIRED = "expired"
    EXERCISED = "exercised"
    ASSIGNED = "assigned"


class Greeks(BaseModel):
    """Option Greeks."""
    delta: Decimal
    gamma: Decimal
    theta: Decimal
    vega: Decimal
    rho: Decimal
    calculated_at: datetime = Field(default_factory=datetime.utcnow)


class OptionContract(BaseModel):
    """Option contract specification."""
    symbol: str  # e.g., "BTC-50000-20240331-C"
    underlying_asset: str
    strike_price: Decimal
    expiry_date: datetime
    option_type: OptionType
    option_style: OptionStyle
    contract_size: Decimal = Decimal("1")
    quote_asset: str = "USD"
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True
    
    def is_itm(self, spot_price: Decimal) -> bool:
        """Check if option is in the money."""
        if self.option_type == OptionType.CALL:
            return spot_price > self.strike_price
        else:
            return spot_price < self.strike_price
    
    def time_to_expiry(self) -> float:
        """Calculate time to expiry in years."""
        td = self.expiry_date - datetime.utcnow()
        return max(0, td.total_seconds() / (365.25 * 24 * 3600))


class OptionPosition(BaseModel):
    """User's option position."""
    position_id: str
    user_id: str
    symbol: str
    option_type: OptionType
    size: Decimal  # Positive for long, negative for short
    entry_price: Decimal
    mark_price: Decimal
    implied_volatility: Decimal
    greeks: Optional[Greeks] = None
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    created_at: datetime
    updated_at: datetime
    
    class Config:
        use_enum_values = True


class OptionOrder(BaseModel):
    """Option order."""
    order_id: str
    user_id: str
    symbol: str
    side: str  # buy or sell
    size: Decimal
    price: Optional[Decimal] = None
    order_type: str = "limit"
    time_in_force: str = "GTC"
    status: str = "pending"
    filled_size: Decimal = Decimal("0")
    average_price: Optional[Decimal] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class OptionPricing(BaseModel):
    """Option pricing data."""
    symbol: str
    spot_price: Decimal
    strike_price: Decimal
    time_to_expiry: float
    risk_free_rate: float
    implied_volatility: Decimal
    theoretical_price: Decimal
    bid_price: Optional[Decimal] = None
    ask_price: Optional[Decimal] = None
    mid_price: Optional[Decimal] = None
    greeks: Greeks
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class VolatilitySurface(BaseModel):
    """Volatility surface data."""
    underlying_asset: str
    surface_data: Dict[str, Dict[str, float]]  # expiry -> strike -> IV
    at_the_money_vol: Decimal
    skew: Dict[str, float]  # expiry -> skew parameter
    term_structure: Dict[str, float]  # expiry -> ATM vol
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class OptionMarketStats(BaseModel):
    """Market statistics for options."""
    symbol: str
    underlying_price: Decimal
    bid_price: Optional[Decimal]
    ask_price: Optional[Decimal]
    last_price: Optional[Decimal]
    volume_24h: Decimal
    open_interest: Decimal
    implied_volatility: Optional[Decimal]
    put_call_ratio: Optional[Decimal]
    greeks: Optional[Greeks]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class OptionChain(BaseModel):
    """Option chain for an underlying asset."""
    underlying_asset: str
    spot_price: Decimal
    expiry_date: datetime
    calls: List[OptionPricing]
    puts: List[OptionPricing]
    at_the_money_strike: Decimal
    total_call_volume: Decimal
    total_put_volume: Decimal
    put_call_ratio: Decimal
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class OptionStrategy(BaseModel):
    """Pre-defined option strategy."""
    strategy_id: str
    name: str  # e.g., "Bull Call Spread", "Iron Condor"
    description: str
    legs: List[Dict]  # List of option legs with quantities
    max_profit: Optional[Decimal]
    max_loss: Optional[Decimal]
    breakeven_points: List[Decimal]
    required_margin: Decimal
    
    
class ExerciseRequest(BaseModel):
    """Request to exercise an option."""
    position_id: str
    exercise_type: str = "full"  # full or partial
    quantity: Optional[Decimal] = None  # For partial exercise 