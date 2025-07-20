"""Futures contract models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, List
from pydantic import BaseModel, Field


class ContractType(str, Enum):
    """Types of futures contracts."""
    PERPETUAL = "perpetual"
    FIXED_EXPIRY = "fixed_expiry"
    QUARTERLY = "quarterly"
    MONTHLY = "monthly"


class SettlementType(str, Enum):
    """Settlement types for futures."""
    CASH = "cash"
    PHYSICAL = "physical"


class PositionSide(str, Enum):
    """Position sides."""
    LONG = "long"
    SHORT = "short"


class FuturesContract(BaseModel):
    """Futures contract specification."""
    symbol: str
    underlying_asset: str
    quote_asset: str
    contract_type: ContractType
    settlement_type: SettlementType
    contract_size: Decimal
    tick_size: Decimal
    expiry_date: Optional[datetime] = None
    initial_margin_rate: Decimal
    maintenance_margin_rate: Decimal
    max_leverage: int
    funding_interval_hours: Optional[int] = None  # For perpetuals
    is_active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class FuturesPosition(BaseModel):
    """User's futures position."""
    position_id: str
    user_id: str
    symbol: str
    side: PositionSide
    size: Decimal  # Number of contracts
    entry_price: Decimal
    mark_price: Decimal
    liquidation_price: Optional[Decimal] = None
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    margin_used: Decimal
    funding_paid: Decimal = Decimal("0")
    created_at: datetime
    updated_at: datetime
    
    class Config:
        use_enum_values = True


class FuturesOrder(BaseModel):
    """Futures order."""
    order_id: str
    user_id: str
    symbol: str
    side: PositionSide
    size: Decimal
    price: Optional[Decimal] = None  # None for market orders
    order_type: str  # limit, market, stop, etc.
    time_in_force: str = "GTC"
    reduce_only: bool = False
    post_only: bool = False
    status: str = "pending"
    filled_size: Decimal = Decimal("0")
    average_price: Optional[Decimal] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    
    class Config:
        use_enum_values = True


class FundingRate(BaseModel):
    """Funding rate for perpetual futures."""
    symbol: str
    funding_rate: Decimal
    mark_price: Decimal
    index_price: Decimal
    timestamp: datetime
    next_funding_time: datetime
    interest_rate: Decimal = Decimal("0.0001")  # 0.01% default


class SettlementRecord(BaseModel):
    """Settlement record for futures contracts."""
    settlement_id: str
    symbol: str
    settlement_price: Decimal
    settlement_type: SettlementType
    positions_settled: int
    total_volume: Decimal
    timestamp: datetime
    status: str = "pending"  # pending, processing, completed, failed
    details: Optional[Dict] = None
    
    class Config:
        use_enum_values = True


class MarginRequirement(BaseModel):
    """Margin requirements for a position."""
    user_id: str
    symbol: str
    initial_margin: Decimal
    maintenance_margin: Decimal
    current_margin: Decimal
    margin_ratio: Decimal  # current_margin / maintenance_margin
    available_balance: Decimal
    liquidation_price: Optional[Decimal]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class FuturesMarketStats(BaseModel):
    """Market statistics for futures contracts."""
    symbol: str
    last_price: Decimal
    mark_price: Decimal
    index_price: Decimal
    volume_24h: Decimal
    turnover_24h: Decimal
    open_interest: Decimal
    funding_rate: Optional[Decimal]
    next_funding_time: Optional[datetime]
    high_24h: Decimal
    low_24h: Decimal
    price_change_24h: Decimal
    price_change_percent_24h: Decimal
    timestamp: datetime = Field(default_factory=datetime.utcnow) 