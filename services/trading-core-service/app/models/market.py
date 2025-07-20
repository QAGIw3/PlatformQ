"""Market and product type models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class MarketStatus(str, Enum):
    """Market status."""
    PRE_OPEN = "pre_open"
    OPEN = "open"
    HALTED = "halted"
    CLOSED = "closed"
    POST_CLOSE = "post_close"
    MAINTENANCE = "maintenance"


class MarketType(str, Enum):
    """Market type."""
    SPOT = "spot"
    FUTURES = "futures"
    PERPETUAL = "perpetual"
    OPTIONS = "options"
    STRUCTURED = "structured"
    COMPUTE = "compute"
    SYNTHETIC = "synthetic"


class ProductType(str, Enum):
    """Product types across all markets."""
    # Spot markets
    SPOT = "spot"
    
    # Futures
    FUTURES = "futures"
    PERPETUAL_FUTURES = "perpetual_futures"
    COMPUTE_FUTURES = "compute_futures"
    
    # Options
    VANILLA_OPTION = "vanilla_option"
    BINARY_OPTION = "binary_option"
    ASIAN_OPTION = "asian_option"
    BARRIER_OPTION = "barrier_option"
    COMPUTE_OPTION = "compute_option"
    
    # Structured products
    STRUCTURED_NOTE = "structured_note"
    AUTOCALLABLE = "autocallable"
    ACCUMULATOR = "accumulator"
    
    # Synthetic products
    VARIANCE_SWAP = "variance_swap"
    VOLATILITY_SWAP = "volatility_swap"
    CORRELATION_SWAP = "correlation_swap"
    
    # Compute market products
    COMPUTE_SPOT = "compute_spot"
    BURST_DERIVATIVE = "burst_derivative"
    CAPACITY_TOKEN = "capacity_token"


class Market(BaseModel):
    """Market definition."""
    market_id: str
    symbol: str
    name: str
    market_type: MarketType
    product_type: ProductType
    status: MarketStatus = MarketStatus.CLOSED
    
    # Base and quote assets
    base_asset: str
    quote_asset: str
    
    # Trading parameters
    tick_size: Decimal = Field(..., gt=0)
    lot_size: Decimal = Field(..., gt=0)
    min_notional: Decimal = Field(..., gt=0)
    max_order_size: Optional[Decimal] = Field(None, gt=0)
    
    # Price limits
    price_filter: Optional[Dict[str, Decimal]] = None
    daily_price_limit: Optional[Decimal] = None
    
    # Market hours (optional)
    trading_hours: Optional[Dict[str, Any]] = None
    
    # Product-specific configuration
    product_config: Dict[str, Any] = Field(default_factory=dict)
    
    # Fees
    maker_fee: Decimal = Field(default=Decimal("0.0002"), ge=0)
    taker_fee: Decimal = Field(default=Decimal("0.0005"), ge=0)
    
    # Risk parameters
    initial_margin_rate: Decimal = Field(default=Decimal("0.1"), ge=0, le=1)
    maintenance_margin_rate: Decimal = Field(default=Decimal("0.05"), ge=0, le=1)
    max_leverage: Decimal = Field(default=Decimal("10"), gt=0)
    
    # Circuit breaker
    circuit_breaker_enabled: bool = True
    price_band_percentage: Decimal = Field(default=Decimal("0.1"), gt=0)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class MarketStats(BaseModel):
    """Real-time market statistics."""
    market_id: str
    last_price: Decimal
    volume_24h: Decimal
    volume_quote_24h: Decimal
    high_24h: Decimal
    low_24h: Decimal
    open_24h: Decimal
    price_change_24h: Decimal
    price_change_percent_24h: Decimal
    
    # Additional stats
    trades_24h: int
    open_interest: Optional[Decimal] = None
    funding_rate: Optional[Decimal] = None
    mark_price: Optional[Decimal] = None
    index_price: Optional[Decimal] = None
    
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        } 