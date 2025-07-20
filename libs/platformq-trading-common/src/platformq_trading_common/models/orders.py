"""Shared order models for all trading services"""

from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
import time


class MarketType(Enum):
    """Types of markets"""
    SPOT = "spot"
    FUTURES = "futures"
    PERPETUAL = "perpetual"
    OPTIONS = "options"
    COMPUTE_FUTURES = "compute_futures"
    COMPUTE_OPTIONS = "compute_options"
    PREDICTION = "prediction"
    SYNTHETIC = "synthetic"


class OrderType(Enum):
    """Order types"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    ICEBERG = "iceberg"
    POST_ONLY = "post_only"
    REDUCE_ONLY = "reduce_only"
    TRAILING_STOP = "trailing_stop"


class OrderSide(Enum):
    """Order side"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """Order status"""
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    TRIGGERED = "triggered"  # For stop orders


class TimeInForce(Enum):
    """Time in force"""
    GTC = "gtc"  # Good Till Cancel
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill
    GTD = "gtd"  # Good Till Date
    DAY = "day"  # Day order
    GTT = "gtt"  # Good Till Time


class TriggerType(Enum):
    """Trigger types for conditional orders"""
    PRICE = "price"
    TIME = "time"
    VOLUME = "volume"
    VOLATILITY = "volatility"
    FUNDING_RATE = "funding_rate"


@dataclass
class BaseOrder:
    """Base order structure"""
    order_id: str
    market_id: str
    market_type: MarketType
    trader_id: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    
    # Optional fields
    price: Optional[Decimal] = None
    filled_quantity: Decimal = Decimal(0)
    average_fill_price: Optional[Decimal] = None
    status: OrderStatus = OrderStatus.PENDING
    time_in_force: TimeInForce = TimeInForce.GTC
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    expire_at: Optional[datetime] = None
    
    # Fees
    fees_paid: Decimal = Decimal(0)
    rebate_received: Decimal = Decimal(0)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    client_order_id: Optional[str] = None
    tenant_id: Optional[str] = None
    
    @property
    def remaining_quantity(self) -> Decimal:
        return self.quantity - self.filled_quantity
    
    @property
    def is_active(self) -> bool:
        return self.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
    
    @property
    def is_complete(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED)


@dataclass
class SpotOrder(BaseOrder):
    """Spot market order"""
    pass


@dataclass
class FuturesOrder(BaseOrder):
    """Futures market order"""
    contract_id: Optional[str] = None
    leverage: Decimal = Decimal(1)
    margin_type: str = "cross"  # cross or isolated
    reduce_only: bool = False
    post_only: bool = False


@dataclass
class OptionsOrder(BaseOrder):
    """Options market order"""
    strike_price: Decimal = Decimal(0)
    expiry_date: Optional[datetime] = None
    option_type: str = "call"  # call or put
    exercise_style: str = "european"  # european, american, bermudan


@dataclass
class ConditionalOrder(BaseOrder):
    """Conditional order with triggers"""
    trigger_type: TriggerType = TriggerType.PRICE
    trigger_price: Optional[Decimal] = None
    trigger_time: Optional[datetime] = None
    trigger_condition: str = "gte"  # gte, lte, eq
    trail_amount: Optional[Decimal] = None
    trail_percent: Optional[Decimal] = None


@dataclass
class Trade:
    """Trade execution record"""
    trade_id: str
    market_id: str
    market_type: MarketType
    price: Decimal
    quantity: Decimal
    buyer_order_id: str
    seller_order_id: str
    buyer_id: str
    seller_id: str
    buyer_fee: Decimal
    seller_fee: Decimal
    executed_at: datetime = field(default_factory=datetime.utcnow)
    settlement_time: Optional[datetime] = None
    
    # Additional fields for different market types
    funding_rate: Optional[Decimal] = None  # For perpetuals
    settlement_price: Optional[Decimal] = None  # For futures
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "market_id": self.market_id,
            "market_type": self.market_type.value,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "buyer_order_id": self.buyer_order_id,
            "seller_order_id": self.seller_order_id,
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "buyer_fee": str(self.buyer_fee),
            "seller_fee": str(self.seller_fee),
            "executed_at": self.executed_at.isoformat(),
            "settlement_time": self.settlement_time.isoformat() if self.settlement_time else None
        }


@dataclass
class OrderBook:
    """Order book snapshot"""
    market_id: str
    bids: List[List[Decimal]]  # [[price, quantity], ...]
    asks: List[List[Decimal]]  # [[price, quantity], ...]
    last_update_id: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0][0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0][0] if self.asks else None
    
    @property
    def spread(self) -> Optional[Decimal]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return None 