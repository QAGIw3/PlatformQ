from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
import time


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    ICEBERG = "iceberg"
    POST_ONLY = "post_only"


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class TimeInForce(Enum):
    GTC = "gtc"  # Good Till Cancel
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill
    GTD = "gtd"  # Good Till Date
    DAY = "day"  # Day order


@dataclass
class Order:
    """High-performance order structure optimized for matching"""
    # Required fields
    order_id: str
    market_id: str
    trader_id: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    
    # Optional fields with defaults
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    filled_quantity: Decimal = Decimal(0)
    average_fill_price: Optional[Decimal] = None
    status: OrderStatus = OrderStatus.PENDING
    time_in_force: TimeInForce = TimeInForce.GTC
    
    # Timestamps (using nanosecond precision)
    created_at_ns: int = field(default_factory=lambda: time.time_ns())
    updated_at_ns: int = field(default_factory=lambda: time.time_ns())
    expire_at: Optional[datetime] = None
    
    # Iceberg order fields
    display_quantity: Optional[Decimal] = None
    hidden_quantity: Optional[Decimal] = None
    
    # Execution fields
    fees: Decimal = Decimal(0)
    rebate: Decimal = Decimal(0)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    client_order_id: Optional[str] = None
    
    # Performance optimization fields
    _price_int: Optional[int] = None  # Price as integer for faster comparison
    _quantity_int: Optional[int] = None  # Quantity as integer
    _sequence: Optional[int] = None  # Global sequence number
    
    def __post_init__(self):
        """Convert decimals to integers for performance"""
        if self.price:
            self._price_int = int(self.price * 100000000)  # 8 decimal places
        self._quantity_int = int(self.quantity * 100000000)
    
    @property
    def remaining_quantity(self) -> Decimal:
        return self.quantity - self.filled_quantity
    
    @property
    def is_buy(self) -> bool:
        return self.side == OrderSide.BUY
    
    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED
    
    @property
    def is_active(self) -> bool:
        return self.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
    
    def can_match_price(self, other_price: Decimal) -> bool:
        """Check if order can match at given price"""
        if self.order_type == OrderType.MARKET:
            return True
        
        if self.price is None:
            return False
        
        if self.is_buy:
            return other_price <= self.price
        else:
            return other_price >= self.price
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "order_id": self.order_id,
            "market_id": self.market_id,
            "trader_id": self.trader_id,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": str(self.quantity),
            "price": str(self.price) if self.price else None,
            "filled_quantity": str(self.filled_quantity),
            "status": self.status.value,
            "time_in_force": self.time_in_force.value,
            "created_at": self.created_at_ns,
            "metadata": self.metadata
        }


@dataclass
class Trade:
    """Trade execution record"""
    trade_id: str
    market_id: str
    price: Decimal
    quantity: Decimal
    buyer_order_id: str
    seller_order_id: str
    buyer_id: str
    seller_id: str
    buyer_fee: Decimal
    seller_fee: Decimal
    executed_at_ns: int = field(default_factory=lambda: time.time_ns())
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "market_id": self.market_id,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "buyer_order_id": self.buyer_order_id,
            "seller_order_id": self.seller_order_id,
            "buyer_id": self.buyer_id,
            "seller_id": self.seller_id,
            "buyer_fee": str(self.buyer_fee),
            "seller_fee": str(self.seller_fee),
            "executed_at": self.executed_at_ns
        } 