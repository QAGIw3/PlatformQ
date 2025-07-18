from enum import Enum
from typing import Optional, Dict, Any
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field
import uuid


class OrderStatus(Enum):
    """Order status types"""
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class OrderType(Enum):
    """Order types"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"
    ICEBERG = "iceberg"
    POST_ONLY = "post_only"


class OrderSide(Enum):
    """Order side"""
    BUY = "buy"
    SELL = "sell"


class TimeInForce(Enum):
    """Time in force options"""
    GTC = "gtc"  # Good Till Cancelled
    IOC = "ioc"  # Immediate Or Cancel
    FOK = "fok"  # Fill Or Kill
    GTD = "gtd"  # Good Till Date
    GTT = "gtt"  # Good Till Time


@dataclass
class Order:
    """
    Derivative order representation
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    market_id: str = ""
    user_id: str = ""
    side: OrderSide = OrderSide.BUY
    order_type: OrderType = OrderType.LIMIT
    status: OrderStatus = OrderStatus.PENDING
    
    # Order sizing
    size: Decimal = Decimal("0")
    filled_size: Decimal = Decimal("0")
    remaining_size: Decimal = Decimal("0")
    
    # Pricing
    price: Optional[Decimal] = None  # None for market orders
    stop_price: Optional[Decimal] = None  # For stop orders
    average_fill_price: Optional[Decimal] = None
    
    # Time constraints
    time_in_force: TimeInForce = TimeInForce.GTC
    expire_time: Optional[datetime] = None
    
    # Fees
    fee_paid: Decimal = Decimal("0")
    maker_fee_rate: Decimal = Decimal("0")
    taker_fee_rate: Decimal = Decimal("0")
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Advanced order features
    reduce_only: bool = False
    post_only: bool = False
    hidden: bool = False  # For iceberg orders
    visible_size: Optional[Decimal] = None  # For iceberg orders
    trigger_price: Optional[Decimal] = None  # For conditional orders
    trailing_delta: Optional[Decimal] = None  # For trailing stop
    
    # Client metadata
    client_order_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize calculated fields"""
        self.remaining_size = self.size - self.filled_size
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "market_id": self.market_id,
            "user_id": self.user_id,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "status": self.status.value,
            "size": str(self.size),
            "filled_size": str(self.filled_size),
            "remaining_size": str(self.remaining_size),
            "price": str(self.price) if self.price else None,
            "stop_price": str(self.stop_price) if self.stop_price else None,
            "average_fill_price": str(self.average_fill_price) if self.average_fill_price else None,
            "time_in_force": self.time_in_force.value,
            "expire_time": self.expire_time.isoformat() if self.expire_time else None,
            "fee_paid": str(self.fee_paid),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "reduce_only": self.reduce_only,
            "post_only": self.post_only,
            "hidden": self.hidden,
            "client_order_id": self.client_order_id,
            "metadata": self.metadata
        }
    
    def update_fill(self, fill_size: Decimal, fill_price: Decimal, fee: Decimal):
        """Update order with a fill"""
        self.filled_size += fill_size
        self.remaining_size = self.size - self.filled_size
        self.fee_paid += fee
        
        # Update average fill price
        if self.average_fill_price is None:
            self.average_fill_price = fill_price
        else:
            total_value = (self.average_fill_price * (self.filled_size - fill_size)) + (fill_price * fill_size)
            self.average_fill_price = total_value / self.filled_size
        
        # Update status
        if self.remaining_size == 0:
            self.status = OrderStatus.FILLED
        else:
            self.status = OrderStatus.PARTIALLY_FILLED
            
        self.updated_at = datetime.utcnow()


@dataclass
class Trade:
    """
    Executed trade representation
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    market_id: str = ""
    
    # Order details
    buyer_order_id: str = ""
    seller_order_id: str = ""
    buyer_user_id: str = ""
    seller_user_id: str = ""
    
    # Trade details
    price: Decimal = Decimal("0")
    size: Decimal = Decimal("0")
    value: Decimal = Decimal("0")  # price * size
    
    # Fees
    buyer_fee: Decimal = Decimal("0")
    seller_fee: Decimal = Decimal("0")
    buyer_is_maker: bool = False
    
    # Timestamps
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Settlement
    settled: bool = False
    settlement_timestamp: Optional[datetime] = None
    
    # Metadata
    trade_type: str = "regular"  # regular, liquidation, adl
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate derived fields"""
        self.value = self.price * self.size
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "market_id": self.market_id,
            "buyer_order_id": self.buyer_order_id,
            "seller_order_id": self.seller_order_id,
            "buyer_user_id": self.buyer_user_id,
            "seller_user_id": self.seller_user_id,
            "price": str(self.price),
            "size": str(self.size),
            "value": str(self.value),
            "buyer_fee": str(self.buyer_fee),
            "seller_fee": str(self.seller_fee),
            "buyer_is_maker": self.buyer_is_maker,
            "timestamp": self.timestamp.isoformat(),
            "settled": self.settled,
            "settlement_timestamp": self.settlement_timestamp.isoformat() if self.settlement_timestamp else None,
            "trade_type": self.trade_type,
            "metadata": self.metadata
        } 