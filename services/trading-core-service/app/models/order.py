"""Unified order models for all product types."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
import uuid


class OrderType(str, Enum):
    """Order types supported across all products."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    ICEBERG = "iceberg"
    POST_ONLY = "post_only"
    FILL_OR_KILL = "fill_or_kill"
    IMMEDIATE_OR_CANCEL = "immediate_or_cancel"


class OrderSide(str, Enum):
    """Order side."""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """Order status."""
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    TRIGGERED = "triggered"  # For stop orders


class TimeInForce(str, Enum):
    """Time in force options."""
    GTC = "good_till_cancelled"
    IOC = "immediate_or_cancel"
    FOK = "fill_or_kill"
    GTD = "good_till_date"
    POST_ONLY = "post_only"


class Order(BaseModel):
    """Unified order model for all product types."""
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    market_id: str
    product_type: str  # futures, options, spot, etc.
    
    # Order details
    type: OrderType
    side: OrderSide
    status: OrderStatus = OrderStatus.PENDING
    time_in_force: TimeInForce = TimeInForce.GTC
    
    # Quantities and prices
    quantity: Decimal = Field(..., gt=0)
    filled_quantity: Decimal = Field(default=Decimal("0"), ge=0)
    remaining_quantity: Decimal = Field(default=None)
    
    price: Optional[Decimal] = Field(None, gt=0)  # For limit orders
    stop_price: Optional[Decimal] = Field(None, gt=0)  # For stop orders
    average_fill_price: Optional[Decimal] = None
    
    # Iceberg order fields
    display_quantity: Optional[Decimal] = Field(None, gt=0)
    
    # Product-specific data
    product_data: Dict[str, Any] = Field(default_factory=dict)
    
    # Risk and margin
    initial_margin: Optional[Decimal] = None
    maintenance_margin: Optional[Decimal] = None
    
    # Fees
    commission: Decimal = Field(default=Decimal("0"), ge=0)
    fees: Dict[str, Decimal] = Field(default_factory=dict)
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    expire_time: Optional[datetime] = None
    
    # Metadata
    client_order_id: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Internal fields
    sequence_number: Optional[int] = None
    version: int = Field(default=1)
    
    @validator('remaining_quantity', always=True)
    def calculate_remaining(cls, v, values):
        """Calculate remaining quantity."""
        if v is None and 'quantity' in values and 'filled_quantity' in values:
            return values['quantity'] - values['filled_quantity']
        return v
    
    @validator('status')
    def validate_status_transition(cls, v, values):
        """Validate status transitions."""
        # Add status transition validation logic here
        return v
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class OrderRequest(BaseModel):
    """Request to create a new order."""
    market_id: str
    product_type: str
    type: OrderType
    side: OrderSide
    quantity: Decimal = Field(..., gt=0)
    
    price: Optional[Decimal] = Field(None, gt=0)
    stop_price: Optional[Decimal] = Field(None, gt=0)
    time_in_force: TimeInForce = TimeInForce.GTC
    
    display_quantity: Optional[Decimal] = Field(None, gt=0)
    expire_time: Optional[datetime] = None
    
    client_order_id: Optional[str] = None
    product_data: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('price')
    def validate_price_for_order_type(cls, v, values):
        """Validate price requirements based on order type."""
        if 'type' in values:
            if values['type'] in [OrderType.LIMIT, OrderType.STOP_LIMIT] and v is None:
                raise ValueError(f"Price is required for {values['type']} orders")
        return v


class OrderUpdate(BaseModel):
    """Request to update an existing order."""
    order_id: str
    quantity: Optional[Decimal] = Field(None, gt=0)
    price: Optional[Decimal] = Field(None, gt=0)
    stop_price: Optional[Decimal] = Field(None, gt=0)
    metadata: Optional[Dict[str, Any]] = None


class OrderCancel(BaseModel):
    """Request to cancel an order."""
    order_id: str
    reason: Optional[str] = None


class OrderFilter(BaseModel):
    """Filter criteria for querying orders."""
    user_id: Optional[str] = None
    market_id: Optional[str] = None
    product_type: Optional[str] = None
    status: Optional[List[OrderStatus]] = None
    side: Optional[OrderSide] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0) 