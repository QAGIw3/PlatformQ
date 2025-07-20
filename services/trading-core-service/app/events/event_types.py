"""Event types for Flink processing."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Event types."""
    ORDER_NEW = "order_new"
    ORDER_UPDATE = "order_update"
    ORDER_CANCEL = "order_cancel"
    ORDER_FILL = "order_fill"
    TRADE_EXECUTE = "trade_execute"
    POSITION_OPEN = "position_open"
    POSITION_UPDATE = "position_update"
    POSITION_CLOSE = "position_close"
    MARKET_UPDATE = "market_update"
    RISK_ALERT = "risk_alert"


class EventPriority(str, Enum):
    """Event priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class BaseEvent(BaseModel):
    """Base event class."""
    event_id: str
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    priority: EventPriority = EventPriority.NORMAL
    source: str = "trading-core"
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            Decimal: str
        }


class OrderEvent(BaseEvent):
    """Order event."""
    order_id: str
    user_id: str
    market_id: str
    product_type: str
    order_data: Dict[str, Any]


class TradeEvent(BaseEvent):
    """Trade event."""
    trade_id: str
    market_id: str
    product_type: str
    taker_order_id: str
    maker_order_id: str
    price: Decimal
    quantity: Decimal
    trade_data: Dict[str, Any]


class PositionEvent(BaseEvent):
    """Position event."""
    position_id: str
    user_id: str
    market_id: str
    product_type: str
    position_data: Dict[str, Any]
    pnl_data: Optional[Dict[str, Decimal]] = None


class MarketEvent(BaseEvent):
    """Market event."""
    market_id: str
    update_type: str  # price, status, config
    market_data: Dict[str, Any]


class RiskEvent(BaseEvent):
    """Risk event."""
    user_id: str
    risk_type: str  # margin_call, liquidation_warning, position_limit
    severity: str  # info, warning, critical
    risk_data: Dict[str, Any] 