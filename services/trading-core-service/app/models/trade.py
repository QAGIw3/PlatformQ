"""Trade models for executed trades."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
import uuid


class TradeStatus(str, Enum):
    """Trade status."""
    PENDING = "pending"
    EXECUTED = "executed"
    SETTLED = "settled"
    FAILED = "failed"


class TradeSide(str, Enum):
    """Trade side from perspective of the taker."""
    BUY = "buy"
    SELL = "sell"


class Trade(BaseModel):
    """Executed trade record."""
    trade_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    market_id: str
    product_type: str
    
    # Order information
    taker_order_id: str
    maker_order_id: str
    
    # User information
    taker_user_id: str
    maker_user_id: str
    
    # Trade details
    price: Decimal = Field(..., gt=0)
    quantity: Decimal = Field(..., gt=0)
    value: Decimal = Field(default=None)
    side: TradeSide  # From taker perspective
    
    # Fees
    taker_fee: Decimal = Field(default=Decimal("0"), ge=0)
    maker_fee: Decimal = Field(default=Decimal("0"), ge=0)
    taker_fee_asset: Optional[str] = None
    maker_fee_asset: Optional[str] = None
    
    # Status
    status: TradeStatus = TradeStatus.EXECUTED
    
    # Timestamps
    executed_at: datetime = Field(default_factory=datetime.utcnow)
    settled_at: Optional[datetime] = None
    
    # Sequence numbers for ordering
    sequence_number: Optional[int] = None
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def is_buy(self) -> bool:
        """Check if trade is a buy from taker perspective."""
        return self.side == TradeSide.BUY
    
    def calculate_value(self) -> Decimal:
        """Calculate trade value."""
        if self.value is None:
            self.value = self.price * self.quantity
        return self.value
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class TradeEvent(BaseModel):
    """Trade event for publishing."""
    event_type: str = Field(default="trade")
    trade: Trade
    market_stats_update: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        } 