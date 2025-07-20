"""Position models for tracking user positions across products."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class PositionSide(str, Enum):
    """Position side."""
    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"  # For complex positions


class Position(BaseModel):
    """User position in a market."""
    position_id: str
    user_id: str
    market_id: str
    product_type: str
    
    # Position details
    side: PositionSide
    quantity: Decimal = Field(default=Decimal("0"))
    notional_value: Decimal = Field(default=Decimal("0"))
    
    # Pricing
    entry_price: Decimal = Field(default=Decimal("0"))
    mark_price: Decimal = Field(default=Decimal("0"))
    liquidation_price: Optional[Decimal] = None
    
    # P&L
    unrealized_pnl: Decimal = Field(default=Decimal("0"))
    realized_pnl: Decimal = Field(default=Decimal("0"))
    total_pnl: Decimal = Field(default=Decimal("0"))
    
    # Margin
    initial_margin: Decimal = Field(default=Decimal("0"))
    maintenance_margin: Decimal = Field(default=Decimal("0"))
    margin_ratio: Decimal = Field(default=Decimal("0"))
    collateral: Decimal = Field(default=Decimal("0"))
    
    # Product-specific data
    product_data: Dict[str, Any] = Field(default_factory=dict)
    
    # Risk metrics
    leverage: Decimal = Field(default=Decimal("1"))
    liquidation_risk: Decimal = Field(default=Decimal("0"))  # 0-1 scale
    
    # Fees and funding
    total_fees_paid: Decimal = Field(default=Decimal("0"))
    funding_paid: Decimal = Field(default=Decimal("0"))  # For perpetuals
    
    # Timestamps
    opened_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    
    # Status
    is_open: bool = True
    is_liquidated: bool = False
    
    # Metadata
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def calculate_pnl(self, current_price: Decimal) -> Dict[str, Decimal]:
        """Calculate current P&L."""
        if self.side == PositionSide.LONG:
            unrealized = (current_price - self.entry_price) * self.quantity
        elif self.side == PositionSide.SHORT:
            unrealized = (self.entry_price - current_price) * self.quantity
        else:
            unrealized = Decimal("0")
        
        self.unrealized_pnl = unrealized
        self.total_pnl = self.unrealized_pnl + self.realized_pnl
        
        return {
            "unrealized_pnl": self.unrealized_pnl,
            "realized_pnl": self.realized_pnl,
            "total_pnl": self.total_pnl
        }
    
    def calculate_margin_ratio(self) -> Decimal:
        """Calculate current margin ratio."""
        if self.maintenance_margin > 0:
            self.margin_ratio = (self.collateral + self.unrealized_pnl) / self.maintenance_margin
        else:
            self.margin_ratio = Decimal("999")
        return self.margin_ratio
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class PositionUpdate(BaseModel):
    """Position update event."""
    position: Position
    update_type: str  # open, update, close, liquidate
    trade_id: Optional[str] = None
    price_update: Optional[Decimal] = None
    quantity_change: Optional[Decimal] = None
    realized_pnl_change: Optional[Decimal] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class PositionEvent(BaseModel):
    """Position event for publishing."""
    event_type: str = Field(default="position_update")
    user_id: str
    position_update: PositionUpdate
    risk_metrics: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        } 