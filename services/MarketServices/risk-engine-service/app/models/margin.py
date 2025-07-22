"""Margin-related models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


class MarginRequirement(BaseModel):
    """Margin requirement details."""
    position_id: str
    initial_margin: Decimal
    maintenance_margin: Decimal
    variation_margin: Decimal = Decimal("0")
    margin_ratio: Decimal  # collateral / maintenance_margin
    margin_usage: Decimal  # margin used / total collateral
    liquidation_price: Optional[Decimal] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class MarginCall(BaseModel):
    """Margin call details."""
    call_id: str
    user_id: str
    amount_required: Decimal
    amount_deposited: Decimal = Decimal("0")
    deadline: datetime
    reason: str
    issued_by: str
    issued_at: datetime = Field(default_factory=datetime.utcnow)
    met_at: Optional[datetime] = None
    status: str = "active"  # active, met, expired, cancelled
