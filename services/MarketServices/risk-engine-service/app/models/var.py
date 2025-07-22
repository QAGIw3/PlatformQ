"""Value at Risk (VaR) models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class VaRParameters(BaseModel):
    """Parameters for VaR calculation."""
    confidence_level: float = Field(0.95, ge=0.9, le=0.99)
    time_horizon_days: int = Field(1, ge=1, le=30)
    lookback_days: int = Field(30, ge=10, le=365)
    method: str = Field("historical", pattern="^(historical|parametric|monte_carlo)$")
    include_correlations: bool = True


class VaRResult(BaseModel):
    """Result of VaR calculation."""
    portfolio_id: str
    var_amount: Decimal
    var_percentage: Decimal
    cvar_amount: Optional[Decimal] = None  # Conditional VaR
    cvar_percentage: Optional[Decimal] = None
    confidence_level: float
    time_horizon_days: int
    method: str
    positions_count: int
    calculation_time_ms: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Additional metrics
    expected_shortfall: Optional[Decimal] = None
    max_loss: Optional[Decimal] = None
    volatility: Optional[Decimal] = None
