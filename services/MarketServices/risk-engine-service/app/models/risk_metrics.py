"""Risk metrics models."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class RiskAlert(BaseModel):
    """Risk alert notification."""
    alert_type: str  # margin_call, leverage_exceeded, concentration_risk, etc.
    severity: str  # info, warning, critical
    message: str
    position_id: Optional[str] = None
    metric_value: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class RiskMetrics(BaseModel):
    """Base risk metrics."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class PositionRisk(RiskMetrics):
    """Risk metrics for a single position."""
    position_id: str
    market_id: str
    user_id: str
    
    # Margin metrics
    initial_margin: Decimal
    maintenance_margin: Decimal
    margin_ratio: Decimal  # collateral / maintenance_margin
    margin_usage: Decimal  # margin used / total collateral
    
    # Value metrics
    notional_value: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    
    # Risk metrics
    var_1d: Decimal  # 1-day Value at Risk
    var_percentage: Decimal
    leverage: Decimal
    liquidation_price: Optional[Decimal] = None
    
    # Greeks (for options)
    delta: Decimal = Decimal("0")
    gamma: Decimal = Decimal("0")
    vega: Decimal = Decimal("0")
    theta: Decimal = Decimal("0")
    
    # Risk scores
    risk_score: int = Field(ge=0, le=100)  # 0-100 scale


class PortfolioRisk(RiskMetrics):
    """Aggregated risk metrics for a portfolio."""
    user_id: str
    total_positions: int
    
    # Value metrics
    total_value: Decimal
    total_collateral: Decimal
    total_unrealized_pnl: Decimal
    
    # Margin metrics
    total_initial_margin: Decimal
    total_maintenance_margin: Decimal
    margin_usage: Decimal
    
    # Risk metrics
    portfolio_var: Decimal
    portfolio_leverage: Decimal
    max_position_leverage: Decimal
    
    # Concentration metrics
    concentration_by_market: Dict[str, Decimal]
    max_concentration: Decimal
    
    # Greeks (aggregated)
    total_delta: Decimal
    total_gamma: Decimal
    total_vega: Decimal
    total_theta: Decimal
    
    # Stress test results
    stress_test_results: Dict[str, Any]
    worst_case_loss: Decimal
    
    # Alerts
    alerts: List[RiskAlert] = Field(default_factory=list)
    risk_score: int = Field(ge=0, le=100) 