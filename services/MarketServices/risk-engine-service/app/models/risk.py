"""Risk models and data structures."""

from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel, Field
from dataclasses import dataclass, field


class RiskLevel(str, Enum):
    """Risk level classifications."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertLevel(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    HIGH = "high"
    CRITICAL = "critical"


class RiskType(str, Enum):
    """Types of risk."""
    MARKET = "market"
    POSITION = "position"
    LEVERAGE = "leverage"
    CONCENTRATION = "concentration"
    LIQUIDATION = "liquidation"
    SYSTEMIC = "systemic"


@dataclass
class RiskMetrics:
    """Core risk metrics for a portfolio or position."""
    var_95: Decimal
    var_99: Decimal
    cvar_95: Decimal
    cvar_99: Decimal
    total_exposure: Decimal
    net_exposure: Decimal
    leverage: Decimal
    largest_position_pct: Decimal
    concentration_score: Decimal


@dataclass
class MarginStatus:
    """Margin status information."""
    margin_level: Decimal  # Percentage
    margin_used: Decimal
    free_margin: Decimal
    equity: Decimal
    health_status: str  # "healthy", "warning", "critical"


@dataclass
class MarketRisk:
    """Market-level risk assessment"""
    market_id: str
    timestamp: datetime
    current_volatility: Decimal
    predicted_volatility: Decimal
    anomaly_score: float
    var_95: Decimal  # Value at Risk at 95% confidence
    var_99: Decimal  # Value at Risk at 99% confidence
    liquidity_score: Decimal
    correlation_risk: Decimal
    recommended_params: Dict[str, Decimal]
    risk_level: str  # "low", "medium", "high", "critical"
    warnings: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for API responses"""
        return {
            "market_id": self.market_id,
            "timestamp": self.timestamp.isoformat(),
            "current_volatility": str(self.current_volatility),
            "predicted_volatility": str(self.predicted_volatility),
            "anomaly_score": self.anomaly_score,
            "var_95": str(self.var_95),
            "var_99": str(self.var_99),
            "liquidity_score": str(self.liquidity_score),
            "correlation_risk": str(self.correlation_risk),
            "recommended_params": {k: str(v) for k, v in self.recommended_params.items()},
            "risk_level": self.risk_level,
            "warnings": self.warnings
        }


@dataclass
class PositionRisk:
    """Position-specific risk assessment"""
    position_id: str
    market_risk: MarketRisk
    liquidation_probability: Decimal
    expected_shortfall: Decimal
    margin_utilization: Decimal
    health_factor: Decimal
    stress_test_results: List[Dict]
    recommendations: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for API responses"""
        return {
            "position_id": self.position_id,
            "market_risk": self.market_risk.to_dict(),
            "liquidation_probability": str(self.liquidation_probability),
            "expected_shortfall": str(self.expected_shortfall),
            "margin_utilization": str(self.margin_utilization),
            "health_factor": str(self.health_factor),
            "stress_test_results": self.stress_test_results,
            "recommendations": self.recommendations
        }


@dataclass
class RiskState:
    """Real-time risk state for a user"""
    user_id: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Current risk levels
    overall_risk_level: RiskLevel = RiskLevel.LOW
    margin_status: Optional[MarginStatus] = None
    
    # Metrics
    total_collateral: Decimal = Decimal("0")
    margin_used: Decimal = Decimal("0")
    position_count: int = 0
    
    # Alerts
    active_alerts: List[Dict] = field(default_factory=list)
    has_critical_alerts: bool = False
    has_high_alerts: bool = False
    
    # Risk violations
    violations: List[Dict] = field(default_factory=list)
    
    def update_from_monitoring(
        self,
        risk_metrics: Dict[str, Any],
        margin_status: MarginStatus,
        alerts: List[Dict],
        violations: List[Dict]
    ):
        """Update state from monitoring result"""
        self.timestamp = datetime.utcnow()
        self.margin_status = margin_status
        self.active_alerts = alerts
        self.violations = violations
        
        # Update alert flags
        self.has_critical_alerts = any(a.get("severity") == "critical" for a in alerts)
        self.has_high_alerts = any(a.get("severity") == "high" for a in alerts)
        
        # Update risk level
        if self.has_critical_alerts or margin_status.health_status == "critical":
            self.overall_risk_level = RiskLevel.CRITICAL
        elif self.has_high_alerts or margin_status.health_status == "warning":
            self.overall_risk_level = RiskLevel.HIGH
        elif violations:
            self.overall_risk_level = RiskLevel.MEDIUM
        else:
            self.overall_risk_level = RiskLevel.LOW


# API Request/Response Models

class RiskLimitsRequest(BaseModel):
    """Request to set risk limits."""
    max_position_size: Decimal = Field(..., gt=0)
    max_leverage: Decimal = Field(..., ge=1, le=100)
    max_loss_per_trade: Decimal = Field(..., gt=0)
    max_daily_loss: Decimal = Field(..., gt=0)
    max_open_positions: int = Field(..., ge=1, le=100)
    min_margin_level: Decimal = Field(..., ge=100)  # Minimum 100%
    concentration_limit: Decimal = Field(..., ge=0, le=100)  # Percentage


class RiskCheckResponse(BaseModel):
    """Response for risk check."""
    user_id: str
    timestamp: datetime
    margin_level: Decimal
    margin_used: Decimal
    free_margin: Decimal
    equity: Decimal
    health_status: str
    alerts: List[Dict]
    violations: List[str]
    actions_required: List[Dict]
    var_95: Optional[Decimal] = None
    total_exposure: Optional[Decimal] = None
    net_exposure: Optional[Decimal] = None
    largest_position_pct: Optional[Decimal] = None
    ml_assessment: Optional[Dict] = None


class MarketRiskAssessmentResponse(BaseModel):
    """Response for market risk assessment."""
    market_id: str
    timestamp: datetime
    risk_assessment: Dict
    recommended_parameters: Dict[str, str]
    warnings: List[str]


class PositionRiskAssessmentResponse(BaseModel):
    """Response for position risk assessment."""
    position_id: str
    timestamp: datetime
    risk_assessment: Dict
    stress_test_results: List[Dict]
    recommendations: List[str]


class RiskMonitoringStatusResponse(BaseModel):
    """Response for monitoring status."""
    monitored_users: int
    total_positions: int
    total_margin_used: str
    users_at_risk: int
    ml_predictions_active: int
    cache_status: Dict[str, int]
    ml_engine_status: Dict[str, Any] 