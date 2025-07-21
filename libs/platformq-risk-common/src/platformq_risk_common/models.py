"""Common risk models for PlatformQ."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any


class RiskMetric(Enum):
    """Types of risk metrics calculated."""
    VALUE_AT_RISK = "var"  # Value at Risk
    CONDITIONAL_VAR = "cvar"  # Conditional VaR (Expected Shortfall)
    PORTFOLIO_BETA = "beta"
    SHARPE_RATIO = "sharpe"
    MAX_DRAWDOWN = "max_drawdown"
    CORRELATION_RISK = "correlation"
    LIQUIDATION_RISK = "liquidation"
    FUNDING_RISK = "funding"
    DELTA_EXPOSURE = "delta"
    GAMMA_EXPOSURE = "gamma"
    VEGA_EXPOSURE = "vega"
    THETA_EXPOSURE = "theta"
    RHO_EXPOSURE = "rho"


class RiskAlert(Enum):
    """Types of risk alerts."""
    POSITION_LIMIT_WARNING = "position_limit_warning"
    POSITION_LIMIT_BREACH = "position_limit_breach"
    LEVERAGE_WARNING = "leverage_warning"
    LEVERAGE_BREACH = "leverage_breach"
    LIQUIDATION_WARNING = "liquidation_warning"
    CONCENTRATION_WARNING = "concentration_warning"
    VOLATILITY_SPIKE = "volatility_spike"
    CORRELATION_BREAKDOWN = "correlation_breakdown"
    FUNDING_RATE_SPIKE = "funding_rate_spike"
    SYSTEM_RISK_ELEVATED = "system_risk_elevated"
    MARGIN_CALL = "margin_call"
    STOP_LOSS_TRIGGERED = "stop_loss_triggered"


class MitigationAction(Enum):
    """Automated risk mitigation actions."""
    REDUCE_POSITION = "reduce_position"
    DELEVERAGE = "deleverage"
    HEDGE_POSITION = "hedge_position"
    HALT_TRADING = "halt_trading"
    INCREASE_MARGIN = "increase_margin"
    LIQUIDATE_POSITION = "liquidate_position"
    NOTIFY_RISK_TEAM = "notify_risk_team"


class LimitType(Enum):
    """Types of risk limits."""
    POSITION_SIZE = "position_size"
    NOTIONAL_EXPOSURE = "notional_exposure"
    DELTA_LIMIT = "delta_limit"
    GAMMA_LIMIT = "gamma_limit"
    VEGA_LIMIT = "vega_limit"
    VAR_LIMIT = "var_limit"
    LOSS_LIMIT = "loss_limit"
    CONCENTRATION = "concentration"
    LEVERAGE = "leverage"
    MARGIN_USAGE = "margin_usage"
    DRAWDOWN_LIMIT = "drawdown_limit"
    CORRELATION_LIMIT = "correlation_limit"


class LimitAction(Enum):
    """Actions when limit is breached."""
    BLOCK = "block"  # Block new trades
    WARN = "warn"  # Allow but warn
    REDUCE_ONLY = "reduce_only"  # Only allow reducing positions
    LIQUIDATE = "liquidate"  # Force liquidation
    NOTIFY = "notify"  # Notify risk team
    ESCALATE = "escalate"  # Escalate to senior management


@dataclass
class RiskProfile:
    """Comprehensive risk profile for a position or portfolio."""
    timestamp: datetime
    var_95: Decimal  # 95% VaR
    var_99: Decimal  # 99% VaR
    cvar_95: Decimal  # 95% CVaR
    cvar_99: Optional[Decimal] = None
    portfolio_beta: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: Decimal = Decimal("0")
    liquidation_price: Optional[Decimal] = None
    margin_ratio: Decimal = Decimal("0")
    risk_score: int = 0  # 0-100 risk score
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    stress_test_results: Optional[Dict[str, Decimal]] = None
    # Greeks exposure
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    rho: Optional[Decimal] = None


@dataclass
class RiskLimit:
    """Individual risk limit configuration."""
    limit_id: str
    limit_type: LimitType
    limit_value: Decimal
    current_value: Decimal = Decimal("0")
    warning_threshold: Decimal = Decimal("0.8")  # 80% of limit
    action: LimitAction = LimitAction.WARN
    applies_to: str = "portfolio"  # portfolio, account, position
    active: bool = True
    last_checked: Optional[datetime] = None
    last_breach: Optional[datetime] = None
    breach_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def utilization(self) -> Decimal:
        """Calculate limit utilization percentage."""
        if self.limit_value == 0:
            return Decimal("0")
        return (self.current_value / self.limit_value) * Decimal("100")

    @property
    def is_breached(self) -> bool:
        """Check if limit is breached."""
        return self.current_value > self.limit_value

    @property
    def is_warning(self) -> bool:
        """Check if limit is in warning zone."""
        return self.current_value > (self.limit_value * self.warning_threshold)


@dataclass
class StressTestScenario:
    """Stress test scenario definition."""
    scenario_id: str
    name: str
    description: str
    market_shocks: Dict[str, Decimal]  # Asset -> price change %
    volatility_shocks: Dict[str, Decimal]  # Asset -> vol change
    correlation_shocks: Optional[Dict[str, Dict[str, Decimal]]] = None
    interest_rate_shock: Optional[Decimal] = None
    liquidity_haircuts: Optional[Dict[str, Decimal]] = None
    severity: str = "moderate"  # mild, moderate, severe, extreme


@dataclass
class RiskReport:
    """Comprehensive risk report."""
    report_id: str
    timestamp: datetime
    portfolio_id: str
    risk_profile: RiskProfile
    limit_breaches: List[RiskLimit]
    active_alerts: List[Dict[str, Any]]
    recommendations: List[Dict[str, Any]]
    stress_test_results: Optional[Dict[str, Dict[str, Decimal]]] = None
    attribution: Optional[Dict[str, Decimal]] = None
    metadata: Dict[str, Any] = field(default_factory=dict) 