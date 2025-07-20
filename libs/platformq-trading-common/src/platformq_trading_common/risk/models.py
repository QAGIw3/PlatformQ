"""Shared risk models and calculations"""

from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
import numpy as np
from scipy import stats


class RiskMetric(Enum):
    """Risk metric types"""
    VAR_95 = "var_95"
    VAR_99 = "var_99"
    CVAR_95 = "cvar_95"
    MAX_DRAWDOWN = "max_drawdown"
    SHARPE_RATIO = "sharpe_ratio"
    SORTINO_RATIO = "sortino_ratio"
    BETA = "beta"
    CORRELATION = "correlation"


class PositionSide(Enum):
    """Position side"""
    LONG = "long"
    SHORT = "short"


@dataclass
class Position:
    """Trading position"""
    position_id: str
    market_id: str
    trader_id: str
    side: PositionSide
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal
    leverage: Decimal = Decimal(1)
    
    # Margin fields
    initial_margin: Decimal = Decimal(0)
    maintenance_margin: Decimal = Decimal(0)
    margin_used: Decimal = Decimal(0)
    
    # P&L fields
    realized_pnl: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    fees_paid: Decimal = Decimal(0)
    
    # Risk fields
    liquidation_price: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    
    # Metadata
    opened_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def notional_value(self) -> Decimal:
        """Calculate notional value"""
        return self.size * self.mark_price
    
    @property
    def pnl(self) -> Decimal:
        """Calculate total P&L"""
        return self.realized_pnl + self.unrealized_pnl - self.fees_paid
    
    @property
    def margin_ratio(self) -> Decimal:
        """Calculate margin ratio"""
        if self.margin_used == 0:
            return Decimal(0)
        return (self.notional_value - self.unrealized_pnl) / self.margin_used
    
    def calculate_unrealized_pnl(self, current_price: Decimal) -> Decimal:
        """Calculate unrealized P&L at given price"""
        if self.side == PositionSide.LONG:
            return self.size * (current_price - self.entry_price)
        else:
            return self.size * (self.entry_price - current_price)
    
    def calculate_liquidation_price(self, maintenance_margin_rate: Decimal) -> Decimal:
        """Calculate liquidation price"""
        if self.side == PositionSide.LONG:
            return self.entry_price * (1 - maintenance_margin_rate / self.leverage)
        else:
            return self.entry_price * (1 + maintenance_margin_rate / self.leverage)


@dataclass
class Portfolio:
    """Portfolio of positions"""
    portfolio_id: str
    trader_id: str
    positions: List[Position] = field(default_factory=list)
    cash_balance: Decimal = Decimal(0)
    
    @property
    def total_value(self) -> Decimal:
        """Calculate total portfolio value"""
        positions_value = sum(p.notional_value + p.unrealized_pnl for p in self.positions)
        return self.cash_balance + positions_value
    
    @property
    def total_margin_used(self) -> Decimal:
        """Calculate total margin used"""
        return sum(p.margin_used for p in self.positions)
    
    @property
    def free_margin(self) -> Decimal:
        """Calculate free margin"""
        return self.cash_balance - self.total_margin_used
    
    @property
    def margin_level(self) -> Decimal:
        """Calculate margin level percentage"""
        if self.total_margin_used == 0:
            return Decimal("999999")  # Infinite
        return (self.total_value / self.total_margin_used) * 100
    
    def add_position(self, position: Position):
        """Add position to portfolio"""
        self.positions.append(position)
    
    def remove_position(self, position_id: str):
        """Remove position from portfolio"""
        self.positions = [p for p in self.positions if p.position_id != position_id]
    
    def get_position(self, position_id: str) -> Optional[Position]:
        """Get position by ID"""
        for position in self.positions:
            if position.position_id == position_id:
                return position
        return None


@dataclass
class RiskLimits:
    """Risk limits for a trader or portfolio"""
    max_position_size: Decimal
    max_leverage: Decimal
    max_loss_per_trade: Decimal
    max_daily_loss: Decimal
    max_open_positions: int
    min_margin_level: Decimal  # Percentage
    concentration_limit: Decimal  # Max % in single asset
    
    # Optional advanced limits
    max_var_95: Optional[Decimal] = None
    max_correlation: Optional[Decimal] = None
    max_beta: Optional[Decimal] = None


@dataclass
class RiskMetrics:
    """Calculated risk metrics for a portfolio"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Basic metrics
    total_exposure: Decimal = Decimal(0)
    net_exposure: Decimal = Decimal(0)
    gross_exposure: Decimal = Decimal(0)
    
    # VaR metrics
    var_95: Decimal = Decimal(0)
    var_99: Decimal = Decimal(0)
    cvar_95: Decimal = Decimal(0)
    
    # Performance metrics
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    current_drawdown: float = 0.0
    
    # Greeks (for derivatives)
    portfolio_delta: Decimal = Decimal(0)
    portfolio_gamma: Decimal = Decimal(0)
    portfolio_vega: Decimal = Decimal(0)
    portfolio_theta: Decimal = Decimal(0)
    
    # Concentration metrics
    largest_position_pct: float = 0.0
    concentration_score: float = 0.0
    
    # Correlation metrics
    average_correlation: float = 0.0
    max_correlation: float = 0.0


class RiskCalculator:
    """Risk calculation utilities"""
    
    @staticmethod
    def calculate_var(returns: np.ndarray, confidence_level: float = 0.95) -> float:
        """Calculate Value at Risk"""
        if len(returns) == 0:
            return 0.0
        
        # Use historical method
        var_percentile = (1 - confidence_level) * 100
        return np.percentile(returns, var_percentile)
    
    @staticmethod
    def calculate_cvar(returns: np.ndarray, confidence_level: float = 0.95) -> float:
        """Calculate Conditional Value at Risk (Expected Shortfall)"""
        if len(returns) == 0:
            return 0.0
        
        var = RiskCalculator.calculate_var(returns, confidence_level)
        # Get returns worse than VaR
        tail_returns = returns[returns <= var]
        
        if len(tail_returns) == 0:
            return var
        
        return np.mean(tail_returns)
    
    @staticmethod
    def calculate_sharpe_ratio(returns: np.ndarray, risk_free_rate: float = 0.0) -> float:
        """Calculate Sharpe ratio"""
        if len(returns) < 2:
            return 0.0
        
        excess_returns = returns - risk_free_rate
        
        if np.std(excess_returns) == 0:
            return 0.0
        
        return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)  # Annualized
    
    @staticmethod
    def calculate_sortino_ratio(returns: np.ndarray, risk_free_rate: float = 0.0) -> float:
        """Calculate Sortino ratio"""
        if len(returns) < 2:
            return 0.0
        
        excess_returns = returns - risk_free_rate
        downside_returns = excess_returns[excess_returns < 0]
        
        if len(downside_returns) == 0 or np.std(downside_returns) == 0:
            return 0.0
        
        return np.mean(excess_returns) / np.std(downside_returns) * np.sqrt(252)
    
    @staticmethod
    def calculate_max_drawdown(equity_curve: np.ndarray) -> float:
        """Calculate maximum drawdown"""
        if len(equity_curve) < 2:
            return 0.0
        
        cumulative_returns = np.cumprod(1 + equity_curve)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdown = (cumulative_returns - running_max) / running_max
        
        return np.min(drawdown)
    
    @staticmethod
    def calculate_portfolio_var(
        positions: List[Position], 
        returns_data: Dict[str, np.ndarray],
        confidence_level: float = 0.95
    ) -> Decimal:
        """Calculate portfolio VaR considering correlations"""
        if not positions or not returns_data:
            return Decimal(0)
        
        # Create portfolio returns
        portfolio_returns = np.zeros(len(next(iter(returns_data.values()))))
        total_value = sum(p.notional_value for p in positions)
        
        for position in positions:
            if position.market_id in returns_data:
                weight = float(position.notional_value / total_value)
                portfolio_returns += weight * returns_data[position.market_id]
        
        var = RiskCalculator.calculate_var(portfolio_returns, confidence_level)
        return Decimal(str(var)) * total_value
    
    @staticmethod
    def check_risk_limits(portfolio: Portfolio, limits: RiskLimits) -> Dict[str, bool]:
        """Check if portfolio violates any risk limits"""
        violations = {}
        
        # Check position size limits
        for position in portfolio.positions:
            if position.size > limits.max_position_size:
                violations[f"position_size_{position.position_id}"] = True
            
            if position.leverage > limits.max_leverage:
                violations[f"leverage_{position.position_id}"] = True
        
        # Check portfolio limits
        if len(portfolio.positions) > limits.max_open_positions:
            violations["max_positions"] = True
        
        if portfolio.margin_level < limits.min_margin_level:
            violations["margin_level"] = True
        
        # Check concentration
        if portfolio.total_value > 0:
            for position in portfolio.positions:
                concentration = (position.notional_value / portfolio.total_value) * 100
                if concentration > limits.concentration_limit:
                    violations[f"concentration_{position.position_id}"] = True
        
        return violations 