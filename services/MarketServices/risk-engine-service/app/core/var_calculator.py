"""Value at Risk (VaR) calculator."""

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional
import numpy as np
from scipy import stats

from platformq_risk_common import calculate_var, calculate_cvar

logger = logging.getLogger(__name__)


class VaRCalculator:
    """Calculates Value at Risk for positions and portfolios."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.confidence_level = config.get("var_confidence_level", 0.95)
        self.time_horizon_days = config.get("var_time_horizon_days", 1)
        self.lookback_days = config.get("var_lookback_days", 30)
        self.method = config.get("var_method", "historical")
        
    async def calculate_position_var(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any],
        confidence_level: Optional[float] = None,
        time_horizon_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """Calculate VaR for a single position."""
        if confidence_level is None:
            confidence_level = self.confidence_level
        if time_horizon_days is None:
            time_horizon_days = self.time_horizon_days
            
        # Get historical returns
        returns = market_data.get("historical_returns", [])
        if not returns:
            returns = self._simulate_returns(market_data)
        
        # Scale returns to time horizon
        scaled_returns = self._scale_returns(returns, time_horizon_days)
        
        # Calculate position value
        mark_price = Decimal(str(market_data.get("price", "0")))
        quantity = Decimal(str(position.get("quantity", "0")))
        contract_size = Decimal(str(position.get("contract_size", "1")))
        position_value = abs(quantity) * contract_size * mark_price
        
        # Calculate VaR
        var_percentage = calculate_var(
            scaled_returns,
            confidence_level,
            self.method
        )
        
        var_amount = position_value * Decimal(str(var_percentage))
        
        return {
            "var_amount": var_amount,
            "var_percentage": Decimal(str(var_percentage)),
            "position_value": position_value,
            "confidence_level": confidence_level,
            "time_horizon_days": time_horizon_days
        }
    
    async def calculate_portfolio_var(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        confidence_level: Optional[float] = None,
        time_horizon_days: Optional[int] = None,
        method: Optional[str] = None,
        lookback_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """Calculate VaR for a portfolio."""
        if confidence_level is None:
            confidence_level = self.confidence_level
        if time_horizon_days is None:
            time_horizon_days = self.time_horizon_days
        if method is None:
            method = self.method
        if lookback_days is None:
            lookback_days = self.lookback_days
        
        # Calculate portfolio value
        portfolio_value = Decimal("0")
        position_values = []
        returns_matrix = []
        
        for position in positions:
            market_id = position.get("market_id")
            if market_id not in market_data:
                continue
                
            # Get position value
            mark_price = Decimal(str(market_data[market_id].get("price", "0")))
            quantity = Decimal(str(position.get("quantity", "0")))
            contract_size = Decimal(str(position.get("contract_size", "1")))
            position_value = quantity * contract_size * mark_price  # Keep sign for direction
            
            portfolio_value += abs(position_value)
            position_values.append(float(position_value))
            
            # Get returns
            returns = market_data[market_id].get("historical_returns", [])
            if not returns:
                returns = self._simulate_returns(market_data[market_id])
            returns_matrix.append(returns[:lookback_days])
        
        if not position_values:
            return {
                "var_amount": Decimal("0"),
                "var_percentage": Decimal("0"),
                "portfolio_value": Decimal("0")
            }
        
        # Convert to numpy arrays
        position_values = np.array(position_values)
        returns_matrix = np.array(returns_matrix)
        
        # Calculate portfolio returns
        portfolio_returns = np.dot(position_values, returns_matrix) / float(portfolio_value)
        
        # Scale returns to time horizon
        scaled_returns = portfolio_returns * np.sqrt(time_horizon_days)
        
        # Calculate VaR based on method
        if method == "historical":
            var_percentage = np.percentile(scaled_returns, (1 - confidence_level) * 100)
        elif method == "parametric":
            mean = np.mean(scaled_returns)
            std = np.std(scaled_returns)
            var_percentage = mean - stats.norm.ppf(confidence_level) * std
        else:  # monte_carlo
            var_percentage = self._monte_carlo_var(scaled_returns, confidence_level)
        
        var_amount = portfolio_value * abs(Decimal(str(var_percentage)))
        
        return {
            "var_amount": var_amount,
            "var_percentage": abs(Decimal(str(var_percentage))),
            "portfolio_value": portfolio_value,
            "confidence_level": confidence_level,
            "time_horizon_days": time_horizon_days,
            "method": method
        }
    
    async def calculate_portfolio_cvar(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        confidence_level: Optional[float] = None,
        time_horizon_days: Optional[int] = None,
        method: Optional[str] = None,
        lookback_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """Calculate Conditional VaR (CVaR) for a portfolio."""
        # First calculate regular VaR
        var_result = await self.calculate_portfolio_var(
            positions, market_data, confidence_level, time_horizon_days, method, lookback_days
        )
        
        # Calculate CVaR (expected shortfall)
        # This is simplified - in production would be more sophisticated
        cvar_multiplier = Decimal("1.2")  # CVaR is typically 20% higher than VaR
        cvar_amount = var_result["var_amount"] * cvar_multiplier
        cvar_percentage = var_result["var_percentage"] * cvar_multiplier
        
        return {
            "cvar_amount": cvar_amount,
            "cvar_percentage": cvar_percentage,
            "var_amount": var_result["var_amount"],
            "var_percentage": var_result["var_percentage"],
            "portfolio_value": var_result["portfolio_value"]
        }
    
    async def backtest_var(
        self,
        portfolio_id: str,
        start_date: datetime,
        end_date: datetime,
        confidence_level: float
    ) -> Dict[str, Any]:
        """Backtest VaR model performance."""
        # Simplified backtesting - in production would fetch historical data
        total_days = (end_date - start_date).days
        expected_breaches = int(total_days * (1 - confidence_level))
        
        # Simulate some breaches for demo
        actual_breaches = np.random.poisson(expected_breaches)
        breach_dates = []
        breach_sequence = []
        
        for i in range(total_days):
            if i < actual_breaches:
                breach_dates.append(start_date + timedelta(days=i))
                breach_sequence.append(1)
            else:
                breach_sequence.append(0)
        
        return {
            "portfolio_id": portfolio_id,
            "start_date": start_date,
            "end_date": end_date,
            "total_days": total_days,
            "breaches": actual_breaches,
            "expected_breaches": expected_breaches,
            "breach_percentage": actual_breaches / total_days if total_days > 0 else 0,
            "breach_dates": breach_dates,
            "breach_sequence": breach_sequence
        }
    
    def kupiec_test(self, breaches: int, total_observations: int, confidence_level: float) -> Dict[str, Any]:
        """Perform Kupiec test for VaR model validation."""
        expected_breaches = total_observations * (1 - confidence_level)
        
        # Calculate likelihood ratio
        if breaches == 0:
            lr = -2 * total_observations * np.log(1 - confidence_level)
        elif breaches == total_observations:
            lr = -2 * total_observations * np.log(confidence_level)
        else:
            lr = -2 * (
                breaches * np.log(breaches / expected_breaches) +
                (total_observations - breaches) * np.log(
                    (total_observations - breaches) / (total_observations - expected_breaches)
                )
            )
        
        # Chi-square test with 1 degree of freedom
        p_value = 1 - stats.chi2.cdf(lr, 1)
        
        return {
            "test_statistic": lr,
            "p_value": p_value,
            "reject_model": p_value < 0.05,
            "breaches": breaches,
            "expected_breaches": expected_breaches
        }
    
    def christoffersen_test(self, breach_sequence: List[int], confidence_level: float) -> Dict[str, Any]:
        """Perform Christoffersen test for independence of VaR breaches."""
        # Count transitions
        n00 = n01 = n10 = n11 = 0
        
        for i in range(1, len(breach_sequence)):
            if breach_sequence[i-1] == 0 and breach_sequence[i] == 0:
                n00 += 1
            elif breach_sequence[i-1] == 0 and breach_sequence[i] == 1:
                n01 += 1
            elif breach_sequence[i-1] == 1 and breach_sequence[i] == 0:
                n10 += 1
            else:  # 1 to 1
                n11 += 1
        
        # Calculate probabilities
        total = n00 + n01 + n10 + n11
        if total == 0:
            return {
                "test_statistic": 0,
                "p_value": 1,
                "reject_independence": False
            }
        
        p01 = n01 / (n00 + n01) if (n00 + n01) > 0 else 0
        p11 = n11 / (n10 + n11) if (n10 + n11) > 0 else 0
        p = (n01 + n11) / total
        
        # Likelihood ratio test
        if p01 == 0 or p11 == 0 or p == 0 or p == 1:
            lr_ind = 0
        else:
            lr_ind = -2 * (
                n00 * np.log(1 - p) + n01 * np.log(p) +
                n10 * np.log(1 - p) + n11 * np.log(p) -
                n00 * np.log(1 - p01) - n01 * np.log(p01) -
                n10 * np.log(1 - p11) - n11 * np.log(p11)
            )
        
        # Chi-square test with 1 degree of freedom
        p_value = 1 - stats.chi2.cdf(lr_ind, 1)
        
        return {
            "test_statistic": lr_ind,
            "p_value": p_value,
            "reject_independence": p_value < 0.05,
            "transition_matrix": {
                "n00": n00, "n01": n01,
                "n10": n10, "n11": n11
            }
        }
    
    def _simulate_returns(self, market_data: Dict[str, Any]) -> List[float]:
        """Simulate returns when historical data is not available."""
        # Simple simulation based on volatility
        volatility = float(market_data.get("volatility", 0.02))  # 2% default
        return list(np.random.normal(0, volatility, self.lookback_days))
    
    def _scale_returns(self, returns: List[float], time_horizon_days: int) -> np.ndarray:
        """Scale returns to the specified time horizon."""
        return np.array(returns) * np.sqrt(time_horizon_days)
    
    def _monte_carlo_var(self, returns: np.ndarray, confidence_level: float, simulations: int = 10000) -> float:
        """Calculate VaR using Monte Carlo simulation."""
        mean = np.mean(returns)
        std = np.std(returns)
        
        # Generate simulations
        simulated_returns = np.random.normal(mean, std, simulations)
        
        # Calculate VaR
        var_percentage = np.percentile(simulated_returns, (1 - confidence_level) * 100)
        return float(var_percentage)
