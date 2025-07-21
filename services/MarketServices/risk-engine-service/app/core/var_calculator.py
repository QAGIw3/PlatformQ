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
        
        # Calculate CVaR (Conditional VaR)
        cvar_percentage = calculate_cvar(
            scaled_returns,
            confidence_level,
            var_percentage
        )
        cvar_amount = position_value * Decimal(str(cvar_percentage))
        
        # Adjust for position side
        side = position.get("side", "long")
        if side == "short":
            # For short positions, losses come from price increases
            var_amount = -var_amount
            cvar_amount = -cvar_amount
        
        return {
            "var_amount": abs(var_amount),
            "var_percentage": Decimal(str(var_percentage)),
            "cvar_amount": abs(cvar_amount),
            "cvar_percentage": Decimal(str(cvar_percentage)),
            "confidence_level": confidence_level,
            "time_horizon_days": time_horizon_days,
            "method": self.method,
            "position_value": position_value
        }
    
    async def calculate_portfolio_var(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        confidence_level: Optional[float] = None,
        time_horizon_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """Calculate VaR for a portfolio of positions."""
        if confidence_level is None:
            confidence_level = self.confidence_level
        if time_horizon_days is None:
            time_horizon_days = self.time_horizon_days
        
        if not positions:
            return {
                "var_amount": Decimal("0"),
                "var_percentage": Decimal("0"),
                "cvar_amount": Decimal("0"),
                "cvar_percentage": Decimal("0"),
                "confidence_level": confidence_level,
                "time_horizon_days": time_horizon_days,
                "method": self.method,
                "portfolio_value": Decimal("0")
            }
        
        # Calculate portfolio returns considering correlations
        portfolio_returns = self._calculate_portfolio_returns(
            positions,
            market_data
        )
        
        # Scale returns to time horizon
        scaled_returns = self._scale_returns(portfolio_returns, time_horizon_days)
        
        # Calculate total portfolio value
        portfolio_value = Decimal("0")
        for position in positions:
            market_id = position.get("market_id")
            if market_id in market_data:
                mark_price = Decimal(str(market_data[market_id].get("price", "0")))
                quantity = Decimal(str(position.get("quantity", "0")))
                contract_size = Decimal(str(position.get("contract_size", "1")))
                portfolio_value += abs(quantity) * contract_size * mark_price
        
        # Calculate portfolio VaR
        var_percentage = calculate_var(
            scaled_returns,
            confidence_level,
            self.method
        )
        var_amount = portfolio_value * Decimal(str(var_percentage))
        
        # Calculate portfolio CVaR
        cvar_percentage = calculate_cvar(
            scaled_returns,
            confidence_level,
            var_percentage
        )
        cvar_amount = portfolio_value * Decimal(str(cvar_percentage))
        
        # Component VaR (contribution of each position)
        component_var = await self._calculate_component_var(
            positions,
            market_data,
            var_amount
        )
        
        return {
            "var_amount": var_amount,
            "var_percentage": Decimal(str(var_percentage)),
            "cvar_amount": cvar_amount,
            "cvar_percentage": Decimal(str(cvar_percentage)),
            "confidence_level": confidence_level,
            "time_horizon_days": time_horizon_days,
            "method": self.method,
            "portfolio_value": portfolio_value,
            "component_var": component_var
        }
    
    def _simulate_returns(self, market_data: Dict[str, Any]) -> List[float]:
        """Simulate returns when historical data is not available."""
        volatility = float(market_data.get("volatility", 0.2))
        drift = float(market_data.get("drift", 0.0))
        
        # Generate returns using normal distribution
        returns = np.random.normal(
            drift / 252,  # Daily drift
            volatility / np.sqrt(252),  # Daily volatility
            self.lookback_days
        )
        
        return returns.tolist()
    
    def _scale_returns(self, returns: List[float], time_horizon_days: int) -> List[float]:
        """Scale returns to the specified time horizon."""
        if time_horizon_days == 1:
            return returns
        
        # Scale by square root of time
        scaling_factor = np.sqrt(time_horizon_days)
        return [r * scaling_factor for r in returns]
    
    def _calculate_portfolio_returns(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]]
    ) -> List[float]:
        """Calculate portfolio returns considering correlations."""
        # Get returns for each asset
        asset_returns = {}
        weights = {}
        total_value = Decimal("0")
        
        for position in positions:
            market_id = position.get("market_id")
            if market_id not in market_data:
                continue
                
            # Get returns
            returns = market_data[market_id].get("historical_returns", [])
            if not returns:
                returns = self._simulate_returns(market_data[market_id])
            
            asset_returns[market_id] = returns
            
            # Calculate weight
            mark_price = Decimal(str(market_data[market_id].get("price", "0")))
            quantity = Decimal(str(position.get("quantity", "0")))
            contract_size = Decimal(str(position.get("contract_size", "1")))
            position_value = quantity * contract_size * mark_price  # Keep sign for long/short
            
            weights[market_id] = float(position_value)
            total_value += abs(position_value)
        
        # Normalize weights
        if total_value > 0:
            for market_id in weights:
                weights[market_id] /= float(total_value)
        
        # Calculate portfolio returns
        portfolio_returns = []
        min_length = min(len(returns) for returns in asset_returns.values())
        
        for i in range(min_length):
            portfolio_return = sum(
                weights[market_id] * asset_returns[market_id][i]
                for market_id in weights
            )
            portfolio_returns.append(portfolio_return)
        
        return portfolio_returns
    
    async def _calculate_component_var(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        portfolio_var: Decimal
    ) -> Dict[str, Decimal]:
        """Calculate VaR contribution of each position."""
        component_var = {}
        
        # Simple allocation based on position value
        # More sophisticated methods would use marginal VaR
        total_value = Decimal("0")
        position_values = {}
        
        for position in positions:
            market_id = position.get("market_id")
            position_id = position.get("position_id")
            
            if market_id in market_data:
                mark_price = Decimal(str(market_data[market_id].get("price", "0")))
                quantity = Decimal(str(position.get("quantity", "0")))
                contract_size = Decimal(str(position.get("contract_size", "1")))
                position_value = abs(quantity) * contract_size * mark_price
                
                position_values[position_id] = position_value
                total_value += position_value
        
        # Allocate VaR proportionally
        if total_value > 0:
            for position_id, value in position_values.items():
                component_var[position_id] = portfolio_var * (value / total_value)
        
        return component_var
    
    async def calculate_incremental_var(
        self,
        existing_positions: List[Dict[str, Any]],
        new_position: Dict[str, Any],
        market_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate the incremental VaR of adding a new position."""
        # Calculate current portfolio VaR
        current_var = await self.calculate_portfolio_var(
            existing_positions,
            market_data
        )
        
        # Calculate portfolio VaR with new position
        all_positions = existing_positions + [new_position]
        new_var = await self.calculate_portfolio_var(
            all_positions,
            market_data
        )
        
        incremental_var = new_var["var_amount"] - current_var["var_amount"]
        
        return {
            "current_var": current_var["var_amount"],
            "new_var": new_var["var_amount"],
            "incremental_var": incremental_var,
            "incremental_percentage": (
                incremental_var / current_var["var_amount"] 
                if current_var["var_amount"] > 0 
                else Decimal("0")
            )
        }
