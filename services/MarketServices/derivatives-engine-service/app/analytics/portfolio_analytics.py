"""
Advanced Portfolio Analytics Engine

Provides sophisticated risk analytics and ML-powered predictions.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from scipy import stats
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class PortfolioMetrics:
    """Comprehensive portfolio metrics"""
    total_value: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    
    # Risk metrics
    var_95: Decimal  # Value at Risk 95%
    var_99: Decimal  # Value at Risk 99%
    cvar_95: Decimal  # Conditional VaR
    max_drawdown: Decimal
    current_drawdown: Decimal
    
    # Performance metrics
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # Greeks (for derivatives)
    portfolio_delta: Decimal
    portfolio_gamma: Decimal
    portfolio_vega: Decimal
    portfolio_theta: Decimal
    
    # Correlation metrics
    correlation_risk: float
    concentration_risk: float
    
    # ML predictions
    predicted_var_1d: Decimal
    predicted_volatility: float
    risk_score: int  # 0-100


class PortfolioAnalytics:
    """
    Advanced analytics for derivative portfolios
    """
    
    def __init__(self):
        self.risk_free_rate = 0.05  # 5% annual
        self.lookback_days = 252  # 1 year
        
    async def analyze_portfolio(
        self,
        positions: List[Dict],
        price_history: Dict[str, pd.Series],
        user_profile: Dict
    ) -> PortfolioMetrics:
        """
        Comprehensive portfolio analysis
        """
        # Calculate current portfolio value
        total_value = await self._calculate_portfolio_value(positions)
        
        # Calculate PnL
        unrealized_pnl, realized_pnl = await self._calculate_pnl(positions)
        
        # Risk metrics
        returns = self._calculate_portfolio_returns(positions, price_history)
        var_95, var_99, cvar_95 = self._calculate_var_metrics(returns, total_value)
        
        # Drawdown analysis
        max_dd, current_dd = self._calculate_drawdowns(returns)
        
        # Performance ratios
        sharpe = self._calculate_sharpe_ratio(returns)
        sortino = self._calculate_sortino_ratio(returns)
        calmar = self._calculate_calmar_ratio(returns, max_dd)
        
        # Portfolio Greeks
        greeks = await self._calculate_portfolio_greeks(positions)
        
        # Risk concentrations
        correlation_risk = self._assess_correlation_risk(positions, price_history)
        concentration_risk = self._assess_concentration_risk(positions)
        
        # ML predictions
        predicted_var = await self._predict_var_ml(positions, price_history)
        predicted_vol = await self._predict_volatility_ml(price_history)
        risk_score = await self._calculate_risk_score(
            positions, returns, user_profile
        )
        
        return PortfolioMetrics(
            total_value=total_value,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=realized_pnl,
            var_95=var_95,
            var_99=var_99,
            cvar_95=cvar_95,
            max_drawdown=max_dd,
            current_drawdown=current_dd,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            portfolio_delta=greeks["delta"],
            portfolio_gamma=greeks["gamma"],
            portfolio_vega=greeks["vega"],
            portfolio_theta=greeks["theta"],
            correlation_risk=correlation_risk,
            concentration_risk=concentration_risk,
            predicted_var_1d=predicted_var,
            predicted_volatility=predicted_vol,
            risk_score=risk_score
        )
        
    async def generate_risk_report(
        self,
        metrics: PortfolioMetrics,
        positions: List[Dict]
    ) -> Dict[str, Any]:
        """
        Generate comprehensive risk report
        """
        report = {
            "summary": {
                "total_value": str(metrics.total_value),
                "risk_score": metrics.risk_score,
                "risk_level": self._get_risk_level(metrics.risk_score),
                "key_risks": await self._identify_key_risks(metrics, positions)
            },
            "risk_metrics": {
                "var_95": str(metrics.var_95),
                "var_99": str(metrics.var_99),
                "cvar_95": str(metrics.cvar_95),
                "max_drawdown": f"{metrics.max_drawdown:.2%}",
                "current_drawdown": f"{metrics.current_drawdown:.2%}"
            },
            "performance": {
                "unrealized_pnl": str(metrics.unrealized_pnl),
                "realized_pnl": str(metrics.realized_pnl),
                "sharpe_ratio": f"{metrics.sharpe_ratio:.2f}",
                "sortino_ratio": f"{metrics.sortino_ratio:.2f}",
                "calmar_ratio": f"{metrics.calmar_ratio:.2f}"
            },
            "greeks": {
                "delta": str(metrics.portfolio_delta),
                "gamma": str(metrics.portfolio_gamma),
                "vega": str(metrics.portfolio_vega),
                "theta": str(metrics.portfolio_theta)
            },
            "concentration": {
                "correlation_risk": f"{metrics.correlation_risk:.2%}",
                "concentration_risk": f"{metrics.concentration_risk:.2%}"
            },
            "predictions": {
                "1d_var": str(metrics.predicted_var_1d),
                "volatility_forecast": f"{metrics.predicted_volatility:.2%}"
            },
            "recommendations": await self._generate_recommendations(metrics, positions)
        }
        
        return report
        
    async def stress_test_portfolio(
        self,
        positions: List[Dict],
        scenarios: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """
        Run stress tests on portfolio
        """
        if scenarios is None:
            scenarios = self._get_default_scenarios()
            
        results = {}
        
        for scenario in scenarios:
            scenario_results = await self._run_scenario(positions, scenario)
            results[scenario["name"]] = scenario_results
            
        return {
            "scenarios": results,
            "worst_case": self._find_worst_case(results),
            "recommendations": self._stress_test_recommendations(results)
        }
        
    def _calculate_portfolio_returns(
        self,
        positions: List[Dict],
        price_history: Dict[str, pd.Series]
    ) -> pd.Series:
        """Calculate historical portfolio returns"""
        # Construct portfolio value series
        portfolio_values = pd.Series(index=price_history[list(price_history.keys())[0]].index)
        
        for date in portfolio_values.index:
            daily_value = 0
            for pos in positions:
                if pos["market"] in price_history:
                    price = price_history[pos["market"]].get(date, 0)
                    daily_value += price * float(pos["quantity"])
            portfolio_values[date] = daily_value
            
        # Calculate returns
        returns = portfolio_values.pct_change().dropna()
        return returns
        
    def _calculate_var_metrics(
        self,
        returns: pd.Series,
        portfolio_value: Decimal
    ) -> Tuple[Decimal, Decimal, Decimal]:
        """Calculate VaR and CVaR metrics"""
        if len(returns) < 30:
            return Decimal("0"), Decimal("0"), Decimal("0")
            
        # Calculate VaR
        var_95 = np.percentile(returns, 5)
        var_99 = np.percentile(returns, 1)
        
        # Calculate CVaR (expected shortfall)
        cvar_95 = returns[returns <= var_95].mean()
        
        # Convert to portfolio values
        var_95_value = portfolio_value * Decimal(str(abs(var_95)))
        var_99_value = portfolio_value * Decimal(str(abs(var_99)))
        cvar_95_value = portfolio_value * Decimal(str(abs(cvar_95)))
        
        return var_95_value, var_99_value, cvar_95_value
        
    def _calculate_drawdowns(self, returns: pd.Series) -> Tuple[float, float]:
        """Calculate maximum and current drawdown"""
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        
        max_drawdown = drawdown.min()
        current_drawdown = drawdown.iloc[-1]
        
        return float(max_drawdown), float(current_drawdown)
        
    def _calculate_sharpe_ratio(self, returns: pd.Series) -> float:
        """Calculate Sharpe ratio"""
        if len(returns) < 30:
            return 0.0
            
        excess_returns = returns - self.risk_free_rate / 252
        return float(np.sqrt(252) * excess_returns.mean() / returns.std())
        
    def _calculate_sortino_ratio(self, returns: pd.Series) -> float:
        """Calculate Sortino ratio (downside deviation)"""
        if len(returns) < 30:
            return 0.0
            
        excess_returns = returns - self.risk_free_rate / 252
        downside_returns = returns[returns < 0]
        
        if len(downside_returns) == 0:
            return float('inf')
            
        downside_std = downside_returns.std()
        if downside_std == 0:
            return float('inf')
            
        return float(np.sqrt(252) * excess_returns.mean() / downside_std)
        
    def _calculate_calmar_ratio(self, returns: pd.Series, max_drawdown: float) -> float:
        """Calculate Calmar ratio (return / max drawdown)"""
        if max_drawdown == 0:
            return float('inf')
            
        annual_return = (1 + returns.mean()) ** 252 - 1
        return float(annual_return / abs(max_drawdown))
        
    async def _calculate_portfolio_greeks(self, positions: List[Dict]) -> Dict[str, Decimal]:
        """Aggregate Greeks across portfolio"""
        total_delta = Decimal("0")
        total_gamma = Decimal("0")
        total_vega = Decimal("0")
        total_theta = Decimal("0")
        
        for pos in positions:
            if "greeks" in pos:
                total_delta += Decimal(str(pos["greeks"].get("delta", 0)))
                total_gamma += Decimal(str(pos["greeks"].get("gamma", 0)))
                total_vega += Decimal(str(pos["greeks"].get("vega", 0)))
                total_theta += Decimal(str(pos["greeks"].get("theta", 0)))
                
        return {
            "delta": total_delta,
            "gamma": total_gamma,
            "vega": total_vega,
            "theta": total_theta
        }
        
    def _assess_correlation_risk(
        self,
        positions: List[Dict],
        price_history: Dict[str, pd.Series]
    ) -> float:
        """Assess correlation concentration risk"""
        if len(positions) < 2:
            return 0.0
            
        # Create correlation matrix
        markets = [pos["market"] for pos in positions]
        price_data = pd.DataFrame({
            market: price_history.get(market, pd.Series())
            for market in markets
        })
        
        if price_data.empty:
            return 0.0
            
        corr_matrix = price_data.pct_change().corr()
        
        # Calculate average correlation
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
        avg_correlation = corr_matrix.where(mask).stack().mean()
        
        return float(avg_correlation)
        
    def _assess_concentration_risk(self, positions: List[Dict]) -> float:
        """Assess position concentration risk"""
        total_value = sum(
            float(pos.get("current_value", 0))
            for pos in positions
        )
        
        if total_value == 0:
            return 0.0
            
        # Calculate Herfindahl index
        herfindahl = sum(
            (float(pos.get("current_value", 0)) / total_value) ** 2
            for pos in positions
        )
        
        return float(herfindahl)
        
    async def _predict_var_ml(
        self,
        positions: List[Dict],
        price_history: Dict[str, pd.Series]
    ) -> Decimal:
        """ML-based VaR prediction"""
        # Simplified - would use trained model in practice
        returns = self._calculate_portfolio_returns(positions, price_history)
        
        if len(returns) < 30:
            return Decimal("0")
            
        # Use GARCH-like approach
        recent_vol = returns.tail(20).std()
        long_vol = returns.std()
        
        # Weight recent volatility more
        predicted_vol = 0.7 * recent_vol + 0.3 * long_vol
        
        # Predict 1-day VaR
        portfolio_value = sum(
            Decimal(str(pos.get("current_value", 0)))
            for pos in positions
        )
        
        var_1d = portfolio_value * Decimal(str(predicted_vol * 2.33))  # 99% confidence
        
        return var_1d
        
    async def _predict_volatility_ml(self, price_history: Dict[str, pd.Series]) -> float:
        """ML-based volatility prediction"""
        # Simplified EWMA approach
        if not price_history:
            return 0.0
            
        all_returns = []
        for market, prices in price_history.items():
            returns = prices.pct_change().dropna()
            all_returns.append(returns)
            
        if not all_returns:
            return 0.0
            
        combined_returns = pd.concat(all_returns, axis=1).mean(axis=1)
        
        # EWMA volatility
        ewma_vol = combined_returns.ewm(span=20).std().iloc[-1]
        
        return float(ewma_vol * np.sqrt(252))
        
    async def _calculate_risk_score(
        self,
        positions: List[Dict],
        returns: pd.Series,
        user_profile: Dict
    ) -> int:
        """Calculate overall risk score 0-100"""
        score = 0
        
        # Position concentration (0-20 points)
        concentration = self._assess_concentration_risk(positions)
        score += min(20, int(concentration * 100))
        
        # Volatility (0-30 points)
        if len(returns) > 0:
            vol = returns.std() * np.sqrt(252)
            score += min(30, int(vol * 100))
            
        # Leverage (0-20 points)
        total_exposure = sum(
            float(pos.get("notional_value", 0))
            for pos in positions
        )
        collateral = float(user_profile.get("total_collateral", 1))
        leverage = total_exposure / collateral if collateral > 0 else 0
        score += min(20, int(leverage * 5))
        
        # Drawdown (0-20 points)
        _, current_dd = self._calculate_drawdowns(returns)
        score += min(20, int(abs(current_dd) * 100))
        
        # Liquidity risk (0-10 points)
        illiquid_positions = sum(
            1 for pos in positions
            if pos.get("liquidity_score", 100) < 50
        )
        score += min(10, illiquid_positions * 2)
        
        return min(100, score)
        
    def _get_risk_level(self, risk_score: int) -> str:
        """Convert risk score to risk level"""
        if risk_score < 20:
            return "Very Low"
        elif risk_score < 40:
            return "Low"
        elif risk_score < 60:
            return "Medium"
        elif risk_score < 80:
            return "High"
        else:
            return "Very High"
            
    async def _identify_key_risks(
        self,
        metrics: PortfolioMetrics,
        positions: List[Dict]
    ) -> List[str]:
        """Identify main risk factors"""
        risks = []
        
        if metrics.concentration_risk > 0.5:
            risks.append("High position concentration")
            
        if metrics.correlation_risk > 0.7:
            risks.append("High correlation between positions")
            
        if abs(metrics.current_drawdown) > 0.1:
            risks.append(f"Significant drawdown: {metrics.current_drawdown:.1%}")
            
        if metrics.portfolio_gamma > metrics.total_value * Decimal("0.01"):
            risks.append("High gamma exposure")
            
        if metrics.predicted_volatility > 0.5:
            risks.append("Elevated volatility forecast")
            
        return risks
        
    async def _generate_recommendations(
        self,
        metrics: PortfolioMetrics,
        positions: List[Dict]
    ) -> List[Dict]:
        """Generate risk management recommendations"""
        recommendations = []
        
        if metrics.concentration_risk > 0.5:
            recommendations.append({
                "type": "diversification",
                "priority": "high",
                "action": "Consider diversifying portfolio across more markets",
                "impact": "Reduce concentration risk by 30-50%"
            })
            
        if metrics.portfolio_delta > metrics.total_value * Decimal("2"):
            recommendations.append({
                "type": "hedging",
                "priority": "high",
                "action": "Delta is too high - consider delta hedging",
                "impact": f"Current delta: {metrics.portfolio_delta}"
            })
            
        if metrics.sharpe_ratio < 0.5:
            recommendations.append({
                "type": "optimization",
                "priority": "medium",
                "action": "Portfolio efficiency is low - consider rebalancing",
                "impact": "Potential to improve Sharpe ratio to 1.0+"
            })
            
        return recommendations
        
    def _get_default_scenarios(self) -> List[Dict]:
        """Get default stress test scenarios"""
        return [
            {
                "name": "Market Crash",
                "shocks": {
                    "all_markets": -0.20,  # 20% drop
                    "volatility": 2.0  # Double volatility
                }
            },
            {
                "name": "Flash Crash",
                "shocks": {
                    "all_markets": -0.10,  # 10% instant drop
                    "liquidity": -0.50  # 50% liquidity reduction
                }
            },
            {
                "name": "Correlation Breakdown",
                "shocks": {
                    "correlation": 1.0,  # All correlations go to 1
                    "volatility": 1.5
                }
            },
            {
                "name": "Platform Risk",
                "shocks": {
                    "platform_token": -0.50,  # 50% platform token drop
                    "collateral_haircut": 0.20  # 20% additional haircut
                }
            }
        ]
        
    async def _run_scenario(
        self,
        positions: List[Dict],
        scenario: Dict
    ) -> Dict[str, Any]:
        """Run a single stress test scenario"""
        # Apply shocks to positions
        shocked_value = Decimal("0")
        original_value = Decimal("0")
        
        for pos in positions:
            original = Decimal(str(pos.get("current_value", 0)))
            original_value += original
            
            # Apply market shock
            shock = Decimal(str(scenario["shocks"].get("all_markets", 0)))
            market_shock = Decimal(str(scenario["shocks"].get(pos["market"], 0)))
            total_shock = shock + market_shock
            
            shocked = original * (1 + total_shock)
            shocked_value += shocked
            
        pnl = shocked_value - original_value
        pnl_percent = pnl / original_value if original_value > 0 else Decimal("0")
        
        return {
            "scenario": scenario["name"],
            "shocked_value": str(shocked_value),
            "pnl": str(pnl),
            "pnl_percent": f"{pnl_percent:.2%}",
            "margin_call": shocked_value < original_value * Decimal("0.8")
        }
        
    def _find_worst_case(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Find worst case scenario"""
        worst = None
        worst_pnl = 0
        
        for scenario_name, result in results.items():
            pnl = float(result["pnl"])
            if pnl < worst_pnl:
                worst_pnl = pnl
                worst = result
                
        return worst
        
    def _stress_test_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate recommendations from stress tests"""
        recommendations = []
        
        margin_calls = sum(
            1 for r in results.values()
            if r.get("margin_call", False)
        )
        
        if margin_calls > 0:
            recommendations.append(
                f"Portfolio fails {margin_calls} stress scenarios - reduce leverage"
            )
            
        return recommendations
        
    async def _calculate_portfolio_value(self, positions: List[Dict]) -> Decimal:
        """Calculate total portfolio value"""
        return sum(
            Decimal(str(pos.get("current_value", 0)))
            for pos in positions
        )
        
    async def _calculate_pnl(self, positions: List[Dict]) -> Tuple[Decimal, Decimal]:
        """Calculate unrealized and realized PnL"""
        unrealized = sum(
            Decimal(str(pos.get("unrealized_pnl", 0)))
            for pos in positions
        )
        
        realized = sum(
            Decimal(str(pos.get("realized_pnl", 0)))
            for pos in positions
        )
        
        return unrealized, realized 