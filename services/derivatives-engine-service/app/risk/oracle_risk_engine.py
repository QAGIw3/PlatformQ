"""
Oracle-Powered Risk Engine

Real-time risk calculation engine that uses aggregated oracle price feeds
for dynamic portfolio risk assessment, margin requirements, and liquidation triggers.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum
import logging
import numpy as np
from dataclasses import dataclass

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from app.models.position import Position
from app.models.market import Market

logger = logging.getLogger(__name__)


class RiskMetric(Enum):
    """Types of risk metrics calculated"""
    VALUE_AT_RISK = "var"  # Value at Risk
    CONDITIONAL_VAR = "cvar"  # Conditional VaR (Expected Shortfall)
    PORTFOLIO_BETA = "beta"
    SHARPE_RATIO = "sharpe"
    MAX_DRAWDOWN = "max_drawdown"
    CORRELATION_RISK = "correlation"
    LIQUIDATION_RISK = "liquidation"
    FUNDING_RISK = "funding"


@dataclass
class RiskProfile:
    """Comprehensive risk profile for a position or portfolio"""
    timestamp: datetime
    var_95: Decimal  # 95% VaR
    var_99: Decimal  # 99% VaR
    cvar_95: Decimal  # 95% CVaR
    portfolio_beta: float
    sharpe_ratio: float
    max_drawdown: Decimal
    liquidation_price: Optional[Decimal]
    margin_ratio: Decimal
    risk_score: int  # 0-100 risk score
    alerts: List[Dict[str, Any]]


class OracleRiskEngine:
    """
    Advanced risk engine powered by real-time oracle price feeds
    """
    
    def __init__(self):
        self.oracle_client = ServiceClient(
            service_name="oracle-aggregator-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        self.graph_client = ServiceClient(
            service_name="graph-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        # Risk calculation parameters
        self.var_confidence_levels = [0.95, 0.99]
        self.lookback_window = 30  # days
        self.update_interval = 10  # seconds
        self.risk_thresholds = {
            "high_risk_margin_ratio": Decimal("0.8"),
            "critical_margin_ratio": Decimal("0.95"),
            "max_correlation": 0.95,
            "min_sharpe": -2.0
        }
        
        # Cache for price history
        self._price_cache: Dict[str, List[Tuple[datetime, Decimal]]] = {}
        self._volatility_cache: Dict[str, float] = {}
        self._correlation_matrix: Optional[np.ndarray] = None
        
    async def calculate_portfolio_risk(
        self,
        tenant_id: str,
        user_id: str,
        positions: List[Position]
    ) -> ProcessingResult:
        """
        Calculate comprehensive risk metrics for a portfolio
        """
        try:
            if not positions:
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data={"message": "No positions to analyze"}
                )
            
            # Get current prices and historical data for all assets
            assets = list(set(p.asset_id for p in positions))
            price_data = await self._get_oracle_prices(assets)
            historical_data = await self._get_historical_prices(assets)
            
            # Calculate individual position risks
            position_risks = []
            total_value = Decimal("0")
            
            for position in positions:
                current_price = price_data.get(position.asset_id, {}).get("price")
                if not current_price:
                    continue
                
                position_value = position.quantity * current_price
                total_value += abs(position_value)
                
                # Calculate position-specific risk
                position_risk = await self._calculate_position_risk(
                    position,
                    current_price,
                    historical_data.get(position.asset_id, [])
                )
                position_risks.append(position_risk)
            
            # Calculate portfolio-level metrics
            portfolio_metrics = await self._calculate_portfolio_metrics(
                positions,
                position_risks,
                price_data,
                historical_data
            )
            
            # Generate risk alerts
            alerts = self._generate_risk_alerts(
                portfolio_metrics,
                position_risks
            )
            
            # Create risk profile
            risk_profile = RiskProfile(
                timestamp=datetime.utcnow(),
                var_95=portfolio_metrics["var_95"],
                var_99=portfolio_metrics["var_99"],
                cvar_95=portfolio_metrics["cvar_95"],
                portfolio_beta=portfolio_metrics["beta"],
                sharpe_ratio=portfolio_metrics["sharpe_ratio"],
                max_drawdown=portfolio_metrics["max_drawdown"],
                liquidation_price=portfolio_metrics.get("worst_case_liquidation"),
                margin_ratio=portfolio_metrics["margin_ratio"],
                risk_score=self._calculate_risk_score(portfolio_metrics),
                alerts=alerts
            )
            
            # Store risk metrics for monitoring
            await self._store_risk_metrics(
                tenant_id,
                user_id,
                risk_profile
            )
            
            # Check if immediate action needed
            if risk_profile.risk_score > 80:
                await self._trigger_risk_mitigation(
                    tenant_id,
                    user_id,
                    risk_profile,
                    positions
                )
            
            logger.info(
                f"Calculated portfolio risk for user {user_id}: "
                f"Risk score {risk_profile.risk_score}, VaR95 {risk_profile.var_95}"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "risk_profile": self._serialize_risk_profile(risk_profile),
                    "position_risks": position_risks,
                    "recommendations": self._generate_recommendations(
                        risk_profile,
                        portfolio_metrics
                    )
                }
            )
            
        except Exception as e:
            logger.error(f"Error calculating portfolio risk: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def monitor_real_time_risk(
        self,
        tenant_id: str,
        user_id: str,
        callback = None
    ):
        """
        Monitor portfolio risk in real-time using oracle price feeds
        """
        try:
            while True:
                # Get current positions
                positions = await self._get_user_positions(tenant_id, user_id)
                
                if positions:
                    # Calculate current risk
                    result = await self.calculate_portfolio_risk(
                        tenant_id,
                        user_id,
                        positions
                    )
                    
                    if result.status == ProcessingStatus.SUCCESS:
                        risk_data = result.data
                        
                        # Call callback if provided
                        if callback:
                            await callback(risk_data)
                        
                        # Check for critical alerts
                        if risk_data["risk_profile"]["risk_score"] > 90:
                            logger.warning(
                                f"Critical risk level for user {user_id}: "
                                f"{risk_data['risk_profile']['risk_score']}"
                            )
                
                # Wait before next update
                await asyncio.sleep(self.update_interval)
                
        except Exception as e:
            logger.error(f"Error in real-time risk monitoring: {str(e)}")
    
    async def calculate_margin_requirements(
        self,
        tenant_id: str,
        positions: List[Position],
        stress_scenario: Optional[str] = None
    ) -> ProcessingResult:
        """
        Calculate dynamic margin requirements based on real-time risk
        """
        try:
            # Get current market conditions
            market_conditions = await self._get_market_conditions()
            
            # Base margin requirements
            base_margins = {}
            stress_margins = {}
            
            for position in positions:
                # Get asset volatility and correlation
                volatility = await self._get_asset_volatility(position.asset_id)
                
                # Base margin = position value * volatility factor
                position_value = abs(position.quantity * position.entry_price)
                volatility_factor = min(volatility * 3, 0.5)  # Cap at 50%
                
                base_margin = position_value * Decimal(str(volatility_factor))
                
                # Adjust for market conditions
                if market_conditions["volatility_regime"] == "high":
                    base_margin *= Decimal("1.5")
                elif market_conditions["volatility_regime"] == "extreme":
                    base_margin *= Decimal("2.0")
                
                base_margins[position.position_id] = base_margin
                
                # Calculate stress test margin
                if stress_scenario:
                    stress_margin = await self._calculate_stress_margin(
                        position,
                        stress_scenario,
                        volatility
                    )
                    stress_margins[position.position_id] = stress_margin
            
            # Portfolio margin benefit (if eligible)
            portfolio_margin = sum(base_margins.values())
            if self._is_portfolio_margin_eligible(positions):
                # Apply portfolio margin reduction
                correlation_benefit = await self._calculate_correlation_benefit(
                    positions
                )
                portfolio_margin *= (Decimal("1") - correlation_benefit)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "base_margins": base_margins,
                    "stress_margins": stress_margins,
                    "portfolio_margin": portfolio_margin,
                    "total_required": max(
                        portfolio_margin,
                        sum(stress_margins.values()) if stress_margins else Decimal("0")
                    ),
                    "market_conditions": market_conditions,
                    "calculation_time": datetime.utcnow().isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Error calculating margin requirements: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def predict_liquidation_scenarios(
        self,
        tenant_id: str,
        positions: List[Position],
        time_horizons: List[int] = [1, 4, 24]  # hours
    ) -> ProcessingResult:
        """
        Predict liquidation scenarios using ML and oracle data
        """
        try:
            scenarios = {}
            
            for horizon in time_horizons:
                # Get price predictions from oracle
                predictions = await self._get_price_predictions(
                    [p.asset_id for p in positions],
                    horizon
                )
                
                # Calculate liquidation probabilities
                liquidation_analysis = {
                    "time_horizon_hours": horizon,
                    "positions_at_risk": [],
                    "portfolio_survival_probability": 1.0
                }
                
                for position in positions:
                    prediction = predictions.get(position.asset_id, {})
                    
                    if not prediction:
                        continue
                    
                    # Calculate liquidation probability
                    liq_prob = self._calculate_liquidation_probability(
                        position,
                        prediction["expected_price"],
                        prediction["price_std"],
                        prediction["extreme_moves"]
                    )
                    
                    if liq_prob > 0.01:  # 1% threshold
                        liquidation_analysis["positions_at_risk"].append({
                            "position_id": position.position_id,
                            "asset": position.asset_id,
                            "liquidation_probability": liq_prob,
                            "expected_loss": self._calculate_expected_loss(
                                position,
                                prediction
                            ),
                            "recommended_action": self._get_risk_mitigation_action(
                                position,
                                liq_prob
                            )
                        })
                    
                    # Update portfolio survival probability
                    liquidation_analysis["portfolio_survival_probability"] *= (
                        1 - liq_prob
                    )
                
                scenarios[f"{horizon}h"] = liquidation_analysis
            
            # Generate hedging recommendations
            hedging_recs = await self._generate_hedging_recommendations(
                positions,
                scenarios
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "liquidation_scenarios": scenarios,
                    "hedging_recommendations": hedging_recs,
                    "risk_summary": self._summarize_liquidation_risk(scenarios)
                }
            )
            
        except Exception as e:
            logger.error(f"Error predicting liquidation scenarios: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _get_oracle_prices(
        self,
        assets: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Get current prices from oracle aggregator"""
        try:
            response = await self.oracle_client.post(
                "/api/v1/prices/batch",
                json={"assets": assets}
            )
            
            if response.status_code == 200:
                return response.json()["prices"]
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching oracle prices: {str(e)}")
            return {}
    
    async def _get_historical_prices(
        self,
        assets: List[str]
    ) -> Dict[str, List[Tuple[datetime, Decimal]]]:
        """Get historical price data for risk calculations"""
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=self.lookback_window)
            
            response = await self.oracle_client.post(
                "/api/v1/prices/historical",
                json={
                    "assets": assets,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "interval": "1h"
                }
            )
            
            if response.status_code == 200:
                historical_data = {}
                for asset, data in response.json()["data"].items():
                    historical_data[asset] = [
                        (datetime.fromisoformat(d["timestamp"]), Decimal(d["price"]))
                        for d in data
                    ]
                return historical_data
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching historical prices: {str(e)}")
            return {}
    
    async def _calculate_position_risk(
        self,
        position: Position,
        current_price: Decimal,
        price_history: List[Tuple[datetime, Decimal]]
    ) -> Dict[str, Any]:
        """Calculate risk metrics for individual position"""
        if not price_history:
            return {
                "position_id": position.position_id,
                "error": "No historical data"
            }
        
        # Calculate returns
        prices = [float(p[1]) for p in price_history]
        returns = np.diff(np.log(prices))
        
        # Position value and PnL
        position_value = position.quantity * current_price
        pnl = position_value - (position.quantity * position.entry_price)
        pnl_percentage = (pnl / (position.quantity * position.entry_price)) * 100
        
        # Risk metrics
        volatility = np.std(returns) * np.sqrt(252)  # Annualized
        var_95 = np.percentile(returns, 5) * float(position_value)
        var_99 = np.percentile(returns, 1) * float(position_value)
        
        # Liquidation analysis
        maintenance_margin = position.initial_margin * Decimal("0.5")
        liquidation_price = self._calculate_liquidation_price(
            position,
            maintenance_margin
        )
        
        distance_to_liquidation = abs(
            (current_price - liquidation_price) / current_price
        ) if liquidation_price else Decimal("1")
        
        return {
            "position_id": position.position_id,
            "asset": position.asset_id,
            "position_value": float(position_value),
            "pnl": float(pnl),
            "pnl_percentage": float(pnl_percentage),
            "volatility": volatility,
            "var_95": var_95,
            "var_99": var_99,
            "liquidation_price": float(liquidation_price) if liquidation_price else None,
            "distance_to_liquidation": float(distance_to_liquidation),
            "margin_usage": float(
                position.used_margin / position.initial_margin
            ) if position.initial_margin > 0 else 0
        }
    
    async def _calculate_portfolio_metrics(
        self,
        positions: List[Position],
        position_risks: List[Dict[str, Any]],
        price_data: Dict[str, Dict[str, Any]],
        historical_data: Dict[str, List[Tuple[datetime, Decimal]]]
    ) -> Dict[str, Any]:
        """Calculate portfolio-level risk metrics"""
        # Portfolio value
        portfolio_value = sum(r["position_value"] for r in position_risks if "position_value" in r)
        
        # Portfolio returns
        portfolio_returns = self._calculate_portfolio_returns(
            positions,
            historical_data
        )
        
        # Risk metrics
        portfolio_volatility = np.std(portfolio_returns) * np.sqrt(252)
        var_95 = Decimal(str(np.percentile(portfolio_returns, 5) * portfolio_value))
        var_99 = Decimal(str(np.percentile(portfolio_returns, 1) * portfolio_value))
        cvar_95 = Decimal(str(
            np.mean([r for r in portfolio_returns if r <= np.percentile(portfolio_returns, 5)])
            * portfolio_value
        ))
        
        # Performance metrics
        risk_free_rate = 0.02  # 2% annual
        excess_returns = portfolio_returns - risk_free_rate / 252
        sharpe_ratio = (
            np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)
            if np.std(excess_returns) > 0 else 0
        )
        
        # Beta calculation (vs market)
        market_returns = await self._get_market_returns()
        portfolio_beta = self._calculate_beta(portfolio_returns, market_returns)
        
        # Drawdown analysis
        cumulative_returns = np.cumprod(1 + portfolio_returns)
        running_max = np.maximum.accumulate(cumulative_returns)
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = Decimal(str(abs(np.min(drawdown))))
        
        # Margin analysis
        total_margin = sum(p.initial_margin for p in positions)
        used_margin = sum(p.used_margin for p in positions)
        margin_ratio = used_margin / total_margin if total_margin > 0 else Decimal("0")
        
        # Worst case liquidation
        worst_case_liquidation = min(
            r.get("liquidation_price", float('inf'))
            for r in position_risks
            if "liquidation_price" in r and r["liquidation_price"] is not None
        )
        
        return {
            "portfolio_value": Decimal(str(portfolio_value)),
            "portfolio_volatility": portfolio_volatility,
            "var_95": var_95,
            "var_99": var_99,
            "cvar_95": cvar_95,
            "sharpe_ratio": sharpe_ratio,
            "beta": portfolio_beta,
            "max_drawdown": max_drawdown,
            "margin_ratio": margin_ratio,
            "worst_case_liquidation": Decimal(str(worst_case_liquidation)) if worst_case_liquidation != float('inf') else None
        }
    
    def _calculate_risk_score(self, metrics: Dict[str, Any]) -> int:
        """Calculate overall risk score (0-100)"""
        score = 0
        
        # Margin ratio component (0-40 points)
        margin_ratio = float(metrics["margin_ratio"])
        if margin_ratio > 0.9:
            score += 40
        elif margin_ratio > 0.8:
            score += 30
        elif margin_ratio > 0.6:
            score += 20
        elif margin_ratio > 0.4:
            score += 10
        
        # Volatility component (0-20 points)
        volatility = metrics["portfolio_volatility"]
        if volatility > 1.0:  # 100% annual vol
            score += 20
        elif volatility > 0.5:
            score += 15
        elif volatility > 0.3:
            score += 10
        elif volatility > 0.2:
            score += 5
        
        # Drawdown component (0-20 points)
        drawdown = float(metrics["max_drawdown"])
        if drawdown > 0.3:  # 30% drawdown
            score += 20
        elif drawdown > 0.2:
            score += 15
        elif drawdown > 0.1:
            score += 10
        elif drawdown > 0.05:
            score += 5
        
        # Sharpe ratio component (0-20 points)
        sharpe = metrics["sharpe_ratio"]
        if sharpe < -1:
            score += 20
        elif sharpe < 0:
            score += 15
        elif sharpe < 0.5:
            score += 10
        elif sharpe < 1:
            score += 5
        
        return min(score, 100)
    
    def _generate_risk_alerts(
        self,
        portfolio_metrics: Dict[str, Any],
        position_risks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate risk alerts based on metrics"""
        alerts = []
        
        # Margin alerts
        margin_ratio = portfolio_metrics["margin_ratio"]
        if margin_ratio > self.risk_thresholds["critical_margin_ratio"]:
            alerts.append({
                "type": "critical",
                "category": "margin",
                "message": f"Critical margin level: {margin_ratio:.1%}",
                "action": "Reduce positions immediately"
            })
        elif margin_ratio > self.risk_thresholds["high_risk_margin_ratio"]:
            alerts.append({
                "type": "warning",
                "category": "margin",
                "message": f"High margin usage: {margin_ratio:.1%}",
                "action": "Consider reducing positions"
            })
        
        # Liquidation alerts
        for risk in position_risks:
            if risk.get("distance_to_liquidation", 1) < 0.1:
                alerts.append({
                    "type": "critical",
                    "category": "liquidation",
                    "message": f"Position {risk['position_id']} near liquidation",
                    "action": "Add margin or reduce position"
                })
        
        # Performance alerts
        if portfolio_metrics["sharpe_ratio"] < self.risk_thresholds["min_sharpe"]:
            alerts.append({
                "type": "warning",
                "category": "performance",
                "message": "Poor risk-adjusted returns",
                "action": "Review strategy"
            })
        
        # Drawdown alerts
        if portfolio_metrics["max_drawdown"] > Decimal("0.2"):
            alerts.append({
                "type": "warning",
                "category": "drawdown",
                "message": f"Significant drawdown: {portfolio_metrics['max_drawdown']:.1%}",
                "action": "Implement risk controls"
            })
        
        return alerts
    
    async def _store_risk_metrics(
        self,
        tenant_id: str,
        user_id: str,
        risk_profile: RiskProfile
    ) -> None:
        """Store risk metrics for monitoring and analysis"""
        # This would store in database/time series for tracking
        pass
    
    async def _trigger_risk_mitigation(
        self,
        tenant_id: str,
        user_id: str,
        risk_profile: RiskProfile,
        positions: List[Position]
    ) -> None:
        """Trigger automated risk mitigation actions"""
        # This would implement automated risk controls
        # For now, just log
        logger.warning(
            f"Risk mitigation triggered for user {user_id}: "
            f"Risk score {risk_profile.risk_score}"
        )
    
    def _serialize_risk_profile(self, profile: RiskProfile) -> Dict[str, Any]:
        """Serialize risk profile for API response"""
        return {
            "timestamp": profile.timestamp.isoformat(),
            "var_95": str(profile.var_95),
            "var_99": str(profile.var_99),
            "cvar_95": str(profile.cvar_95),
            "portfolio_beta": profile.portfolio_beta,
            "sharpe_ratio": profile.sharpe_ratio,
            "max_drawdown": str(profile.max_drawdown),
            "liquidation_price": str(profile.liquidation_price) if profile.liquidation_price else None,
            "margin_ratio": str(profile.margin_ratio),
            "risk_score": profile.risk_score,
            "alerts": profile.alerts
        }
    
    def _generate_recommendations(
        self,
        risk_profile: RiskProfile,
        metrics: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Generate risk management recommendations"""
        recommendations = []
        
        if risk_profile.risk_score > 70:
            recommendations.append({
                "priority": "high",
                "action": "Reduce leverage",
                "reason": "High overall risk score"
            })
        
        if risk_profile.margin_ratio > Decimal("0.7"):
            recommendations.append({
                "priority": "high",
                "action": "Add collateral or close positions",
                "reason": "High margin utilization"
            })
        
        if metrics["portfolio_volatility"] > 0.5:
            recommendations.append({
                "priority": "medium",
                "action": "Diversify portfolio",
                "reason": "High portfolio volatility"
            })
        
        if risk_profile.sharpe_ratio < 0:
            recommendations.append({
                "priority": "medium",
                "action": "Review trading strategy",
                "reason": "Negative risk-adjusted returns"
            })
        
        return recommendations
    
    async def _get_user_positions(
        self,
        tenant_id: str,
        user_id: str
    ) -> List[Position]:
        """Get user's current positions"""
        # This would fetch from database
        # For now, return empty list
        return []
    
    async def _get_market_conditions(self) -> Dict[str, Any]:
        """Get current market conditions from oracle"""
        try:
            response = await self.oracle_client.get("/api/v1/market-conditions")
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "volatility_regime": "normal",
                    "correlation_regime": "normal"
                }
                
        except Exception as e:
            logger.error(f"Error fetching market conditions: {str(e)}")
            return {
                "volatility_regime": "normal",
                "correlation_regime": "normal"
            }
    
    async def _get_asset_volatility(self, asset_id: str) -> float:
        """Get asset volatility from oracle"""
        if asset_id in self._volatility_cache:
            return self._volatility_cache[asset_id]
        
        try:
            response = await self.oracle_client.get(
                f"/api/v1/volatility/{asset_id}"
            )
            
            if response.status_code == 200:
                volatility = response.json()["volatility"]
                self._volatility_cache[asset_id] = volatility
                return volatility
            else:
                return 0.3  # Default 30% volatility
                
        except Exception as e:
            logger.error(f"Error fetching volatility: {str(e)}")
            return 0.3
    
    def _calculate_portfolio_returns(
        self,
        positions: List[Position],
        historical_data: Dict[str, List[Tuple[datetime, Decimal]]]
    ) -> np.ndarray:
        """Calculate historical portfolio returns"""
        # This is a simplified calculation
        # In practice, would need to handle position changes over time
        
        if not historical_data:
            return np.array([])
        
        # Get common timestamps
        all_timestamps = set()
        for data in historical_data.values():
            all_timestamps.update(t[0] for t in data)
        
        common_timestamps = sorted(all_timestamps)
        
        # Calculate weighted returns
        portfolio_returns = []
        
        for i in range(1, len(common_timestamps)):
            period_return = 0
            total_weight = 0
            
            for position in positions:
                if position.asset_id in historical_data:
                    prices = dict(historical_data[position.asset_id])
                    
                    if common_timestamps[i-1] in prices and common_timestamps[i] in prices:
                        prev_price = float(prices[common_timestamps[i-1]])
                        curr_price = float(prices[common_timestamps[i]])
                        
                        if prev_price > 0:
                            asset_return = (curr_price - prev_price) / prev_price
                            weight = abs(float(position.quantity * position.entry_price))
                            period_return += asset_return * weight
                            total_weight += weight
            
            if total_weight > 0:
                portfolio_returns.append(period_return / total_weight)
        
        return np.array(portfolio_returns)
    
    async def _get_market_returns(self) -> np.ndarray:
        """Get market index returns for beta calculation"""
        # This would fetch market index data
        # For now, return random data
        return np.random.normal(0.0001, 0.01, 252)
    
    def _calculate_beta(
        self,
        portfolio_returns: np.ndarray,
        market_returns: np.ndarray
    ) -> float:
        """Calculate portfolio beta vs market"""
        if len(portfolio_returns) == 0 or len(market_returns) == 0:
            return 1.0
        
        # Align lengths
        min_len = min(len(portfolio_returns), len(market_returns))
        portfolio_returns = portfolio_returns[-min_len:]
        market_returns = market_returns[-min_len:]
        
        # Calculate beta
        covariance = np.cov(portfolio_returns, market_returns)[0, 1]
        market_variance = np.var(market_returns)
        
        if market_variance > 0:
            return covariance / market_variance
        else:
            return 1.0
    
    def _calculate_liquidation_price(
        self,
        position: Position,
        maintenance_margin: Decimal
    ) -> Optional[Decimal]:
        """Calculate liquidation price for a position"""
        if position.quantity == 0:
            return None
        
        # Long position: price drops below maintenance
        # Short position: price rises above maintenance
        
        if position.quantity > 0:  # Long
            liquidation_price = (
                position.entry_price * position.quantity -
                position.initial_margin + maintenance_margin
            ) / position.quantity
        else:  # Short
            liquidation_price = (
                position.entry_price * abs(position.quantity) +
                position.initial_margin - maintenance_margin
            ) / abs(position.quantity)
        
        return max(liquidation_price, Decimal("0"))
    
    def _is_portfolio_margin_eligible(self, positions: List[Position]) -> bool:
        """Check if portfolio is eligible for portfolio margin"""
        # Simplified eligibility check
        return len(positions) >= 3 and all(p.initial_margin > 1000 for p in positions)
    
    async def _calculate_correlation_benefit(
        self,
        positions: List[Position]
    ) -> Decimal:
        """Calculate margin reduction from diversification"""
        # This would calculate actual correlations
        # For now, return a fixed benefit
        return Decimal("0.15")  # 15% reduction
    
    async def _calculate_stress_margin(
        self,
        position: Position,
        scenario: str,
        volatility: float
    ) -> Decimal:
        """Calculate margin under stress scenario"""
        stress_multipliers = {
            "market_crash": 3.0,
            "flash_crash": 5.0,
            "black_swan": 10.0
        }
        
        multiplier = stress_multipliers.get(scenario, 2.0)
        position_value = abs(position.quantity * position.entry_price)
        
        return position_value * Decimal(str(volatility * multiplier))
    
    async def _get_price_predictions(
        self,
        assets: List[str],
        horizon_hours: int
    ) -> Dict[str, Dict[str, Any]]:
        """Get price predictions from oracle/ML service"""
        try:
            response = await self.oracle_client.post(
                "/api/v1/predictions",
                json={
                    "assets": assets,
                    "horizon_hours": horizon_hours
                }
            )
            
            if response.status_code == 200:
                return response.json()["predictions"]
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching price predictions: {str(e)}")
            return {}
    
    def _calculate_liquidation_probability(
        self,
        position: Position,
        expected_price: Decimal,
        price_std: Decimal,
        extreme_moves: Dict[str, float]
    ) -> float:
        """Calculate probability of liquidation"""
        liquidation_price = self._calculate_liquidation_price(
            position,
            position.initial_margin * Decimal("0.5")
        )
        
        if not liquidation_price:
            return 0.0
        
        # Calculate z-score to liquidation
        if position.quantity > 0:  # Long position
            z_score = float((liquidation_price - expected_price) / price_std)
        else:  # Short position
            z_score = float((expected_price - liquidation_price) / price_std)
        
        # Use normal distribution CDF
        from scipy.stats import norm
        base_probability = norm.cdf(z_score)
        
        # Adjust for extreme moves
        tail_risk = extreme_moves.get("tail_risk_adjustment", 1.0)
        
        return min(base_probability * tail_risk, 1.0)
    
    def _calculate_expected_loss(
        self,
        position: Position,
        prediction: Dict[str, Any]
    ) -> Decimal:
        """Calculate expected loss if liquidated"""
        liquidation_price = self._calculate_liquidation_price(
            position,
            position.initial_margin * Decimal("0.5")
        )
        
        if not liquidation_price:
            return Decimal("0")
        
        if position.quantity > 0:  # Long position
            loss = position.quantity * (position.entry_price - liquidation_price)
        else:  # Short position
            loss = abs(position.quantity) * (liquidation_price - position.entry_price)
        
        return max(loss, Decimal("0"))
    
    def _get_risk_mitigation_action(
        self,
        position: Position,
        liquidation_probability: float
    ) -> str:
        """Recommend risk mitigation action"""
        if liquidation_probability > 0.5:
            return "Close position immediately"
        elif liquidation_probability > 0.3:
            return "Reduce position by 50%"
        elif liquidation_probability > 0.1:
            return "Add collateral or hedge"
        else:
            return "Monitor closely"
    
    async def _generate_hedging_recommendations(
        self,
        positions: List[Position],
        scenarios: Dict[str, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Generate hedging recommendations based on risk scenarios"""
        recommendations = []
        
        # Analyze positions at risk
        high_risk_positions = []
        for scenario in scenarios.values():
            for pos_risk in scenario.get("positions_at_risk", []):
                if pos_risk["liquidation_probability"] > 0.2:
                    high_risk_positions.append(pos_risk)
        
        # Generate hedging strategies
        if high_risk_positions:
            recommendations.append({
                "strategy": "protective_options",
                "description": "Buy put options to limit downside",
                "positions": [p["position_id"] for p in high_risk_positions],
                "estimated_cost": "2-3% of position value"
            })
        
        return recommendations
    
    def _summarize_liquidation_risk(
        self,
        scenarios: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Summarize liquidation risk across scenarios"""
        worst_case_survival = min(
            s["portfolio_survival_probability"]
            for s in scenarios.values()
        )
        
        total_positions_at_risk = set()
        for scenario in scenarios.values():
            for pos in scenario.get("positions_at_risk", []):
                total_positions_at_risk.add(pos["position_id"])
        
        return {
            "worst_case_survival_probability": worst_case_survival,
            "unique_positions_at_risk": len(total_positions_at_risk),
            "risk_level": "critical" if worst_case_survival < 0.8 else
                         "high" if worst_case_survival < 0.9 else
                         "moderate" if worst_case_survival < 0.95 else "low"
        } 