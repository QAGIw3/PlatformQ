"""Main risk calculator for unified risk assessment."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional
import asyncio

from ..models import RiskMetrics, PortfolioRisk, PositionRisk, RiskAlert
from .margin_calculator import MarginCalculator
from .var_calculator import VaRCalculator
from .stress_tester import StressTester


logger = logging.getLogger(__name__)


class RiskCalculator:
    """Calculates comprehensive risk metrics for positions and portfolios."""
    
    def __init__(
        self,
        margin_calculator: MarginCalculator,
        var_calculator: VaRCalculator,
        stress_tester: StressTester,
        config: Dict[str, Any]
    ):
        self.margin_calculator = margin_calculator
        self.var_calculator = var_calculator
        self.stress_tester = stress_tester
        self.config = config
        
        # Risk thresholds
        self.warning_thresholds = {
            "margin_ratio": Decimal("1.5"),
            "leverage": Decimal("15"),
            "var_percentage": Decimal("0.05"),
            "concentration": Decimal("0.3")
        }
        
        self.critical_thresholds = {
            "margin_ratio": Decimal("1.2"),
            "leverage": Decimal("18"),
            "var_percentage": Decimal("0.08"),
            "concentration": Decimal("0.5")
        }
    
    async def calculate_position_risk(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> PositionRisk:
        """Calculate risk metrics for a single position."""
        position_id = position["position_id"]
        
        # Calculate margin requirements
        margin_result = await self.margin_calculator.calculate_margin(
            position,
            market_data
        )
        
        # Calculate Value at Risk
        var_result = await self.var_calculator.calculate_position_var(
            position,
            market_data,
            confidence_level=self.config.get("var_confidence_level", 0.95),
            time_horizon_days=self.config.get("var_time_horizon_days", 1)
        )
        
        # Calculate current metrics
        mark_price = Decimal(str(market_data.get("price", "0")))
        quantity = Decimal(str(position.get("quantity", "0")))
        notional_value = abs(quantity) * mark_price
        
        # Calculate P&L
        entry_price = Decimal(str(position.get("entry_price", "0")))
        side = position.get("side", "long")
        
        if side == "long":
            unrealized_pnl = (mark_price - entry_price) * quantity
        else:
            unrealized_pnl = (entry_price - mark_price) * abs(quantity)
        
        # Calculate leverage
        collateral = Decimal(str(position.get("collateral", "1")))
        leverage = notional_value / collateral if collateral > 0 else Decimal("999")
        
        # Create position risk object
        position_risk = PositionRisk(
            position_id=position_id,
            market_id=position.get("market_id"),
            user_id=position.get("user_id"),
            
            # Margin metrics
            initial_margin=margin_result["initial_margin"],
            maintenance_margin=margin_result["maintenance_margin"],
            margin_ratio=margin_result["margin_ratio"],
            margin_usage=margin_result["margin_usage"],
            
            # Value metrics
            notional_value=notional_value,
            mark_price=mark_price,
            unrealized_pnl=unrealized_pnl,
            
            # Risk metrics
            var_1d=var_result["var_amount"],
            var_percentage=var_result["var_percentage"],
            leverage=leverage,
            liquidation_price=margin_result.get("liquidation_price"),
            
            # Greeks (for options)
            delta=Decimal(str(position.get("delta", "0"))),
            gamma=Decimal(str(position.get("gamma", "0"))),
            vega=Decimal(str(position.get("vega", "0"))),
            theta=Decimal(str(position.get("theta", "0"))),
            
            # Risk scores
            risk_score=self._calculate_risk_score(margin_result, var_result, leverage),
            timestamp=datetime.utcnow()
        )
        
        return position_risk
    
    async def calculate_portfolio_risk(
        self,
        user_id: str,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]]
    ) -> PortfolioRisk:
        """Calculate aggregated risk metrics for a portfolio."""
        if not positions:
            return self._empty_portfolio_risk(user_id)
        
        # Calculate individual position risks
        position_risks = []
        for position in positions:
            market_id = position.get("market_id")
            if market_id in market_data:
                position_risk = await self.calculate_position_risk(
                    position,
                    market_data[market_id]
                )
                position_risks.append(position_risk)
        
        # Aggregate metrics
        total_value = sum(pr.notional_value for pr in position_risks)
        total_margin = sum(pr.maintenance_margin for pr in position_risks)
        total_unrealized_pnl = sum(pr.unrealized_pnl for pr in position_risks)
        
        # Calculate portfolio VaR
        portfolio_var = await self.var_calculator.calculate_portfolio_var(
            positions,
            market_data,
            confidence_level=self.config.get("var_confidence_level", 0.95)
        )
        
        # Run stress tests
        stress_results = await self.stress_tester.run_stress_tests(
            positions,
            market_data,
            self.config.get("stress_test_scenarios", [])
        )
        
        # Calculate concentration risk
        concentration_by_market = self._calculate_concentration(position_risks)
        max_concentration = max(concentration_by_market.values()) if concentration_by_market else Decimal("0")
        
        # Calculate overall leverage
        total_collateral = sum(Decimal(str(p.get("collateral", "0"))) for p in positions)
        portfolio_leverage = total_value / total_collateral if total_collateral > 0 else Decimal("0")
        
        # Identify alerts
        alerts = self._generate_alerts(
            position_risks,
            portfolio_leverage,
            portfolio_var["var_percentage"],
            max_concentration
        )
        
        portfolio_risk = PortfolioRisk(
            user_id=user_id,
            total_positions=len(positions),
            
            # Value metrics
            total_value=total_value,
            total_collateral=total_collateral,
            total_unrealized_pnl=total_unrealized_pnl,
            
            # Margin metrics
            total_initial_margin=sum(pr.initial_margin for pr in position_risks),
            total_maintenance_margin=total_margin,
            margin_usage=total_margin / total_collateral if total_collateral > 0 else Decimal("999"),
            
            # Risk metrics
            portfolio_var=portfolio_var["var_amount"],
            portfolio_leverage=portfolio_leverage,
            max_position_leverage=max(pr.leverage for pr in position_risks) if position_risks else Decimal("0"),
            
            # Concentration
            concentration_by_market=concentration_by_market,
            max_concentration=max_concentration,
            
            # Greeks (aggregated)
            total_delta=sum(pr.delta for pr in position_risks),
            total_gamma=sum(pr.gamma for pr in position_risks),
            total_vega=sum(pr.vega for pr in position_risks),
            total_theta=sum(pr.theta for pr in position_risks),
            
            # Stress test results
            stress_test_results=stress_results,
            worst_case_loss=stress_results.get("worst_case", {}).get("loss", Decimal("0")),
            
            # Alerts
            alerts=alerts,
            risk_score=self._calculate_portfolio_risk_score(
                portfolio_leverage,
                portfolio_var["var_percentage"],
                max_concentration,
                len(alerts)
            ),
            
            timestamp=datetime.utcnow()
        )
        
        return portfolio_risk
    
    def _calculate_risk_score(
        self,
        margin_result: Dict[str, Any],
        var_result: Dict[str, Any],
        leverage: Decimal
    ) -> int:
        """Calculate risk score from 0-100."""
        score = 0
        
        # Margin ratio component (0-40 points)
        margin_ratio = margin_result["margin_ratio"]
        if margin_ratio < self.critical_thresholds["margin_ratio"]:
            score += 40
        elif margin_ratio < self.warning_thresholds["margin_ratio"]:
            score += 20
        
        # Leverage component (0-30 points)
        if leverage > self.critical_thresholds["leverage"]:
            score += 30
        elif leverage > self.warning_thresholds["leverage"]:
            score += 15
        
        # VaR component (0-30 points)
        var_pct = var_result["var_percentage"]
        if var_pct > self.critical_thresholds["var_percentage"]:
            score += 30
        elif var_pct > self.warning_thresholds["var_percentage"]:
            score += 15
        
        return min(score, 100)
    
    def _calculate_concentration(
        self,
        position_risks: List[PositionRisk]
    ) -> Dict[str, Decimal]:
        """Calculate concentration by market."""
        total_value = sum(pr.notional_value for pr in position_risks)
        if total_value == 0:
            return {}
        
        concentration = {}
        for pr in position_risks:
            market_id = pr.market_id
            if market_id not in concentration:
                concentration[market_id] = Decimal("0")
            concentration[market_id] += pr.notional_value / total_value
        
        return concentration
    
    def _generate_alerts(
        self,
        position_risks: List[PositionRisk],
        portfolio_leverage: Decimal,
        portfolio_var_pct: Decimal,
        max_concentration: Decimal
    ) -> List[RiskAlert]:
        """Generate risk alerts based on thresholds."""
        alerts = []
        
        # Position-level alerts
        for pr in position_risks:
            if pr.margin_ratio < self.critical_thresholds["margin_ratio"]:
                alerts.append(RiskAlert(
                    alert_type="margin_call",
                    severity="critical",
                    message=f"Position {pr.position_id} margin ratio {pr.margin_ratio} below critical",
                    position_id=pr.position_id,
                    metric_value=str(pr.margin_ratio)
                ))
            elif pr.margin_ratio < self.warning_thresholds["margin_ratio"]:
                alerts.append(RiskAlert(
                    alert_type="margin_warning",
                    severity="warning",
                    message=f"Position {pr.position_id} margin ratio {pr.margin_ratio} approaching critical",
                    position_id=pr.position_id,
                    metric_value=str(pr.margin_ratio)
                ))
        
        # Portfolio-level alerts
        if portfolio_leverage > self.critical_thresholds["leverage"]:
            alerts.append(RiskAlert(
                alert_type="leverage_exceeded",
                severity="critical",
                message=f"Portfolio leverage {portfolio_leverage} exceeds limit",
                metric_value=str(portfolio_leverage)
            ))
        
        if max_concentration > self.critical_thresholds["concentration"]:
            alerts.append(RiskAlert(
                alert_type="concentration_risk",
                severity="warning",
                message=f"Portfolio concentration {max_concentration} exceeds limit",
                metric_value=str(max_concentration)
            ))
        
        return alerts
    
    def _calculate_portfolio_risk_score(
        self,
        leverage: Decimal,
        var_pct: Decimal,
        concentration: Decimal,
        alert_count: int
    ) -> int:
        """Calculate portfolio risk score from 0-100."""
        score = 0
        
        # Leverage (0-30)
        if leverage > self.critical_thresholds["leverage"]:
            score += 30
        elif leverage > self.warning_thresholds["leverage"]:
            score += 15
        
        # VaR (0-30)
        if var_pct > self.critical_thresholds["var_percentage"]:
            score += 30
        elif var_pct > self.warning_thresholds["var_percentage"]:
            score += 15
        
        # Concentration (0-20)
        if concentration > self.critical_thresholds["concentration"]:
            score += 20
        elif concentration > self.warning_thresholds["concentration"]:
            score += 10
        
        # Alerts (0-20)
        score += min(alert_count * 5, 20)
        
        return min(score, 100)
    
    def _empty_portfolio_risk(self, user_id: str) -> PortfolioRisk:
        """Return empty portfolio risk object."""
        return PortfolioRisk(
            user_id=user_id,
            total_positions=0,
            total_value=Decimal("0"),
            total_collateral=Decimal("0"),
            total_unrealized_pnl=Decimal("0"),
            total_initial_margin=Decimal("0"),
            total_maintenance_margin=Decimal("0"),
            margin_usage=Decimal("0"),
            portfolio_var=Decimal("0"),
            portfolio_leverage=Decimal("0"),
            max_position_leverage=Decimal("0"),
            concentration_by_market={},
            max_concentration=Decimal("0"),
            total_delta=Decimal("0"),
            total_gamma=Decimal("0"),
            total_vega=Decimal("0"),
            total_theta=Decimal("0"),
            stress_test_results={},
            worst_case_loss=Decimal("0"),
            alerts=[],
            risk_score=0,
            timestamp=datetime.utcnow()
        ) 