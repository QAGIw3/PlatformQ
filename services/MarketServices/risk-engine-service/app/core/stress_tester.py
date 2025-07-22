"""Stress testing engine for extreme market scenarios."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional
import numpy as np
from platformq_risk_common import StressTestScenario

logger = logging.getLogger(__name__)


class StressTester:
    """Runs stress tests on positions and portfolios."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.default_scenarios = self._load_default_scenarios()
        
    def _load_default_scenarios(self) -> List[StressTestScenario]:
        """Load default stress test scenarios."""
        scenarios = []
        
        # Market crash scenarios
        scenarios.append(StressTestScenario(
            scenario_id="market_crash_20",
            name="Market Crash -20%",
            description="20% market decline across all assets",
            market_shocks={"default": Decimal("-0.20")},
            volatility_shocks={"default": Decimal("2.0")},
            severity="severe"
        ))
        
        scenarios.append(StressTestScenario(
            scenario_id="flash_crash",
            name="Flash Crash",
            description="Sudden 10% drop with liquidity issues",
            market_shocks={"default": Decimal("-0.10")},
            volatility_shocks={"default": Decimal("3.0")},
            liquidity_haircuts={"default": Decimal("0.5")},
            severity="moderate"
        ))
        
        # Volatility spike
        scenarios.append(StressTestScenario(
            scenario_id="vol_spike",
            name="Volatility Spike",
            description="Volatility doubles without price movement",
            market_shocks={"default": Decimal("0")},
            volatility_shocks={"default": Decimal("2.0")},
            severity="moderate"
        ))
        
        # Liquidity crisis
        scenarios.append(StressTestScenario(
            scenario_id="liquidity_crisis",
            name="Liquidity Crisis",
            description="Severe liquidity constraints",
            market_shocks={"default": Decimal("-0.05")},
            volatility_shocks={"default": Decimal("1.5")},
            liquidity_haircuts={"default": Decimal("0.7")},
            severity="severe"
        ))
        
        # Black swan event
        scenarios.append(StressTestScenario(
            scenario_id="black_swan",
            name="Black Swan Event",
            description="Extreme market dislocation",
            market_shocks={"default": Decimal("-0.35")},
            volatility_shocks={"default": Decimal("5.0")},
            liquidity_haircuts={"default": Decimal("0.9")},
            severity="extreme"
        ))
        
        return scenarios
    
    async def run_stress_tests(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        scenarios: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Run multiple stress test scenarios."""
        if scenarios is None:
            scenarios = self.config.get("stress_test_scenarios", [])
        
        results = {}
        worst_case = {
            "scenario": None,
            "loss": Decimal("0")
        }
        
        for scenario_dict in scenarios:
            # Convert dict to StressTestScenario if needed
            if isinstance(scenario_dict, dict):
                scenario = StressTestScenario(
                    scenario_id=scenario_dict.get("name", "custom"),
                    name=scenario_dict.get("name", "Custom Scenario"),
                    description=scenario_dict.get("description", ""),
                    market_shocks={"default": Decimal(str(scenario_dict.get("price_change", "-0.1")))},
                    volatility_shocks={"default": Decimal(str(scenario_dict.get("vol_multiplier", "2.0")))},
                    severity=scenario_dict.get("severity", "moderate"),
                    created_by="system",
                    created_at=datetime.utcnow()
                )
            else:
                scenario = scenario_dict
            
            # Run the scenario
            result = await self.run_scenario(
                scenario=scenario,
                positions=positions,
                market_data=market_data
            )
            
            results[scenario.name] = {
                "loss": str(result.loss_amount),
                "loss_percentage": str(result.loss_percentage),
                "var_breach": result.var_breach,
                "margin_call": result.margin_call,
                "liquidations": len(result.liquidations)
            }
            
            # Track worst case
            if result.loss_amount > worst_case["loss"]:
                worst_case["scenario"] = scenario.name
                worst_case["loss"] = result.loss_amount
        
        return {
            "scenarios": results,
            "worst_case": worst_case,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def run_scenario(
        self,
        scenario: StressTestScenario,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        include_correlations: bool = True,
        include_liquidity: bool = True
    ) -> "StressTestResult":
        """Run a single stress test scenario."""
        from ..models import StressTestResult
        
        # Calculate initial portfolio value
        portfolio_value = Decimal("0")
        position_values = {}
        
        for position in positions:
            market_id = position.get("market_id")
            position_id = position.get("position_id")
            
            if market_id in market_data:
                mark_price = Decimal(str(market_data[market_id].get("price", "0")))
                quantity = Decimal(str(position.get("quantity", "0")))
                contract_size = Decimal(str(position.get("contract_size", "1")))
                position_value = abs(quantity) * contract_size * mark_price
                
                portfolio_value += position_value
                position_values[position_id] = position_value
        
        # Apply stress scenario
        stressed_value = Decimal("0")
        position_impacts = {}
        liquidations = []
        
        for position in positions:
            market_id = position.get("market_id")
            position_id = position.get("position_id")
            
            if market_id not in market_data:
                continue
            
            # Get market shock for this asset
            shock = scenario.market_shocks.get(market_id, scenario.market_shocks.get("default", Decimal("0")))
            
            # Calculate stressed price
            current_price = Decimal(str(market_data[market_id].get("price", "0")))
            stressed_price = current_price * (Decimal("1") + shock)
            
            # Apply liquidity haircut if enabled
            if include_liquidity:
                haircut = scenario.liquidity_haircuts.get(
                    market_id, 
                    scenario.liquidity_haircuts.get("default", Decimal("0"))
                )
                stressed_price = stressed_price * (Decimal("1") - haircut)
            
            # Calculate stressed position value
            quantity = Decimal(str(position.get("quantity", "0")))
            contract_size = Decimal(str(position.get("contract_size", "1")))
            side = position.get("side", "long")
            
            if side == "long":
                stressed_position_value = quantity * contract_size * stressed_price
            else:  # short
                stressed_position_value = -quantity * contract_size * stressed_price
            
            stressed_value += abs(stressed_position_value)
            
            # Calculate position impact
            original_value = position_values.get(position_id, Decimal("0"))
            loss = original_value - abs(stressed_position_value)
            
            position_impacts[position_id] = {
                "original_value": str(original_value),
                "stressed_value": str(abs(stressed_position_value)),
                "loss": str(loss),
                "loss_percentage": str(loss / original_value * 100) if original_value > 0 else "0"
            }
            
            # Check for liquidation
            margin_ratio = position.get("margin_ratio", Decimal("999"))
            stressed_margin_ratio = margin_ratio * (abs(stressed_position_value) / original_value) if original_value > 0 else Decimal("0")
            
            if stressed_margin_ratio < self.config.get("liquidation_margin_ratio", Decimal("1.1")):
                liquidations.append(position_id)
        
        # Calculate overall metrics
        loss_amount = portfolio_value - stressed_value
        loss_percentage = loss_amount / portfolio_value if portfolio_value > 0 else Decimal("0")
        
        # Check for VaR breach (simplified)
        var_breach = loss_percentage > Decimal("0.05")  # 5% threshold
        
        # Check for margin call
        margin_call = loss_percentage > Decimal("0.03")  # 3% threshold
        
        # Calculate stressed risk metrics
        stressed_var = loss_amount * Decimal("1.2")  # Simplified
        stressed_leverage = Decimal("10") * (Decimal("1") + loss_percentage)  # Simplified
        stressed_margin_ratio = Decimal("2") * (Decimal("1") - loss_percentage)  # Simplified
        
        return StressTestResult(
            test_id=f"test_{datetime.utcnow().timestamp()}",
            scenario_id=scenario.scenario_id,
            portfolio_id="portfolio_combined",  # Simplified
            portfolio_value=portfolio_value,
            stressed_value=stressed_value,
            loss_amount=loss_amount,
            loss_percentage=loss_percentage,
            stressed_var=stressed_var,
            stressed_leverage=stressed_leverage,
            stressed_margin_ratio=stressed_margin_ratio,
            var_breach=var_breach,
            margin_call=margin_call,
            liquidations=liquidations,
            position_impacts=position_impacts,
            execution_time_ms=100.0  # Simplified
        )
    
    def _dict_to_scenario(self, scenario_dict: Dict[str, Any]) -> StressTestScenario:
        """Convert dictionary to StressTestScenario object."""
        return StressTestScenario(
            scenario_id=scenario_dict.get("scenario_id", "custom"),
            name=scenario_dict.get("name", "Custom Scenario"),
            description=scenario_dict.get("description", ""),
            market_shocks={k: Decimal(str(v)) for k, v in scenario_dict.get("market_shocks", {}).items()},
            volatility_shocks={k: Decimal(str(v)) for k, v in scenario_dict.get("volatility_shocks", {}).items()},
            correlation_shocks=scenario_dict.get("correlation_shocks"),
            interest_rate_shock=Decimal(str(scenario_dict.get("interest_rate_shock", "0"))),
            liquidity_haircuts={k: Decimal(str(v)) for k, v in scenario_dict.get("liquidity_haircuts", {}).items()},
            severity=scenario_dict.get("severity", "moderate")
        )
    
    def _apply_shocks(
        self,
        market_data: Dict[str, Dict[str, Any]],
        scenario: StressTestScenario
    ) -> Dict[str, Dict[str, Any]]:
        """Apply scenario shocks to market data."""
        shocked_data = {}
        
        for market_id, data in market_data.items():
            shocked_data[market_id] = data.copy()
            
            # Apply price shock
            price_shock = scenario.market_shocks.get(market_id, scenario.market_shocks.get("default", Decimal("0")))
            current_price = Decimal(str(data.get("price", "0")))
            shocked_data[market_id]["price"] = str(current_price * (Decimal("1") + price_shock))
            
            # Apply volatility shock
            vol_shock = scenario.volatility_shocks.get(market_id, scenario.volatility_shocks.get("default", Decimal("1")))
            current_vol = Decimal(str(data.get("volatility", "0.2")))
            shocked_data[market_id]["volatility"] = str(current_vol * vol_shock)
            
            # Apply liquidity haircut
            liquidity_haircut = scenario.liquidity_haircuts.get(market_id, scenario.liquidity_haircuts.get("default", Decimal("1")))
            shocked_data[market_id]["liquidity_factor"] = str(liquidity_haircut)
            
            # Apply interest rate shock if applicable
            if scenario.interest_rate_shock:
                shocked_data[market_id]["risk_free_rate"] = str(
                    Decimal(str(data.get("risk_free_rate", "0.02"))) + scenario.interest_rate_shock
                )
        
        return shocked_data
    
    def _calculate_portfolio_value(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]]
    ) -> Decimal:
        """Calculate total portfolio value."""
        total_value = Decimal("0")
        
        for position in positions:
            market_id = position.get("market_id")
            if market_id in market_data:
                price = Decimal(str(market_data[market_id].get("price", "0")))
                quantity = Decimal(str(position.get("quantity", "0")))
                contract_size = Decimal(str(position.get("contract_size", "1")))
                
                # Consider position side
                side = position.get("side", "long")
                if side == "long":
                    position_value = quantity * contract_size * price
                else:
                    # Short position value
                    entry_price = Decimal(str(position.get("entry_price", price)))
                    position_value = quantity * contract_size * (2 * entry_price - price)
                
                total_value += position_value
        
        return total_value
    
    async def _calculate_stressed_margins(
        self,
        positions: List[Dict[str, Any]],
        shocked_market_data: Dict[str, Dict[str, Any]],
        scenario: StressTestScenario
    ) -> Dict[str, Any]:
        """Calculate margin requirements under stressed conditions."""
        total_initial_margin = Decimal("0")
        total_maintenance_margin = Decimal("0")
        margin_breaches = []
        
        for position in positions:
            market_id = position.get("market_id")
            if market_id not in shocked_market_data:
                continue
            
            # Simple stressed margin calculation
            # In practice, this would use the MarginCalculator with stressed parameters
            shocked_price = Decimal(str(shocked_market_data[market_id].get("price", "0")))
            quantity = abs(Decimal(str(position.get("quantity", "0"))))
            contract_size = Decimal(str(position.get("contract_size", "1")))
            
            notional_value = quantity * contract_size * shocked_price
            
            # Increase margin requirements based on scenario severity
            severity_multiplier = {
                "mild": Decimal("1.2"),
                "moderate": Decimal("1.5"),
                "severe": Decimal("2.0"),
                "extreme": Decimal("3.0")
            }.get(scenario.severity, Decimal("1.5"))
            
            base_margin_rate = Decimal("0.1")
            stressed_margin_rate = base_margin_rate * severity_multiplier
            
            initial_margin = notional_value * stressed_margin_rate
            maintenance_margin = initial_margin * Decimal("0.5")
            
            total_initial_margin += initial_margin
            total_maintenance_margin += maintenance_margin
            
            # Check for margin breaches
            collateral = Decimal(str(position.get("collateral", "0")))
            if collateral < maintenance_margin:
                margin_breaches.append({
                    "position_id": position.get("position_id"),
                    "required_margin": maintenance_margin,
                    "available_collateral": collateral,
                    "shortfall": maintenance_margin - collateral
                })
        
        return {
            "total_initial_margin": total_initial_margin,
            "total_maintenance_margin": total_maintenance_margin,
            "margin_breaches": margin_breaches,
            "breach_count": len(margin_breaches)
        }
    
    async def run_custom_scenario(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        custom_shocks: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Run a custom stress test scenario."""
        scenario = StressTestScenario(
            scenario_id="custom",
            name="Custom Scenario",
            description="User-defined stress test",
            market_shocks={k: Decimal(str(v)) for k, v in custom_shocks.get("price_shocks", {}).items()},
            volatility_shocks={k: Decimal(str(v)) for k, v in custom_shocks.get("vol_shocks", {}).items()},
            interest_rate_shock=Decimal(str(custom_shocks.get("rate_shock", "0"))),
            severity="moderate"
        )
        
        results = await self.run_stress_tests(positions, market_data, [scenario])
        return results.get("custom", {})
