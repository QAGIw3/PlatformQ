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
        
        # Interest rate shock
        scenarios.append(StressTestScenario(
            scenario_id="rate_hike",
            name="Interest Rate Spike",
            description="50bp rate increase",
            market_shocks={"default": Decimal("-0.02")},
            volatility_shocks={"default": Decimal("1.2")},
            interest_rate_shock=Decimal("0.005"),
            severity="mild"
        ))
        
        # Correlation breakdown
        scenarios.append(StressTestScenario(
            scenario_id="correlation_breakdown",
            name="Correlation Breakdown",
            description="All correlations go to 1 in crisis",
            market_shocks={"default": Decimal("-0.15")},
            volatility_shocks={"default": Decimal("1.5")},
            correlation_shocks={"all": {"all": Decimal("1.0")}},
            severity="severe"
        ))
        
        return scenarios
    
    async def run_stress_tests(
        self,
        positions: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]],
        scenarios: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Run stress tests on a portfolio."""
        if scenarios is None:
            scenarios = self.default_scenarios
        else:
            # Convert dict scenarios to StressTestScenario objects
            scenarios = [self._dict_to_scenario(s) for s in scenarios]
        
        results = {}
        worst_case = None
        worst_loss = Decimal("0")
        
        # Calculate baseline portfolio value
        baseline_value = self._calculate_portfolio_value(positions, market_data)
        
        for scenario in scenarios:
            # Apply scenario shocks
            shocked_market_data = self._apply_shocks(market_data, scenario)
            
            # Calculate stressed portfolio value
            stressed_value = self._calculate_portfolio_value(positions, shocked_market_data)
            
            # Calculate loss
            loss = baseline_value - stressed_value
            loss_percentage = (loss / baseline_value * Decimal("100")) if baseline_value > 0 else Decimal("0")
            
            # Calculate stressed margins
            stressed_margins = await self._calculate_stressed_margins(
                positions,
                shocked_market_data,
                scenario
            )
            
            results[scenario.scenario_id] = {
                "scenario_name": scenario.name,
                "baseline_value": baseline_value,
                "stressed_value": stressed_value,
                "loss": loss,
                "loss_percentage": loss_percentage,
                "stressed_margins": stressed_margins,
                "severity": scenario.severity
            }
            
            # Track worst case
            if loss > worst_loss:
                worst_loss = loss
                worst_case = scenario.scenario_id
        
        # Add worst case summary
        if worst_case:
            results["worst_case"] = results[worst_case].copy()
            results["worst_case"]["scenario_id"] = worst_case
        
        # Calculate aggregate metrics
        results["summary"] = {
            "scenarios_run": len(scenarios),
            "baseline_value": baseline_value,
            "worst_loss": worst_loss,
            "worst_loss_percentage": (worst_loss / baseline_value * Decimal("100")) if baseline_value > 0 else Decimal("0"),
            "timestamp": datetime.utcnow()
        }
        
        return results
    
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
