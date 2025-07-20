"""Monte Carlo risk simulation engine"""

from typing import Dict, Any, Optional, List
import logging
import numpy as np
from datetime import datetime
import asyncio
from concurrent.futures import ProcessPoolExecutor

from app.risk_engines.base import BaseRiskEngine
from app.models.settlement import Settlement, ProviderMetrics, RiskLevel, MonteCarloSimulation
from app.config import settings

logger = logging.getLogger(__name__)


class MonteCarloRiskEngine(BaseRiskEngine):
    """
    Advanced Monte Carlo simulations for scenario-based loss distributions
    at 95% confidence levels. Calculates VaR and CVaR for physical settlement risk.
    """
    
    def __init__(self):
        self.num_simulations = settings.risk_monte_carlo_simulations
        self.confidence_level = settings.risk_confidence_level
        self.downtime_penalty_factor = settings.risk_downtime_penalty_factor
        self.executor = ProcessPoolExecutor(max_workers=4)
    
    async def calculate_risk(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Calculate risk using Monte Carlo simulation"""
        
        if not self.validate_inputs(settlement, provider_metrics):
            raise ValueError("Invalid input data for risk calculation")
        
        # Prepare simulation parameters
        sim_params = self._prepare_simulation_parameters(
            settlement, provider_metrics, market_data
        )
        
        # Run Monte Carlo simulation
        simulation_results = await self._run_monte_carlo_simulation(sim_params)
        
        # Calculate risk metrics
        var = self._calculate_var(simulation_results.simulated_losses)
        cvar = self._calculate_cvar(simulation_results.simulated_losses, var)
        
        # Normalize risk score
        risk_score = min(var / settlement.total_value, 1.0)
        risk_level = self._determine_risk_level(risk_score)
        
        # Analyze scenarios
        scenario_analysis = self._analyze_scenarios(
            simulation_results, settlement.total_value
        )
        
        return {
            "risk_score": risk_score,
            "risk_level": risk_level,
            "value_at_risk": var,
            "conditional_value_at_risk": cvar,
            "expected_loss": simulation_results.expected_loss,
            "worst_case_loss": simulation_results.worst_case_loss,
            "confidence_level": self.confidence_level,
            "num_simulations": self.num_simulations,
            "calculation_method": "monte_carlo",
            "simulation_stats": simulation_results.loss_distribution_stats,
            "scenario_analysis": scenario_analysis,
            "factors": {
                "uptime_mean": sim_params["expected_uptime"],
                "uptime_volatility": sim_params["uptime_volatility"],
                "downtime_penalty": self.downtime_penalty_factor
            },
            "recommendations": self._generate_recommendations(
                risk_score, var, cvar, settlement.total_value, scenario_analysis
            )
        }
    
    def get_engine_name(self) -> str:
        return "Monte Carlo Risk Engine"
    
    def _prepare_simulation_parameters(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Prepare parameters for Monte Carlo simulation"""
        
        # Historical uptime as expected value
        expected_uptime = provider_metrics.uptime_percentage
        if expected_uptime > 1:
            expected_uptime = expected_uptime / 100
        
        # Calculate volatility from historical data
        uptime_volatility = self._calculate_uptime_volatility(
            provider_metrics, market_data
        )
        
        # Capacity value at risk
        capacity_value = settlement.total_value
        
        # Time horizon
        delivery_hours = (
            settlement.delivery_end - settlement.delivery_start
        ).total_seconds() / 3600
        
        # Market conditions
        market_stress_factor = 1.0
        if market_data:
            if market_data.get("market_stress", False):
                market_stress_factor = 1.5
            if market_data.get("provider_congestion", 0) > 0.8:
                market_stress_factor *= 1.2
        
        return {
            "settlement_id": settlement.id,
            "expected_uptime": expected_uptime,
            "uptime_volatility": uptime_volatility,
            "capacity_value": capacity_value,
            "delivery_hours": delivery_hours,
            "downtime_penalty_factor": self.downtime_penalty_factor,
            "market_stress_factor": market_stress_factor,
            "provider_overcommit": provider_metrics.overcommit_ratio,
            "critical_incidents": provider_metrics.critical_incidents
        }
    
    async def _run_monte_carlo_simulation(
        self, 
        params: Dict[str, Any]
    ) -> MonteCarloSimulation:
        """Run Monte Carlo simulation in parallel"""
        
        # Split simulations across workers
        chunk_size = self.num_simulations // 4
        tasks = []
        
        loop = asyncio.get_event_loop()
        for i in range(4):
            start_idx = i * chunk_size
            end_idx = start_idx + chunk_size if i < 3 else self.num_simulations
            
            task = loop.run_in_executor(
                self.executor,
                self._simulate_chunk,
                params,
                end_idx - start_idx,
                i  # seed offset
            )
            tasks.append(task)
        
        # Gather results
        chunk_results = await asyncio.gather(*tasks)
        
        # Combine results
        all_losses = []
        for losses in chunk_results:
            all_losses.extend(losses)
        
        # Calculate statistics
        losses_array = np.array(all_losses)
        
        return MonteCarloSimulation(
            settlement_id=params["settlement_id"],
            num_simulations=self.num_simulations,
            confidence_level=self.confidence_level,
            expected_uptime=params["expected_uptime"],
            uptime_volatility=params["uptime_volatility"],
            capacity_value=params["capacity_value"],
            downtime_penalty_factor=params["downtime_penalty_factor"],
            simulated_losses=all_losses,
            value_at_risk=np.percentile(losses_array, self.confidence_level * 100),
            conditional_value_at_risk=np.mean(
                losses_array[losses_array >= np.percentile(losses_array, self.confidence_level * 100)]
            ),
            expected_loss=np.mean(losses_array),
            worst_case_loss=np.max(losses_array),
            loss_distribution_stats={
                "mean": float(np.mean(losses_array)),
                "std": float(np.std(losses_array)),
                "min": float(np.min(losses_array)),
                "max": float(np.max(losses_array)),
                "p25": float(np.percentile(losses_array, 25)),
                "p50": float(np.percentile(losses_array, 50)),
                "p75": float(np.percentile(losses_array, 75)),
                "p90": float(np.percentile(losses_array, 90)),
                "p95": float(np.percentile(losses_array, 95)),
                "p99": float(np.percentile(losses_array, 99)),
                "skewness": float(self._calculate_skewness(losses_array)),
                "kurtosis": float(self._calculate_kurtosis(losses_array))
            }
        )
    
    def _simulate_chunk(
        self, 
        params: Dict[str, Any], 
        num_sims: int,
        seed_offset: int
    ) -> List[float]:
        """Simulate a chunk of scenarios"""
        np.random.seed(42 + seed_offset)  # Reproducible results
        
        losses = []
        
        for _ in range(num_sims):
            # Simulate uptime using log-normal distribution
            # This captures the fat-tail risk of extended outages
            uptime = self._simulate_uptime(
                params["expected_uptime"],
                params["uptime_volatility"]
            )
            
            # Calculate downtime
            downtime = 1 - uptime
            
            # Apply stress factors
            if params["provider_overcommit"] > 1.5:
                # Higher overcommit increases downtime probability
                overcommit_factor = min(params["provider_overcommit"] / 1.5, 2.0)
                downtime *= overcommit_factor
            
            # Critical incident shock
            if np.random.random() < (params["critical_incidents"] / 100):
                # Random critical failure
                downtime = min(downtime + 0.2, 1.0)
            
            # Calculate loss
            # Loss = Downtime × Capacity Value × Penalty Factor × Market Stress
            loss = (
                downtime * 
                params["capacity_value"] * 
                params["downtime_penalty_factor"] * 
                params["market_stress_factor"]
            )
            
            # Add delivery time factor
            # Longer delivery periods have compounding effects
            if params["delivery_hours"] > 168:  # > 1 week
                time_factor = 1 + (params["delivery_hours"] / 168 - 1) * 0.1
                loss *= time_factor
            
            losses.append(loss)
        
        return losses
    
    def _simulate_uptime(self, mean_uptime: float, volatility: float) -> float:
        """Simulate uptime using appropriate distribution"""
        
        # Use beta distribution for bounded [0,1] values
        # Convert mean and volatility to alpha/beta parameters
        if volatility > 0:
            # Calculate alpha and beta from mean and variance
            variance = volatility ** 2
            alpha = mean_uptime * (
                (mean_uptime * (1 - mean_uptime) / variance) - 1
            )
            beta = (1 - mean_uptime) * (
                (mean_uptime * (1 - mean_uptime) / variance) - 1
            )
            
            # Ensure valid parameters
            alpha = max(alpha, 0.1)
            beta = max(beta, 0.1)
            
            uptime = np.random.beta(alpha, beta)
        else:
            # No volatility, use deterministic value
            uptime = mean_uptime
        
        return np.clip(uptime, 0, 1)
    
    def _calculate_uptime_volatility(
        self,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> float:
        """Calculate uptime volatility from historical data"""
        
        # Base volatility estimate
        if provider_metrics.total_incidents == 0:
            base_vol = 0.02  # 2% for stable providers
        else:
            # Higher incidents = higher volatility
            incident_rate = provider_metrics.total_incidents / max(
                provider_metrics.completed_settlements, 1
            )
            base_vol = min(0.05 + incident_rate * 0.5, 0.3)
        
        # Adjust for critical incidents
        if provider_metrics.critical_incidents > 0:
            critical_factor = 1 + (provider_metrics.critical_incidents * 0.2)
            base_vol *= critical_factor
        
        # Market volatility overlay
        if market_data and "historical_volatility" in market_data:
            market_vol = market_data["historical_volatility"]
            base_vol = 0.7 * base_vol + 0.3 * market_vol
        
        return min(base_vol, 0.5)  # Cap at 50%
    
    def _calculate_var(self, losses: List[float]) -> float:
        """Calculate Value at Risk"""
        return np.percentile(losses, self.confidence_level * 100)
    
    def _calculate_cvar(self, losses: List[float], var: float) -> float:
        """Calculate Conditional Value at Risk (Expected Shortfall)"""
        losses_array = np.array(losses)
        tail_losses = losses_array[losses_array >= var]
        return np.mean(tail_losses) if len(tail_losses) > 0 else var
    
    def _calculate_skewness(self, data: np.ndarray) -> float:
        """Calculate skewness of distribution"""
        n = len(data)
        if n < 3:
            return 0.0
        
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
            
        return (n / ((n - 1) * (n - 2))) * np.sum(((data - mean) / std) ** 3)
    
    def _calculate_kurtosis(self, data: np.ndarray) -> float:
        """Calculate kurtosis of distribution"""
        n = len(data)
        if n < 4:
            return 0.0
        
        mean = np.mean(data)
        std = np.std(data)
        if std == 0:
            return 0.0
            
        return (n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))) * \
               np.sum(((data - mean) / std) ** 4) - \
               (3 * (n - 1) ** 2 / ((n - 2) * (n - 3)))
    
    def _analyze_scenarios(
        self,
        simulation: MonteCarloSimulation,
        settlement_value: float
    ) -> Dict[str, Any]:
        """Analyze simulation scenarios"""
        
        losses = np.array(simulation.simulated_losses)
        
        # Define loss thresholds
        minor_threshold = settlement_value * 0.05  # 5% loss
        moderate_threshold = settlement_value * 0.15  # 15% loss
        severe_threshold = settlement_value * 0.30  # 30% loss
        catastrophic_threshold = settlement_value * 0.50  # 50% loss
        
        scenarios = {
            "no_loss": {
                "probability": float(np.sum(losses == 0) / len(losses)),
                "description": "No downtime or penalties"
            },
            "minor_loss": {
                "probability": float(
                    np.sum((losses > 0) & (losses <= minor_threshold)) / len(losses)
                ),
                "threshold": minor_threshold,
                "description": "Minor disruptions, < 5% of value"
            },
            "moderate_loss": {
                "probability": float(
                    np.sum((losses > minor_threshold) & (losses <= moderate_threshold)) / len(losses)
                ),
                "threshold": moderate_threshold,
                "description": "Significant downtime, 5-15% of value"
            },
            "severe_loss": {
                "probability": float(
                    np.sum((losses > moderate_threshold) & (losses <= severe_threshold)) / len(losses)
                ),
                "threshold": severe_threshold,
                "description": "Major outage, 15-30% of value"
            },
            "catastrophic_loss": {
                "probability": float(
                    np.sum(losses > severe_threshold) / len(losses)
                ),
                "threshold": catastrophic_threshold,
                "description": "Critical failure, > 30% of value"
            }
        }
        
        # Add tail risk analysis
        scenarios["tail_risk"] = {
            "p99_loss": float(np.percentile(losses, 99)),
            "max_observed_loss": float(np.max(losses)),
            "extreme_event_probability": float(
                np.sum(losses > catastrophic_threshold) / len(losses)
            )
        }
        
        return scenarios
    
    def _determine_risk_level(self, risk_score: float) -> RiskLevel:
        """Determine risk level based on score"""
        if risk_score < settings.risk_threshold_low:
            return RiskLevel.LOW
        elif risk_score < settings.risk_threshold_medium:
            return RiskLevel.MEDIUM
        elif risk_score < settings.risk_threshold_high:
            return RiskLevel.HIGH
        else:
            return RiskLevel.CRITICAL
    
    def _generate_recommendations(
        self,
        risk_score: float,
        var: float,
        cvar: float,
        settlement_value: float,
        scenario_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate Monte Carlo specific recommendations"""
        
        recommendations = {
            "require_escrow": risk_score > settings.risk_threshold_low,
            "escrow_percentage": 0.0,
            "risk_premium": 0.0,
            "insurance_required": False,
            "insurance_coverage": 0.0,
            "diversification_strategy": [],
            "mitigation_strategies": []
        }
        
        # Calculate escrow based on VaR
        if var > 0:
            var_percentage = var / settlement_value
            if var_percentage > 0.1:  # VaR > 10%
                recommendations["escrow_percentage"] = min(var_percentage * 1.2, 0.3)
                recommendations["mitigation_strategies"].append(
                    f"VaR of {var:.2f} ({var_percentage:.1%}) suggests significant risk"
                )
        
        # Insurance recommendations based on tail risk
        tail_risk_prob = scenario_analysis["tail_risk"]["extreme_event_probability"]
        if tail_risk_prob > 0.01:  # > 1% chance of extreme loss
            recommendations["insurance_required"] = True
            recommendations["insurance_coverage"] = cvar
            recommendations["mitigation_strategies"].append(
                f"Tail risk of {tail_risk_prob:.1%} warrants insurance coverage"
            )
        
        # Risk premium based on expected loss
        if risk_score > settings.risk_threshold_medium:
            recommendations["risk_premium"] = min(risk_score * 0.2, 0.15)
        
        # Diversification strategy
        if scenario_analysis["catastrophic_loss"]["probability"] > 0.05:
            recommendations["diversification_strategy"] = [
                "Split capacity across multiple providers",
                "Implement active-active failover",
                "Maintain hot standby capacity"
            ]
            recommendations["diversification_needed"] = True
        
        # Specific scenario-based recommendations
        if scenario_analysis["severe_loss"]["probability"] > 0.1:
            recommendations["mitigation_strategies"].append(
                "High probability of severe loss - implement redundancy"
            )
        
        if cvar > var * 1.5:
            recommendations["mitigation_strategies"].append(
                "Fat-tail risk detected - CVaR significantly exceeds VaR"
            )
        
        return recommendations
    
    def __del__(self):
        """Cleanup executor on deletion"""
        if hasattr(self, 'executor'):
            self.executor.shutdown() 