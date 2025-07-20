"""SA-CCR (Standardized Approach for Counterparty Credit Risk) engine"""

from typing import Dict, Any, Optional
import logging
import numpy as np
from datetime import datetime, timedelta

from app.risk_engines.base import BaseRiskEngine
from app.models.settlement import Settlement, ProviderMetrics, RiskLevel
from app.config import settings

logger = logging.getLogger(__name__)


class SACCRRiskEngine(BaseRiskEngine):
    """
    Adapted SA-CCR method for compute resources:
    Exposure = α × (RC + PFE)
    where:
    - α = 1.4 (multiplier)
    - RC = Replacement Cost (cost of undelivered capacity)
    - PFE = Potential Future Exposure (based on volatility forecasts)
    """
    
    def __init__(self):
        self.alpha = settings.risk_alpha  # 1.4
        self.volatility_window_days = settings.risk_volatility_window_days
    
    async def calculate_risk(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Calculate risk using adapted SA-CCR method"""
        
        if not self.validate_inputs(settlement, provider_metrics):
            raise ValueError("Invalid input data for risk calculation")
        
        # Calculate Replacement Cost (RC)
        replacement_cost = self._calculate_replacement_cost(
            settlement, provider_metrics, market_data
        )
        
        # Calculate Potential Future Exposure (PFE)
        pfe = self._calculate_potential_future_exposure(
            settlement, provider_metrics, market_data
        )
        
        # Calculate total exposure
        # Exposure = α × (RC + PFE)
        total_exposure = self.alpha * (replacement_cost + pfe)
        
        # Normalize to risk score (0-1)
        risk_score = min(total_exposure / settlement.total_value, 1.0)
        
        # Determine risk level
        risk_level = self._determine_risk_level(risk_score)
        
        # Calculate additional metrics
        maturity_factor = self._calculate_maturity_factor(settlement)
        volatility = self._estimate_volatility(provider_metrics, market_data)
        
        return {
            "risk_score": risk_score,
            "risk_level": risk_level,
            "exposure": total_exposure,
            "replacement_cost": replacement_cost,
            "potential_future_exposure": pfe,
            "alpha": self.alpha,
            "maturity_factor": maturity_factor,
            "volatility": volatility,
            "calculation_method": "sa_ccr",
            "factors": {
                "rc_percentage": replacement_cost / settlement.total_value,
                "pfe_percentage": pfe / settlement.total_value,
                "normalized_exposure": total_exposure / settlement.total_value
            },
            "recommendations": self._generate_recommendations(
                risk_score, total_exposure, settlement.total_value
            )
        }
    
    def get_engine_name(self) -> str:
        return "SA-CCR Risk Engine"
    
    def _calculate_replacement_cost(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> float:
        """Calculate replacement cost of undelivered capacity"""
        
        # Base replacement cost is the settlement value
        base_rc = settlement.total_value
        
        # Adjust for provider reliability
        # Less reliable providers = higher replacement cost
        reliability_factor = 1 + (1 - provider_metrics.uptime_percentage)
        
        # Market premium for urgent replacement
        market_premium = 1.2  # 20% premium for spot replacement
        if market_data and "spot_premium" in market_data:
            market_premium = 1 + market_data["spot_premium"]
        
        # Time criticality factor
        time_to_delivery = (settlement.delivery_start - datetime.utcnow()).total_seconds() / 3600
        urgency_factor = 1.0
        if time_to_delivery < 24:  # Less than 24 hours
            urgency_factor = 1.5
        elif time_to_delivery < 72:  # Less than 3 days
            urgency_factor = 1.2
        
        # Calculate total replacement cost
        replacement_cost = base_rc * reliability_factor * market_premium * urgency_factor
        
        # Adjust for overcommitment risk
        if provider_metrics.overcommit_ratio > 1.2:
            overcommit_penalty = min(provider_metrics.overcommit_ratio - 1, 0.5)
            replacement_cost *= (1 + overcommit_penalty)
        
        return replacement_cost
    
    def _calculate_potential_future_exposure(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> float:
        """Calculate potential future exposure based on volatility"""
        
        # Get volatility estimate
        volatility = self._estimate_volatility(provider_metrics, market_data)
        
        # Calculate time to maturity in years
        maturity_days = (settlement.delivery_end - datetime.utcnow()).days
        maturity_years = maturity_days / 365.0
        
        # PFE formula adapted for compute resources
        # PFE = Notional × Volatility × sqrt(Maturity) × Supervisory Factor
        notional = settlement.total_value
        supervisory_factor = self._get_supervisory_factor(settlement.resource_type.value)
        
        # Base PFE calculation
        pfe = notional * volatility * np.sqrt(maturity_years) * supervisory_factor
        
        # Adjust for contract type and delivery risk
        delivery_risk_factor = 1.0
        if settlement.delivery_end - settlement.delivery_start > timedelta(days=30):
            # Long-term contracts have higher exposure
            delivery_risk_factor = 1.2
        
        # Adjust for provider concentration
        concentration_factor = 1.0
        if market_data and "provider_market_share" in market_data:
            if market_data["provider_market_share"] > 0.3:  # > 30% market share
                concentration_factor = 1.15
        
        return pfe * delivery_risk_factor * concentration_factor
    
    def _estimate_volatility(
        self,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> float:
        """Estimate volatility for the provider/resource"""
        
        # Base volatility from historical performance
        base_volatility = 0.1  # 10% base volatility
        
        # Adjust based on provider stability
        if provider_metrics.total_incidents > 10:
            incident_volatility = min(provider_metrics.total_incidents * 0.01, 0.3)
            base_volatility += incident_volatility
        
        # Market volatility if available
        if market_data and "market_volatility" in market_data:
            market_vol = market_data["market_volatility"]
            base_volatility = 0.7 * base_volatility + 0.3 * market_vol
        
        # Provider-specific adjustments
        if provider_metrics.payment_default_rate > 0.05:  # > 5% default rate
            base_volatility *= 1.5
        
        return min(base_volatility, 0.5)  # Cap at 50%
    
    def _calculate_maturity_factor(self, settlement: Settlement) -> float:
        """Calculate maturity adjustment factor"""
        days_to_maturity = (settlement.delivery_end - datetime.utcnow()).days
        
        if days_to_maturity <= 5:
            return 0.5
        elif days_to_maturity <= 30:
            return 1.0
        elif days_to_maturity <= 90:
            return 1.5
        else:
            return 2.0
    
    def _get_supervisory_factor(self, resource_type: str) -> float:
        """Get supervisory factor based on resource type"""
        factors = {
            "cpu": 0.15,
            "gpu": 0.25,  # Higher for GPU due to scarcity
            "memory": 0.10,
            "storage": 0.08,
            "network": 0.12,
            "composite": 0.20
        }
        return factors.get(resource_type, 0.15)
    
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
        total_exposure: float,
        settlement_value: float
    ) -> Dict[str, Any]:
        """Generate SA-CCR specific recommendations"""
        recommendations = {
            "require_escrow": risk_score > settings.risk_threshold_low,
            "escrow_amount": 0.0,
            "credit_limit": 0.0,
            "collateral_requirements": [],
            "mitigation_strategies": []
        }
        
        # Calculate escrow based on exposure
        if total_exposure > settlement_value:
            excess_exposure = total_exposure - settlement_value
            recommendations["escrow_amount"] = min(excess_exposure, settlement_value * 0.3)
            recommendations["mitigation_strategies"].append(
                f"Exposure exceeds settlement value by {excess_exposure:.2f}"
            )
        
        # Set credit limits
        if risk_score < settings.risk_threshold_low:
            recommendations["credit_limit"] = settlement_value * 5
        elif risk_score < settings.risk_threshold_medium:
            recommendations["credit_limit"] = settlement_value * 2
        else:
            recommendations["credit_limit"] = settlement_value * 1.1
        
        # Collateral requirements
        if risk_score > settings.risk_threshold_medium:
            recommendations["collateral_requirements"].append({
                "type": "cash",
                "amount": settlement_value * 0.2,
                "currency": "USD"
            })
        
        if risk_score > settings.risk_threshold_high:
            recommendations["collateral_requirements"].append({
                "type": "compute_credits",
                "amount": settlement_value * 0.3,
                "provider": "alternative"
            })
            recommendations["mitigation_strategies"].append(
                "High exposure: Require multi-provider fallback arrangements"
            )
        
        return recommendations 