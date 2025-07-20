"""Probabilistic risk calculation engine"""

from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta

from app.risk_engines.base import BaseRiskEngine
from app.models.settlement import Settlement, ProviderMetrics, RiskLevel
from app.config import settings

logger = logging.getLogger(__name__)


class ProbabilisticRiskEngine(BaseRiskEngine):
    """
    Basic probabilistic risk model:
    Risk Score = (1 - SLA uptime %) × (resold capacity value × downtime penalty factor)
    """
    
    def __init__(self):
        self.downtime_penalty_factor = settings.risk_downtime_penalty_factor
    
    async def calculate_risk(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Calculate risk using basic probabilistic model"""
        
        if not self.validate_inputs(settlement, provider_metrics):
            raise ValueError("Invalid input data for risk calculation")
        
        # Calculate delivery duration in hours
        delivery_duration = (settlement.delivery_end - settlement.delivery_start).total_seconds() / 3600
        
        # Get SLA uptime (convert from percentage to decimal)
        sla_uptime = provider_metrics.uptime_percentage
        if sla_uptime > 1:  # Handle if given as percentage (e.g., 99.9 instead of 0.999)
            sla_uptime = sla_uptime / 100
        
        # Calculate expected downtime probability
        downtime_probability = 1 - sla_uptime
        
        # Calculate potential loss
        # Risk Score = (1 - SLA uptime %) × (resold capacity value × downtime penalty factor)
        risk_score = downtime_probability * (settlement.total_value * self.downtime_penalty_factor)
        
        # Normalize risk score to 0-1 range
        normalized_risk_score = min(risk_score / settlement.total_value, 1.0)
        
        # Determine risk level
        risk_level = self._determine_risk_level(normalized_risk_score)
        
        # Calculate additional risk factors
        provider_reliability = self._calculate_provider_reliability(provider_metrics)
        
        # Adjust risk based on provider history
        adjusted_risk_score = normalized_risk_score * (2 - provider_reliability)
        adjusted_risk_score = min(adjusted_risk_score, 1.0)  # Cap at 1.0
        
        return {
            "risk_score": adjusted_risk_score,
            "risk_level": risk_level,
            "sla_uptime": sla_uptime,
            "downtime_probability": downtime_probability,
            "potential_loss": risk_score,
            "provider_reliability": provider_reliability,
            "delivery_duration_hours": delivery_duration,
            "calculation_method": "probabilistic",
            "factors": {
                "base_risk_score": normalized_risk_score,
                "downtime_penalty_factor": self.downtime_penalty_factor,
                "provider_adjustment": 2 - provider_reliability
            },
            "recommendations": self._generate_recommendations(adjusted_risk_score, provider_metrics)
        }
    
    def get_engine_name(self) -> str:
        return "Probabilistic Risk Engine"
    
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
    
    def _calculate_provider_reliability(self, metrics: ProviderMetrics) -> float:
        """Calculate provider reliability score (0-1)"""
        # Factors: completion rate, dispute rate, incident rate
        total_settlements = (
            metrics.completed_settlements + 
            metrics.failed_settlements + 
            metrics.disputed_settlements
        )
        
        if total_settlements == 0:
            # New provider, assign medium reliability
            return 0.5
        
        completion_rate = metrics.completed_settlements / total_settlements
        dispute_rate = metrics.disputed_settlements / total_settlements
        failure_rate = metrics.failed_settlements / total_settlements
        
        # Weight factors
        reliability = (
            completion_rate * 0.5 +  # 50% weight on completion
            (1 - dispute_rate) * 0.3 +  # 30% weight on no disputes
            (1 - failure_rate) * 0.2  # 20% weight on no failures
        )
        
        # Adjust for incident history
        if metrics.critical_incidents > 0:
            incident_penalty = min(metrics.critical_incidents * 0.1, 0.5)
            reliability = max(reliability - incident_penalty, 0)
        
        return reliability
    
    def _generate_recommendations(
        self, 
        risk_score: float, 
        provider_metrics: ProviderMetrics
    ) -> Dict[str, Any]:
        """Generate risk mitigation recommendations"""
        recommendations = {
            "require_escrow": risk_score > settings.risk_threshold_medium,
            "escrow_percentage": 0.0,
            "risk_premium": 0.0,
            "diversification_needed": False,
            "mitigation_strategies": []
        }
        
        if risk_score > settings.risk_threshold_high:
            recommendations["escrow_percentage"] = 0.2  # 20% escrow
            recommendations["risk_premium"] = 0.1  # 10% premium
            recommendations["mitigation_strategies"].append(
                "High risk: Consider alternative providers"
            )
            recommendations["diversification_needed"] = True
        elif risk_score > settings.risk_threshold_medium:
            recommendations["escrow_percentage"] = 0.1  # 10% escrow
            recommendations["risk_premium"] = 0.05  # 5% premium
            recommendations["mitigation_strategies"].append(
                "Medium risk: Monitor provider performance closely"
            )
        
        # Provider-specific recommendations
        if provider_metrics.overcommit_ratio > 1.5:
            recommendations["mitigation_strategies"].append(
                "Provider shows high overcommitment - verify capacity availability"
            )
        
        if provider_metrics.critical_incidents > 2:
            recommendations["mitigation_strategies"].append(
                f"Provider had {provider_metrics.critical_incidents} critical incidents - require SLA guarantees"
            )
        
        return recommendations 