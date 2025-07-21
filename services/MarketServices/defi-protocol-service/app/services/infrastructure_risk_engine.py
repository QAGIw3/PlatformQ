"""
Infrastructure Risk Engine

Unified risk management for DeFi and infrastructure risks.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum

from ..models import ResourceType, ServiceTier
from .risk_calculator import RiskCalculator
from .price_oracle import PriceOracle
from .resource_valuation import ResourceValuationService

logger = logging.getLogger(__name__)


class RiskFactor(str, Enum):
    """Risk factors for infrastructure"""
    PROVIDER_RELIABILITY = "provider_reliability"
    RESOURCE_VOLATILITY = "resource_volatility"
    TIME_DECAY = "time_decay"
    UTILIZATION_RATE = "utilization_rate"
    GEOGRAPHIC_CONCENTRATION = "geographic_concentration"
    TECHNOLOGY_OBSOLESCENCE = "technology_obsolescence"
    REGULATORY_COMPLIANCE = "regulatory_compliance"
    COUNTERPARTY_RISK = "counterparty_risk"


class InfrastructureRiskEngine:
    """Unified risk engine for DeFi and infrastructure"""
    
    # Risk weights (basis points)
    RISK_WEIGHTS = {
        RiskFactor.PROVIDER_RELIABILITY: 2000,      # 20%
        RiskFactor.RESOURCE_VOLATILITY: 2500,       # 25%
        RiskFactor.TIME_DECAY: 1500,               # 15%
        RiskFactor.UTILIZATION_RATE: 1000,         # 10%
        RiskFactor.GEOGRAPHIC_CONCENTRATION: 1000,  # 10%
        RiskFactor.TECHNOLOGY_OBSOLESCENCE: 1000,   # 10%
        RiskFactor.REGULATORY_COMPLIANCE: 500,      # 5%
        RiskFactor.COUNTERPARTY_RISK: 500          # 5%
    }
    
    # Thresholds for risk levels
    RISK_THRESHOLDS = {
        "low": Decimal("30"),
        "medium": Decimal("50"),
        "high": Decimal("70"),
        "critical": Decimal("90")
    }
    
    def __init__(
        self,
        risk_calculator: RiskCalculator,
        price_oracle: PriceOracle,
        valuation_service: ResourceValuationService
    ):
        self.risk_calculator = risk_calculator
        self.price_oracle = price_oracle
        self.valuation_service = valuation_service
        self._provider_metrics = {}  # Cache provider metrics
        self._risk_cache = {}  # Cache risk calculations
        
    async def calculate_unified_risk(
        self,
        resource_type: ResourceType,
        service_tier: ServiceTier,
        provider: str,
        amount: int,
        duration_days: int,
        region: str,
        loan_amount: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """
        Calculate unified risk score combining DeFi and infrastructure risks
        
        Returns risk score (0-100) and breakdown by factor
        """
        try:
            # Calculate individual risk factors
            risk_factors = {}
            
            # Provider reliability risk
            risk_factors[RiskFactor.PROVIDER_RELIABILITY] = await self._calculate_provider_risk(provider)
            
            # Resource volatility risk
            risk_factors[RiskFactor.RESOURCE_VOLATILITY] = await self._calculate_volatility_risk(
                resource_type, service_tier
            )
            
            # Time decay risk
            risk_factors[RiskFactor.TIME_DECAY] = self._calculate_time_decay_risk(duration_days)
            
            # Utilization risk
            risk_factors[RiskFactor.UTILIZATION_RATE] = await self._calculate_utilization_risk(
                resource_type, region
            )
            
            # Geographic concentration risk
            risk_factors[RiskFactor.GEOGRAPHIC_CONCENTRATION] = await self._calculate_geographic_risk(region)
            
            # Technology obsolescence risk
            risk_factors[RiskFactor.TECHNOLOGY_OBSOLESCENCE] = self._calculate_obsolescence_risk(
                resource_type, duration_days
            )
            
            # Regulatory compliance risk
            risk_factors[RiskFactor.REGULATORY_COMPLIANCE] = await self._calculate_regulatory_risk(region)
            
            # Counterparty risk (if loan involved)
            if loan_amount:
                risk_factors[RiskFactor.COUNTERPARTY_RISK] = await self._calculate_counterparty_risk(
                    provider, loan_amount
                )
            else:
                risk_factors[RiskFactor.COUNTERPARTY_RISK] = Decimal("0")
            
            # Calculate weighted risk score
            total_risk = Decimal("0")
            for factor, score in risk_factors.items():
                weight = Decimal(self.RISK_WEIGHTS[factor]) / Decimal("10000")
                total_risk += score * weight
                
            # Determine risk level
            risk_level = self._get_risk_level(total_risk)
            
            # Calculate risk-adjusted parameters
            risk_premium = self._calculate_risk_premium(total_risk)
            required_collateral_ratio = self._calculate_required_collateral(total_risk)
            
            return {
                "risk_score": float(total_risk),
                "risk_level": risk_level,
                "risk_factors": {k.value: float(v) for k, v in risk_factors.items()},
                "risk_premium": float(risk_premium),
                "required_collateral_ratio": float(required_collateral_ratio),
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error calculating unified risk: {e}")
            raise
            
    async def _calculate_provider_risk(self, provider: str) -> Decimal:
        """Calculate risk based on provider metrics"""
        # Get provider metrics (from cache or fetch)
        metrics = await self._get_provider_metrics(provider)
        
        if not metrics:
            return Decimal("80")  # High risk for unknown providers
            
        # Score based on reputation, uptime, and SLA compliance
        reputation_score = metrics.get("reputation", 0)
        uptime_percentage = metrics.get("uptime", 0)
        sla_compliance = metrics.get("sla_compliance", 0)
        
        # Normalize scores (0-100, where 100 is highest risk)
        provider_risk = Decimal("100")
        provider_risk -= Decimal(reputation_score) / Decimal("10")  # Max 50 points
        provider_risk -= Decimal(uptime_percentage) / Decimal("2")   # Max 50 points
        provider_risk *= (Decimal("100") - Decimal(sla_compliance)) / Decimal("100")
        
        return max(Decimal("0"), min(Decimal("100"), provider_risk))
        
    async def _calculate_volatility_risk(
        self,
        resource_type: ResourceType,
        service_tier: ServiceTier
    ) -> Decimal:
        """Calculate risk based on resource price volatility"""
        # Get historical volatility data
        volatility_factor = self.valuation_service.get_volatility_factor(resource_type)
        
        # Adjust for service tier (higher tiers are more stable)
        tier_multiplier = {
            ServiceTier.STANDARD: Decimal("1.2"),
            ServiceTier.PREMIUM: Decimal("1.0"),
            ServiceTier.GUARANTEED: Decimal("0.8")
        }
        
        volatility_risk = (volatility_factor / Decimal("2")) * tier_multiplier[service_tier]
        
        return min(Decimal("100"), volatility_risk)
        
    def _calculate_time_decay_risk(self, duration_days: int) -> Decimal:
        """Calculate risk based on time decay of resources"""
        # Longer durations have higher time decay risk
        if duration_days <= 7:
            return Decimal("10")
        elif duration_days <= 30:
            return Decimal("25")
        elif duration_days <= 90:
            return Decimal("50")
        elif duration_days <= 180:
            return Decimal("70")
        else:
            return Decimal("90")
            
    async def _calculate_utilization_risk(
        self,
        resource_type: ResourceType,
        region: str
    ) -> Decimal:
        """Calculate risk based on resource utilization rates"""
        # Get utilization data from oracle
        utilization_key = f"UTILIZATION_{resource_type.value}_{region}"
        utilization = await self.price_oracle.get_price(utilization_key)
        
        if not utilization:
            utilization = 70  # Default to 70% if no data
            
        # High utilization increases risk (supply constraints)
        if utilization < 50:
            return Decimal("20")
        elif utilization < 70:
            return Decimal("40")
        elif utilization < 85:
            return Decimal("60")
        elif utilization < 95:
            return Decimal("80")
        else:
            return Decimal("100")
            
    async def _calculate_geographic_risk(self, region: str) -> Decimal:
        """Calculate risk based on geographic factors"""
        # Risk scores by region (based on stability, regulations, etc.)
        region_risks = {
            "us-east-1": Decimal("20"),
            "us-west-1": Decimal("20"),
            "eu-west-1": Decimal("25"),
            "eu-central-1": Decimal("25"),
            "ap-southeast-1": Decimal("40"),
            "ap-northeast-1": Decimal("30"),
            "sa-east-1": Decimal("60"),
            "cn-north-1": Decimal("70")
        }
        
        return region_risks.get(region, Decimal("50"))
        
    def _calculate_obsolescence_risk(
        self,
        resource_type: ResourceType,
        duration_days: int
    ) -> Decimal:
        """Calculate technology obsolescence risk"""
        # GPU and specialized compute have higher obsolescence risk
        base_risk = {
            ResourceType.CPU: Decimal("20"),
            ResourceType.GPU: Decimal("60"),  # Rapid evolution
            ResourceType.STORAGE: Decimal("10"),
            ResourceType.BANDWIDTH: Decimal("5"),
            ResourceType.MEMORY: Decimal("15")
        }
        
        # Increase risk for longer durations
        duration_multiplier = Decimal("1") + (Decimal(duration_days) / Decimal("365"))
        
        return min(Decimal("100"), base_risk[resource_type] * duration_multiplier)
        
    async def _calculate_regulatory_risk(self, region: str) -> Decimal:
        """Calculate regulatory compliance risk"""
        # Some regions have stricter data regulations
        regulatory_risks = {
            "us-east-1": Decimal("30"),
            "us-west-1": Decimal("30"),
            "eu-west-1": Decimal("40"),  # GDPR
            "eu-central-1": Decimal("40"),  # GDPR
            "ap-southeast-1": Decimal("35"),
            "ap-northeast-1": Decimal("35"),
            "sa-east-1": Decimal("45"),
            "cn-north-1": Decimal("80")  # High regulatory uncertainty
        }
        
        return regulatory_risks.get(region, Decimal("50"))
        
    async def _calculate_counterparty_risk(
        self,
        provider: str,
        loan_amount: Decimal
    ) -> Decimal:
        """Calculate counterparty risk for loans"""
        # Get provider's financial metrics
        metrics = await self._get_provider_metrics(provider)
        
        if not metrics:
            return Decimal("90")  # High risk for unknown providers
            
        # Check collateral coverage
        total_collateral = Decimal(metrics.get("total_collateral", "0"))
        total_loans = Decimal(metrics.get("total_loans", "0"))
        
        if total_collateral == 0:
            return Decimal("100")
            
        coverage_ratio = total_collateral / (total_loans + loan_amount)
        
        # Lower coverage means higher risk
        if coverage_ratio > 2:
            return Decimal("10")
        elif coverage_ratio > 1.5:
            return Decimal("30")
        elif coverage_ratio > 1.2:
            return Decimal("50")
        elif coverage_ratio > 1:
            return Decimal("70")
        else:
            return Decimal("90")
            
    async def _get_provider_metrics(self, provider: str) -> Dict[str, Any]:
        """Get provider metrics from cache or fetch"""
        if provider in self._provider_metrics:
            cached = self._provider_metrics[provider]
            if cached["timestamp"] > datetime.utcnow() - timedelta(minutes=5):
                return cached["data"]
                
        # Fetch from oracle or database
        # This would integrate with the infrastructure monitoring system
        metrics = {
            "reputation": 500,  # 0-1000 scale
            "uptime": 99.5,    # Percentage
            "sla_compliance": 98,  # Percentage
            "total_collateral": "1000000",  # USD value
            "total_loans": "500000"  # USD value
        }
        
        # Cache the metrics
        self._provider_metrics[provider] = {
            "data": metrics,
            "timestamp": datetime.utcnow()
        }
        
        return metrics
        
    def _get_risk_level(self, risk_score: Decimal) -> str:
        """Determine risk level based on score"""
        if risk_score < self.RISK_THRESHOLDS["low"]:
            return "low"
        elif risk_score < self.RISK_THRESHOLDS["medium"]:
            return "medium"
        elif risk_score < self.RISK_THRESHOLDS["high"]:
            return "high"
        elif risk_score < self.RISK_THRESHOLDS["critical"]:
            return "critical"
        else:
            return "extreme"
            
    def _calculate_risk_premium(self, risk_score: Decimal) -> Decimal:
        """Calculate additional interest rate premium based on risk"""
        # Base premium increases with risk score
        # 0-30: 0-1%, 30-50: 1-3%, 50-70: 3-6%, 70-90: 6-10%, 90+: 10%+
        if risk_score < 30:
            return risk_score / Decimal("30") * Decimal("0.01")
        elif risk_score < 50:
            return Decimal("0.01") + (risk_score - 30) / Decimal("20") * Decimal("0.02")
        elif risk_score < 70:
            return Decimal("0.03") + (risk_score - 50) / Decimal("20") * Decimal("0.03")
        elif risk_score < 90:
            return Decimal("0.06") + (risk_score - 70) / Decimal("20") * Decimal("0.04")
        else:
            return Decimal("0.10") + (risk_score - 90) / Decimal("10") * Decimal("0.02")
            
    def _calculate_required_collateral(self, risk_score: Decimal) -> Decimal:
        """Calculate required collateral ratio based on risk"""
        # Base collateral requirement increases with risk
        # Low risk: 120%, Medium: 150%, High: 200%, Critical: 300%
        if risk_score < 30:
            return Decimal("1.2")
        elif risk_score < 50:
            return Decimal("1.5")
        elif risk_score < 70:
            return Decimal("2.0")
        elif risk_score < 90:
            return Decimal("3.0")
        else:
            return Decimal("5.0")
            
    async def monitor_portfolio_risk(
        self,
        portfolio: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Monitor risk across a portfolio of resources"""
        total_risk = Decimal("0")
        risk_breakdown = {}
        high_risk_positions = []
        
        for position in portfolio:
            risk_result = await self.calculate_unified_risk(
                resource_type=ResourceType(position["resource_type"]),
                service_tier=ServiceTier(position["service_tier"]),
                provider=position["provider"],
                amount=position["amount"],
                duration_days=position["duration_days"],
                region=position["region"],
                loan_amount=position.get("loan_amount")
            )
            
            position_value = Decimal(position["value"])
            weighted_risk = Decimal(risk_result["risk_score"]) * position_value
            total_risk += weighted_risk
            
            # Track high-risk positions
            if risk_result["risk_level"] in ["high", "critical", "extreme"]:
                high_risk_positions.append({
                    "position_id": position["id"],
                    "risk_score": risk_result["risk_score"],
                    "risk_level": risk_result["risk_level"],
                    "main_risk_factors": sorted(
                        risk_result["risk_factors"].items(),
                        key=lambda x: x[1],
                        reverse=True
                    )[:3]
                })
                
            # Aggregate risk factors
            for factor, score in risk_result["risk_factors"].items():
                if factor not in risk_breakdown:
                    risk_breakdown[factor] = Decimal("0")
                risk_breakdown[factor] += Decimal(score) * position_value
                
        # Calculate portfolio metrics
        total_value = sum(Decimal(p["value"]) for p in portfolio)
        portfolio_risk_score = total_risk / total_value if total_value > 0 else Decimal("0")
        
        # Normalize risk breakdown
        for factor in risk_breakdown:
            risk_breakdown[factor] = float(risk_breakdown[factor] / total_value) if total_value > 0 else 0
            
        return {
            "portfolio_risk_score": float(portfolio_risk_score),
            "portfolio_risk_level": self._get_risk_level(portfolio_risk_score),
            "risk_breakdown": risk_breakdown,
            "high_risk_positions": high_risk_positions,
            "total_positions": len(portfolio),
            "total_value": float(total_value),
            "recommendations": self._generate_risk_recommendations(
                portfolio_risk_score,
                risk_breakdown,
                high_risk_positions
            )
        }
        
    def _generate_risk_recommendations(
        self,
        portfolio_risk_score: Decimal,
        risk_breakdown: Dict[str, float],
        high_risk_positions: List[Dict]
    ) -> List[str]:
        """Generate risk mitigation recommendations"""
        recommendations = []
        
        # Overall portfolio risk
        if portfolio_risk_score > 70:
            recommendations.append("Consider reducing overall portfolio risk exposure")
            
        # Specific risk factors
        for factor, score in risk_breakdown.items():
            if score > 60:
                if factor == RiskFactor.PROVIDER_RELIABILITY.value:
                    recommendations.append("Diversify across more reliable providers")
                elif factor == RiskFactor.RESOURCE_VOLATILITY.value:
                    recommendations.append("Consider hedging volatile resource positions")
                elif factor == RiskFactor.TIME_DECAY.value:
                    recommendations.append("Reduce exposure to long-duration resources")
                elif factor == RiskFactor.GEOGRAPHIC_CONCENTRATION.value:
                    recommendations.append("Diversify across multiple geographic regions")
                    
        # High-risk positions
        if len(high_risk_positions) > 0:
            recommendations.append(f"Review and potentially reduce {len(high_risk_positions)} high-risk positions")
            
        return recommendations 