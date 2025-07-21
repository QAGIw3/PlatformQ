"""
Graph Intelligence Integration

Integrates Graph Intelligence Service for advanced risk assessment,
relationship analysis, and recommendation generation.
"""

import httpx
import logging
from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import networkx as nx
import numpy as np
from collections import defaultdict

from app.integrations import IgniteCache, PulsarEventPublisher

logger = logging.getLogger(__name__)


class RiskCategory(Enum):
    """Risk assessment categories"""
    COUNTERPARTY = "counterparty"
    OPERATIONAL = "operational"
    MARKET = "market"
    LIQUIDITY = "liquidity"
    REPUTATION = "reputation"
    SYSTEMIC = "systemic"


@dataclass
class TrustScore:
    """Multi-dimensional trust score"""
    overall_score: float  # 0-100
    reliability: float    # Track record of fulfilling obligations
    competence: float     # Technical capability
    integrity: float      # Ethical behavior
    transparency: float   # Information sharing
    collaboration: float  # Cooperation with others
    last_updated: datetime
    confidence: float     # Confidence in the score (0-1)


@dataclass
class RiskAssessment:
    """Comprehensive risk assessment"""
    entity_id: str
    risk_score: float  # 0-100, higher is riskier
    risk_categories: Dict[RiskCategory, float]
    trust_score: TrustScore
    network_centrality: float
    cluster_risk: float
    recommendations: List[str]
    mitigations: List[Dict[str, Any]]
    assessment_date: datetime


@dataclass
class ComputeUsagePattern:
    """Historical compute usage patterns"""
    entity_id: str
    avg_daily_gpu_hours: Decimal
    avg_daily_cpu_hours: Decimal
    peak_usage_hours: List[int]  # Hours of day with peak usage
    preferred_providers: List[str]
    reliability_score: float  # 0-1, how reliably they pay
    typical_workload_type: str
    seasonal_patterns: Dict[str, float]


@dataclass
class RelationshipInsight:
    """Insights about entity relationships"""
    entity_id: str
    total_connections: int
    trusted_connections: int
    clusters: List[str]  # Community clusters they belong to
    influence_score: float
    bridge_score: float  # How much they connect different communities
    collaboration_frequency: Dict[str, int]  # entity_id -> interaction count


class GraphIntelligenceIntegration:
    """Integration with Graph Intelligence Service"""
    
    def __init__(
        self,
        graph_service_url: str = "http://graph-intelligence-service:8000",
        ignite_cache: Optional[IgniteCache] = None,
        pulsar_publisher: Optional[PulsarEventPublisher] = None
    ):
        self.graph_service_url = graph_service_url
        self.ignite_cache = ignite_cache
        self.pulsar_publisher = pulsar_publisher
        
        # HTTP client
        self.client = httpx.AsyncClient(
            base_url=graph_service_url,
            timeout=30.0
        )
        
        # Local graph cache for fast computations
        self.local_graph = nx.DiGraph()
        self.last_graph_sync = datetime.min
        
    async def assess_counterparty_risk(
        self,
        trader_id: str,
        transaction_type: str = "compute_futures",
        transaction_value: Optional[Decimal] = None
    ) -> RiskAssessment:
        """Comprehensive counterparty risk assessment"""
        try:
            # Get trust score
            trust_score = await self.get_trust_score(trader_id)
            
            # Get network analysis
            network_analysis = await self.analyze_network_position(trader_id)
            
            # Get compute usage patterns
            usage_patterns = await self.get_compute_usage_patterns(trader_id)
            
            # Get transaction history
            transaction_history = await self._get_transaction_history(trader_id)
            
            # Calculate risk scores
            risk_categories = {
                RiskCategory.COUNTERPARTY: self._calculate_counterparty_risk(
                    trust_score, transaction_history
                ),
                RiskCategory.OPERATIONAL: self._calculate_operational_risk(
                    usage_patterns
                ),
                RiskCategory.MARKET: self._calculate_market_risk(
                    transaction_value, usage_patterns
                ),
                RiskCategory.LIQUIDITY: self._calculate_liquidity_risk(
                    transaction_history, network_analysis
                ),
                RiskCategory.REPUTATION: 100 - trust_score.overall_score,
                RiskCategory.SYSTEMIC: network_analysis.get("systemic_risk", 0)
            }
            
            # Overall risk score (weighted average)
            weights = {
                RiskCategory.COUNTERPARTY: 0.3,
                RiskCategory.OPERATIONAL: 0.2,
                RiskCategory.MARKET: 0.15,
                RiskCategory.LIQUIDITY: 0.15,
                RiskCategory.REPUTATION: 0.15,
                RiskCategory.SYSTEMIC: 0.05
            }
            
            overall_risk = sum(
                risk_categories[cat] * weights[cat]
                for cat in risk_categories
            )
            
            # Generate recommendations
            recommendations = self._generate_risk_recommendations(
                risk_categories,
                trust_score,
                network_analysis
            )
            
            # Generate mitigations
            mitigations = self._generate_risk_mitigations(
                risk_categories,
                transaction_type,
                transaction_value
            )
            
            assessment = RiskAssessment(
                entity_id=trader_id,
                risk_score=overall_risk,
                risk_categories=risk_categories,
                trust_score=trust_score,
                network_centrality=network_analysis.get("centrality", 0),
                cluster_risk=network_analysis.get("cluster_risk", 0),
                recommendations=recommendations,
                mitigations=mitigations,
                assessment_date=datetime.utcnow()
            )
            
            # Cache assessment
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"risk_assessment:{trader_id}",
                    assessment.__dict__,
                    ttl=3600  # 1 hour cache
                )
            
            # Publish risk event if high risk
            if overall_risk > 70:
                await self._publish_high_risk_alert(assessment)
            
            return assessment
            
        except Exception as e:
            logger.error(f"Error assessing counterparty risk: {e}")
            # Return conservative assessment on error
            return self._get_default_risk_assessment(trader_id)
    
    async def get_trust_score(self, entity_id: str) -> TrustScore:
        """Get multi-dimensional trust score from Graph Intelligence"""
        try:
            response = await self.client.get(
                f"/api/v1/trust/score/{entity_id}"
            )
            
            if response.status_code == 200:
                data = response.json()
                return TrustScore(
                    overall_score=data["overall_score"],
                    reliability=data["dimensions"]["reliability"],
                    competence=data["dimensions"]["competence"],
                    integrity=data["dimensions"]["integrity"],
                    transparency=data["dimensions"]["transparency"],
                    collaboration=data["dimensions"]["collaboration"],
                    last_updated=datetime.fromisoformat(data["last_updated"]),
                    confidence=data["confidence"]
                )
            else:
                return self._get_default_trust_score()
                
        except Exception as e:
            logger.error(f"Error getting trust score: {e}")
            return self._get_default_trust_score()
    
    async def analyze_network_position(
        self,
        entity_id: str
    ) -> Dict[str, Any]:
        """Analyze entity's position in the trust network"""
        try:
            response = await self.client.post(
                "/api/v1/network/analyze",
                json={
                    "entity_id": entity_id,
                    "metrics": [
                        "centrality",
                        "clustering",
                        "influence",
                        "bridge_score"
                    ]
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Calculate additional metrics
                cluster_risk = await self._calculate_cluster_risk(
                    entity_id,
                    data.get("clusters", [])
                )
                
                systemic_risk = self._calculate_systemic_risk(
                    data.get("centrality", 0),
                    data.get("influence", 0)
                )
                
                return {
                    **data,
                    "cluster_risk": cluster_risk,
                    "systemic_risk": systemic_risk
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing network position: {e}")
            return {}
    
    async def get_compute_usage_patterns(
        self,
        entity_id: str
    ) -> ComputeUsagePattern:
        """Get historical compute usage patterns"""
        try:
            # Get usage data from graph intelligence
            response = await self.client.get(
                f"/api/v1/entities/{entity_id}/compute-history"
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Analyze patterns
                patterns = self._analyze_usage_patterns(data["usage_history"])
                
                return ComputeUsagePattern(
                    entity_id=entity_id,
                    avg_daily_gpu_hours=Decimal(str(patterns["avg_gpu_hours"])),
                    avg_daily_cpu_hours=Decimal(str(patterns["avg_cpu_hours"])),
                    peak_usage_hours=patterns["peak_hours"],
                    preferred_providers=patterns["preferred_providers"],
                    reliability_score=patterns["payment_reliability"],
                    typical_workload_type=patterns["workload_type"],
                    seasonal_patterns=patterns["seasonal_patterns"]
                )
            else:
                return self._get_default_usage_pattern(entity_id)
                
        except Exception as e:
            logger.error(f"Error getting compute usage patterns: {e}")
            return self._get_default_usage_pattern(entity_id)
    
    async def get_recommendations(
        self,
        context: str,
        entity_id: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Get AI-powered recommendations"""
        try:
            request_data = {
                "context": context,
                "parameters": parameters or {}
            }
            
            if entity_id:
                request_data["entity_id"] = entity_id
            
            response = await self.client.post(
                "/api/v1/recommendations/generate",
                json=request_data
            )
            
            if response.status_code == 200:
                return response.json()["recommendations"]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []
    
    async def find_similar_entities(
        self,
        entity_id: str,
        similarity_threshold: float = 0.7,
        limit: int = 10
    ) -> List[Tuple[str, float]]:
        """Find entities with similar behavior patterns"""
        try:
            response = await self.client.post(
                "/api/v1/similarity/find",
                json={
                    "entity_id": entity_id,
                    "threshold": similarity_threshold,
                    "limit": limit,
                    "features": [
                        "compute_usage",
                        "trading_behavior",
                        "network_position"
                    ]
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                return [(e["entity_id"], e["similarity"]) for e in data["similar_entities"]]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error finding similar entities: {e}")
            return []
    
    async def predict_behavior(
        self,
        entity_id: str,
        prediction_type: str,
        horizon_days: int = 7
    ) -> Dict[str, Any]:
        """Predict future behavior using graph intelligence"""
        try:
            response = await self.client.post(
                "/api/v1/predictions/behavior",
                json={
                    "entity_id": entity_id,
                    "prediction_type": prediction_type,
                    "horizon_days": horizon_days
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error predicting behavior: {e}")
            return {}
    
    # Private helper methods
    def _calculate_counterparty_risk(
        self,
        trust_score: TrustScore,
        transaction_history: Dict[str, Any]
    ) -> float:
        """Calculate counterparty-specific risk"""
        # Base risk from trust score
        base_risk = 100 - trust_score.overall_score
        
        # Adjust based on transaction history
        if transaction_history:
            failure_rate = transaction_history.get("failure_rate", 0)
            avg_delay = transaction_history.get("avg_settlement_delay_hours", 0)
            
            # Increase risk based on failures and delays
            history_risk = (failure_rate * 50) + min(avg_delay * 2, 20)
            
            # Weight by confidence in data
            confidence = min(transaction_history.get("transaction_count", 0) / 10, 1.0)
            
            return base_risk * (1 - confidence) + history_risk * confidence
        
        return base_risk
    
    def _calculate_operational_risk(
        self,
        usage_patterns: ComputeUsagePattern
    ) -> float:
        """Calculate operational risk based on usage patterns"""
        risk = 0
        
        # High variability in usage increases risk
        if usage_patterns.seasonal_patterns:
            variability = np.std(list(usage_patterns.seasonal_patterns.values()))
            risk += min(variability * 10, 30)
        
        # Low reliability increases risk
        risk += (1 - usage_patterns.reliability_score) * 40
        
        # Concentration in few providers increases risk
        if len(usage_patterns.preferred_providers) < 2:
            risk += 20
        
        return min(risk, 100)
    
    def _calculate_market_risk(
        self,
        transaction_value: Optional[Decimal],
        usage_patterns: ComputeUsagePattern
    ) -> float:
        """Calculate market risk"""
        if not transaction_value:
            return 30  # Default medium risk
        
        # Large transactions relative to typical usage increase risk
        typical_daily_value = (
            usage_patterns.avg_daily_gpu_hours * Decimal("2.5") +  # $2.5/GPU hour
            usage_patterns.avg_daily_cpu_hours * Decimal("0.1")    # $0.1/CPU hour
        )
        
        if typical_daily_value > 0:
            size_ratio = float(transaction_value / typical_daily_value)
            if size_ratio > 10:  # 10x typical size
                return min(50 + (size_ratio - 10) * 2, 90)
            
        return 30
    
    def _calculate_liquidity_risk(
        self,
        transaction_history: Dict[str, Any],
        network_analysis: Dict[str, Any]
    ) -> float:
        """Calculate liquidity risk"""
        risk = 20  # Base risk
        
        # Low network connections increase liquidity risk
        if network_analysis.get("total_connections", 0) < 5:
            risk += 30
        
        # History of payment delays
        if transaction_history:
            avg_delay = transaction_history.get("avg_settlement_delay_hours", 0)
            risk += min(avg_delay * 3, 40)
        
        return min(risk, 100)
    
    async def _calculate_cluster_risk(
        self,
        entity_id: str,
        clusters: List[str]
    ) -> float:
        """Calculate risk from cluster membership"""
        if not clusters:
            return 50  # Unknown cluster risk
        
        total_risk = 0
        for cluster_id in clusters:
            # Get cluster risk score
            try:
                response = await self.client.get(
                    f"/api/v1/clusters/{cluster_id}/risk-score"
                )
                if response.status_code == 200:
                    total_risk += response.json()["risk_score"]
            except:
                total_risk += 30  # Default cluster risk
        
        return min(total_risk / len(clusters), 100) if clusters else 50
    
    def _calculate_systemic_risk(
        self,
        centrality: float,
        influence: float
    ) -> float:
        """Calculate systemic risk based on network position"""
        # High centrality and influence = high systemic risk
        return min((centrality * 50 + influence * 50), 100)
    
    def _generate_risk_recommendations(
        self,
        risk_categories: Dict[RiskCategory, float],
        trust_score: TrustScore,
        network_analysis: Dict[str, Any]
    ) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # High counterparty risk
        if risk_categories[RiskCategory.COUNTERPARTY] > 70:
            recommendations.append("Require additional collateral or use escrow")
            recommendations.append("Consider shorter contract durations")
        
        # Low trust score
        if trust_score.overall_score < 50:
            recommendations.append("Request verifiable credentials")
            recommendations.append("Start with small transactions to build trust")
        
        # High operational risk
        if risk_categories[RiskCategory.OPERATIONAL] > 70:
            recommendations.append("Implement redundancy with multiple providers")
            recommendations.append("Add SLA penalties to contracts")
        
        # Low network connections
        if network_analysis.get("total_connections", 0) < 5:
            recommendations.append("Limited network visibility - gather more information")
        
        # High systemic risk
        if risk_categories[RiskCategory.SYSTEMIC] > 50:
            recommendations.append("This entity is highly connected - failure could cascade")
            recommendations.append("Consider position limits")
        
        return recommendations
    
    def _generate_risk_mitigations(
        self,
        risk_categories: Dict[RiskCategory, float],
        transaction_type: str,
        transaction_value: Optional[Decimal]
    ) -> List[Dict[str, Any]]:
        """Generate specific risk mitigation strategies"""
        mitigations = []
        
        # Collateral requirements
        if risk_categories[RiskCategory.COUNTERPARTY] > 50:
            collateral_ratio = 0.2 + (risk_categories[RiskCategory.COUNTERPARTY] - 50) / 100
            mitigations.append({
                "type": "collateral",
                "description": "Require additional collateral",
                "parameters": {
                    "collateral_ratio": collateral_ratio,
                    "acceptable_collateral": ["USDC", "ETH", "platform_tokens"]
                }
            })
        
        # Margin requirements
        if risk_categories[RiskCategory.MARKET] > 60:
            margin_ratio = 0.1 + (risk_categories[RiskCategory.MARKET] - 60) / 200
            mitigations.append({
                "type": "margin",
                "description": "Increase margin requirements",
                "parameters": {
                    "initial_margin": margin_ratio,
                    "maintenance_margin": margin_ratio * 0.7
                }
            })
        
        # Position limits
        if risk_categories[RiskCategory.LIQUIDITY] > 70:
            mitigations.append({
                "type": "position_limit",
                "description": "Impose position limits",
                "parameters": {
                    "max_position_size": float(transaction_value) * 0.5 if transaction_value else 10000,
                    "max_concentration": 0.2
                }
            })
        
        # Monitoring
        if any(risk > 60 for risk in risk_categories.values()):
            mitigations.append({
                "type": "monitoring",
                "description": "Enhanced monitoring",
                "parameters": {
                    "check_frequency": "5m",
                    "alert_threshold": 0.8,
                    "auto_liquidation": risk_categories[RiskCategory.COUNTERPARTY] > 80
                }
            })
        
        return mitigations
    
    async def _get_transaction_history(
        self,
        entity_id: str
    ) -> Dict[str, Any]:
        """Get transaction history for entity"""
        try:
            response = await self.client.get(
                f"/api/v1/entities/{entity_id}/transaction-history",
                params={"days": 90}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting transaction history: {e}")
            return {}
    
    def _analyze_usage_patterns(
        self,
        usage_history: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze usage history to extract patterns"""
        if not usage_history:
            return self._get_default_patterns()
        
        # Calculate averages
        gpu_hours = [u.get("gpu_hours", 0) for u in usage_history]
        cpu_hours = [u.get("cpu_hours", 0) for u in usage_history]
        
        # Find peak hours
        hour_usage = defaultdict(list)
        for usage in usage_history:
            if "timestamp" in usage:
                hour = datetime.fromisoformat(usage["timestamp"]).hour
                hour_usage[hour].append(usage.get("total_usage", 0))
        
        peak_hours = sorted(
            hour_usage.keys(),
            key=lambda h: sum(hour_usage[h]),
            reverse=True
        )[:3]
        
        # Find preferred providers
        provider_counts = defaultdict(int)
        for usage in usage_history:
            if "provider" in usage:
                provider_counts[usage["provider"]] += 1
        
        preferred_providers = sorted(
            provider_counts.keys(),
            key=lambda p: provider_counts[p],
            reverse=True
        )
        
        # Calculate payment reliability
        successful_payments = sum(
            1 for u in usage_history
            if u.get("payment_status") == "completed"
        )
        total_payments = len([u for u in usage_history if "payment_status" in u])
        reliability = successful_payments / total_payments if total_payments > 0 else 0
        
        # Detect workload type
        avg_gpu = np.mean(gpu_hours) if gpu_hours else 0
        avg_cpu = np.mean(cpu_hours) if cpu_hours else 0
        
        if avg_gpu > avg_cpu * 10:
            workload_type = "gpu_intensive"
        elif avg_cpu > avg_gpu * 10:
            workload_type = "cpu_intensive"
        else:
            workload_type = "balanced"
        
        # Seasonal patterns (simplified)
        monthly_usage = defaultdict(list)
        for usage in usage_history:
            if "timestamp" in usage:
                month = datetime.fromisoformat(usage["timestamp"]).strftime("%B")
                monthly_usage[month].append(usage.get("total_usage", 0))
        
        seasonal_patterns = {
            month: np.mean(usage_list) if usage_list else 0
            for month, usage_list in monthly_usage.items()
        }
        
        return {
            "avg_gpu_hours": avg_gpu,
            "avg_cpu_hours": avg_cpu,
            "peak_hours": peak_hours,
            "preferred_providers": preferred_providers,
            "payment_reliability": reliability,
            "workload_type": workload_type,
            "seasonal_patterns": seasonal_patterns
        }
    
    def _get_default_patterns(self) -> Dict[str, Any]:
        """Default patterns for new users"""
        return {
            "avg_gpu_hours": 0,
            "avg_cpu_hours": 0,
            "peak_hours": [9, 10, 11],  # Business hours
            "preferred_providers": [],
            "payment_reliability": 0.5,  # Unknown
            "workload_type": "unknown",
            "seasonal_patterns": {}
        }
    
    def _get_default_trust_score(self) -> TrustScore:
        """Default conservative trust score"""
        return TrustScore(
            overall_score=30,
            reliability=30,
            competence=30,
            integrity=30,
            transparency=30,
            collaboration=30,
            last_updated=datetime.utcnow(),
            confidence=0.1
        )
    
    def _get_default_usage_pattern(self, entity_id: str) -> ComputeUsagePattern:
        """Default usage pattern"""
        return ComputeUsagePattern(
            entity_id=entity_id,
            avg_daily_gpu_hours=Decimal("0"),
            avg_daily_cpu_hours=Decimal("0"),
            peak_usage_hours=[],
            preferred_providers=[],
            reliability_score=0.5,
            typical_workload_type="unknown",
            seasonal_patterns={}
        )
    
    def _get_default_risk_assessment(self, entity_id: str) -> RiskAssessment:
        """Conservative default risk assessment"""
        default_risk = 70  # High risk for unknown entities
        
        return RiskAssessment(
            entity_id=entity_id,
            risk_score=default_risk,
            risk_categories={
                cat: default_risk for cat in RiskCategory
            },
            trust_score=self._get_default_trust_score(),
            network_centrality=0,
            cluster_risk=default_risk,
            recommendations=[
                "Limited information available - proceed with caution",
                "Start with small transactions",
                "Require upfront payment or escrow"
            ],
            mitigations=[
                {
                    "type": "collateral",
                    "description": "Require full collateral",
                    "parameters": {"collateral_ratio": 1.0}
                }
            ],
            assessment_date=datetime.utcnow()
        )
    
    async def _publish_high_risk_alert(self, assessment: RiskAssessment):
        """Publish alert for high-risk entities"""
        if self.pulsar_publisher:
            await self.pulsar_publisher.publish(
                "persistent://platformq/risk/high-risk-alerts",
                {
                    "entity_id": assessment.entity_id,
                    "risk_score": assessment.risk_score,
                    "risk_categories": {
                        cat.value: score
                        for cat, score in assessment.risk_categories.items()
                    },
                    "recommendations": assessment.recommendations,
                    "timestamp": assessment.assessment_date.isoformat()
                }
            )
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose() 