"""
Graph Intelligence Integration for Simulation Service

Integrates with Graph Intelligence Service for simulation collaboration trust,
resource sharing recommendations, and simulation quality assessment.
"""

import httpx
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SimulationCollaboratorProfile:
    """Profile of a simulation collaborator"""
    collaborator_id: str
    simulation_expertise: float  # 0-100
    resource_sharing_score: float  # How willing to share resources
    collaboration_reliability: float  # How reliable in collaborations
    specializations: List[str]  # Types of simulations they excel at
    avg_contribution_quality: float
    total_simulations: int
    successful_collaborations: int


@dataclass
class ResourceSharingRecommendation:
    """Recommendation for resource sharing"""
    provider_id: str
    resource_type: str  # GPU, CPU, specialized hardware
    availability_score: float
    trust_score: float
    past_sharing_success_rate: float
    recommended_terms: Dict[str, Any]


@dataclass
class SimulationQualityAssessment:
    """Quality assessment for collaborative simulations"""
    simulation_id: str
    predicted_quality_score: float
    collaboration_synergy_score: float  # How well collaborators work together
    resource_efficiency_score: float
    risk_factors: List[str]
    optimization_suggestions: List[str]


class SimulationGraphIntelligence:
    """Graph Intelligence integration for Simulation Service"""
    
    def __init__(self, graph_service_url: str = "http://graph-intelligence-service:8000"):
        self.graph_service_url = graph_service_url
        self.client = httpx.AsyncClient(
            base_url=graph_service_url,
            timeout=30.0
        )
    
    async def assess_collaboration_trust(
        self,
        initiator_id: str,
        collaborator_ids: List[str],
        simulation_type: str
    ) -> Dict[str, Any]:
        """Assess trust for simulation collaboration"""
        try:
            # Get trust scores between all parties
            trust_matrix = {}
            
            for collaborator_id in collaborator_ids:
                response = await self.client.post(
                    "/api/v1/trust/pairwise",
                    json={
                        "entity_a": initiator_id,
                        "entity_b": collaborator_id,
                        "context": "simulation_collaboration"
                    }
                )
                
                if response.status_code == 200:
                    trust_data = response.json()
                    trust_matrix[collaborator_id] = trust_data["trust_score"]
                else:
                    trust_matrix[collaborator_id] = 50  # Default neutral trust
            
            # Calculate overall collaboration risk
            avg_trust = sum(trust_matrix.values()) / len(trust_matrix) if trust_matrix else 50
            min_trust = min(trust_matrix.values()) if trust_matrix else 50
            
            # Get collaboration history
            collaboration_success_rate = await self._get_collaboration_success_rate(
                initiator_id,
                collaborator_ids
            )
            
            return {
                "trust_matrix": trust_matrix,
                "average_trust": avg_trust,
                "minimum_trust": min_trust,
                "collaboration_risk": 100 - avg_trust,
                "past_success_rate": collaboration_success_rate,
                "recommendation": self._generate_collaboration_recommendation(
                    avg_trust,
                    min_trust,
                    collaboration_success_rate
                )
            }
            
        except Exception as e:
            logger.error(f"Error assessing collaboration trust: {e}")
            return {
                "trust_matrix": {},
                "average_trust": 50,
                "minimum_trust": 50,
                "collaboration_risk": 50,
                "recommendation": "Unable to assess - proceed with standard precautions"
            }
    
    async def get_resource_sharing_recommendations(
        self,
        requester_id: str,
        resource_requirements: Dict[str, Any],
        budget: float
    ) -> List[ResourceSharingRecommendation]:
        """Get recommendations for resource sharing partners"""
        try:
            response = await self.client.post(
                "/api/v1/recommendations/resource-providers",
                json={
                    "requester_id": requester_id,
                    "requirements": resource_requirements,
                    "budget": budget,
                    "context": "simulation_resources"
                }
            )
            
            if response.status_code == 200:
                recommendations = []
                
                for rec in response.json().get("recommendations", []):
                    # Get detailed provider profile
                    provider_profile = await self._get_provider_profile(
                        rec["provider_id"]
                    )
                    
                    recommendations.append(ResourceSharingRecommendation(
                        provider_id=rec["provider_id"],
                        resource_type=rec["resource_type"],
                        availability_score=rec["availability"],
                        trust_score=provider_profile.get("trust_score", 50),
                        past_sharing_success_rate=provider_profile.get("success_rate", 0.5),
                        recommended_terms={
                            "price_per_hour": rec.get("suggested_price", budget / 10),
                            "sla_uptime": provider_profile.get("avg_uptime", 0.95),
                            "penalty_rate": 0.1,  # 10% penalty for SLA breach
                            "payment_terms": "hourly"
                        }
                    ))
                
                # Sort by trust score
                recommendations.sort(key=lambda x: x.trust_score, reverse=True)
                return recommendations[:10]  # Top 10 recommendations
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting resource sharing recommendations: {e}")
            return []
    
    async def predict_simulation_quality(
        self,
        simulation_metadata: Dict[str, Any],
        collaborator_ids: List[str]
    ) -> SimulationQualityAssessment:
        """Predict quality of collaborative simulation"""
        try:
            # Get collaborator profiles
            collaborator_profiles = []
            for cid in collaborator_ids:
                profile = await self.get_collaborator_profile(cid)
                collaborator_profiles.append(profile)
            
            # Calculate collaboration synergy
            synergy_score = self._calculate_collaboration_synergy(
                collaborator_profiles,
                simulation_metadata.get("type", "general")
            )
            
            # Predict resource efficiency
            resource_efficiency = self._predict_resource_efficiency(
                simulation_metadata,
                collaborator_profiles
            )
            
            # Overall quality prediction
            quality_score = (
                synergy_score * 0.3 +
                resource_efficiency * 0.3 +
                sum(p.simulation_expertise for p in collaborator_profiles) / len(collaborator_profiles) * 0.4
            )
            
            # Identify risk factors
            risk_factors = []
            if synergy_score < 60:
                risk_factors.append("Low collaboration synergy")
            if resource_efficiency < 70:
                risk_factors.append("Suboptimal resource allocation")
            if any(p.collaboration_reliability < 60 for p in collaborator_profiles):
                risk_factors.append("Unreliable collaborators detected")
            
            # Generate optimization suggestions
            suggestions = []
            if synergy_score < 80:
                suggestions.append("Consider adding collaborators with complementary skills")
            if resource_efficiency < 80:
                suggestions.append("Optimize resource allocation algorithm")
            if quality_score < 70:
                suggestions.append("Run pilot simulation before full execution")
            
            return SimulationQualityAssessment(
                simulation_id=simulation_metadata.get("id", "unknown"),
                predicted_quality_score=quality_score,
                collaboration_synergy_score=synergy_score,
                resource_efficiency_score=resource_efficiency,
                risk_factors=risk_factors,
                optimization_suggestions=suggestions
            )
            
        except Exception as e:
            logger.error(f"Error predicting simulation quality: {e}")
            return SimulationQualityAssessment(
                simulation_id=simulation_metadata.get("id", "unknown"),
                predicted_quality_score=50,
                collaboration_synergy_score=50,
                resource_efficiency_score=50,
                risk_factors=["Unable to analyze - limited data"],
                optimization_suggestions=["Proceed with caution"]
            )
    
    async def get_collaborator_profile(
        self,
        collaborator_id: str
    ) -> SimulationCollaboratorProfile:
        """Get profile of a simulation collaborator"""
        try:
            # Get trust score
            response = await self.client.get(
                f"/api/v1/trust/score/{collaborator_id}"
            )
            trust_data = response.json() if response.status_code == 200 else {}
            
            # Get simulation history
            response = await self.client.get(
                f"/api/v1/entities/{collaborator_id}/simulation-history"
            )
            sim_history = response.json() if response.status_code == 200 else {}
            
            # Calculate metrics
            total_sims = len(sim_history.get("simulations", []))
            successful_collabs = len([
                s for s in sim_history.get("simulations", [])
                if s.get("collaborative", False) and s.get("success", False)
            ])
            
            # Extract specializations
            specializations = list(set(
                s.get("type", "general")
                for s in sim_history.get("simulations", [])
            ))[:5]  # Top 5 types
            
            return SimulationCollaboratorProfile(
                collaborator_id=collaborator_id,
                simulation_expertise=trust_data.get("dimensions", {}).get("competence", 50),
                resource_sharing_score=trust_data.get("dimensions", {}).get("collaboration", 50),
                collaboration_reliability=trust_data.get("dimensions", {}).get("reliability", 50),
                specializations=specializations,
                avg_contribution_quality=sim_history.get("avg_quality_score", 0.5),
                total_simulations=total_sims,
                successful_collaborations=successful_collabs
            )
            
        except Exception as e:
            logger.error(f"Error getting collaborator profile: {e}")
            return SimulationCollaboratorProfile(
                collaborator_id=collaborator_id,
                simulation_expertise=50,
                resource_sharing_score=50,
                collaboration_reliability=50,
                specializations=[],
                avg_contribution_quality=0.5,
                total_simulations=0,
                successful_collaborations=0
            )
    
    async def analyze_resource_network(
        self,
        resource_type: str,
        region: Optional[str] = None
    ) -> Dict[str, Any]:
        """Analyze the resource sharing network"""
        try:
            response = await self.client.post(
                "/api/v1/network/analyze-resources",
                json={
                    "resource_type": resource_type,
                    "region": region,
                    "context": "simulation_resources"
                }
            )
            
            if response.status_code == 200:
                network_data = response.json()
                
                return {
                    "total_providers": network_data.get("provider_count", 0),
                    "total_capacity": network_data.get("total_capacity", 0),
                    "utilization_rate": network_data.get("utilization", 0),
                    "price_range": network_data.get("price_range", {}),
                    "top_providers": network_data.get("top_providers", []),
                    "network_health": self._assess_network_health(network_data)
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing resource network: {e}")
            return {}
    
    # Private helper methods
    async def _get_collaboration_success_rate(
        self,
        initiator_id: str,
        collaborator_ids: List[str]
    ) -> float:
        """Get historical success rate of collaborations"""
        try:
            response = await self.client.post(
                "/api/v1/history/collaboration-success",
                json={
                    "initiator_id": initiator_id,
                    "collaborator_ids": collaborator_ids
                }
            )
            
            if response.status_code == 200:
                return response.json().get("success_rate", 0.5)
            else:
                return 0.5  # Default 50% success rate
                
        except:
            return 0.5
    
    async def _get_provider_profile(
        self,
        provider_id: str
    ) -> Dict[str, Any]:
        """Get resource provider profile"""
        try:
            response = await self.client.get(
                f"/api/v1/providers/{provider_id}/profile"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
                
        except:
            return {}
    
    def _generate_collaboration_recommendation(
        self,
        avg_trust: float,
        min_trust: float,
        success_rate: float
    ) -> str:
        """Generate collaboration recommendation"""
        if avg_trust > 80 and min_trust > 60 and success_rate > 0.8:
            return "Excellent collaboration potential - proceed with confidence"
        elif avg_trust > 60 and min_trust > 40:
            return "Good collaboration potential - standard safeguards recommended"
        elif min_trust < 30:
            return "High risk collaboration - require upfront payment or escrow"
        else:
            return "Moderate risk - implement milestone-based payments"
    
    def _calculate_collaboration_synergy(
        self,
        profiles: List[SimulationCollaboratorProfile],
        simulation_type: str
    ) -> float:
        """Calculate how well collaborators work together"""
        if not profiles:
            return 50
        
        # Check specialization overlap
        all_specializations = set()
        for p in profiles:
            all_specializations.update(p.specializations)
        
        # Diversity is good
        diversity_score = len(all_specializations) * 10
        
        # But need relevant expertise
        relevance_score = sum(
            20 for p in profiles
            if simulation_type in p.specializations
        )
        
        # Past collaboration success
        collab_score = sum(
            p.successful_collaborations / max(p.total_simulations, 1) * 50
            for p in profiles
        ) / len(profiles)
        
        return min(
            diversity_score * 0.3 + relevance_score * 0.4 + collab_score * 0.3,
            100
        )
    
    def _predict_resource_efficiency(
        self,
        simulation_metadata: Dict[str, Any],
        profiles: List[SimulationCollaboratorProfile]
    ) -> float:
        """Predict resource efficiency of collaboration"""
        base_efficiency = 70
        
        # Boost for experienced collaborators
        experience_boost = sum(
            min(p.total_simulations / 10, 10)
            for p in profiles
        ) / len(profiles) if profiles else 0
        
        # Penalty for complex simulations with inexperienced team
        complexity = simulation_metadata.get("complexity", 0.5)
        if complexity > 0.7 and experience_boost < 5:
            base_efficiency -= 20
        
        return min(base_efficiency + experience_boost, 100)
    
    def _assess_network_health(self, network_data: Dict[str, Any]) -> str:
        """Assess health of resource network"""
        utilization = network_data.get("utilization", 0)
        provider_count = network_data.get("provider_count", 0)
        
        if utilization > 0.9:
            return "Strained - high utilization, may face availability issues"
        elif utilization < 0.3 and provider_count > 10:
            return "Oversupplied - good availability, competitive pricing"
        elif provider_count < 5:
            return "Limited - few providers, potential monopoly risk"
        else:
            return "Healthy - balanced supply and demand"
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose() 