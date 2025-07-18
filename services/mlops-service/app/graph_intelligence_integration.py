"""
Graph Intelligence Integration for MLOps Service

Integrates with Graph Intelligence Service to assess model creator reputation,
collaboration patterns, and model quality predictions.
"""

import httpx
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ModelCreatorProfile:
    """Profile of a model creator based on graph intelligence"""
    creator_id: str
    overall_reputation: float  # 0-100
    model_quality_score: float  # Average quality of their models
    collaboration_score: float  # How well they work with others
    innovation_score: float  # Uniqueness of contributions
    reliability_score: float  # Consistency in delivering quality
    total_models: int
    successful_models: int
    average_model_performance: float
    specializations: List[str]  # Areas of expertise
    trust_network_size: int
    last_updated: datetime


@dataclass
class ModelQualityPrediction:
    """Prediction of model quality before deployment"""
    model_id: str
    predicted_accuracy: float
    predicted_reliability: float
    predicted_adoption_rate: float
    confidence: float
    risk_factors: List[str]
    recommendations: List[str]


@dataclass
class CollaborationRecommendation:
    """Recommendation for model collaboration"""
    collaborator_id: str
    collaboration_score: float
    complementary_skills: List[str]
    past_success_rate: float
    recommended_projects: List[str]


class MLOpsGraphIntelligence:
    """Graph Intelligence integration for MLOps service"""
    
    def __init__(self, graph_service_url: str = "http://graph-intelligence-service:8000"):
        self.graph_service_url = graph_service_url
        self.client = httpx.AsyncClient(
            base_url=graph_service_url,
            timeout=30.0
        )
        
    async def get_model_creator_profile(
        self,
        creator_id: str
    ) -> ModelCreatorProfile:
        """Get comprehensive profile of a model creator"""
        try:
            # Get trust score
            response = await self.client.get(
                f"/api/v1/trust/score/{creator_id}"
            )
            trust_score = response.json() if response.status_code == 200 else {}
            
            # Get model creation history
            response = await self.client.get(
                f"/api/v1/entities/{creator_id}/model-history"
            )
            model_history = response.json() if response.status_code == 200 else {"models": []}
            
            # Get collaboration patterns
            response = await self.client.get(
                f"/api/v1/entities/{creator_id}/collaborations"
            )
            collaborations = response.json() if response.status_code == 200 else {}
            
            # Get specializations
            response = await self.client.get(
                f"/api/v1/entities/{creator_id}/specializations"
            )
            specializations = response.json() if response.status_code == 200 else {"areas": []}
            
            # Calculate profile metrics
            successful_models = len([
                m for m in model_history.get("models", [])
                if m.get("performance_score", 0) > 0.7
            ])
            
            avg_performance = np.mean([
                m.get("performance_score", 0)
                for m in model_history.get("models", [])
            ]) if model_history.get("models") else 0
            
            profile = ModelCreatorProfile(
                creator_id=creator_id,
                overall_reputation=trust_score.get("overall_score", 50),
                model_quality_score=trust_score.get("dimensions", {}).get("competence", 50),
                collaboration_score=trust_score.get("dimensions", {}).get("collaboration", 50),
                innovation_score=self._calculate_innovation_score(model_history),
                reliability_score=trust_score.get("dimensions", {}).get("reliability", 50),
                total_models=len(model_history.get("models", [])),
                successful_models=successful_models,
                average_model_performance=avg_performance,
                specializations=specializations.get("areas", []),
                trust_network_size=collaborations.get("network_size", 0),
                last_updated=datetime.utcnow()
            )
            
            return profile
            
        except Exception as e:
            logger.error(f"Error getting model creator profile: {e}")
            return self._get_default_profile(creator_id)
    
    async def predict_model_quality(
        self,
        model_metadata: Dict[str, Any],
        creator_id: str
    ) -> ModelQualityPrediction:
        """Predict model quality before deployment"""
        try:
            # Get creator profile
            creator_profile = await self.get_model_creator_profile(creator_id)
            
            # Analyze model characteristics
            response = await self.client.post(
                "/api/v1/predictions/model-quality",
                json={
                    "model_metadata": model_metadata,
                    "creator_profile": {
                        "reputation": creator_profile.overall_reputation,
                        "past_performance": creator_profile.average_model_performance,
                        "specializations": creator_profile.specializations
                    }
                }
            )
            
            if response.status_code == 200:
                prediction_data = response.json()
                
                # Identify risk factors
                risk_factors = []
                if creator_profile.total_models < 5:
                    risk_factors.append("Limited track record")
                if creator_profile.reliability_score < 60:
                    risk_factors.append("Inconsistent past performance")
                if model_metadata.get("complexity", 0) > 0.8:
                    risk_factors.append("High model complexity")
                
                # Generate recommendations
                recommendations = []
                if risk_factors:
                    recommendations.append("Consider thorough testing before production")
                if creator_profile.collaboration_score > 80:
                    recommendations.append("Creator works well with others - consider team deployment")
                if creator_profile.innovation_score > 80:
                    recommendations.append("Innovative approach - monitor for edge cases")
                
                return ModelQualityPrediction(
                    model_id=model_metadata.get("model_id", "unknown"),
                    predicted_accuracy=prediction_data.get("accuracy", 0.7),
                    predicted_reliability=prediction_data.get("reliability", 0.7),
                    predicted_adoption_rate=prediction_data.get("adoption_rate", 0.5),
                    confidence=prediction_data.get("confidence", 0.6),
                    risk_factors=risk_factors,
                    recommendations=recommendations
                )
            else:
                return self._get_default_prediction(model_metadata.get("model_id", "unknown"))
                
        except Exception as e:
            logger.error(f"Error predicting model quality: {e}")
            return self._get_default_prediction(model_metadata.get("model_id", "unknown"))
    
    async def get_collaboration_recommendations(
        self,
        creator_id: str,
        project_type: str,
        limit: int = 5
    ) -> List[CollaborationRecommendation]:
        """Get recommendations for potential collaborators"""
        try:
            response = await self.client.post(
                "/api/v1/recommendations/collaborators",
                json={
                    "entity_id": creator_id,
                    "context": "model_development",
                    "project_type": project_type,
                    "limit": limit
                }
            )
            
            if response.status_code == 200:
                recommendations = []
                for rec in response.json().get("recommendations", []):
                    recommendations.append(CollaborationRecommendation(
                        collaborator_id=rec["entity_id"],
                        collaboration_score=rec["score"],
                        complementary_skills=rec.get("skills", []),
                        past_success_rate=rec.get("success_rate", 0.5),
                        recommended_projects=rec.get("projects", [])
                    ))
                return recommendations
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting collaboration recommendations: {e}")
            return []
    
    async def analyze_model_lineage(
        self,
        model_id: str
    ) -> Dict[str, Any]:
        """Analyze model lineage and dependencies"""
        try:
            response = await self.client.get(
                f"/api/v1/lineage/model/{model_id}"
            )
            
            if response.status_code == 200:
                lineage = response.json()
                
                # Analyze trust in lineage
                trust_analysis = await self._analyze_lineage_trust(lineage)
                
                return {
                    "lineage": lineage,
                    "trust_analysis": trust_analysis,
                    "risk_score": self._calculate_lineage_risk(lineage, trust_analysis)
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing model lineage: {e}")
            return {}
    
    async def get_model_adoption_prediction(
        self,
        model_id: str,
        creator_id: str,
        target_audience: List[str]
    ) -> Dict[str, Any]:
        """Predict model adoption rates"""
        try:
            # Get creator network
            response = await self.client.get(
                f"/api/v1/network/connections/{creator_id}"
            )
            creator_network = response.json() if response.status_code == 200 else {}
            
            # Analyze target audience overlap
            network_overlap = len(
                set(creator_network.get("connections", [])) & 
                set(target_audience)
            )
            
            # Get similar models adoption
            response = await self.client.post(
                "/api/v1/similarity/models",
                json={"model_id": model_id, "limit": 10}
            )
            similar_models = response.json() if response.status_code == 200 else {"models": []}
            
            # Calculate adoption prediction
            base_adoption = 0.1  # 10% base rate
            
            # Boost for creator reputation
            creator_profile = await self.get_model_creator_profile(creator_id)
            reputation_boost = creator_profile.overall_reputation / 1000  # Up to 10% boost
            
            # Boost for network overlap
            network_boost = min(network_overlap / 100, 0.2)  # Up to 20% boost
            
            # Boost based on similar models
            similar_success_rate = np.mean([
                m.get("adoption_rate", 0.1)
                for m in similar_models.get("models", [])
            ]) if similar_models.get("models") else 0.1
            
            predicted_adoption = min(
                base_adoption + reputation_boost + network_boost + similar_success_rate,
                0.9  # Cap at 90%
            )
            
            return {
                "predicted_adoption_rate": predicted_adoption,
                "factors": {
                    "base_rate": base_adoption,
                    "reputation_boost": reputation_boost,
                    "network_boost": network_boost,
                    "similar_models_boost": similar_success_rate
                },
                "network_overlap": network_overlap,
                "confidence": 0.7 if similar_models.get("models") else 0.4
            }
            
        except Exception as e:
            logger.error(f"Error predicting model adoption: {e}")
            return {"predicted_adoption_rate": 0.1, "confidence": 0.1}
    
    # Private helper methods
    def _calculate_innovation_score(self, model_history: Dict[str, Any]) -> float:
        """Calculate innovation score based on model uniqueness"""
        if not model_history.get("models"):
            return 50.0
        
        # Simple heuristic: diversity of model types and techniques
        model_types = set()
        techniques = set()
        
        for model in model_history.get("models", []):
            model_types.add(model.get("type", "unknown"))
            techniques.update(model.get("techniques", []))
        
        diversity_score = len(model_types) * 10 + len(techniques) * 5
        return min(diversity_score, 100.0)
    
    async def _analyze_lineage_trust(self, lineage: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze trust in model lineage"""
        trust_scores = []
        
        for ancestor in lineage.get("ancestors", []):
            creator_id = ancestor.get("creator_id")
            if creator_id:
                try:
                    response = await self.client.get(
                        f"/api/v1/trust/score/{creator_id}"
                    )
                    if response.status_code == 200:
                        trust_scores.append(response.json().get("overall_score", 50))
                except:
                    trust_scores.append(50)
        
        return {
            "average_trust": np.mean(trust_scores) if trust_scores else 50,
            "min_trust": min(trust_scores) if trust_scores else 50,
            "trust_chain_length": len(trust_scores)
        }
    
    def _calculate_lineage_risk(
        self,
        lineage: Dict[str, Any],
        trust_analysis: Dict[str, Any]
    ) -> float:
        """Calculate risk score for model lineage"""
        risk = 0
        
        # Risk increases with chain length
        risk += min(trust_analysis["trust_chain_length"] * 5, 30)
        
        # Risk from low trust ancestors
        if trust_analysis["min_trust"] < 50:
            risk += (50 - trust_analysis["min_trust"]) * 0.5
        
        # Risk from unverified components
        unverified = len([
            a for a in lineage.get("ancestors", [])
            if not a.get("verified", False)
        ])
        risk += unverified * 10
        
        return min(risk, 100)
    
    def _get_default_profile(self, creator_id: str) -> ModelCreatorProfile:
        """Default profile for unknown creators"""
        return ModelCreatorProfile(
            creator_id=creator_id,
            overall_reputation=50,
            model_quality_score=50,
            collaboration_score=50,
            innovation_score=50,
            reliability_score=50,
            total_models=0,
            successful_models=0,
            average_model_performance=0,
            specializations=[],
            trust_network_size=0,
            last_updated=datetime.utcnow()
        )
    
    def _get_default_prediction(self, model_id: str) -> ModelQualityPrediction:
        """Default conservative prediction"""
        return ModelQualityPrediction(
            model_id=model_id,
            predicted_accuracy=0.6,
            predicted_reliability=0.5,
            predicted_adoption_rate=0.1,
            confidence=0.3,
            risk_factors=["Unable to analyze - limited data"],
            recommendations=["Proceed with caution", "Extensive testing recommended"]
        )
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose() 