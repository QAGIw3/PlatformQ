"""
Risk Intelligence API endpoints

Uses Graph Intelligence for advanced risk assessment and recommendations.
"""

from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.main import graph_intelligence


router = APIRouter(prefix="/api/v1/risk-intelligence", tags=["risk-intelligence"])


class RiskAssessmentRequest(BaseModel):
    """Request for risk assessment"""
    entity_id: str = Field(..., description="Entity to assess")
    transaction_type: str = Field(default="compute_futures")
    transaction_value: Optional[str] = Field(None, description="Transaction value in USD")


class TrustScoreResponse(BaseModel):
    """Trust score response"""
    overall_score: float
    reliability: float
    competence: float
    integrity: float
    transparency: float
    collaboration: float
    confidence: float
    last_updated: datetime


class RiskAssessmentResponse(BaseModel):
    """Risk assessment response"""
    entity_id: str
    risk_score: float
    risk_categories: Dict[str, float]
    trust_score: TrustScoreResponse
    network_centrality: float
    cluster_risk: float
    recommendations: List[str]
    mitigations: List[Dict[str, Any]]
    assessment_date: datetime


class ComputeUsagePatternResponse(BaseModel):
    """Compute usage pattern response"""
    entity_id: str
    avg_daily_gpu_hours: str
    avg_daily_cpu_hours: str
    peak_usage_hours: List[int]
    preferred_providers: List[str]
    reliability_score: float
    typical_workload_type: str
    seasonal_patterns: Dict[str, float]


class RecommendationRequest(BaseModel):
    """Request for AI recommendations"""
    context: str = Field(..., description="Context for recommendations")
    entity_id: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


@router.post("/assess-risk", response_model=RiskAssessmentResponse)
async def assess_counterparty_risk(
    request: RiskAssessmentRequest,
    current_user: dict = Depends(get_current_user)
):
    """Perform comprehensive counterparty risk assessment"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        # Convert transaction value if provided
        transaction_value = None
        if request.transaction_value:
            transaction_value = Decimal(request.transaction_value)
        
        # Perform risk assessment
        assessment = await graph_intelligence.assess_counterparty_risk(
            trader_id=request.entity_id,
            transaction_type=request.transaction_type,
            transaction_value=transaction_value
        )
        
        # Convert to response format
        return RiskAssessmentResponse(
            entity_id=assessment.entity_id,
            risk_score=assessment.risk_score,
            risk_categories={
                cat.value: score 
                for cat, score in assessment.risk_categories.items()
            },
            trust_score=TrustScoreResponse(
                overall_score=assessment.trust_score.overall_score,
                reliability=assessment.trust_score.reliability,
                competence=assessment.trust_score.competence,
                integrity=assessment.trust_score.integrity,
                transparency=assessment.trust_score.transparency,
                collaboration=assessment.trust_score.collaboration,
                confidence=assessment.trust_score.confidence,
                last_updated=assessment.trust_score.last_updated
            ),
            network_centrality=assessment.network_centrality,
            cluster_risk=assessment.cluster_risk,
            recommendations=assessment.recommendations,
            mitigations=assessment.mitigations,
            assessment_date=assessment.assessment_date
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trust-score/{entity_id}", response_model=TrustScoreResponse)
async def get_trust_score(
    entity_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get multi-dimensional trust score for an entity"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        trust_score = await graph_intelligence.get_trust_score(entity_id)
        
        return TrustScoreResponse(
            overall_score=trust_score.overall_score,
            reliability=trust_score.reliability,
            competence=trust_score.competence,
            integrity=trust_score.integrity,
            transparency=trust_score.transparency,
            collaboration=trust_score.collaboration,
            confidence=trust_score.confidence,
            last_updated=trust_score.last_updated
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usage-patterns/{entity_id}", response_model=ComputeUsagePatternResponse)
async def get_compute_usage_patterns(
    entity_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get historical compute usage patterns for an entity"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        patterns = await graph_intelligence.get_compute_usage_patterns(entity_id)
        
        return ComputeUsagePatternResponse(
            entity_id=patterns.entity_id,
            avg_daily_gpu_hours=str(patterns.avg_daily_gpu_hours),
            avg_daily_cpu_hours=str(patterns.avg_daily_cpu_hours),
            peak_usage_hours=patterns.peak_usage_hours,
            preferred_providers=patterns.preferred_providers,
            reliability_score=patterns.reliability_score,
            typical_workload_type=patterns.typical_workload_type,
            seasonal_patterns=patterns.seasonal_patterns
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recommendations")
async def get_recommendations(
    request: RecommendationRequest,
    current_user: dict = Depends(get_current_user)
):
    """Get AI-powered recommendations based on context"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        recommendations = await graph_intelligence.get_recommendations(
            context=request.context,
            entity_id=request.entity_id,
            parameters=request.parameters
        )
        
        return {
            "recommendations": recommendations,
            "context": request.context,
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/similar-entities/{entity_id}")
async def find_similar_entities(
    entity_id: str,
    similarity_threshold: float = Query(0.7, ge=0.0, le=1.0),
    limit: int = Query(10, ge=1, le=100),
    current_user: dict = Depends(get_current_user)
):
    """Find entities with similar behavior patterns"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        similar_entities = await graph_intelligence.find_similar_entities(
            entity_id=entity_id,
            similarity_threshold=similarity_threshold,
            limit=limit
        )
        
        return {
            "entity_id": entity_id,
            "similar_entities": [
                {"entity_id": eid, "similarity": sim}
                for eid, sim in similar_entities
            ],
            "threshold": similarity_threshold
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict-behavior")
async def predict_entity_behavior(
    entity_id: str,
    prediction_type: str,
    horizon_days: int = Query(7, ge=1, le=90),
    current_user: dict = Depends(get_current_user)
):
    """Predict future behavior using graph intelligence"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        prediction = await graph_intelligence.predict_behavior(
            entity_id=entity_id,
            prediction_type=prediction_type,
            horizon_days=horizon_days
        )
        
        return {
            "entity_id": entity_id,
            "prediction_type": prediction_type,
            "horizon_days": horizon_days,
            "prediction": prediction,
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/network-analysis/{entity_id}")
async def analyze_network_position(
    entity_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Analyze entity's position in the trust network"""
    if not graph_intelligence:
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        analysis = await graph_intelligence.analyze_network_position(entity_id)
        
        return {
            "entity_id": entity_id,
            "network_analysis": analysis,
            "analyzed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 