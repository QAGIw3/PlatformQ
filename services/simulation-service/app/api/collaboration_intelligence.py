"""
Collaboration Intelligence API endpoints

Uses Graph Intelligence for collaboration trust assessment and recommendations.
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from ..api.deps import get_current_tenant_and_user


router = APIRouter(prefix="/api/v1/collaboration-intelligence", tags=["collaboration-intelligence"])


class CollaborationTrustRequest(BaseModel):
    """Request for collaboration trust assessment"""
    collaborator_ids: List[str] = Field(..., description="List of collaborator IDs")
    simulation_type: str = Field(..., description="Type of simulation")


class ResourceSharingRequest(BaseModel):
    """Request for resource sharing recommendations"""
    resource_requirements: Dict[str, Any] = Field(..., description="Required resources")
    budget: float = Field(..., description="Budget in USD")


class SimulationQualityRequest(BaseModel):
    """Request for simulation quality prediction"""
    simulation_metadata: Dict[str, Any] = Field(..., description="Simulation metadata")
    collaborator_ids: List[str] = Field(..., description="List of collaborator IDs")


@router.post("/assess-trust", response_model=Dict[str, Any])
async def assess_collaboration_trust(
    request: CollaborationTrustRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Assess trust for simulation collaboration"""
    from ..main import app
    
    if not hasattr(app.state, 'graph_intelligence'):
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    initiator_id = context["user_id"]
    
    try:
        trust_assessment = await app.state.graph_intelligence.assess_collaboration_trust(
            initiator_id=initiator_id,
            collaborator_ids=request.collaborator_ids,
            simulation_type=request.simulation_type
        )
        
        return trust_assessment
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/resource-recommendations", response_model=List[Dict[str, Any]])
async def get_resource_sharing_recommendations(
    request: ResourceSharingRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get recommendations for resource sharing partners"""
    from ..main import app
    
    if not hasattr(app.state, 'graph_intelligence'):
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    requester_id = context["user_id"]
    
    try:
        recommendations = await app.state.graph_intelligence.get_resource_sharing_recommendations(
            requester_id=requester_id,
            resource_requirements=request.resource_requirements,
            budget=request.budget
        )
        
        return [
            {
                "provider_id": rec.provider_id,
                "resource_type": rec.resource_type,
                "availability_score": rec.availability_score,
                "trust_score": rec.trust_score,
                "past_sharing_success_rate": rec.past_sharing_success_rate,
                "recommended_terms": rec.recommended_terms
            }
            for rec in recommendations
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict-quality", response_model=Dict[str, Any])
async def predict_simulation_quality(
    request: SimulationQualityRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Predict quality of collaborative simulation"""
    from ..main import app
    
    if not hasattr(app.state, 'graph_intelligence'):
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        quality_assessment = await app.state.graph_intelligence.predict_simulation_quality(
            simulation_metadata=request.simulation_metadata,
            collaborator_ids=request.collaborator_ids
        )
        
        return {
            "simulation_id": quality_assessment.simulation_id,
            "predicted_quality_score": quality_assessment.predicted_quality_score,
            "collaboration_synergy_score": quality_assessment.collaboration_synergy_score,
            "resource_efficiency_score": quality_assessment.resource_efficiency_score,
            "risk_factors": quality_assessment.risk_factors,
            "optimization_suggestions": quality_assessment.optimization_suggestions
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/collaborator-profile/{collaborator_id}", response_model=Dict[str, Any])
async def get_collaborator_profile(
    collaborator_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get profile of a simulation collaborator"""
    from ..main import app
    
    if not hasattr(app.state, 'graph_intelligence'):
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        profile = await app.state.graph_intelligence.get_collaborator_profile(collaborator_id)
        
        return {
            "collaborator_id": profile.collaborator_id,
            "simulation_expertise": profile.simulation_expertise,
            "resource_sharing_score": profile.resource_sharing_score,
            "collaboration_reliability": profile.collaboration_reliability,
            "specializations": profile.specializations,
            "avg_contribution_quality": profile.avg_contribution_quality,
            "total_simulations": profile.total_simulations,
            "successful_collaborations": profile.successful_collaborations
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/resource-network/{resource_type}", response_model=Dict[str, Any])
async def analyze_resource_network(
    resource_type: str,
    region: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Analyze the resource sharing network"""
    from ..main import app
    
    if not hasattr(app.state, 'graph_intelligence'):
        raise HTTPException(status_code=503, detail="Graph intelligence not initialized")
    
    try:
        network_analysis = await app.state.graph_intelligence.analyze_resource_network(
            resource_type=resource_type,
            region=region
        )
        
        return network_analysis
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 