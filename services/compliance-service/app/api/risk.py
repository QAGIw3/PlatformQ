"""
Risk Assessment API endpoints for Compliance Service.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..core.risk_scorer import RiskScorer

logger = logging.getLogger(__name__)

router = APIRouter()


class RiskFactorUpdate(BaseModel):
    """Update risk factors for assessment"""
    factor_name: str = Field(..., description="Name of risk factor")
    weight: float = Field(..., ge=0, le=1, description="Weight of factor")
    threshold: float = Field(..., description="Threshold value")


class RiskProfileResponse(BaseModel):
    """User risk profile"""
    user_id: str
    overall_score: float
    risk_level: str
    factors: List[Dict[str, Any]]
    last_updated: datetime
    next_review: datetime


def get_risk_scorer(request) -> RiskScorer:
    """Dependency to get risk scorer instance"""
    return request.app.state.risk_scorer


@router.get("/profile/{user_id}", response_model=RiskProfileResponse)
async def get_risk_profile(
    user_id: str,
    current_user: Dict = Depends(get_current_user),
    risk_scorer: RiskScorer = Depends(get_risk_scorer)
):
    """
    Get comprehensive risk profile for a user.
    
    Combines KYC level, AML risk, transaction patterns, and behavioral analysis.
    """
    try:
        # Check permissions
        if current_user["id"] != user_id and "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        
        profile = await risk_scorer.calculate_risk_profile(user_id)
        
        return RiskProfileResponse(
            user_id=user_id,
            overall_score=profile["overall_score"],
            risk_level=profile["risk_level"],
            factors=profile["factors"],
            last_updated=profile["last_updated"],
            next_review=profile["next_review"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting risk profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calculate")
async def calculate_risk_score(
    entity_type: str,
    entity_id: str,
    factors: Dict[str, Any],
    current_user: Dict = Depends(get_current_user),
    risk_scorer: RiskScorer = Depends(get_risk_scorer)
):
    """
    Calculate risk score for any entity.
    
    Can be used for users, transactions, or other entities.
    """
    try:
        score = await risk_scorer.calculate_score(
            entity_type=entity_type,
            entity_id=entity_id,
            factors=factors
        )
        
        return {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "risk_score": score["score"],
            "risk_level": score["level"],
            "contributing_factors": score["contributing_factors"]
        }
        
    except Exception as e:
        logger.error(f"Error calculating risk score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/thresholds")
async def get_risk_thresholds(
    current_user: Dict = Depends(get_current_user),
    risk_scorer: RiskScorer = Depends(get_risk_scorer)
):
    """
    Get current risk thresholds and configuration.
    
    Only accessible to compliance officers.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can view thresholds"
            )
        
        thresholds = await risk_scorer.get_thresholds()
        
        return {
            "thresholds": thresholds,
            "last_updated": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting thresholds: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/thresholds")
async def update_risk_thresholds(
    updates: List[RiskFactorUpdate],
    current_user: Dict = Depends(get_current_user),
    risk_scorer: RiskScorer = Depends(get_risk_scorer)
):
    """
    Update risk thresholds and weights.
    
    Only accessible to compliance officers.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can update thresholds"
            )
        
        result = await risk_scorer.update_thresholds(
            updates=[u.dict() for u in updates],
            updated_by=current_user["id"]
        )
        
        return {
            "status": "updated",
            "factors_updated": len(updates),
            "effective_from": result["effective_from"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating thresholds: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history/{user_id}")
async def get_risk_score_history(
    user_id: str,
    days: int = 90,
    current_user: Dict = Depends(get_current_user),
    risk_scorer: RiskScorer = Depends(get_risk_scorer)
):
    """
    Get historical risk scores for a user.
    
    Shows how risk profile has changed over time.
    """
    try:
        # Check permissions
        if current_user["id"] != user_id and "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        
        history = await risk_scorer.get_score_history(
            user_id=user_id,
            days=days
        )
        
        return {
            "user_id": user_id,
            "period_days": days,
            "scores": history["scores"],
            "events": history["events"],
            "trend": history["trend"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting risk history: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 