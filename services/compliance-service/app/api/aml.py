"""
AML API endpoints for Compliance Service.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, date
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..core.aml_engine import AMLEngine

logger = logging.getLogger(__name__)

router = APIRouter()


class TransactionScreeningRequest(BaseModel):
    """Request to screen a transaction"""
    transaction_id: str = Field(..., description="Transaction identifier")
    from_address: str = Field(..., description="Sender address/account")
    to_address: str = Field(..., description="Receiver address/account")
    amount: Decimal = Field(..., description="Transaction amount")
    currency: str = Field(..., description="Currency code")
    transaction_type: str = Field(..., description="Type of transaction")
    chain: Optional[str] = Field(None, description="Blockchain if applicable")


class SanctionsCheckRequest(BaseModel):
    """Request to check sanctions lists"""
    entity_type: str = Field(..., description="individual or organization")
    name: str = Field(..., description="Full name to check")
    aliases: List[str] = Field(default_factory=list, description="Known aliases")
    country: Optional[str] = Field(None, description="Country code")
    date_of_birth: Optional[date] = Field(None, description="DOB for individuals")
    identifiers: Dict[str, str] = Field(default_factory=dict, description="IDs like passport, national ID")


class RiskAssessmentResponse(BaseModel):
    """Risk assessment result"""
    risk_score: float = Field(..., ge=0, le=100)
    risk_level: str = Field(..., description="low, medium, high, critical")
    factors: List[Dict[str, Any]]
    recommendations: List[str]
    requires_edd: bool = Field(..., description="Enhanced Due Diligence required")


def get_aml_engine(request) -> AMLEngine:
    """Dependency to get AML engine instance"""
    return request.app.state.aml_engine


@router.post("/screen-transaction")
async def screen_transaction(
    request: TransactionScreeningRequest,
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Screen a transaction for AML/CFT risks.
    
    Checks for suspicious patterns, sanctioned entities, and high-risk jurisdictions.
    """
    try:
        # Screen transaction
        result = await aml_engine.screen_transaction(
            transaction_id=request.transaction_id,
            from_address=request.from_address,
            to_address=request.to_address,
            amount=request.amount,
            currency=request.currency,
            transaction_type=request.transaction_type,
            chain=request.chain
        )
        
        return {
            "transaction_id": request.transaction_id,
            "screening_result": result["result"],  # pass, review, block
            "risk_score": result["risk_score"],
            "alerts": result.get("alerts", []),
            "matched_rules": result.get("matched_rules", []),
            "recommendations": result.get("recommendations", [])
        }
        
    except Exception as e:
        logger.error(f"Error screening transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/check-sanctions")
async def check_sanctions(
    request: SanctionsCheckRequest,
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Check if an entity appears on sanctions lists.
    
    Searches OFAC, EU, UN, and other configured sanctions databases.
    """
    try:
        # Check sanctions
        result = await aml_engine.check_sanctions(
            entity_type=request.entity_type,
            name=request.name,
            aliases=request.aliases,
            country=request.country,
            date_of_birth=request.date_of_birth,
            identifiers=request.identifiers
        )
        
        return {
            "entity": request.name,
            "is_sanctioned": result["is_sanctioned"],
            "matches": result.get("matches", []),
            "lists_checked": result["lists_checked"],
            "confidence_score": result.get("confidence_score", 0),
            "checked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error checking sanctions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/risk-assessment/{user_id}", response_model=RiskAssessmentResponse)
async def get_risk_assessment(
    user_id: str,
    current_user: Dict = Depends(get_current_user),
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Get comprehensive AML risk assessment for a user.
    
    Analyzes transaction patterns, KYC data, and behavioral indicators.
    """
    try:
        # Admin check or own account
        if current_user["id"] != user_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        
        # Get risk assessment
        assessment = await aml_engine.assess_user_risk(user_id)
        
        return RiskAssessmentResponse(
            risk_score=assessment["risk_score"],
            risk_level=assessment["risk_level"],
            factors=assessment["factors"],
            recommendations=assessment["recommendations"],
            requires_edd=assessment["requires_edd"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting risk assessment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/report-suspicious-activity")
async def report_suspicious_activity(
    user_id: str,
    activity_type: str,
    description: str,
    transaction_ids: List[str] = [],
    current_user: Dict = Depends(get_current_user),
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Report suspicious activity for investigation.
    
    Creates a Suspicious Activity Report (SAR) for regulatory filing.
    """
    try:
        # Only compliance officers can file SARs
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can file SARs"
            )
        
        # Create SAR
        sar = await aml_engine.create_suspicious_activity_report(
            user_id=user_id,
            activity_type=activity_type,
            description=description,
            transaction_ids=transaction_ids,
            reported_by=current_user["id"]
        )
        
        return {
            "sar_id": sar["id"],
            "status": "created",
            "filing_deadline": sar["filing_deadline"],
            "jurisdiction": sar["jurisdiction"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reporting suspicious activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/transaction-patterns/{user_id}")
async def get_transaction_patterns(
    user_id: str,
    days: int = Query(30, description="Number of days to analyze"),
    current_user: Dict = Depends(get_current_user),
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Analyze transaction patterns for a user.
    
    Identifies unusual patterns that may indicate money laundering.
    """
    try:
        # Permission check
        if current_user["id"] != user_id and "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        
        # Get patterns
        patterns = await aml_engine.analyze_transaction_patterns(
            user_id=user_id,
            days=days
        )
        
        return {
            "user_id": user_id,
            "period_days": days,
            "patterns": patterns["patterns"],
            "anomalies": patterns["anomalies"],
            "risk_indicators": patterns["risk_indicators"],
            "velocity_score": patterns["velocity_score"],
            "structuring_score": patterns["structuring_score"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enhanced-due-diligence/{user_id}")
async def trigger_enhanced_due_diligence(
    user_id: str,
    reason: str,
    current_user: Dict = Depends(get_current_user),
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Trigger Enhanced Due Diligence (EDD) for a high-risk user.
    """
    try:
        # Only compliance team can trigger EDD
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can trigger EDD"
            )
        
        # Trigger EDD
        edd_case = await aml_engine.trigger_edd(
            user_id=user_id,
            reason=reason,
            triggered_by=current_user["id"]
        )
        
        return {
            "edd_case_id": edd_case["id"],
            "user_id": user_id,
            "status": "initiated",
            "required_documents": edd_case["required_documents"],
            "deadline": edd_case["deadline"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering EDD: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/watchlist-matches/{user_id}")
async def get_watchlist_matches(
    user_id: str,
    current_user: Dict = Depends(get_current_user),
    aml_engine: AMLEngine = Depends(get_aml_engine)
):
    """
    Get any watchlist matches for a user.
    
    Shows matches from PEP lists, adverse media, and other watchlists.
    """
    try:
        # Permission check
        if current_user["id"] != user_id and "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Insufficient permissions"
            )
        
        # Get matches
        matches = await aml_engine.get_watchlist_matches(user_id)
        
        return {
            "user_id": user_id,
            "total_matches": len(matches),
            "matches": matches,
            "last_checked": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting watchlist matches: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 