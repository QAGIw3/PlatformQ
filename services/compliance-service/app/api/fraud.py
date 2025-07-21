"""
Fraud Detection API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..core.deps import get_current_tenant_and_user, get_fraud_engine
from ..fraud import FraudDetectionEngine

router = APIRouter()


class FraudCheckRequest(BaseModel):
    """Request model for fraud detection check"""
    entity_ids: List[str] = Field(..., description="List of entity IDs to check")
    check_depth: int = Field(2, description="Graph traversal depth for analysis")
    include_network_analysis: bool = Field(True, description="Include network-based fraud indicators")


class FraudCheckResponse(BaseModel):
    """Response model for fraud check"""
    job_id: str
    status: str
    immediate_results: List[Dict[str, Any]]
    total_entities: int
    message: str


class FraudResultsResponse(BaseModel):
    """Response model for fraud check results"""
    job_id: str
    status: str
    fraud_analysis: Optional[Dict[str, Any]] = None
    suspicious_entities: Optional[List[str]] = None
    execution_time: Optional[str] = None
    error: Optional[str] = None


@router.post("/check", response_model=FraudCheckResponse)
async def check_fraud(
    request: FraudCheckRequest,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    fraud_engine: FraudDetectionEngine = Depends(get_fraud_engine)
):
    """
    Check entities for fraud indicators using graph analysis
    
    This endpoint submits a fraud detection job that:
    - Analyzes entity relationships and patterns
    - Checks against known fraud patterns
    - Calculates fraud risk scores
    - Optionally performs network analysis
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Submit fraud detection job
        job_id = await fraud_engine.check_entities_for_fraud(
            entity_ids=request.entity_ids,
            check_depth=request.check_depth,
            include_network_analysis=request.include_network_analysis,
            tenant_id=tenant_id
        )
        
        # Get immediate results for first 5 entities
        immediate_results = await fraud_engine.get_immediate_fraud_indicators(
            entity_ids=request.entity_ids,
            tenant_id=tenant_id
        )
        
        return FraudCheckResponse(
            job_id=job_id,
            status="processing",
            immediate_results=immediate_results,
            total_entities=len(request.entity_ids),
            message="Full fraud analysis in progress"
        )
        
    except Exception as e:
        logger.error(f"Error checking fraud: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/results/{job_id}", response_model=FraudResultsResponse)
async def get_fraud_results(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    fraud_engine: FraudDetectionEngine = Depends(get_fraud_engine)
):
    """
    Get results of fraud detection job
    
    Returns the results of a previously submitted fraud detection job,
    including fraud scores, suspicious entities, and pattern matches.
    """
    tenant_id = context["tenant_id"]
    
    try:
        results = await fraud_engine.get_fraud_check_results(job_id, tenant_id)
        
        if not results:
            raise HTTPException(status_code=404, detail="Job not found")
            
        response = FraudResultsResponse(
            job_id=job_id,
            status=results["status"]
        )
        
        if results["status"] == "completed":
            response.fraud_analysis = results["results"].get("fraud_analysis", {})
            response.suspicious_entities = results["results"].get("suspicious_entities", [])
            response.execution_time = results.get("completed_at")
        elif results["status"] == "failed" or results["status"] == "timeout":
            response.error = results.get("error", "Unknown error")
            
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting fraud results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns", response_model=List[Dict[str, Any]])
async def get_fraud_patterns(
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get available fraud detection patterns
    
    Returns the list of fraud patterns used by the detection engine.
    """
    from ..fraud.fraud_patterns import COMMON_PATTERNS
    
    return [pattern.to_dict() for pattern in COMMON_PATTERNS]


@router.get("/statistics", response_model=Dict[str, Any])
async def get_fraud_statistics(
    days: int = 30,
    context: dict = Depends(get_current_tenant_and_user),
    fraud_engine: FraudDetectionEngine = Depends(get_fraud_engine)
):
    """
    Get fraud detection statistics
    
    Returns statistics about fraud detection over the specified time period.
    """
    # This would query cached statistics
    # For now, return mock data
    return {
        "period_days": days,
        "total_checks": 1523,
        "suspicious_entities": 47,
        "fraud_patterns_detected": {
            "money_laundering": 12,
            "structuring": 8,
            "rapid_movement": 15,
            "circular_flow": 3,
            "high_risk_jurisdiction": 9
        },
        "average_fraud_score": 0.23,
        "high_risk_entities": 18,
        "last_updated": datetime.utcnow().isoformat()
    }


# Import logger
import logging
logger = logging.getLogger(__name__) 