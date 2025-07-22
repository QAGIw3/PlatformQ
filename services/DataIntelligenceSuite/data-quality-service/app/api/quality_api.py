"""
Data Quality API endpoints

Provides API for data quality operations including checks, validation,
and remediation.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Path, Depends
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/quality", tags=["quality"])


# Request/Response Models
class QualityCheckRequest(BaseModel):
    """Request for quality check"""
    dataset: str = Field(..., description="Dataset identifier")
    check_type: str = Field(default="full", description="Type of check: full, incremental, sample")
    sample_size: Optional[int] = Field(None, description="Sample size for sample checks")
    rules: Optional[List[str]] = Field(None, description="Specific rules to apply")
    auto_remediate: bool = Field(default=False, description="Enable auto-remediation")


class QualityCheckResponse(BaseModel):
    """Response for quality check"""
    request_id: str
    dataset: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    metrics: Dict[str, float]
    issues_found: int
    issues_resolved: int
    details: List[Dict[str, Any]]


class ValidationRequest(BaseModel):
    """Request for data validation"""
    data: Dict[str, Any] = Field(..., description="Data to validate")
    schema: Optional[Dict[str, Any]] = Field(None, description="Schema to validate against")
    rules: Optional[List[str]] = Field(None, description="Validation rules to apply")
    strict: bool = Field(default=True, description="Strict validation mode")


class ValidationResponse(BaseModel):
    """Response for data validation"""
    valid: bool
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    metadata: Dict[str, Any]


class RemediationRequest(BaseModel):
    """Request for data remediation"""
    dataset: str = Field(..., description="Dataset identifier")
    issue_ids: List[str] = Field(..., description="Issue IDs to remediate")
    strategy: str = Field(default="auto", description="Remediation strategy")
    dry_run: bool = Field(default=False, description="Preview changes without applying")


class RemediationResponse(BaseModel):
    """Response for data remediation"""
    request_id: str
    dataset: str
    issues_processed: int
    issues_resolved: int
    failed_remediations: List[Dict[str, Any]]
    changes_applied: List[Dict[str, Any]]


class ProfileRequest(BaseModel):
    """Request for data profiling"""
    dataset: str = Field(..., description="Dataset to profile")
    columns: Optional[List[str]] = Field(None, description="Specific columns to profile")
    profile_type: str = Field(default="standard", description="Profile type: standard, detailed, minimal")


# API Endpoints
@router.post("/check", response_model=QualityCheckResponse)
async def check_quality(request: QualityCheckRequest):
    """
    Run quality checks on a dataset
    """
    try:
        logger.info("quality_check_requested", dataset=request.dataset)
        
        # Get service instance from app state (injected by main.py)
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Perform quality check
        result = await service.quality_engine.check_quality(
            dataset=request.dataset,
            check_type=request.check_type,
            sample_size=request.sample_size,
            rule_ids=request.rules,
            auto_remediate=request.auto_remediate
        )
        
        return QualityCheckResponse(
            request_id=result["request_id"],
            dataset=request.dataset,
            status=result["status"],
            started_at=result["started_at"],
            completed_at=result.get("completed_at"),
            metrics=result["metrics"],
            issues_found=result["issues_found"],
            issues_resolved=result.get("issues_resolved", 0),
            details=result["details"]
        )
        
    except Exception as e:
        logger.error("quality_check_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate", response_model=ValidationResponse)
async def validate_data(request: ValidationRequest):
    """
    Validate data against schema and rules
    """
    try:
        logger.info("validation_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Perform validation
        result = await service.quality_engine.validate_transformation(
            data=request.data,
            schema=request.schema,
            rule_ids=request.rules,
            strict=request.strict
        )
        
        return ValidationResponse(
            valid=result["valid"],
            errors=result["errors"],
            warnings=result["warnings"],
            metadata=result["metadata"]
        )
        
    except Exception as e:
        logger.error("validation_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remediate", response_model=RemediationResponse)
async def remediate_issues(request: RemediationRequest):
    """
    Remediate data quality issues
    """
    try:
        logger.info("remediation_requested", dataset=request.dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Perform remediation
        result = await service.remediation_orchestrator.remediate_issues(
            dataset=request.dataset,
            issue_ids=request.issue_ids,
            strategy=request.strategy,
            dry_run=request.dry_run
        )
        
        return RemediationResponse(
            request_id=result["request_id"],
            dataset=request.dataset,
            issues_processed=len(request.issue_ids),
            issues_resolved=result["resolved_count"],
            failed_remediations=result["failures"],
            changes_applied=result["changes"] if not request.dry_run else []
        )
        
    except Exception as e:
        logger.error("remediation_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/profile", response_model=Dict[str, Any])
async def profile_dataset(request: ProfileRequest):
    """
    Profile a dataset
    """
    try:
        logger.info("profiling_requested", dataset=request.dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Perform profiling
        profile = await service.quality_engine.profile_dataset(
            dataset=request.dataset,
            columns=request.columns,
            profile_type=request.profile_type
        )
        
        return profile
        
    except Exception as e:
        logger.error("profiling_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/issues/{dataset}")
async def get_quality_issues(
    dataset: str = Path(..., description="Dataset identifier"),
    status: Optional[str] = Query(None, description="Filter by status"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    limit: int = Query(100, description="Maximum results")
):
    """
    Get quality issues for a dataset
    """
    try:
        logger.info("get_issues_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get issues
        issues = await service.quality_engine.get_quality_issues(
            dataset=dataset,
            status=status,
            severity=severity,
            limit=limit
        )
        
        return {
            "dataset": dataset,
            "total_issues": len(issues),
            "issues": issues
        }
        
    except Exception as e:
        logger.error("get_issues_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history/{dataset}")
async def get_quality_history(
    dataset: str = Path(..., description="Dataset identifier"),
    days: int = Query(7, description="Number of days of history"),
    metric: Optional[str] = Query(None, description="Specific metric to retrieve")
):
    """
    Get quality history for a dataset
    """
    try:
        logger.info("get_history_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get history
        history = await service.quality_monitor.get_dataset_metrics(dataset)
        
        return {
            "dataset": dataset,
            "period_days": days,
            "history": history
        }
        
    except Exception as e:
        logger.error("get_history_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/{dataset}")
async def get_quality_recommendations(
    dataset: str = Path(..., description="Dataset identifier")
):
    """
    Get quality improvement recommendations
    """
    try:
        logger.info("get_recommendations_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get recommendations
        recommendations = await service.quality_engine.get_recommendations(dataset)
        
        return {
            "dataset": dataset,
            "recommendations": recommendations
        }
        
    except Exception as e:
        logger.error("get_recommendations_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 