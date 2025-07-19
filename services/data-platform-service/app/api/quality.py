"""
Data quality API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum
import uuid
import logging

from .. import main

router = APIRouter()
logger = logging.getLogger(__name__)


class QualityDimension(str, Enum):
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


class RuleSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class RuleType(str, Enum):
    NULL_CHECK = "null_check"
    RANGE_CHECK = "range_check"
    PATTERN_CHECK = "pattern_check"
    REFERENCE_CHECK = "reference_check"
    CUSTOM_SQL = "custom_sql"
    STATISTICAL = "statistical"


class QualityRule(BaseModel):
    """Data quality rule definition"""
    name: str = Field(..., description="Rule name")
    description: Optional[str] = Field(None)
    rule_type: RuleType
    dimension: QualityDimension
    severity: RuleSeverity
    expression: str = Field(..., description="Rule expression/SQL")
    parameters: Optional[Dict[str, Any]] = Field(None)
    threshold: Optional[float] = Field(None, ge=0, le=1)


class QualityProfile(BaseModel):
    """Quality profile for assets"""
    name: str = Field(..., description="Profile name")
    description: Optional[str] = Field(None)
    rules: List[QualityRule]
    asset_pattern: str = Field(..., description="Asset matching pattern")
    enabled: bool = Field(True)
    tags: List[str] = Field(default_factory=list)


class QualityProfileResponse(QualityProfile):
    """Quality profile response"""
    profile_id: str
    created_at: datetime
    updated_at: datetime
    created_by: str
    applied_to_count: int = 0


class QualityCheckResult(BaseModel):
    """Quality check result"""
    check_id: str
    asset_id: str
    profile_id: str
    executed_at: datetime
    overall_score: float
    dimension_scores: Dict[QualityDimension, float]
    rule_results: List[Dict[str, Any]]
    passed: bool
    issues_found: int = 0
    critical_issues: int = 0


class ProfileRequest(BaseModel):
    """Profile request"""
    data_source: str = Field(..., description="Data source type")
    source_config: Dict[str, Any] = Field(..., description="Source configuration")
    options: Dict[str, Any] = Field(default_factory=dict, description="Profiling options")


@router.post("/profiles", response_model=QualityProfileResponse)
async def create_quality_profile(
    profile: QualityProfile,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Create a quality profile"""
    try:
        profile_id = str(uuid.uuid4())
        
        # Store profile (in production would persist to database)
        profile_data = {
            **profile.dict(),
            "profile_id": profile_id,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": tenant_id,
            "applied_to_count": 0
        }
        
        return QualityProfileResponse(**profile_data)
    except Exception as e:
        logger.error(f"Failed to create quality profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/profile-data")
async def profile_data(
    profile_request: ProfileRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Profile data from a source"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    try:
        # Run profiling
        result = await main.quality_profiler.profile_data(
            data_source=profile_request.data_source,
            source_config=profile_request.source_config,
            options=profile_request.options
        )
        
        return result
    except Exception as e:
        logger.error(f"Failed to profile data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/check/{asset_id}")
async def run_quality_check(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    profile_id: Optional[str] = Query(None, description="Profile ID to use"),
    rules: Optional[List[QualityRule]] = Body(None, description="Ad-hoc rules to apply")
):
    """Run quality check on an asset"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Verify asset exists
        asset = await main.catalog_manager.get_asset(asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        # Prepare rules
        rule_configs = []
        if rules:
            for rule in rules:
                rule_configs.append({
                    "name": rule.name,
                    "type": rule.rule_type.value,
                    "dimension": rule.dimension.value,
                    "severity": rule.severity.value,
                    "expression": rule.expression,
                    "parameters": rule.parameters,
                    "threshold": rule.threshold
                })
        
        # Run quality checks
        check_results = await main.quality_profiler.validate_quality(
            data_source=asset.get("location"),
            rules=rule_configs if rule_configs else None
        )
        
        # Calculate scores
        overall_score = check_results.get("quality_score", 0.0)
        dimension_scores = check_results.get("dimension_scores", {})
        
        return QualityCheckResult(
            check_id=check_results.get("check_id", str(uuid.uuid4())),
            asset_id=asset_id,
            profile_id=profile_id or "adhoc",
            executed_at=datetime.utcnow(),
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            rule_results=check_results.get("rule_results", []),
            passed=overall_score >= 0.8,
            issues_found=check_results.get("total_issues", 0),
            critical_issues=check_results.get("critical_issues", 0)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to run quality check: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/checks/{asset_id}/history")
async def get_quality_history(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get quality check history for an asset"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    try:
        # Get quality history
        history = await main.quality_profiler.get_quality_history(
            asset_id=asset_id,
            days=days,
            limit=limit
        )
        
        return {
            "asset_id": asset_id,
            "period_days": days,
            "checks": history.get("checks", []),
            "average_score": history.get("average_score", 0.0),
            "trend": history.get("trend", "stable")
        }
    except Exception as e:
        logger.error(f"Failed to get quality history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies")
async def detect_anomalies(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    asset_ids: Optional[List[str]] = Query(None),
    sensitivity: float = Query(0.05, ge=0.01, le=0.5),
    limit: int = Query(100, ge=1, le=1000)
):
    """Detect data anomalies across assets"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    try:
        # Detect anomalies
        anomalies = await main.quality_profiler.detect_anomalies(
            asset_ids=asset_ids,
            sensitivity=sensitivity,
            limit=limit
        )
        
        return {
            "anomalies": anomalies.get("anomalies", []),
            "total_found": anomalies.get("total_found", 0),
            "analysis_timestamp": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Failed to detect anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drift/{asset_id}")
async def analyze_drift(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    baseline_date: Optional[datetime] = Query(None),
    metrics: Optional[List[str]] = Query(None)
):
    """Analyze data drift for an asset"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    try:
        # Analyze drift
        drift_results = await main.quality_profiler.analyze_drift(
            asset_id=asset_id,
            baseline_date=baseline_date,
            metrics=metrics
        )
        
        return {
            "asset_id": asset_id,
            "baseline_date": baseline_date or drift_results.get("baseline_date"),
            "current_date": datetime.utcnow(),
            "drift_detected": drift_results.get("drift_detected", False),
            "drift_score": drift_results.get("drift_score", 0.0),
            "drifted_columns": drift_results.get("drifted_columns", []),
            "recommendations": drift_results.get("recommendations", [])
        }
    except Exception as e:
        logger.error(f"Failed to analyze drift: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules/validate")
async def validate_rules(
    rules: List[QualityRule],
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    test_data: Optional[Dict[str, Any]] = Body(None)
):
    """Validate quality rules"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    try:
        # Validate rules
        validation_results = []
        for rule in rules:
            result = await main.quality_profiler.validate_rule(
                rule_config={
                    "name": rule.name,
                    "type": rule.rule_type.value,
                    "expression": rule.expression,
                    "parameters": rule.parameters
                },
                test_data=test_data
            )
            validation_results.append({
                "rule_name": rule.name,
                "valid": result.get("valid", False),
                "error": result.get("error"),
                "test_result": result.get("test_result")
            })
        
        return {
            "rules_validated": len(rules),
            "valid_rules": sum(1 for r in validation_results if r["valid"]),
            "results": validation_results
        }
    except Exception as e:
        logger.error(f"Failed to validate rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_quality_statistics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    time_range: str = Query("7d", description="Time range (1d, 7d, 30d)")
):
    """Get quality statistics"""
    if not main.quality_profiler:
        raise HTTPException(status_code=503, detail="Quality profiler not available")
    
    try:
        stats = main.quality_profiler.get_statistics()
        
        return {
            "time_range": time_range,
            "total_profiles": stats.get("total_profiles", 0),
            "total_checks": stats.get("total_checks", 0),
            "average_quality_score": stats.get("average_quality_score", 0.0),
            "failing_assets": stats.get("failing_assets", 0),
            "critical_issues": stats.get("critical_issues", 0),
            "dimension_breakdown": stats.get("dimension_breakdown", {}),
            "recent_checks": stats.get("recent_checks", [])
        }
    except Exception as e:
        logger.error(f"Failed to get quality statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 


@router.post("/assessments")
async def receive_quality_assessment(
    request: Request,
    assessment: Dict[str, Any]
) -> Dict[str, Any]:
    """Receive quality assessment from Flink job"""
    try:
        quality_profiler = request.app.state.quality_profiler
        lineage_tracker = request.app.state.lineage_tracker
        catalog_manager = request.app.state.catalog_manager
        
        # Store the assessment
        dataset_id = assessment.get("datasetId")
        if not dataset_id:
            raise HTTPException(status_code=400, detail="Dataset ID required")
        
        # Update quality profile
        await quality_profiler.update_quality_profile(
            dataset_id=dataset_id,
            assessment=assessment
        )
        
        # Update catalog with latest quality score
        quality_score = assessment.get("overallQualityScore", 0)
        await catalog_manager.update_dataset_metadata(
            dataset_id=dataset_id,
            metadata={
                "quality_score": quality_score,
                "last_quality_assessment": datetime.utcnow().isoformat(),
                "quality_dimensions": assessment.get("dimensions", {}),
                "quality_issues": assessment.get("qualityIssues", [])
            }
        )
        
        # Track quality change in lineage
        await lineage_tracker.track_quality_change(
            entity_id=dataset_id,
            quality_score=quality_score,
            assessment_id=assessment.get("assessmentId")
        )
        
        # Check if quality alert needed
        if quality_score < 0.7:
            # Publish quality alert event
            await request.app.state.event_publisher.publish(
                topic="data-quality-alerts",
                event={
                    "type": "QUALITY_THRESHOLD_BREACH",
                    "dataset_id": dataset_id,
                    "quality_score": quality_score,
                    "issues": assessment.get("qualityIssues", []),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        
        return {
            "status": "accepted",
            "dataset_id": dataset_id,
            "quality_score": quality_score,
            "message": "Quality assessment processed successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to process quality assessment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/real-time/{dataset_id}")
async def get_real_time_quality(
    request: Request,
    dataset_id: str,
    time_range: str = Query("1h", description="Time range for real-time data")
) -> Dict[str, Any]:
    """Get real-time quality metrics from cache"""
    try:
        cache_manager = request.app.state.cache_manager
        
        # Try to get from cache first (populated by Flink job via Ignite)
        cache_key = f"quality:realtime:{dataset_id}"
        cached_metrics = await cache_manager.get(cache_key)
        
        if cached_metrics:
            return cached_metrics
        
        # If not in cache, get latest from quality profiler
        quality_profiler = request.app.state.quality_profiler
        
        profile = await quality_profiler.get_dataset_profile(dataset_id)
        
        if not profile:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        return {
            "dataset_id": dataset_id,
            "current_quality_score": profile.get("quality_score", 0),
            "dimensions": profile.get("dimensions", {}),
            "recent_issues": profile.get("recent_issues", []),
            "last_assessment": profile.get("last_assessment_time"),
            "trend": profile.get("quality_trend", "stable")
        }
        
    except Exception as e:
        logger.error(f"Failed to get real-time quality: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anomalies")
async def report_quality_anomaly(
    request: Request,
    anomaly: Dict[str, Any]
) -> Dict[str, Any]:
    """Receive quality anomaly reports from Flink job"""
    try:
        dataset_id = anomaly.get("datasetId")
        if not dataset_id:
            raise HTTPException(status_code=400, detail="Dataset ID required")
        
        # Store anomaly
        quality_profiler = request.app.state.quality_profiler
        await quality_profiler.record_anomaly(anomaly)
        
        # Trigger automated remediation if configured
        severity = anomaly.get("severity", "MEDIUM")
        if severity == "CRITICAL":
            # Trigger data quarantine workflow
            await request.app.state.pipeline_coordinator.trigger_pipeline(
                pipeline_type="data_quarantine",
                parameters={
                    "dataset_id": dataset_id,
                    "reason": "critical_quality_anomaly",
                    "anomaly_details": anomaly
                }
            )
        
        # Notify relevant stakeholders
        await request.app.state.event_publisher.publish(
            topic="data-quality-anomalies",
            event={
                "type": "QUALITY_ANOMALY_DETECTED",
                "dataset_id": dataset_id,
                "severity": severity,
                "anomaly": anomaly,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {
            "status": "processed",
            "dataset_id": dataset_id,
            "severity": severity,
            "remediation_triggered": severity == "CRITICAL"
        }
        
    except Exception as e:
        logger.error(f"Failed to process quality anomaly: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 