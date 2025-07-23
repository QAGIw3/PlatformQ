"""
Data Quality API endpoints.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field
import pandas as pd

from app.engines.quality import (
    QualityValidator,
    QualityProfiler,
    AnomalyDetector,
    RemediationEngine,
    QualityDimension,
    RuleType,
    ProfileType,
    AnomalyType,
    RemediationType,
    RemediationStrategy
)

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

# Dependency injection
_quality_validator: Optional[QualityValidator] = None
_quality_profiler: Optional[QualityProfiler] = None
_anomaly_detector: Optional[AnomalyDetector] = None
_remediation_engine: Optional[RemediationEngine] = None


def get_quality_validator() -> QualityValidator:
    if _quality_validator is None:
        raise HTTPException(status_code=500, detail="Quality Validator not initialized")
    return _quality_validator


def get_quality_profiler() -> QualityProfiler:
    if _quality_profiler is None:
        raise HTTPException(status_code=500, detail="Quality Profiler not initialized")
    return _quality_profiler


def get_anomaly_detector() -> AnomalyDetector:
    if _anomaly_detector is None:
        raise HTTPException(status_code=500, detail="Anomaly Detector not initialized")
    return _anomaly_detector


def get_remediation_engine() -> RemediationEngine:
    if _remediation_engine is None:
        raise HTTPException(status_code=500, detail="Remediation Engine not initialized")
    return _remediation_engine


def set_engines(validator, profiler, detector, engine):
    global _quality_validator, _quality_profiler, _anomaly_detector, _remediation_engine
    _quality_validator = validator
    _quality_profiler = profiler
    _anomaly_detector = detector
    _remediation_engine = engine


# Request/Response Models
class QualityRuleRequest(BaseModel):
    name: str
    description: str
    dimension: QualityDimension
    rule_type: RuleType
    expression: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    severity: str = "medium"
    threshold: float = 0.95
    enabled: bool = True


class ValidationRequest(BaseModel):
    dataset_id: str
    data: List[Dict[str, Any]]
    rule_set: Optional[str] = None
    rules: Optional[List[str]] = None
    sample_size: int = 100


class ProfileRequest(BaseModel):
    dataset_id: str
    data: List[Dict[str, Any]]
    profile_type: ProfileType = ProfileType.BASIC
    columns: Optional[List[str]] = None


class AnomalyDetectionRequest(BaseModel):
    dataset_id: str
    data: List[Dict[str, Any]]
    anomaly_type: AnomalyType = AnomalyType.OUTLIER
    method: Optional[str] = None
    columns: Optional[List[str]] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class RemediationRequest(BaseModel):
    dataset_id: str
    data: List[Dict[str, Any]]
    quality_issues: List[Dict[str, Any]]
    actions: Optional[List[Dict[str, Any]]] = None
    auto_select: bool = True


class QualityResponse(BaseModel):
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# Validation Endpoints
@router.post("/validate", response_model=QualityResponse)
async def validate_data(
    request: ValidationRequest,
    background_tasks: BackgroundTasks,
    validator: QualityValidator = Depends(get_quality_validator)
):
    """Validate data quality."""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(request.data)
        
        # Perform validation
        result = await validator.validate_data(
            data=df,
            dataset_id=request.dataset_id,
            rule_set=request.rule_set,
            rules=request.rules,
            sample_size=request.sample_size
        )
        
        return QualityResponse(
            status="success",
            message=f"Validation completed for dataset {request.dataset_id}",
            data={
                "validation_id": result.validation_id,
                "quality_score": result.quality_score,
                "total_records": result.total_records,
                "passed_records": result.passed_records,
                "failed_records": result.failed_records,
                "dimensions": {k.value: v for k, v in result.dimensions.items()},
                "issues": [
                    {
                        "rule_id": issue.rule_id,
                        "dimension": issue.dimension.value,
                        "severity": issue.severity.value,
                        "description": issue.description,
                        "affected_records": issue.affected_records
                    }
                    for issue in result.issues
                ],
                "execution_time_ms": result.execution_time_ms
            }
        )
        
    except Exception as e:
        logger.error(f"Error validating data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules", response_model=QualityResponse)
async def create_quality_rule(
    request: QualityRuleRequest,
    validator: QualityValidator = Depends(get_quality_validator)
):
    """Create a new quality rule."""
    try:
        from app.engines.quality.quality_validator import QualityRule, SeverityLevel
        
        rule = QualityRule(
            rule_id=f"rule_{datetime.utcnow().timestamp()}",
            name=request.name,
            description=request.description,
            dimension=request.dimension,
            rule_type=request.rule_type,
            expression=request.expression,
            parameters=request.parameters,
            severity=SeverityLevel(request.severity),
            threshold=request.threshold,
            enabled=request.enabled
        )
        
        await validator.register_rule(rule)
        
        return QualityResponse(
            status="success",
            message=f"Rule '{request.name}' created successfully",
            data={"rule_id": rule.rule_id}
        )
        
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rules", response_model=QualityResponse)
async def list_quality_rules(
    validator: QualityValidator = Depends(get_quality_validator)
):
    """List all quality rules."""
    try:
        rules = [
            {
                "rule_id": rule.rule_id,
                "name": rule.name,
                "description": rule.description,
                "dimension": rule.dimension.value,
                "rule_type": rule.rule_type.value,
                "enabled": rule.enabled
            }
            for rule in validator.rules.values()
        ]
        
        return QualityResponse(
            status="success",
            message=f"Found {len(rules)} quality rules",
            data={"rules": rules}
        )
        
    except Exception as e:
        logger.error(f"Error listing rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Profiling Endpoints
@router.post("/profile", response_model=QualityResponse)
async def profile_data(
    request: ProfileRequest,
    background_tasks: BackgroundTasks,
    profiler: QualityProfiler = Depends(get_quality_profiler)
):
    """Profile data quality."""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(request.data)
        
        # Perform profiling
        profile = await profiler.profile_data(
            data=df,
            dataset_id=request.dataset_id,
            profile_type=request.profile_type,
            columns=request.columns
        )
        
        return QualityResponse(
            status="success",
            message=f"Profiling completed for dataset {request.dataset_id}",
            data={
                "profile_id": profile.profile_id,
                "quality_score": profile.estimated_quality_score,
                "row_count": profile.row_count,
                "column_count": profile.column_count,
                "memory_usage_mb": profile.memory_usage_mb,
                "columns": {
                    name: {
                        "data_type": col.data_type,
                        "completeness": col.completeness,
                        "unique_count": col.unique_count,
                        "null_count": col.null_count,
                        "mean": col.mean,
                        "median": col.median,
                        "std_dev": col.std_dev
                    }
                    for name, col in profile.columns.items()
                },
                "execution_time_ms": profile.execution_time_ms
            }
        )
        
    except Exception as e:
        logger.error(f"Error profiling data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profiles/{profile_id}", response_model=QualityResponse)
async def get_profile(
    profile_id: str,
    profiler: QualityProfiler = Depends(get_quality_profiler)
):
    """Get profile details."""
    try:
        summary = profiler.get_profile_summary(profile_id)
        
        if not summary:
            raise HTTPException(status_code=404, detail="Profile not found")
        
        return QualityResponse(
            status="success",
            message=f"Profile {profile_id} retrieved",
            data=summary
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Anomaly Detection Endpoints
@router.post("/anomalies/detect", response_model=QualityResponse)
async def detect_anomalies(
    request: AnomalyDetectionRequest,
    background_tasks: BackgroundTasks,
    detector: AnomalyDetector = Depends(get_anomaly_detector)
):
    """Detect anomalies in data."""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(request.data)
        
        # Detect anomalies
        result = await detector.detect_anomalies(
            data=df,
            dataset_id=request.dataset_id,
            anomaly_type=request.anomaly_type,
            method=request.method,
            columns=request.columns,
            **request.parameters
        )
        
        return QualityResponse(
            status="success",
            message=f"Anomaly detection completed for dataset {request.dataset_id}",
            data={
                "detection_id": result.detection_id,
                "anomaly_type": result.anomaly_type.value,
                "method": result.method.value,
                "total_records": result.total_records,
                "anomaly_count": result.anomaly_count,
                "anomaly_rate": result.anomaly_rate,
                "severity": result.severity,
                "confidence": result.confidence,
                "anomaly_indices": result.anomaly_indices[:100],  # Limit for response
                "execution_time_ms": result.execution_time_ms
            }
        )
        
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies/patterns/{dataset_id}", response_model=QualityResponse)
async def get_anomaly_patterns(
    dataset_id: str,
    days: int = Query(7, description="Time window in days"),
    detector: AnomalyDetector = Depends(get_anomaly_detector)
):
    """Get anomaly patterns for a dataset."""
    try:
        from datetime import timedelta
        
        patterns = await detector.get_anomaly_patterns(
            dataset_id=dataset_id,
            time_window=timedelta(days=days)
        )
        
        return QualityResponse(
            status="success",
            message=f"Anomaly patterns retrieved for dataset {dataset_id}",
            data=patterns
        )
        
    except Exception as e:
        logger.error(f"Error getting anomaly patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Remediation Endpoints
@router.post("/remediate", response_model=QualityResponse)
async def remediate_data(
    request: RemediationRequest,
    background_tasks: BackgroundTasks,
    engine: RemediationEngine = Depends(get_remediation_engine)
):
    """Remediate data quality issues."""
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(request.data)
        
        # Convert actions if provided
        actions = None
        if request.actions:
            from app.engines.quality.remediation_engine import RemediationAction
            actions = [
                RemediationAction(
                    action_id=a["action_id"],
                    remediation_type=RemediationType(a["remediation_type"]),
                    strategy=RemediationStrategy(a["strategy"]),
                    target_columns=a["target_columns"],
                    parameters=a.get("parameters", {}),
                    description=a.get("description", "")
                )
                for a in request.actions
            ]
        
        # Perform remediation
        result = await engine.remediate_data(
            data=df,
            dataset_id=request.dataset_id,
            quality_issues=request.quality_issues,
            actions=actions,
            auto_select=request.auto_select
        )
        
        return QualityResponse(
            status="success",
            message=f"Remediation completed for dataset {request.dataset_id}",
            data={
                "remediation_id": result.remediation_id,
                "original_quality_score": result.original_quality_score,
                "final_quality_score": result.final_quality_score,
                "quality_improvement": result.quality_improvement,
                "records_modified": result.records_modified,
                "cells_modified": result.cells_modified,
                "columns_affected": result.columns_affected,
                "actions_applied": len(result.actions),
                "success": result.success,
                "errors": result.errors,
                "execution_time_ms": result.execution_time_ms
            }
        )
        
    except Exception as e:
        logger.error(f"Error remediating data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remediate/recommend", response_model=QualityResponse)
async def get_remediation_recommendations(
    quality_issues: List[Dict[str, Any]],
    engine: RemediationEngine = Depends(get_remediation_engine)
):
    """Get remediation recommendations for quality issues."""
    try:
        recommendations = await engine.get_remediation_recommendations(quality_issues)
        
        return QualityResponse(
            status="success",
            message=f"Generated {len(recommendations)} remediation recommendations",
            data={
                "recommendations": [
                    {
                        "action_id": rec.action_id,
                        "remediation_type": rec.remediation_type.value,
                        "strategy": rec.strategy.value,
                        "target_columns": rec.target_columns,
                        "description": rec.description
                    }
                    for rec in recommendations
                ]
            }
        )
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Statistics Endpoints
@router.get("/statistics", response_model=QualityResponse)
async def get_quality_statistics(
    validator: QualityValidator = Depends(get_quality_validator)
):
    """Get quality validation statistics."""
    try:
        stats = validator.get_statistics()
        
        return QualityResponse(
            status="success",
            message="Quality statistics retrieved",
            data=stats
        )
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 