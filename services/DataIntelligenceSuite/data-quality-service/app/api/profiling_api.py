"""
Profiling API endpoints

Provides API for data profiling operations.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/profiling", tags=["profiling"])


# Request/Response Models
class ProfilingRequest(BaseModel):
    """Data profiling request"""
    dataset: str = Field(..., description="Dataset to profile")
    columns: Optional[List[str]] = Field(None, description="Specific columns to profile")
    sample_size: Optional[int] = Field(None, description="Sample size for profiling")
    profile_type: str = Field(default="standard", description="Profile type: minimal, standard, detailed")
    include_statistics: bool = Field(default=True, description="Include statistical analysis")
    include_patterns: bool = Field(default=True, description="Include pattern detection")
    include_outliers: bool = Field(default=True, description="Include outlier detection")


class ColumnProfile(BaseModel):
    """Column profile data"""
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float
    statistics: Optional[Dict[str, Any]]
    patterns: Optional[List[Dict[str, Any]]]
    outliers: Optional[List[Any]]
    sample_values: List[Any]


class DatasetProfile(BaseModel):
    """Dataset profile data"""
    dataset: str
    profiled_at: datetime
    row_count: int
    column_count: int
    columns: List[ColumnProfile]
    data_quality_score: float
    issues_found: List[Dict[str, Any]]
    recommendations: List[str]


class AnomalyDetectionRequest(BaseModel):
    """Anomaly detection request"""
    dataset: str = Field(..., description="Dataset to analyze")
    columns: List[str] = Field(..., description="Columns to check for anomalies")
    method: str = Field(default="statistical", description="Detection method: statistical, isolation_forest, dbscan")
    sensitivity: float = Field(default=0.95, description="Sensitivity level (0-1)")


# API Endpoints
@router.post("/profile", response_model=DatasetProfile)
async def profile_dataset(request: ProfilingRequest):
    """
    Profile a dataset
    """
    try:
        logger.info("profile_dataset_requested", dataset=request.dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Perform profiling
        profile_data = await service.profiler.profile_dataset(
            dataset=request.dataset,
            columns=request.columns,
            sample_size=request.sample_size,
            include_statistics=request.include_statistics,
            include_patterns=request.include_patterns,
            include_outliers=request.include_outliers
        )
        
        # Convert to response model
        columns = []
        for col_name, col_data in profile_data.get("columns", {}).items():
            columns.append(ColumnProfile(
                column_name=col_name,
                data_type=col_data.get("data_type", "unknown"),
                null_count=col_data.get("null_count", 0),
                null_percentage=col_data.get("null_percentage", 0.0),
                unique_count=col_data.get("unique_count", 0),
                unique_percentage=col_data.get("unique_percentage", 0.0),
                statistics=col_data.get("statistics") if request.include_statistics else None,
                patterns=col_data.get("patterns") if request.include_patterns else None,
                outliers=col_data.get("outliers") if request.include_outliers else None,
                sample_values=col_data.get("sample_values", [])
            ))
        
        return DatasetProfile(
            dataset=request.dataset,
            profiled_at=datetime.utcnow(),
            row_count=profile_data.get("row_count", 0),
            column_count=profile_data.get("column_count", 0),
            columns=columns,
            data_quality_score=profile_data.get("quality_score", 0.0),
            issues_found=profile_data.get("issues", []),
            recommendations=profile_data.get("recommendations", [])
        )
        
    except Exception as e:
        logger.error("profile_dataset_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profile/{dataset}")
async def get_latest_profile(
    dataset: str = Path(..., description="Dataset identifier")
):
    """
    Get the latest profile for a dataset
    """
    try:
        logger.info("get_latest_profile_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get latest profile from cache or storage
        profile = await service.profiler.get_latest_profile(dataset)
        
        if not profile:
            raise HTTPException(status_code=404, detail="No profile found for dataset")
        
        return profile
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_latest_profile_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profiles")
async def list_profiles(
    dataset: Optional[str] = Query(None, description="Filter by dataset"),
    days: int = Query(7, description="Profiles from last N days"),
    limit: int = Query(50, description="Maximum results")
):
    """
    List dataset profiles
    """
    try:
        logger.info("list_profiles_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get profiles
        profiles = await service.profiler.list_profiles(
            dataset=dataset,
            days=days,
            limit=limit
        )
        
        return {
            "total": len(profiles),
            "profiles": profiles
        }
        
    except Exception as e:
        logger.error("list_profiles_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anomalies/detect")
async def detect_anomalies(request: AnomalyDetectionRequest):
    """
    Detect anomalies in dataset
    """
    try:
        logger.info("detect_anomalies_requested", dataset=request.dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Detect anomalies
        anomalies = await service.profiler.detect_anomalies(
            dataset=request.dataset,
            columns=request.columns,
            method=request.method,
            sensitivity=request.sensitivity
        )
        
        return {
            "dataset": request.dataset,
            "method": request.method,
            "total_anomalies": len(anomalies),
            "anomalies": anomalies
        }
        
    except Exception as e:
        logger.error("detect_anomalies_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics/{dataset}")
async def get_column_statistics(
    dataset: str = Path(..., description="Dataset identifier"),
    column: str = Query(..., description="Column name")
):
    """
    Get detailed statistics for a column
    """
    try:
        logger.info("get_column_statistics_requested", dataset=dataset, column=column)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get column statistics
        stats = await service.profiler.get_column_statistics(dataset, column)
        
        if not stats:
            raise HTTPException(status_code=404, detail="Column not found or no statistics available")
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_column_statistics_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns/{dataset}")
async def get_data_patterns(
    dataset: str = Path(..., description="Dataset identifier"),
    column: Optional[str] = Query(None, description="Specific column"),
    pattern_type: Optional[str] = Query(None, description="Pattern type: format, frequency, sequence")
):
    """
    Get data patterns for a dataset or column
    """
    try:
        logger.info("get_data_patterns_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get patterns
        patterns = await service.profiler.get_patterns(
            dataset=dataset,
            column=column,
            pattern_type=pattern_type
        )
        
        return {
            "dataset": dataset,
            "column": column,
            "patterns": patterns
        }
        
    except Exception as e:
        logger.error("get_data_patterns_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compare")
async def compare_profiles(
    dataset1: str = Body(..., description="First dataset"),
    dataset2: str = Body(..., description="Second dataset"),
    columns: Optional[List[str]] = Body(None, description="Specific columns to compare")
):
    """
    Compare profiles of two datasets
    """
    try:
        logger.info("compare_profiles_requested", dataset1=dataset1, dataset2=dataset2)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Compare profiles
        comparison = await service.profiler.compare_profiles(
            dataset1=dataset1,
            dataset2=dataset2,
            columns=columns
        )
        
        return comparison
        
    except Exception as e:
        logger.error("compare_profiles_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/drift/{dataset}")
async def detect_data_drift(
    dataset: str = Path(..., description="Dataset identifier"),
    reference_period_days: int = Query(30, description="Reference period for comparison"),
    current_period_days: int = Query(7, description="Current period to analyze")
):
    """
    Detect data drift in dataset
    """
    try:
        logger.info("detect_drift_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Detect drift
        drift_analysis = await service.profiler.detect_drift(
            dataset=dataset,
            reference_period_days=reference_period_days,
            current_period_days=current_period_days
        )
        
        return drift_analysis
        
    except Exception as e:
        logger.error("detect_drift_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 