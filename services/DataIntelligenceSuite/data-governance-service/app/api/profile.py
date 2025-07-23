"""
Data profiling API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field

from platformq_shared.logging import get_logger
from ..core import QualityProfiler

logger = get_logger(__name__)

# Create router
profile_router = APIRouter()

# Global reference to profiler
quality_profiler: Optional[QualityProfiler] = None


def get_profiler() -> QualityProfiler:
    """Get profiler instance"""
    if quality_profiler is None:
        raise HTTPException(status_code=503, detail="Quality profiler not initialized")
    return quality_profiler


# Request/Response models

class ProfileRequest(BaseModel):
    """Profile request"""
    dataset_id: str = Field(..., description="Dataset identifier")
    data_location: Optional[str] = Field(None, description="Data location (S3, file path, etc.)")
    data: Optional[Dict[str, Any]] = Field(None, description="Inline data for profiling")
    profile_types: Optional[List[str]] = Field(
        None,
        description="Types of profiling to perform: basic, statistical, pattern, correlation, distribution, anomaly"
    )
    sample_size: Optional[int] = Field(None, description="Sample size for profiling (if sampling)")
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "customer_data",
                "data_location": "s3://data-lake/customers/",
                "profile_types": ["basic", "statistical", "pattern"]
            }
        }


class ProfileResponse(BaseModel):
    """Profile response"""
    dataset_id: str
    timestamp: str
    row_count: int
    column_count: int
    memory_usage_mb: float
    profile_types: List[str]
    execution_time_ms: float
    columns: Dict[str, Any]
    correlations: Optional[Dict[str, Any]] = None
    patterns: Optional[Dict[str, Any]] = None
    recommendations: List[Dict[str, Any]]


class ColumnAnalysisRequest(BaseModel):
    """Column analysis request"""
    dataset_id: str
    column_name: str
    analysis_type: str = Field(
        "comprehensive",
        description="Type of analysis: comprehensive, distribution, pattern, anomaly"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "sales_data",
                "column_name": "revenue",
                "analysis_type": "distribution"
            }
        }


# API Endpoints

@profile_router.post("/analyze", response_model=ProfileResponse)
async def profile_dataset(
    request: ProfileRequest,
    background_tasks: BackgroundTasks,
    profiler: QualityProfiler = Depends(get_profiler)
) -> ProfileResponse:
    """
    Profile a dataset
    
    Performs comprehensive data profiling to understand data characteristics.
    """
    logger.info(f"profiling_dataset", dataset_id=request.dataset_id)
    
    try:
        # Load data if needed
        if request.data:
            data = request.data
        elif request.data_location:
            data = await profiler.load_data(request.data_location, sample_size=request.sample_size)
        else:
            raise HTTPException(status_code=400, detail="Either data or data_location must be provided")
        
        # Start profiling
        start_time = datetime.utcnow()
        
        # Perform profiling
        profile_result = await profiler.profile_dataset(
            dataset_id=request.dataset_id,
            data=data,
            profile_types=request.profile_types
        )
        
        # Calculate execution time
        execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Schedule background task to save profile
        background_tasks.add_task(
            profiler.save_profile,
            request.dataset_id,
            profile_result
        )
        
        return ProfileResponse(
            dataset_id=request.dataset_id,
            timestamp=profile_result["timestamp"],
            row_count=profile_result["row_count"],
            column_count=profile_result["column_count"],
            memory_usage_mb=profile_result["memory_usage"],
            profile_types=request.profile_types or ["basic", "statistical", "pattern"],
            execution_time_ms=execution_time_ms,
            columns=profile_result["columns"],
            correlations=profile_result.get("correlations"),
            patterns=profile_result.get("patterns"),
            recommendations=profile_result["recommendations"]
        )
        
    except Exception as e:
        logger.error(f"dataset_profiling_failed", error=str(e), dataset_id=request.dataset_id)
        raise HTTPException(status_code=500, detail=f"Profiling failed: {str(e)}")


@profile_router.get("/profile/{dataset_id}")
async def get_dataset_profile(
    dataset_id: str,
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Get existing dataset profile
    
    Retrieves a previously generated dataset profile.
    """
    logger.info(f"getting_dataset_profile", dataset_id=dataset_id)
    
    try:
        profile = await profiler.get_profile(dataset_id)
        
        if not profile:
            raise HTTPException(status_code=404, detail=f"No profile found for dataset {dataset_id}")
        
        return profile
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_get_profile", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get profile: {str(e)}")


@profile_router.post("/column/analyze")
async def analyze_column(
    request: ColumnAnalysisRequest,
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Analyze specific column
    
    Performs detailed analysis on a specific column.
    """
    logger.info(f"analyzing_column", dataset_id=request.dataset_id, column=request.column_name)
    
    try:
        # Get column data
        column_data = await profiler.get_column_data(request.dataset_id, request.column_name)
        
        if column_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"Column {request.column_name} not found in dataset {request.dataset_id}"
            )
        
        # Perform analysis
        if request.analysis_type == "distribution":
            result = await profiler.analyze_distribution(column_data, request.column_name)
        elif request.analysis_type == "pattern":
            result = await profiler.analyze_patterns(column_data, request.column_name)
        elif request.analysis_type == "anomaly":
            result = await profiler.detect_anomalies(column_data, request.column_name)
        else:  # comprehensive
            result = await profiler.analyze_column_comprehensive(
                column_data,
                request.column_name,
                request.dataset_id
            )
        
        return {
            "dataset_id": request.dataset_id,
            "column_name": request.column_name,
            "analysis_type": request.analysis_type,
            "timestamp": datetime.utcnow().isoformat(),
            "results": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"column_analysis_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Column analysis failed: {str(e)}")


@profile_router.get("/correlations/{dataset_id}")
async def get_correlations(
    dataset_id: str,
    threshold: float = Query(0.5, description="Minimum correlation threshold"),
    method: str = Query("pearson", description="Correlation method: pearson, spearman, kendall"),
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Get column correlations
    
    Analyzes correlations between numeric columns in the dataset.
    """
    logger.info(f"getting_correlations", dataset_id=dataset_id, method=method)
    
    try:
        correlations = await profiler.calculate_correlations(
            dataset_id=dataset_id,
            method=method,
            threshold=threshold
        )
        
        return {
            "dataset_id": dataset_id,
            "method": method,
            "threshold": threshold,
            "correlations": correlations,
            "strong_correlations": [
                corr for corr in correlations
                if abs(corr.get("correlation", 0)) >= threshold
            ]
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_correlations", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get correlations: {str(e)}")


@profile_router.get("/patterns/{dataset_id}")
async def get_data_patterns(
    dataset_id: str,
    column: Optional[str] = Query(None, description="Specific column to analyze"),
    pattern_type: str = Query("all", description="Pattern type: format, semantic, temporal, all"),
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Get data patterns
    
    Identifies patterns in the dataset.
    """
    logger.info(f"getting_data_patterns", dataset_id=dataset_id, pattern_type=pattern_type)
    
    try:
        patterns = await profiler.identify_patterns(
            dataset_id=dataset_id,
            column=column,
            pattern_type=pattern_type
        )
        
        return {
            "dataset_id": dataset_id,
            "column": column,
            "pattern_type": pattern_type,
            "patterns": patterns,
            "summary": {
                "total_patterns": len(patterns),
                "columns_analyzed": len(set(p.get("column") for p in patterns if p.get("column")))
            }
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_patterns", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get patterns: {str(e)}")


@profile_router.get("/distributions/{dataset_id}")
async def get_distributions(
    dataset_id: str,
    columns: Optional[List[str]] = Query(None, description="Specific columns to analyze"),
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Get data distributions
    
    Analyzes statistical distributions of numeric columns.
    """
    logger.info(f"getting_distributions", dataset_id=dataset_id)
    
    try:
        distributions = await profiler.analyze_distributions(
            dataset_id=dataset_id,
            columns=columns
        )
        
        return {
            "dataset_id": dataset_id,
            "columns_analyzed": columns or "all_numeric",
            "distributions": distributions,
            "summary": {
                "normal_distributions": sum(
                    1 for d in distributions.values()
                    if d.get("distribution_type") == "normal"
                ),
                "skewed_distributions": sum(
                    1 for d in distributions.values()
                    if abs(d.get("skewness", 0)) > 1
                )
            }
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_distributions", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get distributions: {str(e)}")


@profile_router.get("/semantic-types/{dataset_id}")
async def detect_semantic_types(
    dataset_id: str,
    confidence_threshold: float = Query(0.8, description="Minimum confidence for semantic type detection"),
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Detect semantic data types
    
    Identifies semantic types (email, phone, address, etc.) in the dataset.
    """
    logger.info(f"detecting_semantic_types", dataset_id=dataset_id)
    
    try:
        semantic_types = await profiler.detect_semantic_types(
            dataset_id=dataset_id,
            confidence_threshold=confidence_threshold
        )
        
        # Group by semantic type
        types_summary = {}
        for column, info in semantic_types.items():
            sem_type = info.get("semantic_type", "unknown")
            if sem_type not in types_summary:
                types_summary[sem_type] = []
            types_summary[sem_type].append(column)
        
        return {
            "dataset_id": dataset_id,
            "confidence_threshold": confidence_threshold,
            "semantic_types": semantic_types,
            "summary": types_summary,
            "pii_columns": [
                col for col, info in semantic_types.items()
                if info.get("is_pii", False)
            ]
        }
        
    except Exception as e:
        logger.error(f"failed_to_detect_semantic_types", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to detect semantic types: {str(e)}")


@profile_router.get("/recommendations/{dataset_id}")
async def get_quality_recommendations(
    dataset_id: str,
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Get quality improvement recommendations
    
    Provides recommendations based on profiling results.
    """
    logger.info(f"getting_quality_recommendations", dataset_id=dataset_id)
    
    try:
        # Get latest profile
        profile = await profiler.get_profile(dataset_id)
        
        if not profile:
            raise HTTPException(status_code=404, detail=f"No profile found for dataset {dataset_id}")
        
        # Generate recommendations
        recommendations = await profiler.generate_recommendations(profile)
        
        # Prioritize recommendations
        high_priority = [r for r in recommendations if r.get("priority") == "high"]
        medium_priority = [r for r in recommendations if r.get("priority") == "medium"]
        low_priority = [r for r in recommendations if r.get("priority") == "low"]
        
        return {
            "dataset_id": dataset_id,
            "total_recommendations": len(recommendations),
            "high_priority": high_priority,
            "medium_priority": medium_priority,
            "low_priority": low_priority,
            "estimated_quality_improvement": sum(
                r.get("estimated_improvement", 0) for r in recommendations
            )
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_get_recommendations", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get recommendations: {str(e)}")


@profile_router.post("/compare")
async def compare_datasets(
    dataset_ids: List[str] = Query(..., description="Dataset IDs to compare"),
    profiler: QualityProfiler = Depends(get_profiler)
) -> Dict[str, Any]:
    """
    Compare multiple datasets
    
    Compares profiles of multiple datasets to identify differences.
    """
    logger.info(f"comparing_datasets", dataset_ids=dataset_ids)
    
    if len(dataset_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 datasets required for comparison")
    
    try:
        comparison_result = await profiler.compare_datasets(dataset_ids)
        
        return {
            "datasets": dataset_ids,
            "comparison": comparison_result,
            "summary": {
                "schema_match": comparison_result.get("schema_match", False),
                "quality_delta": comparison_result.get("quality_delta", {}),
                "recommendations": comparison_result.get("recommendations", [])
            }
        }
        
    except Exception as e:
        logger.error(f"failed_to_compare_datasets", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to compare datasets: {str(e)}")


# Set profiler reference
def set_profiler(profiler: QualityProfiler):
    """Set global profiler reference"""
    global quality_profiler
    quality_profiler = profiler 