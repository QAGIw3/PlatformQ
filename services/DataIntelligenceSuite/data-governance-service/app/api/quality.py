"""
Quality validation API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field

from platformq_shared.logging import get_logger
from ..core import QualityEngine

logger = get_logger(__name__)

# Create router
quality_router = APIRouter()

# Global reference to quality engine
quality_engine: Optional[QualityEngine] = None


def get_quality_engine() -> QualityEngine:
    """Get quality engine instance"""
    if quality_engine is None:
        raise HTTPException(status_code=503, detail="Quality engine not initialized")
    return quality_engine


# Request/Response models

class QualityCheckRequest(BaseModel):
    """Quality check request"""
    dataset_id: str = Field(..., description="Dataset identifier")
    data_location: Optional[str] = Field(None, description="Data location (S3, file path, etc.)")
    data: Optional[Dict[str, Any]] = Field(None, description="Inline data for validation")
    dimensions: Optional[List[str]] = Field(None, description="Specific dimensions to check")
    rules: Optional[List[Dict[str, Any]]] = Field(None, description="Custom rules to apply")
    mode: str = Field("comprehensive", description="Validation mode: comprehensive, quick, custom")
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "sales_data_2024",
                "data_location": "s3://data-lake/sales/2024/",
                "dimensions": ["completeness", "accuracy", "consistency"],
                "mode": "comprehensive"
            }
        }


class RuleValidationRequest(BaseModel):
    """Rule validation request"""
    dataset_id: str
    rule: Dict[str, Any]
    sample_size: Optional[int] = Field(100, description="Sample size for validation")
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "customer_data",
                "rule": {
                    "name": "email_format",
                    "type": "regex",
                    "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
                    "column": "email"
                }
            }
        }


class QualityScoreResponse(BaseModel):
    """Quality score response"""
    dataset_id: str
    timestamp: str
    overall_score: float
    dimension_scores: Dict[str, float]
    issues_found: int
    critical_issues: int
    validation_time_ms: float
    details: Optional[Dict[str, Any]] = None


# API Endpoints

@quality_router.post("/validate", response_model=QualityScoreResponse)
async def validate_dataset(
    request: QualityCheckRequest,
    background_tasks: BackgroundTasks,
    engine: QualityEngine = Depends(get_quality_engine)
) -> QualityScoreResponse:
    """
    Validate dataset quality
    
    Performs comprehensive or targeted quality validation on a dataset.
    """
    logger.info(f"validating_dataset", dataset_id=request.dataset_id, mode=request.mode)
    
    try:
        # Load data if needed
        if request.data:
            # Use inline data
            data = request.data
        elif request.data_location:
            # Load from location
            data = await engine.load_data(request.data_location)
        else:
            raise HTTPException(status_code=400, detail="Either data or data_location must be provided")
        
        # Start validation
        start_time = datetime.utcnow()
        
        # Run validation based on mode
        if request.mode == "comprehensive":
            result = await engine.validate_comprehensive(
                dataset_id=request.dataset_id,
                data=data,
                custom_rules=request.rules
            )
        elif request.mode == "quick":
            result = await engine.validate_quick(
                dataset_id=request.dataset_id,
                data=data,
                dimensions=request.dimensions or ["completeness", "validity"]
            )
        elif request.mode == "custom":
            if not request.rules:
                raise HTTPException(status_code=400, detail="Custom mode requires rules")
            result = await engine.validate_custom(
                dataset_id=request.dataset_id,
                data=data,
                rules=request.rules
            )
        else:
            raise HTTPException(status_code=400, detail=f"Invalid mode: {request.mode}")
        
        # Calculate validation time
        validation_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Schedule background quality trend update
        background_tasks.add_task(
            engine.update_quality_trends,
            request.dataset_id,
            result
        )
        
        return QualityScoreResponse(
            dataset_id=request.dataset_id,
            timestamp=datetime.utcnow().isoformat(),
            overall_score=result.get("overall_score", 0),
            dimension_scores=result.get("dimension_scores", {}),
            issues_found=result.get("total_issues", 0),
            critical_issues=result.get("critical_issues", 0),
            validation_time_ms=validation_time_ms,
            details=result.get("details")
        )
        
    except Exception as e:
        logger.error(f"dataset_validation_failed", error=str(e), dataset_id=request.dataset_id)
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


@quality_router.post("/rules/validate")
async def validate_rule(
    request: RuleValidationRequest,
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Validate a quality rule
    
    Tests a quality rule against sample data to ensure it works correctly.
    """
    logger.info(f"validating_rule", dataset_id=request.dataset_id, rule_name=request.rule.get("name"))
    
    try:
        # Validate rule syntax
        validation_result = await engine.validate_rule_syntax(request.rule)
        if not validation_result["valid"]:
            return {
                "valid": False,
                "errors": validation_result["errors"],
                "warnings": validation_result.get("warnings", [])
            }
        
        # Test rule on sample data
        test_result = await engine.test_rule(
            dataset_id=request.dataset_id,
            rule=request.rule,
            sample_size=request.sample_size
        )
        
        return {
            "valid": True,
            "rule": request.rule,
            "test_results": test_result,
            "performance_metrics": {
                "execution_time_ms": test_result.get("execution_time_ms", 0),
                "rows_processed": test_result.get("rows_processed", 0),
                "issues_found": test_result.get("issues_found", 0)
            }
        }
        
    except Exception as e:
        logger.error(f"rule_validation_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Rule validation failed: {str(e)}")


@quality_router.get("/score/{dataset_id}")
async def get_quality_score(
    dataset_id: str,
    dimension: Optional[str] = Query(None, description="Specific dimension to query"),
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Get current quality score for a dataset
    
    Retrieves the latest quality score and metrics for a dataset.
    """
    logger.info(f"getting_quality_score", dataset_id=dataset_id, dimension=dimension)
    
    try:
        # Get latest score
        score = await engine.get_quality_score(dataset_id, dimension)
        
        if not score:
            raise HTTPException(status_code=404, detail=f"No quality score found for dataset {dataset_id}")
        
        return score
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_get_quality_score", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get quality score: {str(e)}")


@quality_router.get("/history/{dataset_id}")
async def get_quality_history(
    dataset_id: str,
    days: int = Query(7, description="Number of days of history"),
    dimension: Optional[str] = Query(None, description="Specific dimension to query"),
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Get quality score history
    
    Retrieves historical quality scores and trends for a dataset.
    """
    logger.info(f"getting_quality_history", dataset_id=dataset_id, days=days)
    
    try:
        history = await engine.get_quality_history(
            dataset_id=dataset_id,
            days=days,
            dimension=dimension
        )
        
        return {
            "dataset_id": dataset_id,
            "dimension": dimension,
            "days": days,
            "history": history,
            "trend": await engine.calculate_quality_trend(history) if history else None
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_quality_history", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get quality history: {str(e)}")


@quality_router.get("/issues/{dataset_id}")
async def get_quality_issues(
    dataset_id: str,
    severity: Optional[str] = Query(None, description="Filter by severity: critical, high, medium, low"),
    dimension: Optional[str] = Query(None, description="Filter by dimension"),
    limit: int = Query(100, description="Maximum number of issues to return"),
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Get quality issues for a dataset
    
    Retrieves detailed quality issues found during validation.
    """
    logger.info(f"getting_quality_issues", dataset_id=dataset_id, severity=severity)
    
    try:
        issues = await engine.get_quality_issues(
            dataset_id=dataset_id,
            severity=severity,
            dimension=dimension,
            limit=limit
        )
        
        return {
            "dataset_id": dataset_id,
            "total_issues": len(issues),
            "filters": {
                "severity": severity,
                "dimension": dimension
            },
            "issues": issues
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_quality_issues", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get quality issues: {str(e)}")


@quality_router.post("/rules")
async def create_quality_rule(
    rule: Dict[str, Any],
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Create a new quality rule
    
    Adds a new quality validation rule to the system.
    """
    logger.info(f"creating_quality_rule", rule_name=rule.get("name"))
    
    try:
        # Validate rule
        validation = await engine.validate_rule_syntax(rule)
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=f"Invalid rule: {validation['errors']}")
        
        # Create rule
        created_rule = await engine.create_rule(rule)
        
        return {
            "status": "created",
            "rule": created_rule
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_create_rule", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to create rule: {str(e)}")


@quality_router.get("/rules")
async def list_quality_rules(
    dimension: Optional[str] = Query(None, description="Filter by dimension"),
    active: Optional[bool] = Query(None, description="Filter by active status"),
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    List quality rules
    
    Retrieves all quality validation rules in the system.
    """
    logger.info(f"listing_quality_rules", dimension=dimension, active=active)
    
    try:
        rules = await engine.list_rules(dimension=dimension, active=active)
        
        return {
            "total": len(rules),
            "filters": {
                "dimension": dimension,
                "active": active
            },
            "rules": rules
        }
        
    except Exception as e:
        logger.error(f"failed_to_list_rules", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list rules: {str(e)}")


@quality_router.put("/rules/{rule_id}")
async def update_quality_rule(
    rule_id: str,
    rule_update: Dict[str, Any],
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Update a quality rule
    
    Updates an existing quality validation rule.
    """
    logger.info(f"updating_quality_rule", rule_id=rule_id)
    
    try:
        # Validate update
        if "id" in rule_update and rule_update["id"] != rule_id:
            raise HTTPException(status_code=400, detail="Rule ID cannot be changed")
        
        # Update rule
        updated_rule = await engine.update_rule(rule_id, rule_update)
        
        return {
            "status": "updated",
            "rule": updated_rule
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_update_rule", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to update rule: {str(e)}")


@quality_router.delete("/rules/{rule_id}")
async def delete_quality_rule(
    rule_id: str,
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Delete a quality rule
    
    Deletes a quality validation rule from the system.
    """
    logger.info(f"deleting_quality_rule", rule_id=rule_id)
    
    try:
        # Delete rule
        await engine.delete_rule(rule_id)
        
        return {
            "status": "deleted",
            "rule_id": rule_id
        }
        
    except Exception as e:
        logger.error(f"failed_to_delete_rule", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to delete rule: {str(e)}")


@quality_router.get("/thresholds/{dataset_id}")
async def get_quality_thresholds(
    dataset_id: str,
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Get quality thresholds for a dataset
    
    Retrieves configured quality thresholds for validation.
    """
    logger.info(f"getting_quality_thresholds", dataset_id=dataset_id)
    
    try:
        thresholds = await engine.get_quality_thresholds(dataset_id)
        
        return {
            "dataset_id": dataset_id,
            "thresholds": thresholds,
            "defaults": await engine.get_default_thresholds()
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_thresholds", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get thresholds: {str(e)}")


@quality_router.put("/thresholds/{dataset_id}")
async def update_quality_thresholds(
    dataset_id: str,
    thresholds: Dict[str, float],
    engine: QualityEngine = Depends(get_quality_engine)
) -> Dict[str, Any]:
    """
    Update quality thresholds
    
    Updates quality validation thresholds for a dataset.
    """
    logger.info(f"updating_quality_thresholds", dataset_id=dataset_id)
    
    try:
        # Validate thresholds
        for dimension, value in thresholds.items():
            if not 0 <= value <= 1:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid threshold for {dimension}: must be between 0 and 1"
                )
        
        # Update thresholds
        updated = await engine.update_thresholds(dataset_id, thresholds)
        
        return {
            "status": "updated",
            "dataset_id": dataset_id,
            "thresholds": updated
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_update_thresholds", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to update thresholds: {str(e)}")


# Set quality engine reference
def set_quality_engine(engine: QualityEngine):
    """Set global quality engine reference"""
    global quality_engine
    quality_engine = engine 