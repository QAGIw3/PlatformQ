"""
Remediation API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field

from platformq_shared.logging import get_logger
from ..core import RemediationOrchestrator, RemediationMode, RemediationStatus

logger = get_logger(__name__)

# Create router
remediation_router = APIRouter()

# Global reference
remediation_orchestrator: Optional[RemediationOrchestrator] = None


def get_orchestrator() -> RemediationOrchestrator:
    """Get remediation orchestrator instance"""
    if remediation_orchestrator is None:
        raise HTTPException(status_code=503, detail="Remediation orchestrator not initialized")
    return remediation_orchestrator


# Request/Response models

class RemediationPlanRequest(BaseModel):
    """Remediation plan request"""
    dataset_id: str = Field(..., description="Dataset identifier")
    quality_issues: List[Dict[str, Any]] = Field(..., description="Quality issues to remediate")
    mode: str = Field("supervised", description="Remediation mode: automatic, supervised, manual, simulation")
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "customer_data",
                "quality_issues": [
                    {
                        "dimension": "completeness",
                        "column": "email",
                        "null_count": 150,
                        "null_ratio": 0.15,
                        "severity": "high"
                    }
                ],
                "mode": "supervised"
            }
        }


class RemediationExecuteRequest(BaseModel):
    """Remediation execution request"""
    plan_id: str = Field(..., description="Remediation plan ID")
    executor_id: Optional[str] = Field(None, description="ID of user/system executing the plan")
    
    class Config:
        schema_extra = {
            "example": {
                "plan_id": "plan_customer_data_1234567890",
                "executor_id": "user_123"
            }
        }


class RemediationSimulationRequest(BaseModel):
    """Remediation simulation request"""
    dataset_id: str
    quality_issues: List[Dict[str, Any]]
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "sales_data",
                "quality_issues": [
                    {
                        "dimension": "accuracy",
                        "issue_type": "outlier",
                        "column": "revenue",
                        "outlier_count": 25
                    }
                ]
            }
        }


class MLOptimizationRequest(BaseModel):
    """ML optimization request"""
    dataset_id: str
    optimization_type: str = Field(..., description="Type: rule_selection, threshold_tuning, pipeline_optimization")
    mode: str = Field("balanced", description="Mode: accuracy, performance, balanced, cost_optimized")
    current_config: Dict[str, Any] = Field(..., description="Current configuration to optimize")
    
    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "product_data",
                "optimization_type": "rule_selection",
                "mode": "performance",
                "current_config": {
                    "rules": [{"name": "price_check", "type": "range", "min": 0, "max": 10000}]
                }
            }
        }


# API Endpoints

@remediation_router.post("/plan")
async def create_remediation_plan(
    request: RemediationPlanRequest,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Create remediation plan
    
    Analyzes quality issues and creates an intelligent remediation plan.
    """
    logger.info(f"creating_remediation_plan", dataset_id=request.dataset_id, issue_count=len(request.quality_issues))
    
    try:
        # Convert mode string to enum
        mode = RemediationMode(request.mode)
        
        # Create plan
        plan = await orchestrator.create_remediation_plan(
            dataset_id=request.dataset_id,
            quality_issues=request.quality_issues,
            mode=mode
        )
        
        return {
            "plan_id": plan.plan_id,
            "dataset_id": plan.dataset_id,
            "mode": plan.mode,
            "priority": plan.priority,
            "issue_count": len(plan.issues),
            "action_count": len(plan.actions),
            "estimated_duration_seconds": plan.estimated_duration,
            "impact_assessment": plan.impact_assessment,
            "actions": plan.actions,
            "requires_approval": mode != RemediationMode.AUTOMATIC
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_create_remediation_plan", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to create remediation plan: {str(e)}")


@remediation_router.post("/execute")
async def execute_remediation(
    request: RemediationExecuteRequest,
    background_tasks: BackgroundTasks,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Execute remediation plan
    
    Executes a previously created remediation plan.
    """
    logger.info(f"executing_remediation_plan", plan_id=request.plan_id)
    
    try:
        # Execute remediation
        remediation_id = await orchestrator.execute_remediation(
            plan_id=request.plan_id,
            executor_id=request.executor_id
        )
        
        return {
            "remediation_id": remediation_id,
            "plan_id": request.plan_id,
            "status": "started",
            "message": "Remediation execution started. Use the status endpoint to track progress."
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_execute_remediation", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to execute remediation: {str(e)}")


@remediation_router.get("/status/{remediation_id}")
async def get_remediation_status(
    remediation_id: str,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Get remediation status
    
    Retrieves the current status of a remediation execution.
    """
    logger.info(f"getting_remediation_status", remediation_id=remediation_id)
    
    try:
        result = await orchestrator.get_remediation_status(remediation_id)
        
        # Calculate progress
        progress = 0
        if result.status == RemediationStatus.COMPLETED:
            progress = 100
        elif result.status == RemediationStatus.EXECUTING:
            total_actions = len(result.actions_executed) + result.issues_remaining
            if total_actions > 0:
                progress = int((len(result.actions_executed) / total_actions) * 100)
        
        return {
            "remediation_id": result.remediation_id,
            "plan_id": result.plan_id,
            "status": result.status,
            "progress_percentage": progress,
            "start_time": result.start_time.isoformat(),
            "end_time": result.end_time.isoformat() if result.end_time else None,
            "actions_executed": len(result.actions_executed),
            "rows_affected": result.rows_affected,
            "issues_resolved": result.issues_resolved,
            "issues_remaining": result.issues_remaining,
            "validation_results": result.validation_results,
            "rollback_available": result.rollback_available,
            "error_details": result.error_details
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_get_remediation_status", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get remediation status: {str(e)}")


@remediation_router.post("/simulate")
async def simulate_remediation(
    request: RemediationSimulationRequest,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Simulate remediation
    
    Simulates remediation without applying changes to understand impact.
    """
    logger.info(f"simulating_remediation", dataset_id=request.dataset_id)
    
    try:
        simulation_result = await orchestrator.simulate_remediation(
            dataset_id=request.dataset_id,
            quality_issues=request.quality_issues
        )
        
        return simulation_result
        
    except Exception as e:
        logger.error(f"remediation_simulation_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Remediation simulation failed: {str(e)}")


@remediation_router.post("/rollback/{remediation_id}")
async def rollback_remediation(
    remediation_id: str,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Rollback remediation
    
    Rolls back a completed remediation to restore original state.
    """
    logger.info(f"rolling_back_remediation", remediation_id=remediation_id)
    
    try:
        rollback_result = await orchestrator.rollback_remediation(remediation_id)
        
        return {
            "remediation_id": remediation_id,
            "rollback_status": "completed",
            "rollback_result": rollback_result
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"remediation_rollback_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Remediation rollback failed: {str(e)}")


@remediation_router.get("/history")
async def get_remediation_history(
    dataset_id: Optional[str] = Query(None, description="Filter by dataset ID"),
    limit: int = Query(100, description="Maximum number of results"),
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Get remediation history
    
    Retrieves history of remediation executions.
    """
    logger.info(f"getting_remediation_history", dataset_id=dataset_id, limit=limit)
    
    try:
        history = await orchestrator.get_remediation_history(
            dataset_id=dataset_id,
            limit=limit
        )
        
        # Summarize by status
        status_summary = {}
        for entry in history:
            status = entry.get("status", "unknown")
            status_summary[status] = status_summary.get(status, 0) + 1
        
        return {
            "total": len(history),
            "dataset_id": dataset_id,
            "status_summary": status_summary,
            "history": history
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_remediation_history", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get remediation history: {str(e)}")


@remediation_router.get("/plans/{plan_id}")
async def get_remediation_plan(
    plan_id: str,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Get remediation plan details
    
    Retrieves details of a specific remediation plan.
    """
    logger.info(f"getting_remediation_plan", plan_id=plan_id)
    
    try:
        plan = orchestrator.plans.get(plan_id)
        
        if not plan:
            raise HTTPException(status_code=404, detail=f"Remediation plan {plan_id} not found")
        
        return {
            "plan_id": plan.plan_id,
            "dataset_id": plan.dataset_id,
            "created_at": plan.created_at.isoformat(),
            "mode": plan.mode,
            "priority": plan.priority,
            "issue_count": len(plan.issues),
            "action_count": len(plan.actions),
            "estimated_duration_seconds": plan.estimated_duration,
            "impact_assessment": plan.impact_assessment,
            "rollback_strategy": plan.rollback_strategy,
            "approved": plan.approved,
            "approval_timestamp": plan.approval_timestamp.isoformat() if plan.approval_timestamp else None,
            "approver": plan.approver,
            "actions": plan.actions
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_get_remediation_plan", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get remediation plan: {str(e)}")


# ML Optimization endpoints

@remediation_router.post("/optimize")
async def optimize_configuration(
    request: MLOptimizationRequest,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Optimize configuration using ML
    
    Uses ML to optimize quality rules, thresholds, or pipelines.
    """
    logger.info(f"optimizing_configuration", dataset_id=request.dataset_id, type=request.optimization_type)
    
    try:
        ml_optimizer = orchestrator.ml_optimizer
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        # Perform optimization based on type
        if request.optimization_type == "rule_selection":
            result = await ml_optimizer.optimize_rules(
                dataset_id=request.dataset_id,
                current_rules=request.current_config.get("rules", []),
                quality_metrics=request.current_config.get("quality_metrics", {}),
                mode=request.mode
            )
        elif request.optimization_type == "threshold_tuning":
            # Get historical data for threshold optimization
            historical_data = request.current_config.get("historical_data", {})
            result = await ml_optimizer.optimize_thresholds(
                quality_dimensions=request.current_config.get("quality_dimensions", {}),
                historical_data=historical_data,
                mode=request.mode
            )
        elif request.optimization_type == "pipeline_optimization":
            result = await ml_optimizer.optimize_pipeline(
                pipeline_config=request.current_config.get("pipeline", {}),
                performance_data=request.current_config.get("performance_data", {}),
                mode=request.mode
            )
        else:
            raise HTTPException(status_code=400, detail=f"Invalid optimization type: {request.optimization_type}")
        
        return {
            "optimization_id": result.optimization_id,
            "type": result.type,
            "mode": result.mode,
            "original_config": result.original_config,
            "optimized_config": result.optimized_config,
            "improvement_metrics": result.improvement_metrics,
            "confidence_score": result.confidence_score,
            "timestamp": result.timestamp.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"optimization_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Optimization failed: {str(e)}")


@remediation_router.post("/optimize/apply/{optimization_id}")
async def apply_optimization(
    optimization_id: str,
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Apply ML optimization
    
    Applies a previously generated ML optimization.
    """
    logger.info(f"applying_optimization", optimization_id=optimization_id)
    
    try:
        ml_optimizer = orchestrator.ml_optimizer
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        result = await ml_optimizer.apply_optimization(optimization_id)
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_apply_optimization", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to apply optimization: {str(e)}")


@remediation_router.get("/optimize/history")
async def get_optimization_history(
    limit: int = Query(100, description="Maximum number of results"),
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Get optimization history
    
    Retrieves history of ML optimizations.
    """
    logger.info(f"getting_optimization_history", limit=limit)
    
    try:
        ml_optimizer = orchestrator.ml_optimizer
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        history = await ml_optimizer.get_optimization_history(limit=limit)
        
        return {
            "total": len(history),
            "history": history
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_optimization_history", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get optimization history: {str(e)}")


@remediation_router.post("/anomaly/detect")
async def detect_anomalies(
    dataset_id: str = Query(..., description="Dataset ID"),
    methods: Optional[List[str]] = Query(None, description="Detection methods to use"),
    sensitivity: float = Query(0.95, description="Detection sensitivity (0-1)"),
    auto_remediate: bool = Query(False, description="Automatically remediate detected anomalies"),
    orchestrator: RemediationOrchestrator = Depends(get_orchestrator)
) -> Dict[str, Any]:
    """
    Detect anomalies
    
    Uses ML to detect anomalies in the dataset.
    """
    logger.info(f"detecting_anomalies", dataset_id=dataset_id, auto_remediate=auto_remediate)
    
    try:
        # Get anomaly detector through quality engine
        quality_engine = orchestrator.quality_engine
        if not hasattr(quality_engine, 'anomaly_detector'):
            raise HTTPException(status_code=503, detail="Anomaly detector not available")
        
        # Load dataset
        data = await quality_engine.load_data(dataset_id)
        
        # Detect anomalies
        detection_result = await quality_engine.anomaly_detector.detect_anomalies(
            dataset_id=dataset_id,
            data=data,
            methods=methods,
            sensitivity=sensitivity,
            auto_remediate=auto_remediate
        )
        
        return detection_result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"anomaly_detection_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Anomaly detection failed: {str(e)}")


# Set orchestrator reference
def set_orchestrator(orchestrator: RemediationOrchestrator):
    """Set global orchestrator reference"""
    global remediation_orchestrator
    remediation_orchestrator = orchestrator 