"""Orchestration API endpoints"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Request, Query, Body
from pydantic import BaseModel, Field
import structlog

from app.core.cognitive_orchestrator import WorkflowConfig, WorkflowStatus

logger = structlog.get_logger()
router = APIRouter()


class WorkflowRequest(BaseModel):
    """Workflow execution request"""
    name: str = Field(..., description="Workflow name")
    steps: List[Dict[str, Any]] = Field(..., description="Workflow steps")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Constraints")
    priority: int = Field(1, ge=1, le=10, description="Priority level")
    auto_optimize: bool = Field(True, description="Enable auto-optimization")


class PipelineOptimizationRequest(BaseModel):
    """Pipeline optimization request"""
    pipeline_config: Dict[str, Any] = Field(..., description="Pipeline configuration")
    optimize_for: str = Field("balanced", description="Optimization target: cost, performance, balanced")


@router.post("/execute")
async def execute_workflow(
    request: Request,
    workflow_request: WorkflowRequest
) -> Dict[str, Any]:
    """Execute a workflow with cognitive optimization"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Create workflow config
        workflow_config = WorkflowConfig(
            workflow_id=f"{workflow_request.name}_{workflow_request.priority}",
            name=workflow_request.name,
            steps=workflow_request.steps,
            constraints=workflow_request.constraints,
            priority=workflow_request.priority
        )
        
        # Execute with optimization
        if workflow_request.auto_optimize:
            execution = await orchestrator.execute_workflow(workflow_config)
        else:
            # Execute without optimization (not implemented in this example)
            execution = await orchestrator.execute_workflow(workflow_config)
            
        return {
            "execution_id": execution.execution_id,
            "status": execution.status.value,
            "start_time": execution.start_time.isoformat(),
            "end_time": execution.end_time.isoformat() if execution.end_time else None,
            "metrics": execution.metrics,
            "optimizations_applied": execution.optimizations_applied
        }
        
    except Exception as e:
        logger.error(f"Workflow execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize-pipeline")
async def optimize_pipeline(
    request: Request,
    optimization_request: PipelineOptimizationRequest
) -> Dict[str, Any]:
    """Optimize a pipeline configuration"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Optimize pipeline
        optimized_config = await orchestrator.auto_optimize_pipeline(
            optimization_request.pipeline_config
        )
        
        return {
            "original_config": optimization_request.pipeline_config,
            "optimized_config": optimized_config,
            "optimization_target": optimization_request.optimize_for
        }
        
    except Exception as e:
        logger.error(f"Pipeline optimization failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows")
async def list_workflows(
    request: Request,
    status: Optional[WorkflowStatus] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000)
) -> Dict[str, Any]:
    """List active and recent workflows"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Get active workflows
        active = list(orchestrator.active_workflows.values())
        
        # Get recent history
        history = orchestrator.workflow_history[-limit:]
        
        # Filter by status if requested
        if status:
            active = [w for w in active if w.status == status]
            history = [w for w in history if w.status == status]
            
        return {
            "active_workflows": [
                {
                    "execution_id": w.execution_id,
                    "workflow_id": w.workflow_id,
                    "status": w.status.value,
                    "start_time": w.start_time.isoformat()
                }
                for w in active
            ],
            "recent_workflows": [
                {
                    "execution_id": w.execution_id,
                    "workflow_id": w.workflow_id,
                    "status": w.status.value,
                    "start_time": w.start_time.isoformat(),
                    "end_time": w.end_time.isoformat() if w.end_time else None,
                    "metrics": w.metrics
                }
                for w in history
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to list workflows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{execution_id}")
async def get_workflow_details(
    request: Request,
    execution_id: str
) -> Dict[str, Any]:
    """Get detailed workflow execution information"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Check active workflows
        if execution_id in orchestrator.active_workflows:
            execution = orchestrator.active_workflows[execution_id]
        else:
            # Check history
            execution = next(
                (w for w in orchestrator.workflow_history if w.execution_id == execution_id),
                None
            )
            
        if not execution:
            raise HTTPException(status_code=404, detail="Workflow not found")
            
        return {
            "execution_id": execution.execution_id,
            "workflow_id": execution.workflow_id,
            "status": execution.status.value,
            "start_time": execution.start_time.isoformat(),
            "end_time": execution.end_time.isoformat() if execution.end_time else None,
            "duration": (execution.end_time - execution.start_time).total_seconds() if execution.end_time else None,
            "metrics": execution.metrics,
            "optimizations_applied": execution.optimizations_applied,
            "error": execution.error
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get workflow details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict-resources")
async def predict_resources(
    request: Request,
    workflow_name: str = Body(..., description="Workflow name"),
    time_horizon: int = Body(3600, description="Prediction horizon in seconds")
) -> Dict[str, Any]:
    """Predict resource needs for a workflow"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Create minimal workflow config for prediction
        workflow_config = WorkflowConfig(
            workflow_id=workflow_name,
            name=workflow_name,
            steps=[],  # Would be populated from workflow registry
            constraints={}
        )
        
        # Predict resources
        predictions = await orchestrator.predict_resource_needs(
            workflow_config,
            time_horizon
        )
        
        return predictions
        
    except Exception as e:
        logger.error(f"Resource prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 