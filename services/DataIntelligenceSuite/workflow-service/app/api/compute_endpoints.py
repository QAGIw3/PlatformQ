"""
Compute Orchestration API Endpoints for Workflow Service
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from ..api.deps import get_current_tenant_and_user
from ..compute_orchestration import WorkflowComputeOrchestrator

router = APIRouter()


class WorkflowResourceEstimateRequest(BaseModel):
    """Request to estimate workflow resources"""
    dag_id: str = Field(..., description="Airflow DAG ID")
    workflow_config: Dict[str, Any] = Field(default_factory=dict, description="Workflow configuration")


class WorkflowAllocationRequest(BaseModel):
    """Request to allocate compute for workflow"""
    dag_id: str = Field(..., description="Airflow DAG ID")
    workflow_config: Dict[str, Any] = Field(default_factory=dict, description="Workflow configuration")


class RecurringWorkflowOptimizationRequest(BaseModel):
    """Request to optimize recurring workflow"""
    dag_id: str = Field(..., description="Airflow DAG ID")
    schedule: str = Field(..., description="Cron schedule expression")
    duration_days: int = Field(30, description="Duration to optimize for")
    workflow_config: Dict[str, Any] = Field(default_factory=dict, description="Workflow configuration")


class WorkflowHedgeRequest(BaseModel):
    """Request to create cost hedge for workflow"""
    workflow_id: str = Field(..., description="Workflow ID")
    estimated_cost: float = Field(..., description="Estimated cost in USD")
    hedge_percentage: float = Field(0.5, description="Percentage to hedge (0-1)")


@router.post("/workflows/{workflow_id}/estimate-resources", response_model=Dict[str, Any])
async def estimate_workflow_resources(
    workflow_id: str,
    request: WorkflowResourceEstimateRequest,
    compute_orchestrator: WorkflowComputeOrchestrator = Depends(lambda: router.app.state.compute_orchestrator),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Estimate compute resources needed for workflow execution"""
    try:
        # Add workflow ID to config
        config = request.workflow_config.copy()
        config["workflow_id"] = workflow_id
        
        estimate = await compute_orchestrator.estimate_workflow_resources(
            dag_id=request.dag_id,
            workflow_config=config
        )
        
        return {
            "workflow_id": estimate.workflow_id,
            "dag_id": estimate.dag_id,
            "total_tasks": estimate.total_tasks,
            "task_estimates": estimate.task_estimates,
            "total_gpu_hours": estimate.total_gpu_hours,
            "total_cpu_hours": estimate.total_cpu_hours,
            "total_memory_gb_hours": estimate.total_memory_gb_hours,
            "estimated_duration": estimate.estimated_duration.total_seconds(),
            "critical_path_tasks": estimate.critical_path_tasks,
            "parallelism_factor": estimate.parallelism_factor,
            "resource_profile": estimate.get_resource_profile().value
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/{workflow_id}/allocate-compute", response_model=Dict[str, Any])
async def allocate_workflow_compute(
    workflow_id: str,
    request: WorkflowAllocationRequest,
    compute_orchestrator: WorkflowComputeOrchestrator = Depends(lambda: router.app.state.compute_orchestrator),
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Allocate compute resources for workflow execution"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Add workflow ID to config
        config = request.workflow_config.copy()
        config["workflow_id"] = workflow_id
        
        result = await compute_orchestrator.allocate_workflow_compute(
            dag_id=request.dag_id,
            workflow_config=config,
            tenant_id=tenant_id,
            user_id=user_id
        )
        
        if not result.success:
            raise HTTPException(
                status_code=400,
                detail="Failed to allocate compute resources"
            )
            
        # Trigger workflow execution in background
        background_tasks.add_task(
            trigger_workflow_with_compute,
            workflow_id=workflow_id,
            dag_id=request.dag_id,
            allocation_result=result
        )
        
        return {
            "success": result.success,
            "workflow_id": result.workflow_id,
            "dag_id": result.dag_id,
            "allocation_strategy": result.allocation_strategy,
            "task_allocations": result.task_allocations,
            "total_cost": float(result.total_cost),
            "contracts": result.contracts,
            "estimated_start_time": result.estimated_start_time.isoformat(),
            "estimated_completion_time": result.estimated_completion_time.isoformat(),
            "optimization_notes": result.optimization_notes
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/optimize-recurring", response_model=Dict[str, Any])
async def optimize_recurring_workflow(
    request: RecurringWorkflowOptimizationRequest,
    compute_orchestrator: WorkflowComputeOrchestrator = Depends(lambda: router.app.state.compute_orchestrator),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Optimize resource allocation for recurring workflows"""
    tenant_id = context["tenant_id"]
    
    try:
        recommendations = await compute_orchestrator.optimize_recurring_workflow(
            dag_id=request.dag_id,
            schedule=request.schedule,
            duration_days=request.duration_days,
            workflow_config=request.workflow_config,
            tenant_id=tenant_id
        )
        
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/{workflow_id}/create-hedge", response_model=Dict[str, Any])
async def create_workflow_hedge(
    workflow_id: str,
    request: WorkflowHedgeRequest,
    compute_orchestrator: WorkflowComputeOrchestrator = Depends(lambda: router.app.state.compute_orchestrator),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create cost hedge for workflow using options"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        hedge_result = await compute_orchestrator.create_workflow_hedge(
            workflow_id=workflow_id,
            estimated_cost=Decimal(str(request.estimated_cost)),
            hedge_percentage=request.hedge_percentage,
            tenant_id=tenant_id,
            user_id=user_id
        )
        
        return hedge_result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compute/workflow-pricing", response_model=Dict[str, Any])
async def get_workflow_compute_pricing():
    """Get current compute pricing for workflow planning"""
    try:
        # This would fetch from derivatives engine
        # For now, return sample pricing
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "resource_types": {
                "GPU_T4": {
                    "spot_price": 25.0,
                    "futures_price": 28.0,
                    "reserved_price": 22.0,
                    "description": "Entry-level GPU for light ML tasks"
                },
                "GPU_V100": {
                    "spot_price": 35.0,
                    "futures_price": 38.0,
                    "reserved_price": 32.0,
                    "description": "Mid-range GPU for moderate workloads"
                },
                "GPU_A100": {
                    "spot_price": 45.0,
                    "futures_price": 48.0,
                    "reserved_price": 40.0,
                    "description": "High-end GPU for intensive computing"
                }
            },
            "discounts": {
                "volume_10_hours": "5%",
                "volume_100_hours": "10%",
                "volume_1000_hours": "15%",
                "commitment_30_days": "20%"
            },
            "recommendations": {
                "data_processing": "Use spot instances with T4 GPUs",
                "ml_training": "Reserve V100 GPUs for predictable costs",
                "batch_analytics": "Mix spot and futures for flexibility",
                "production_workflows": "Use reserved capacity with SLA"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Background task helper
async def trigger_workflow_with_compute(
    workflow_id: str,
    dag_id: str,
    allocation_result: Any
):
    """Trigger Airflow DAG with compute allocation details"""
    try:
        # This would integrate with Airflow API
        # Pass allocation details as DAG run configuration
        logger.info(f"Triggering workflow {workflow_id} with compute allocation")
        
    except Exception as e:
        logger.error(f"Failed to trigger workflow: {e}") 