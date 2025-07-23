"""
Workflows API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class WorkflowCreateRequest(BaseModel):
    """Workflow creation request"""
    name: str = Field(..., description="Workflow name")
    type: str = Field(..., description="Workflow type")
    description: str = Field("", description="Workflow description")
    steps: List[Dict[str, Any]] = Field(..., description="Workflow steps")
    schedule: str = Field(None, description="Cron schedule")
    retry_policy: Dict[str, Any] = Field(default={}, description="Retry policy")


class WorkflowTriggerRequest(BaseModel):
    """Workflow trigger request"""
    context: Dict[str, Any] = Field(default={}, description="Execution context")


class WorkflowUpdateRequest(BaseModel):
    """Workflow update request"""
    description: str = Field(None, description="Updated description")
    steps: List[Dict[str, Any]] = Field(None, description="Updated steps")
    schedule: str = Field(None, description="Updated schedule")
    retry_policy: Dict[str, Any] = Field(None, description="Updated retry policy")


@router.post("/workflows", response_model=Dict[str, str])
async def create_workflow(request: WorkflowCreateRequest) -> Dict[str, str]:
    """Create a new workflow"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        workflow_id = await workflow_manager.create_workflow(request.dict())
        
        return {
            "workflow_id": workflow_id,
            "status": "created",
            "message": "Workflow created successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/workflows", response_model=List[Dict[str, Any]])
async def list_workflows(
    status: str = Query(None, description="Filter by status"),
    limit: int = Query(100, description="Maximum results")
) -> List[Dict[str, Any]]:
    """List all workflows"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        workflows = []
        for workflow_id, workflow in workflow_manager.workflows.items():
            if status and workflow["status"] != status:
                continue
            
            workflows.append({
                "id": workflow_id,
                "name": workflow["config"]["name"],
                "type": workflow["config"]["type"],
                "status": workflow["status"],
                "created_at": workflow["created_at"].isoformat(),
                "version": workflow["version"]
            })
        
        return workflows[:limit]
        
    except Exception as e:
        logger.error(f"Error listing workflows: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/workflows/{workflow_id}")
async def get_workflow(workflow_id: str) -> Dict[str, Any]:
    """Get workflow details"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        status = await workflow_manager.get_workflow_status(workflow_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/workflows/{workflow_id}/trigger")
async def trigger_workflow(workflow_id: str, request: WorkflowTriggerRequest) -> Dict[str, str]:
    """Trigger workflow execution"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        run_id = await workflow_manager.trigger_workflow(workflow_id, request.context)
        
        return {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "status": "triggered",
            "message": "Workflow triggered successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error triggering workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/workflows/{workflow_id}/runs")
async def get_workflow_runs(
    workflow_id: str,
    limit: int = Query(10, description="Maximum results")
) -> List[Dict[str, Any]]:
    """Get workflow execution history"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        workflow = workflow_manager.workflows.get(workflow_id)
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        runs = []
        for run_id in workflow["runs"][-limit:]:
            run = workflow_manager.active_runs.get(run_id)
            if run:
                runs.append({
                    "id": run_id,
                    "status": run["status"].value,
                    "started_at": run["started_at"].isoformat(),
                    "completed_at": run["completed_at"].isoformat() if run["completed_at"] else None
                })
        
        return runs
        
    except Exception as e:
        logger.error(f"Error getting workflow runs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/runs/{run_id}")
async def get_run_status(run_id: str) -> Dict[str, Any]:
    """Get workflow run status"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        status = await workflow_manager.get_run_status(run_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting run status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/runs/{run_id}")
async def cancel_run(run_id: str) -> Dict[str, Any]:
    """Cancel workflow run"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        success = await workflow_manager.cancel_workflow(run_id)
        
        return {
            "run_id": run_id,
            "cancelled": success,
            "message": "Run cancelled successfully" if success else "Run could not be cancelled"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling run: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.patch("/workflows/{workflow_id}")
async def update_workflow(workflow_id: str, request: WorkflowUpdateRequest) -> Dict[str, Any]:
    """Update workflow configuration"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        updates = {k: v for k, v in request.dict().items() if v is not None}
        result = await workflow_manager.update_workflow(workflow_id, updates)
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/workflows/{workflow_id}/pause")
async def pause_workflow(workflow_id: str) -> Dict[str, Any]:
    """Pause workflow (disable scheduling)"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        success = await workflow_manager.pause_workflow(workflow_id)
        
        return {
            "workflow_id": workflow_id,
            "paused": success,
            "message": "Workflow paused successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error pausing workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/workflows/{workflow_id}/resume")
async def resume_workflow(workflow_id: str) -> Dict[str, Any]:
    """Resume workflow (enable scheduling)"""
    try:
        from ..main import workflow_manager
        
        if not workflow_manager:
            raise HTTPException(status_code=503, detail="Workflow manager not available")
        
        success = await workflow_manager.resume_workflow(workflow_id)
        
        return {
            "workflow_id": workflow_id,
            "resumed": success,
            "message": "Workflow resumed successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error resuming workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 