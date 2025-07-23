"""
Pipeline orchestration API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from ..core import PipelineManager, PipelineType, PipelineStatus

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/pipelines", tags=["pipelines"])

# Dependency injection
pipeline_manager: Optional[PipelineManager] = None

def set_dependencies(manager: PipelineManager):
    """Set API dependencies"""
    global pipeline_manager
    pipeline_manager = manager


# Request/Response models
class PipelineStep(BaseModel):
    type: str
    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = {}
    dependencies: Optional[List[str]] = []
    retry: Optional[Dict[str, Any]] = {"count": 3, "delay": 60}
    timeout: Optional[int] = 3600
    resources: Optional[Dict[str, Any]] = {"cpu": 1, "memory": "1Gi"}


class CreatePipelineRequest(BaseModel):
    name: str
    type: PipelineType
    steps: List[PipelineStep]
    config: Optional[Dict[str, Any]] = {}
    template: Optional[str] = None
    optimization: Optional[Dict[str, Any]] = {}


class ExecutePipelineRequest(BaseModel):
    context: Optional[Dict[str, Any]] = {}
    async_execution: bool = True


class PipelineResponse(BaseModel):
    id: str
    name: str
    type: PipelineType
    status: PipelineStatus
    created_at: str
    updated_at: str
    version: int
    steps: List[Dict[str, Any]]
    config: Dict[str, Any]
    optimization: Dict[str, Any]


class ExecutionResponse(BaseModel):
    id: str
    pipeline_id: str
    pipeline_name: str
    status: PipelineStatus
    started_at: str
    completed_at: Optional[str] = None
    steps_completed: List[str]
    steps_failed: List[str]
    current_step: Optional[str] = None
    error: Optional[str] = None


# API Endpoints
@router.post("", response_model=PipelineResponse)
async def create_pipeline(request: CreatePipelineRequest = Body(...)):
    """Create a new pipeline"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        pipeline = await pipeline_manager.create_pipeline(
            name=request.name,
            type=request.type,
            steps=[step.dict() for step in request.steps],
            config=request.config,
            template=request.template,
            optimization=request.optimization
        )
        
        return PipelineResponse(**pipeline)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[PipelineResponse])
async def list_pipelines(
    type: Optional[PipelineType] = Query(None),
    status: Optional[PipelineStatus] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List pipelines with optional filtering"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        pipelines = await pipeline_manager.list_pipelines(type=type, status=status)
        
        # Apply pagination
        start = offset
        end = offset + limit
        paginated = pipelines[start:end]
        
        return [PipelineResponse(**p) for p in paginated]
        
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: str):
    """Get pipeline details"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        pipeline = await pipeline_manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
            
        return PipelineResponse(**pipeline)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_id}/execute", response_model=ExecutionResponse)
async def execute_pipeline(
    pipeline_id: str,
    request: ExecutePipelineRequest = Body(...)
):
    """Execute a pipeline"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        execution = await pipeline_manager.execute_pipeline(
            pipeline_id=pipeline_id,
            context=request.context,
            async_execution=request.async_execution
        )
        
        return ExecutionResponse(**execution)
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to execute pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions/{execution_id}", response_model=ExecutionResponse)
async def get_execution(execution_id: str):
    """Get execution details"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        execution = await pipeline_manager.get_execution(execution_id)
        if not execution:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
            
        return ExecutionResponse(**execution)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get execution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/executions/{execution_id}/cancel")
async def cancel_execution(execution_id: str):
    """Cancel a running execution"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        success = await pipeline_manager.cancel_execution(execution_id)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel execution")
            
        return {"message": f"Execution {execution_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel execution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates")
async def get_pipeline_templates():
    """Get available pipeline templates"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        templates = []
        for name, template in pipeline_manager.templates.items():
            templates.append({
                "name": name,
                "description": template.get('description', ''),
                "type": template.get('type', 'transformation'),
                "steps": len(template.get('steps', [])),
                "config": template.get('config', {})
            })
            
        return templates
        
    except Exception as e:
        logger.error(f"Failed to get templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_id}/executions", response_model=List[ExecutionResponse])
async def get_pipeline_executions(
    pipeline_id: str,
    status: Optional[PipelineStatus] = Query(None),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """Get execution history for a pipeline"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        # Get all executions for this pipeline
        executions = [
            exec for exec in pipeline_manager.executions.values()
            if exec['pipeline_id'] == pipeline_id
        ]
        
        # Apply status filter
        if status:
            executions = [e for e in executions if e['status'] == status]
            
        # Sort by start time (newest first)
        executions.sort(key=lambda x: x['started_at'], reverse=True)
        
        # Apply pagination
        start = offset
        end = offset + limit
        paginated = executions[start:end]
        
        return [ExecutionResponse(**e) for e in paginated]
        
    except Exception as e:
        logger.error(f"Failed to get pipeline executions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_id}/executions/{execution_id}/logs")
async def get_execution_logs(
    pipeline_id: str,
    execution_id: str,
    step_id: Optional[str] = Query(None),
    level: Optional[str] = Query(None)
):
    """Get execution logs"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        execution = await pipeline_manager.get_execution(execution_id)
        if not execution or execution['pipeline_id'] != pipeline_id:
            raise HTTPException(status_code=404, detail="Execution not found")
            
        logs = execution.get('logs', [])
        
        # Filter by step if provided
        if step_id:
            logs = [log for log in logs if log.get('step') == step_id]
            
        # Filter by level if provided
        if level:
            logs = [log for log in logs if log.get('level') == level.upper()]
            
        return logs
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get execution logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pipeline_id}/validate")
async def validate_pipeline(pipeline_id: str):
    """Validate pipeline configuration"""
    if not pipeline_manager:
        raise HTTPException(status_code=503, detail="Pipeline manager not initialized")
        
    try:
        pipeline = await pipeline_manager.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
            
        # Validate steps
        validated_steps = await pipeline_manager._validate_steps(pipeline['steps'])
        
        # Check for circular dependencies
        if pipeline_manager._has_cycle(pipeline['dependency_graph']):
            return {
                "valid": False,
                "errors": ["Pipeline contains circular dependencies"]
            }
            
        return {
            "valid": True,
            "validated_steps": validated_steps,
            "dependency_graph": pipeline['dependency_graph']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to validate pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 