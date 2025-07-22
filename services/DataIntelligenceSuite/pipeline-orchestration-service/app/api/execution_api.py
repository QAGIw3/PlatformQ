"""
Pipeline Execution API endpoints

Provides API for pipeline execution operations.
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/executions", tags=["executions"])


# Request/Response Models
class ExecutionRequest(BaseModel):
    """Pipeline execution request"""
    pipeline_id: str = Field(..., description="Pipeline ID to execute")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Execution parameters")
    trigger_type: str = Field(default="manual", description="Trigger type")


class ExecutionResponse(BaseModel):
    """Execution response model"""
    execution_id: str
    pipeline_id: str
    pipeline_name: str
    status: str
    started_at: Optional[str]
    completed_at: Optional[str]
    current_step: Optional[str]
    steps: Dict[str, Any]
    errors: List[Dict[str, Any]]


# API Endpoints
@router.post("/", response_model=Dict[str, str])
async def execute_pipeline(request: ExecutionRequest):
    """Execute a pipeline"""
    try:
        logger.info("execute_pipeline_requested", pipeline_id=request.pipeline_id)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Trigger execution
        execution_id = await service.scheduler.trigger_pipeline(
            pipeline_id=request.pipeline_id,
            trigger_type=request.trigger_type,
            parameters=request.parameters
        )
        
        return {"execution_id": execution_id}
        
    except Exception as e:
        logger.error("execute_pipeline_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution_status(execution_id: str = Path(..., description="Execution ID")):
    """Get execution status"""
    try:
        logger.info("get_execution_status_requested", execution_id=execution_id)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get status
        status = await service.executor.get_execution_status(execution_id)
        if not status:
            raise HTTPException(status_code=404, detail="Execution not found")
        
        return ExecutionResponse(
            execution_id=status["execution_id"],
            pipeline_id=status["pipeline_id"],
            pipeline_name=status["pipeline_name"],
            status=status["status"],
            started_at=status.get("started_at"),
            completed_at=status.get("completed_at"),
            current_step=status.get("current_step"),
            steps=status.get("steps", {}),
            errors=status.get("errors", [])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_execution_status_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{execution_id}/cancel")
async def cancel_execution(execution_id: str = Path(..., description="Execution ID")):
    """Cancel a running execution"""
    try:
        logger.info("cancel_execution_requested", execution_id=execution_id)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Cancel execution
        success = await service.executor.cancel_execution(execution_id)
        if not success:
            raise HTTPException(status_code=404, detail="Execution not found or not running")
        
        return {"message": "Execution cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("cancel_execution_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[Dict[str, Any]])
async def list_executions(
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(100, description="Maximum results")
):
    """List pipeline executions"""
    try:
        logger.info("list_executions_requested")
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get executions
        executions = await service.executor.list_executions(
            pipeline_id=pipeline_id,
            status=status,
            limit=limit
        )
        
        return executions
        
    except Exception as e:
        logger.error("list_executions_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 