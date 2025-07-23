"""
Pipelines API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class PipelineCreateRequest(BaseModel):
    """Pipeline creation request"""
    name: str = Field(..., description="Pipeline name")
    type: str = Field(..., description="Pipeline type")
    description: str = Field("", description="Pipeline description")
    steps: List[Dict[str, Any]] = Field(..., description="Pipeline steps")
    dependencies: Dict[str, List[str]] = Field(default={}, description="Step dependencies")
    config: Dict[str, Any] = Field(default={}, description="Additional configuration")


class PipelineExecuteRequest(BaseModel):
    """Pipeline execution request"""
    input_data: Dict[str, Any] = Field(default={}, description="Input data for pipeline")


@router.post("/pipelines", response_model=Dict[str, str])
async def create_pipeline(request: PipelineCreateRequest) -> Dict[str, str]:
    """Create a new pipeline"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        pipeline_id = await pipeline_manager.create_pipeline(request.dict())
        
        return {
            "pipeline_id": pipeline_id,
            "status": "created",
            "message": "Pipeline created successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/pipelines", response_model=List[Dict[str, Any]])
async def list_pipelines(
    type: str = Query(None, description="Filter by type"),
    status: str = Query(None, description="Filter by status"),
    limit: int = Query(100, description="Maximum results")
) -> List[Dict[str, Any]]:
    """List all pipelines"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        pipelines = []
        for pipeline_id, pipeline in pipeline_manager.pipelines.items():
            if type and pipeline["config"]["type"] != type:
                continue
            if status and pipeline["status"] != status:
                continue
            
            pipelines.append({
                "id": pipeline_id,
                "name": pipeline["config"]["name"],
                "type": pipeline["config"]["type"],
                "status": pipeline["status"],
                "created_at": pipeline["created_at"].isoformat(),
                "version": pipeline["version"]
            })
        
        return pipelines[:limit]
        
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/pipelines/{pipeline_id}")
async def get_pipeline(pipeline_id: str) -> Dict[str, Any]:
    """Get pipeline details"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        status = await pipeline_manager.get_pipeline_status(pipeline_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting pipeline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/pipelines/{pipeline_id}/execute")
async def execute_pipeline(pipeline_id: str, request: PipelineExecuteRequest) -> Dict[str, str]:
    """Execute a pipeline"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        execution_id = await pipeline_manager.execute_pipeline(pipeline_id, request.input_data)
        
        return {
            "pipeline_id": pipeline_id,
            "execution_id": execution_id,
            "status": "queued",
            "message": "Pipeline execution queued"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error executing pipeline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/executions/{execution_id}")
async def get_execution_status(execution_id: str) -> Dict[str, Any]:
    """Get pipeline execution status"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        status = await pipeline_manager.get_execution_status(execution_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting execution status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/executions/{execution_id}")
async def cancel_execution(execution_id: str) -> Dict[str, Any]:
    """Cancel pipeline execution"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        success = await pipeline_manager.cancel_pipeline(execution_id)
        
        return {
            "execution_id": execution_id,
            "cancelled": success,
            "message": "Execution cancelled successfully" if success else "Execution could not be cancelled"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling execution: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/pipelines/{pipeline_id}/optimize")
async def optimize_pipeline(
    pipeline_id: str,
    target: str = Query("balanced", description="Optimization target")
) -> Dict[str, Any]:
    """Get optimization recommendations for pipeline"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        recommendations = await pipeline_manager.optimize_pipeline(pipeline_id, target)
        return recommendations
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error optimizing pipeline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/templates")
async def list_pipeline_templates() -> List[Dict[str, Any]]:
    """List available pipeline templates"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        templates = []
        for name, template in pipeline_manager.templates.items():
            templates.append({
                "name": name,
                "type": template.get("type"),
                "description": template.get("description", ""),
                "steps": len(template.get("steps", []))
            })
        
        return templates
        
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics")
async def get_pipeline_metrics() -> Dict[str, Any]:
    """Get pipeline manager metrics"""
    try:
        from ..main import pipeline_manager
        
        if not pipeline_manager:
            raise HTTPException(status_code=503, detail="Pipeline manager not available")
        
        metrics = await pipeline_manager.get_pipeline_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting pipeline metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 