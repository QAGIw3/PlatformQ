"""
SeaTunnel integration API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field

from platformq_shared.logging import get_logger
from ..seatunnel import SeaTunnelQualityPipelines

logger = get_logger(__name__)

# Create router
seatunnel_router = APIRouter()

# Global reference
seatunnel_pipelines: Optional[SeaTunnelQualityPipelines] = None


def get_seatunnel() -> SeaTunnelQualityPipelines:
    """Get SeaTunnel pipelines instance"""
    if seatunnel_pipelines is None:
        raise HTTPException(status_code=503, detail="SeaTunnel pipelines not initialized")
    return seatunnel_pipelines


# Request/Response models

class PipelineCreateRequest(BaseModel):
    """Pipeline creation request"""
    name: str = Field(..., description="Pipeline name")
    source_config: Dict[str, Any] = Field(..., description="Source configuration")
    sink_config: Dict[str, Any] = Field(..., description="Sink configuration")
    quality_config: Dict[str, Any] = Field(..., description="Quality validation configuration")
    transform_config: Optional[List[Dict[str, Any]]] = Field(None, description="Transform configurations")
    schedule: Optional[str] = Field(None, description="Cron schedule for pipeline")
    
    class Config:
        schema_extra = {
            "example": {
                "name": "customer_data_quality_pipeline",
                "source_config": {
                    "type": "jdbc",
                    "url": "jdbc:postgresql://localhost:5432/customers",
                    "table": "customers"
                },
                "sink_config": {
                    "type": "elasticsearch",
                    "hosts": ["http://localhost:9200"],
                    "index": "customers_validated"
                },
                "quality_config": {
                    "validation_mode": "fail_on_critical",
                    "dimensions": ["completeness", "validity", "consistency"],
                    "rules": [
                        {"column": "email", "type": "regex", "pattern": "^[\\w.-]+@[\\w.-]+\\.\\w+$"}
                    ]
                }
            }
        }


class PipelineExecuteRequest(BaseModel):
    """Pipeline execution request"""
    pipeline_id: str = Field(..., description="Pipeline ID to execute")
    execution_mode: str = Field("batch", description="Execution mode: batch or streaming")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Runtime parameters")
    
    class Config:
        schema_extra = {
            "example": {
                "pipeline_id": "pipeline_123",
                "execution_mode": "batch",
                "parameters": {
                    "date_range": "2024-01-01:2024-01-31"
                }
            }
        }


class QualityGateRequest(BaseModel):
    """Quality gate configuration request"""
    pipeline_id: str
    gate_config: Dict[str, Any]
    
    class Config:
        schema_extra = {
            "example": {
                "pipeline_id": "pipeline_123",
                "gate_config": {
                    "threshold": 0.95,
                    "action_on_failure": "stop",
                    "notification_channels": ["email", "slack"]
                }
            }
        }


# API Endpoints

@seatunnel_router.post("/pipelines")
async def create_pipeline(
    request: PipelineCreateRequest,
    background_tasks: BackgroundTasks,
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Create quality-aware data pipeline
    
    Creates a SeaTunnel pipeline with embedded quality validation.
    """
    logger.info(f"creating_seatunnel_pipeline", pipeline_name=request.name)
    
    try:
        # Create pipeline configuration
        pipeline_config = {
            "name": request.name,
            "source": request.source_config,
            "sink": request.sink_config,
            "transform": request.transform_config or [],
            "quality": request.quality_config
        }
        
        # Add quality transforms
        pipeline_config["transform"].extend(
            await st_pipelines.generate_quality_transforms(request.quality_config)
        )
        
        # Create pipeline
        pipeline_id = await st_pipelines.create_pipeline(pipeline_config)
        
        # Schedule if requested
        if request.schedule:
            background_tasks.add_task(
                st_pipelines.schedule_pipeline,
                pipeline_id,
                request.schedule
            )
        
        return {
            "pipeline_id": pipeline_id,
            "name": request.name,
            "status": "created",
            "quality_enabled": True,
            "scheduled": bool(request.schedule)
        }
        
    except Exception as e:
        logger.error(f"failed_to_create_pipeline", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to create pipeline: {str(e)}")


@seatunnel_router.post("/pipelines/execute")
async def execute_pipeline(
    request: PipelineExecuteRequest,
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Execute data pipeline
    
    Executes a SeaTunnel pipeline with quality validation.
    """
    logger.info(f"executing_pipeline", pipeline_id=request.pipeline_id)
    
    try:
        # Start execution
        execution_id = await st_pipelines.execute_pipeline(
            pipeline_id=request.pipeline_id,
            mode=request.execution_mode,
            parameters=request.parameters
        )
        
        return {
            "execution_id": execution_id,
            "pipeline_id": request.pipeline_id,
            "status": "started",
            "mode": request.execution_mode,
            "start_time": datetime.utcnow().isoformat()
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_execute_pipeline", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to execute pipeline: {str(e)}")


@seatunnel_router.get("/pipelines/{pipeline_id}")
async def get_pipeline(
    pipeline_id: str,
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Get pipeline details
    
    Retrieves configuration and status of a pipeline.
    """
    logger.info(f"getting_pipeline", pipeline_id=pipeline_id)
    
    try:
        pipeline = await st_pipelines.get_pipeline(pipeline_id)
        
        if not pipeline:
            raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
        
        return pipeline
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_get_pipeline", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get pipeline: {str(e)}")


@seatunnel_router.get("/pipelines")
async def list_pipelines(
    status: Optional[str] = Query(None, description="Filter by status"),
    quality_enabled: Optional[bool] = Query(None, description="Filter by quality validation"),
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    List pipelines
    
    Lists all SeaTunnel pipelines with optional filters.
    """
    logger.info(f"listing_pipelines", status=status, quality_enabled=quality_enabled)
    
    try:
        pipelines = await st_pipelines.list_pipelines(
            status=status,
            quality_enabled=quality_enabled
        )
        
        return {
            "total": len(pipelines),
            "pipelines": pipelines
        }
        
    except Exception as e:
        logger.error(f"failed_to_list_pipelines", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list pipelines: {str(e)}")


@seatunnel_router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(
    pipeline_id: str,
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Delete pipeline
    
    Deletes a SeaTunnel pipeline.
    """
    logger.info(f"deleting_pipeline", pipeline_id=pipeline_id)
    
    try:
        await st_pipelines.delete_pipeline(pipeline_id)
        
        return {
            "pipeline_id": pipeline_id,
            "status": "deleted"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_delete_pipeline", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to delete pipeline: {str(e)}")


@seatunnel_router.get("/executions/{execution_id}")
async def get_execution_status(
    execution_id: str,
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Get execution status
    
    Retrieves status of a pipeline execution including quality metrics.
    """
    logger.info(f"getting_execution_status", execution_id=execution_id)
    
    try:
        status = await st_pipelines.get_execution_status(execution_id)
        
        if not status:
            raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"failed_to_get_execution_status", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get execution status: {str(e)}")


@seatunnel_router.get("/executions")
async def list_executions(
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(100, description="Maximum number of results"),
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    List pipeline executions
    
    Lists pipeline executions with quality results.
    """
    logger.info(f"listing_executions", pipeline_id=pipeline_id, status=status)
    
    try:
        executions = await st_pipelines.list_executions(
            pipeline_id=pipeline_id,
            status=status,
            limit=limit
        )
        
        return {
            "total": len(executions),
            "executions": executions
        }
        
    except Exception as e:
        logger.error(f"failed_to_list_executions", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list executions: {str(e)}")


@seatunnel_router.post("/pipelines/{pipeline_id}/quality-gate")
async def configure_quality_gate(
    pipeline_id: str,
    request: QualityGateRequest,
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Configure quality gate
    
    Configures quality validation gate for a pipeline.
    """
    logger.info(f"configuring_quality_gate", pipeline_id=pipeline_id)
    
    try:
        await st_pipelines.configure_quality_gate(
            pipeline_id=pipeline_id,
            gate_config=request.gate_config
        )
        
        return {
            "pipeline_id": pipeline_id,
            "quality_gate": request.gate_config,
            "status": "configured"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_configure_quality_gate", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to configure quality gate: {str(e)}")


@seatunnel_router.get("/pipelines/{pipeline_id}/quality-metrics")
async def get_pipeline_quality_metrics(
    pipeline_id: str,
    days: int = Query(7, description="Number of days of history"),
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Get pipeline quality metrics
    
    Retrieves quality metrics history for a pipeline.
    """
    logger.info(f"getting_pipeline_quality_metrics", pipeline_id=pipeline_id, days=days)
    
    try:
        metrics = await st_pipelines.get_pipeline_quality_metrics(
            pipeline_id=pipeline_id,
            days=days
        )
        
        return {
            "pipeline_id": pipeline_id,
            "days": days,
            "metrics": metrics,
            "summary": {
                "avg_quality_score": sum(m.get("quality_score", 0) for m in metrics) / len(metrics) if metrics else 0,
                "total_executions": len(metrics),
                "failed_quality_gates": sum(1 for m in metrics if not m.get("quality_passed", True))
            }
        }
        
    except Exception as e:
        logger.error(f"failed_to_get_quality_metrics", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to get quality metrics: {str(e)}")


@seatunnel_router.post("/templates/{template_name}/instantiate")
async def instantiate_template(
    template_name: str,
    parameters: Dict[str, Any],
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    Instantiate pipeline template
    
    Creates a pipeline from a pre-built quality template.
    """
    logger.info(f"instantiating_template", template_name=template_name)
    
    try:
        pipeline_id = await st_pipelines.instantiate_template(
            template_name=template_name,
            parameters=parameters
        )
        
        return {
            "pipeline_id": pipeline_id,
            "template": template_name,
            "status": "created",
            "message": f"Pipeline created from template {template_name}"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"failed_to_instantiate_template", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to instantiate template: {str(e)}")


@seatunnel_router.get("/templates")
async def list_templates(
    st_pipelines: SeaTunnelQualityPipelines = Depends(get_seatunnel)
) -> Dict[str, Any]:
    """
    List pipeline templates
    
    Lists available quality-aware pipeline templates.
    """
    logger.info("listing_pipeline_templates")
    
    try:
        templates = await st_pipelines.list_templates()
        
        return {
            "total": len(templates),
            "templates": templates
        }
        
    except Exception as e:
        logger.error(f"failed_to_list_templates", error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to list templates: {str(e)}")


# Set SeaTunnel reference
def set_seatunnel(pipelines: SeaTunnelQualityPipelines):
    """Set global SeaTunnel pipelines reference"""
    global seatunnel_pipelines
    seatunnel_pipelines = pipelines 