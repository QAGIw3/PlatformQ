"""
SeaTunnel API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class SeaTunnelJobRequest(BaseModel):
    """SeaTunnel job request"""
    name: str = Field(..., description="Job name")
    type: str = Field(..., description="Job type (batch, streaming, cdc, sync)")
    source: Dict[str, Any] = Field(..., description="Source configuration")
    sink: Dict[str, Any] = Field(..., description="Sink configuration")
    transform: List[Dict[str, Any]] = Field(None, description="Transform configuration")
    parallelism: int = Field(1, description="Parallelism level")


class SeaTunnelPipelineRequest(BaseModel):
    """SeaTunnel pipeline request"""
    name: str = Field(..., description="Pipeline name")
    template: str = Field(None, description="Template name")
    source: Dict[str, Any] = Field(None, description="Source configuration")
    sink: Dict[str, Any] = Field(None, description="Sink configuration")
    transform: List[Dict[str, Any]] = Field(None, description="Transform configuration")


class DataMovementRequest(BaseModel):
    """Data movement orchestration request"""
    name: str = Field(..., description="Orchestration name")
    movements: List[Dict[str, Any]] = Field(..., description="List of data movements")
    dependencies: Dict[str, List[str]] = Field(default={}, description="Movement dependencies")
    schedule: str = Field(None, description="Cron schedule")


@router.post("/jobs", response_model=Dict[str, str])
async def create_seatunnel_job(request: SeaTunnelJobRequest) -> Dict[str, str]:
    """Create a SeaTunnel job"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        job_id = await seatunnel_orchestrator.create_job(request.dict())
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": "SeaTunnel job created successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating SeaTunnel job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str) -> Dict[str, Any]:
    """Get SeaTunnel job status"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        status = await seatunnel_orchestrator.get_job_status(job_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/jobs/{job_id}")
async def cancel_job(job_id: str) -> Dict[str, Any]:
    """Cancel SeaTunnel job"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        success = await seatunnel_orchestrator.cancel_job(job_id)
        
        return {
            "job_id": job_id,
            "cancelled": success,
            "message": "Job cancelled successfully" if success else "Job could not be cancelled"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/pipelines", response_model=Dict[str, str])
async def create_seatunnel_pipeline(request: SeaTunnelPipelineRequest) -> Dict[str, str]:
    """Create a SeaTunnel pipeline from template"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        job_id = await seatunnel_orchestrator.create_pipeline(request.dict())
        
        return {
            "job_id": job_id,
            "status": "created",
            "message": "SeaTunnel pipeline created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating SeaTunnel pipeline: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/orchestrate")
async def orchestrate_data_movement(request: DataMovementRequest) -> Dict[str, Any]:
    """Orchestrate complex data movements"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        result = await seatunnel_orchestrator.orchestrate_data_movement(
            request.name,
            request.movements,
            request.dependencies,
            request.schedule
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error orchestrating data movement: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/templates")
async def list_seatunnel_templates() -> List[Dict[str, Any]]:
    """List available SeaTunnel templates"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        templates = []
        for name, template in seatunnel_orchestrator.templates.items():
            templates.append({
                "name": name,
                "type": template.get("type"),
                "source_type": template.get("source", {}).get("type"),
                "sink_type": template.get("sink", {}).get("type"),
                "parallelism": template.get("parallelism", 1)
            })
        
        return templates
        
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics")
async def get_seatunnel_metrics() -> Dict[str, Any]:
    """Get SeaTunnel orchestrator metrics"""
    try:
        from ..main import seatunnel_orchestrator
        
        if not seatunnel_orchestrator:
            raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not available")
        
        metrics = await seatunnel_orchestrator.get_seatunnel_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting SeaTunnel metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/connectors")
async def list_supported_connectors() -> Dict[str, List[str]]:
    """List supported SeaTunnel connectors"""
    return {
        "sources": [
            "jdbc", "kafka", "pulsar", "elasticsearch", 
            "mongodb", "s3", "hdfs", "file"
        ],
        "sinks": [
            "jdbc", "kafka", "pulsar", "elasticsearch",
            "clickhouse", "doris", "hive", "iceberg",
            "mongodb", "s3", "hdfs", "console"
        ],
        "transforms": [
            "sql", "filter", "field_mapper", "replace",
            "split", "json_parse", "add_field", "remove_field"
        ]
    } 