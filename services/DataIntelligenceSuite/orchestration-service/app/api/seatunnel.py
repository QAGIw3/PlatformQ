"""
SeaTunnel data movement API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from ..core import SeaTunnelOrchestrator, SeaTunnelJobType, SeaTunnelJobStatus, ConnectorType

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/seatunnel", tags=["seatunnel"])

# Dependency injection
seatunnel_orchestrator: Optional[SeaTunnelOrchestrator] = None

def set_dependencies(orchestrator: SeaTunnelOrchestrator):
    """Set API dependencies"""
    global seatunnel_orchestrator
    seatunnel_orchestrator = orchestrator


# Request/Response models
class DataSource(BaseModel):
    type: ConnectorType
    config: Dict[str, Any]


class DataSink(BaseModel):
    type: ConnectorType
    config: Dict[str, Any]


class Transformation(BaseModel):
    type: str
    config: Optional[Dict[str, Any]] = {}


class CreatePipelineRequest(BaseModel):
    name: str
    source: DataSource
    sink: DataSink
    transformations: Optional[List[Transformation]] = None
    job_type: SeaTunnelJobType = SeaTunnelJobType.BATCH
    template: Optional[str] = None
    orchestration: Optional[Dict[str, Any]] = None


class DataMovement(BaseModel):
    name: str
    source: DataSource
    sink: DataSink
    transformations: Optional[List[Transformation]] = None
    job_type: Optional[SeaTunnelJobType] = SeaTunnelJobType.BATCH
    retries: Optional[int] = 3
    alerts: Optional[List[str]] = []


class OrchestrateRequest(BaseModel):
    name: str
    movements: List[DataMovement]
    dependencies: Optional[Dict[str, List[str]]] = None
    schedule: Optional[str] = None


class SeaTunnelPipelineResponse(BaseModel):
    id: str
    name: str
    job_type: SeaTunnelJobType
    job_id: Optional[str]
    status: SeaTunnelJobStatus
    created_at: str
    config: Dict[str, Any]
    orchestration: Dict[str, Any]


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    error: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None


# API Endpoints
@router.post("/pipelines", response_model=SeaTunnelPipelineResponse)
async def create_seatunnel_pipeline(request: CreatePipelineRequest = Body(...)):
    """Create a SeaTunnel data pipeline"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        pipeline = await seatunnel_orchestrator.create_pipeline(
            name=request.name,
            source=request.source.dict(),
            sink=request.sink.dict(),
            transformations=[t.dict() for t in request.transformations] if request.transformations else None,
            job_type=request.job_type,
            template=request.template,
            orchestration=request.orchestration
        )
        
        return SeaTunnelPipelineResponse(**pipeline)
        
    except Exception as e:
        logger.error(f"Failed to create SeaTunnel pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get SeaTunnel job status"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        status = await seatunnel_orchestrator.get_job_status(job_id)
        
        return JobStatusResponse(
            job_id=job_id,
            **status
        )
        
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    """Cancel a running SeaTunnel job"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        success = await seatunnel_orchestrator.cancel_job(job_id)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel job")
            
        return {"message": f"Job {job_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/orchestrate")
async def orchestrate_data_movement(request: OrchestrateRequest = Body(...)):
    """Orchestrate complex data movements across systems"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        movements = [
            {
                **movement.dict(),
                'transformations': [t.dict() for t in movement.transformations] if movement.transformations else None
            }
            for movement in request.movements
        ]
        
        orchestration = await seatunnel_orchestrator.orchestrate_data_movement(
            name=request.name,
            movements=movements,
            dependencies=request.dependencies,
            schedule=request.schedule
        )
        
        return orchestration
        
    except Exception as e:
        logger.error(f"Failed to orchestrate data movement: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates")
async def get_seatunnel_templates():
    """Get available SeaTunnel pipeline templates"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        templates = await seatunnel_orchestrator.get_templates()
        return templates
        
    except Exception as e:
        logger.error(f"Failed to get templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connectors")
async def get_supported_connectors():
    """Get list of supported connectors"""
    return {
        "sources": [
            {
                "type": ConnectorType.JDBC.value,
                "description": "JDBC data source (PostgreSQL, MySQL, etc.)",
                "required_config": ["url", "table", "user", "password"]
            },
            {
                "type": ConnectorType.PULSAR.value,
                "description": "Apache Pulsar streaming source",
                "required_config": ["topic"]
            },
            {
                "type": ConnectorType.FILE.value,
                "description": "File system source",
                "required_config": ["path", "format"]
            },
            {
                "type": ConnectorType.S3.value,
                "description": "S3-compatible object storage",
                "required_config": ["bucket", "key", "access_key", "secret_key"]
            },
            {
                "type": ConnectorType.ELASTICSEARCH.value,
                "description": "Elasticsearch source",
                "required_config": ["hosts", "index"]
            },
            {
                "type": ConnectorType.CASSANDRA.value,
                "description": "Apache Cassandra source",
                "required_config": ["host", "keyspace", "table"]
            }
        ],
        "sinks": [
            {
                "type": ConnectorType.JDBC.value,
                "description": "JDBC data sink",
                "required_config": ["url", "table", "user", "password"]
            },
            {
                "type": ConnectorType.ELASTICSEARCH.value,
                "description": "Elasticsearch sink",
                "required_config": ["hosts", "index"]
            },
            {
                "type": ConnectorType.IGNITE.value,
                "description": "Apache Ignite sink",
                "required_config": ["cache_name"]
            },
            {
                "type": ConnectorType.PULSAR.value,
                "description": "Apache Pulsar sink",
                "required_config": ["topic"]
            },
            {
                "type": ConnectorType.CONSOLE.value,
                "description": "Console output (for testing)",
                "required_config": []
            }
        ],
        "transformations": [
            {
                "type": "sql",
                "description": "SQL transformation",
                "config_example": {"sql": "SELECT * FROM table WHERE status = 'active'"}
            },
            {
                "type": "quality_check",
                "description": "Data quality validation",
                "config_example": {"rules": [{"field": "id", "type": "not_null"}]}
            },
            {
                "type": "encrypt_pii",
                "description": "Encrypt personally identifiable information",
                "config_example": {"fields": ["email", "phone", "ssn"]}
            },
            {
                "type": "filter",
                "description": "Filter records",
                "config_example": {"condition": {"field": "age", "operator": ">=", "value": 18}}
            },
            {
                "type": "aggregate",
                "description": "Aggregate data",
                "config_example": {"group_by": ["category"], "aggregations": {"count": "COUNT(*)"}}
            }
        ]
    }


@router.get("/jobs")
async def list_jobs(
    status: Optional[SeaTunnelJobStatus] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """List SeaTunnel jobs"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        jobs = list(seatunnel_orchestrator.jobs.values())
        
        # Filter by status
        if status:
            jobs = [j for j in jobs if j.get('status') == status]
            
        # Sort by creation time (newest first)
        jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        # Apply pagination
        start = offset
        end = offset + limit
        paginated = jobs[start:end]
        
        return {
            "total": len(jobs),
            "offset": offset,
            "limit": limit,
            "items": paginated
        }
        
    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate-config")
async def validate_pipeline_config(
    source: DataSource = Body(..., embed=False),
    sink: DataSink = Body(..., embed=False),
    transformations: Optional[List[Transformation]] = Body(None)
):
    """Validate pipeline configuration"""
    if not seatunnel_orchestrator:
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        # Validate source
        source_config = await seatunnel_orchestrator._build_source_config(source.dict())
        
        # Validate sink
        sink_config = await seatunnel_orchestrator._build_sink_config(sink.dict())
        
        # Validate transformations
        transform_configs = []
        if transformations:
            for transform in transformations:
                config = await seatunnel_orchestrator._build_transform_config(transform.dict())
                transform_configs.append(config)
                
        return {
            "valid": True,
            "source_config": source_config,
            "sink_config": sink_config,
            "transform_configs": transform_configs
        }
        
    except Exception as e:
        return {
            "valid": False,
            "error": str(e)
        } 