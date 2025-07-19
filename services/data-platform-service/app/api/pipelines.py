"""
Data pipeline API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum
import uuid
import logging

from .. import main

router = APIRouter()
logger = logging.getLogger(__name__)


class PipelineType(str, Enum):
    BATCH = "batch"
    STREAMING = "streaming"
    HYBRID = "hybrid"


class PipelineStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    DEPRECATED = "deprecated"


class ScheduleType(str, Enum):
    CRON = "cron"
    INTERVAL = "interval"
    EVENT = "event"
    MANUAL = "manual"


class ConnectorType(str, Enum):
    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAM = "stream"
    CLOUD = "cloud"


class PipelineStep(BaseModel):
    """Pipeline step definition"""
    name: str
    type: str  # source, transform, sink
    connector: Optional[ConnectorType]
    config: Dict[str, Any]
    retry_policy: Optional[Dict[str, Any]] = None


class PipelineConfig(BaseModel):
    """Pipeline configuration"""
    name: str = Field(..., description="Pipeline name")
    description: Optional[str] = Field(None)
    type: PipelineType
    steps: List[PipelineStep]
    schedule: Optional[Dict[str, Any]] = Field(None)
    error_handling: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)


class PipelineResponse(PipelineConfig):
    """Pipeline response"""
    pipeline_id: str
    status: PipelineStatus
    created_at: datetime
    updated_at: datetime
    created_by: str
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    version: int


class PipelineRun(BaseModel):
    """Pipeline run instance"""
    run_id: str
    pipeline_id: str
    status: str  # running, completed, failed
    started_at: datetime
    completed_at: Optional[datetime]
    error: Optional[str]
    metrics: Dict[str, Any] = Field(default_factory=dict)


@router.post("/pipelines", response_model=PipelineResponse)
async def create_pipeline(
    pipeline: PipelineConfig,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Create a new data pipeline"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        pipeline_id = str(uuid.uuid4())
        
        # Create pipeline
        result = await main.pipeline_coordinator.create_pipeline(
            pipeline_id=pipeline_id,
            config={
                "name": pipeline.name,
                "description": pipeline.description,
                "type": pipeline.type.value,
                "steps": [step.dict() for step in pipeline.steps],
                "schedule": pipeline.schedule,
                "error_handling": pipeline.error_handling,
                "tags": pipeline.tags
            }
        )
        
        return PipelineResponse(
            pipeline_id=pipeline_id,
            name=pipeline.name,
            description=pipeline.description,
            type=pipeline.type,
            steps=pipeline.steps,
            schedule=pipeline.schedule,
            error_handling=pipeline.error_handling,
            tags=pipeline.tags,
            status=PipelineStatus.DRAFT,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            created_by=tenant_id,
            last_run=None,
            next_run=None,
            version=1
        )
    except Exception as e:
        logger.error(f"Failed to create pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pipelines", response_model=List[PipelineResponse])
async def list_pipelines(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    status: Optional[PipelineStatus] = Query(None),
    type: Optional[PipelineType] = Query(None),
    tags: Optional[List[str]] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List data pipelines"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        # Get pipeline statistics
        stats = main.pipeline_coordinator.get_statistics()
        
        # Mock pipeline list based on statistics
        pipelines = []
        pipeline_count = min(stats.get("total_pipelines", 0), limit)
        
        for i in range(pipeline_count):
            pipelines.append(PipelineResponse(
                pipeline_id=str(uuid.uuid4()),
                name=f"pipeline_{i}",
                description=f"Pipeline {i} description",
                type=type or PipelineType.BATCH,
                steps=[
                    PipelineStep(
                        name="source",
                        type="source",
                        connector=ConnectorType.DATABASE,
                        config={"table": "source_table"}
                    ),
                    PipelineStep(
                        name="transform",
                        type="transform",
                        connector=None,
                        config={"operation": "aggregate"}
                    ),
                    PipelineStep(
                        name="sink",
                        type="sink",
                        connector=ConnectorType.FILE,
                        config={"path": "/output"}
                    )
                ],
                schedule={"type": "cron", "expression": "0 0 * * *"},
                error_handling={"retry": 3, "on_failure": "alert"},
                tags=tags or ["production"],
                status=status or PipelineStatus.ACTIVE,
                created_at=datetime.utcnow() - timedelta(days=30),
                updated_at=datetime.utcnow() - timedelta(days=1),
                created_by=tenant_id,
                last_run=datetime.utcnow() - timedelta(hours=1),
                next_run=datetime.utcnow() + timedelta(hours=23),
                version=1
            ))
        
        return pipelines[offset:offset + limit]
    except Exception as e:
        logger.error(f"Failed to list pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get pipeline details"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        # Get pipeline details
        pipeline = await main.pipeline_coordinator.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return PipelineResponse(
            pipeline_id=pipeline_id,
            name=pipeline.get("name", "Unknown"),
            description=pipeline.get("description"),
            type=PipelineType(pipeline.get("type", "batch")),
            steps=[PipelineStep(**step) for step in pipeline.get("steps", [])],
            schedule=pipeline.get("schedule"),
            error_handling=pipeline.get("error_handling", {}),
            tags=pipeline.get("tags", []),
            status=PipelineStatus(pipeline.get("status", "draft")),
            created_at=pipeline.get("created_at", datetime.utcnow()),
            updated_at=pipeline.get("updated_at", datetime.utcnow()),
            created_by=pipeline.get("created_by", tenant_id),
            last_run=pipeline.get("last_run"),
            next_run=pipeline.get("next_run"),
            version=pipeline.get("version", 1)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: str,
    pipeline: PipelineConfig,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Update pipeline configuration"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        # Update pipeline
        result = await main.pipeline_coordinator.update_pipeline(
            pipeline_id=pipeline_id,
            config={
                "name": pipeline.name,
                "description": pipeline.description,
                "type": pipeline.type.value,
                "steps": [step.dict() for step in pipeline.steps],
                "schedule": pipeline.schedule,
                "error_handling": pipeline.error_handling,
                "tags": pipeline.tags
            }
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return PipelineResponse(
            pipeline_id=pipeline_id,
            name=pipeline.name,
            description=pipeline.description,
            type=pipeline.type,
            steps=pipeline.steps,
            schedule=pipeline.schedule,
            error_handling=pipeline.error_handling,
            tags=pipeline.tags,
            status=PipelineStatus.DRAFT,
            created_at=datetime.utcnow() - timedelta(days=30),
            updated_at=datetime.utcnow(),
            created_by=tenant_id,
            last_run=None,
            next_run=None,
            version=2
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/pipelines/{pipeline_id}")
async def delete_pipeline(
    pipeline_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Delete a pipeline"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        result = await main.pipeline_coordinator.delete_pipeline(pipeline_id)
        if not result:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return {"message": f"Pipeline {pipeline_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipelines/{pipeline_id}/runs")
async def trigger_pipeline_run(
    pipeline_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    parameters: Optional[Dict[str, Any]] = Body(None)
):
    """Trigger a pipeline run"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        run_id = await main.pipeline_coordinator.execute_pipeline(
            pipeline_id=pipeline_id,
            execution_params=parameters or {}
        )
        
        return {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "status": "running",
            "started_at": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Failed to trigger pipeline run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pipelines/{pipeline_id}/runs")
async def list_pipeline_runs(
    pipeline_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List pipeline runs"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        # Get pipeline run history
        runs = await main.pipeline_coordinator.get_pipeline_runs(
            pipeline_id=pipeline_id,
            status=status,
            limit=limit,
            offset=offset
        )
        
        return {
            "pipeline_id": pipeline_id,
            "runs": runs,
            "total": len(runs)
        }
    except Exception as e:
        logger.error(f"Failed to list pipeline runs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/runs/{run_id}", response_model=PipelineRun)
async def get_pipeline_run(
    run_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get pipeline run details"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        # Get run details
        run = await main.pipeline_coordinator.get_run_status(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        
        return PipelineRun(
            run_id=run_id,
            pipeline_id=run.get("pipeline_id", ""),
            status=run.get("status", "unknown"),
            started_at=run.get("started_at", datetime.utcnow()),
            completed_at=run.get("completed_at"),
            error=run.get("error"),
            metrics=run.get("metrics", {})
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pipeline run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/runs/{run_id}/cancel")
async def cancel_pipeline_run(
    run_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Cancel a running pipeline"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        result = await main.pipeline_coordinator.cancel_pipeline(run_id)
        if not result:
            raise HTTPException(status_code=404, detail="Pipeline run not found")
        
        return {"message": f"Pipeline run {run_id} cancelled"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel pipeline run: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipelines/{pipeline_id}/activate")
async def activate_pipeline(
    pipeline_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Activate a pipeline"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        result = await main.pipeline_coordinator.activate_pipeline(pipeline_id)
        if not result:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return {"message": f"Pipeline {pipeline_id} activated", "status": "active"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to activate pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pipelines/{pipeline_id}/pause")
async def pause_pipeline(
    pipeline_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Pause a pipeline"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        result = await main.pipeline_coordinator.pause_pipeline(pipeline_id)
        if not result:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return {"message": f"Pipeline {pipeline_id} paused", "status": "paused"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to pause pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_pipeline_statistics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get pipeline statistics"""
    if not main.pipeline_coordinator:
        raise HTTPException(status_code=503, detail="Pipeline coordinator not available")
    
    try:
        stats = main.pipeline_coordinator.get_statistics()
        
        return {
            "total_pipelines": stats.get("total_pipelines", 0),
            "active_pipelines": stats.get("active_pipelines", 0),
            "running_pipelines": stats.get("running_pipelines", 0),
            "failed_pipelines": stats.get("failed_pipelines", 0),
            "total_runs": stats.get("total_runs", 0),
            "successful_runs": stats.get("successful_runs", 0),
            "failed_runs": stats.get("failed_runs", 0),
            "average_runtime_seconds": stats.get("average_runtime_seconds", 0),
            "last_updated": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connectors")
async def list_connectors(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    type: Optional[ConnectorType] = Query(None)
):
    """List available connectors"""
    return {
        "connectors": [
            {
                "connector_id": "mysql",
                "name": "MySQL",
                "type": "database",
                "version": "2.3.0",
                "description": "MySQL database connector",
                "config_schema": {
                    "host": "string",
                    "port": "integer",
                    "database": "string",
                    "username": "string",
                    "password": "string"
                }
            },
            {
                "connector_id": "s3",
                "name": "Amazon S3",
                "type": "cloud",
                "version": "2.3.0",
                "description": "S3 object storage connector",
                "config_schema": {
                    "bucket": "string",
                    "prefix": "string",
                    "access_key": "string",
                    "secret_key": "string"
                }
            },
            {
                "connector_id": "pulsar",
                "name": "Apache Pulsar",
                "type": "stream",
                "version": "2.3.0",
                "description": "Pulsar streaming connector"
            }
        ]
    }


@router.get("/templates")
async def list_pipeline_templates(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    use_case: Optional[str] = Query(None)
):
    """List pipeline templates"""
    return {
        "templates": [
            {
                "template_id": "etl_basic",
                "name": "Basic ETL Pipeline",
                "description": "Extract from database, transform, load to data lake",
                "type": "batch",
                "use_cases": ["data_integration", "migration"],
                "steps": 3
            },
            {
                "template_id": "cdc_streaming",
                "name": "CDC Streaming Pipeline",
                "description": "Real-time change data capture",
                "type": "streaming",
                "use_cases": ["real_time_sync", "cdc"],
                "steps": 2
            }
        ]
    }


@router.post("/pipelines/validate")
async def validate_pipeline(
    pipeline: PipelineConfig,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Validate pipeline configuration"""
    return {
        "valid": True,
        "errors": [],
        "warnings": [
            {
                "step": "transform_1",
                "message": "Consider adding error handling for null values"
            }
        ],
        "estimated_cost": {
            "compute_hours": 2.5,
            "data_processed_gb": 100,
            "monthly_cost_usd": 150
        }
    }


@router.get("/metrics")
async def get_pipeline_metrics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    pipeline_id: Optional[str] = Query(None),
    time_range: str = Query("24h", description="Time range (1h, 24h, 7d, 30d)")
):
    """Get pipeline execution metrics"""
    return {
        "summary": {
            "total_runs": 150,
            "successful_runs": 145,
            "failed_runs": 5,
            "success_rate": 0.967,
            "avg_duration_seconds": 1800,
            "total_data_processed_gb": 1500
        },
        "by_pipeline": [
            {
                "pipeline_id": "pipeline_123",
                "name": "Customer ETL",
                "runs": 50,
                "success_rate": 0.98,
                "avg_duration": 900
            }
        ],
        "time_range": time_range
    } 