"""
SeaTunnel Service

Provides unified data integration and ETL pipeline management using Apache SeaTunnel.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio
import yaml
import json
import uuid

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, UploadFile, File, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.database import get_db

from .models import (
    DataPipeline,
    PipelineJob,
    DataSource,
    DataSink,
    PipelineTemplate,
    PipelineStatus,
    JobStatus,
    SourceType,
    SinkType,
    ScheduleType
)
from .repository import (
    DataPipelineRepository,
    PipelineJobRepository,
    DataSourceRepository,
    DataSinkRepository,
    PipelineTemplateRepository,
    PipelineMetricsRepository
)
from .event_processors import SeaTunnelEventProcessor
from .pipeline_generator import PipelineGenerator
from .monitoring import PipelineMonitor
from .pipeline_executor import PipelineExecutor

# Setup logging
logger = logging.getLogger(__name__)

# Global instances
event_processor = None
pipeline_generator = None
pipeline_monitor = None
pipeline_executor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor, pipeline_generator, pipeline_monitor, pipeline_executor
    
    # Startup
    logger.info("Initializing SeaTunnel Service...")
    
    # Initialize event processor
    pulsar_client = get_pulsar_client()
    event_processor = SeaTunnelEventProcessor(
        pulsar_client=pulsar_client,
        service_name="seatunnel-service"
    )
    
    # Initialize service components
    pipeline_generator = PipelineGenerator()
    pipeline_monitor = PipelineMonitor()
    pipeline_executor = PipelineExecutor()
    
    # Start event processor
    await event_processor.start()
    
    logger.info("SeaTunnel Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down SeaTunnel Service...")
    
    if event_processor:
        await event_processor.stop()
    
    logger.info("SeaTunnel Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="PlatformQ SeaTunnel Service",
    description="Unified data integration and ETL pipeline management",
    version="1.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Pydantic models for API requests/responses
class PipelineCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    category: str = "etl"  # etl, sync, migration, streaming, batch
    source_configs: List[Dict[str, Any]]
    sink_configs: List[Dict[str, Any]]
    transform_configs: Optional[List[Dict[str, Any]]] = None
    schedule_type: ScheduleType = ScheduleType.ONCE
    schedule_config: Optional[Dict[str, Any]] = None
    parallelism: int = Field(default=1, ge=1, le=10)


class JobExecuteRequest(BaseModel):
    pipeline_id: str
    execution_mode: str = "batch"  # batch, streaming, micro-batch
    runtime_config: Optional[Dict[str, Any]] = None


class DataSourceRequest(BaseModel):
    name: str
    source_type: SourceType
    description: Optional[str] = None
    connection_config: Dict[str, Any]
    test_query: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class DataSinkRequest(BaseModel):
    name: str
    sink_type: SinkType
    description: Optional[str] = None
    connection_config: Dict[str, Any]
    write_mode: str = "append"  # append, overwrite, upsert
    tags: List[str] = Field(default_factory=list)


class DataSyncRequest(BaseModel):
    source_id: str
    sink_id: str
    sync_mode: str = "full"  # full, incremental, cdc
    transform_configs: Optional[List[Dict[str, Any]]] = None
    filter_condition: Optional[str] = None


# API Endpoints

@app.post("/api/v1/pipelines", response_model=Dict[str, Any])
async def create_pipeline(
    request: PipelineCreateRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),  # TODO: Get from auth
    user_id: str = Depends(lambda: "user")  # TODO: Get from auth
):
    """Create a new data pipeline"""
    try:
        # Generate pipeline configuration
        config = pipeline_generator.generate_config(
            sources=request.source_configs,
            sinks=request.sink_configs,
            transforms=request.transform_configs
        )
        
        # Publish pipeline created event
        event_publisher = EventPublisher()
        pipeline_id = f"pipeline-{datetime.utcnow().timestamp()}"
        
        await event_publisher.publish_event(
            {
                "pipeline_id": pipeline_id,
                "tenant_id": tenant_id,
                "name": request.name,
                "description": request.description,
                "category": request.category,
                "config": config,
                "source_configs": request.source_configs,
                "sink_configs": request.sink_configs,
                "transform_configs": request.transform_configs,
                "schedule_type": request.schedule_type.value,
                "schedule_config": request.schedule_config,
                "created_by": user_id
            },
            "persistent://public/default/pipeline-events"
        )
        
        return {
            "pipeline_id": pipeline_id,
            "status": "creating",
            "message": "Pipeline creation initiated"
        }
        
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/pipelines", response_model=List[Dict[str, Any]])
async def list_pipelines(
    category: Optional[str] = None,
    status: Optional[PipelineStatus] = None,
    tags: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """List data pipelines"""
    try:
        pipeline_repo = DataPipelineRepository(db)
        
        pipelines = pipeline_repo.search_pipelines(
            tenant_id=tenant_id,
            category=category,
            status=status,
            tags=tags
        )
        
        return [
            {
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "description": p.description,
                "category": p.category,
                "status": p.status.value,
                "schedule_type": p.schedule_type.value,
                "created_at": p.created_at.isoformat(),
                "is_active": p.is_active
            }
            for p in pipelines
        ]
        
    except Exception as e:
        logger.error(f"Error listing pipelines: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/pipelines/{pipeline_id}", response_model=Dict[str, Any])
async def get_pipeline(
    pipeline_id: str,
    db: Session = Depends(get_db)
):
    """Get pipeline details"""
    try:
        pipeline_repo = DataPipelineRepository(db)
        
        pipeline = pipeline_repo.get_by_pipeline_id(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return {
            "pipeline_id": pipeline.pipeline_id,
            "name": pipeline.name,
            "description": pipeline.description,
            "category": pipeline.category,
            "config": pipeline.config,
            "source_configs": pipeline.source_configs,
            "sink_configs": pipeline.sink_configs,
            "transform_configs": pipeline.transform_configs,
            "status": pipeline.status.value,
            "schedule_type": pipeline.schedule_type.value,
            "schedule_config": pipeline.schedule_config,
            "parallelism": pipeline.parallelism,
            "version": pipeline.version,
            "created_at": pipeline.created_at.isoformat(),
            "updated_at": pipeline.updated_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/pipelines/{pipeline_id}/execute", response_model=Dict[str, Any])
async def execute_pipeline(
    pipeline_id: str,
    request: JobExecuteRequest,
    background_tasks: BackgroundTasks,
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Execute a pipeline"""
    try:
        # Publish job start request
        event_publisher = EventPublisher()
        
        await event_publisher.publish_event(
            {
                "pipeline_id": pipeline_id,
                "tenant_id": tenant_id,
                "execution_mode": request.execution_mode,
                "trigger_type": "manual",
                "runtime_config": request.runtime_config
            },
            "persistent://public/default/job-events"
        )
        
        return {
            "status": "submitted",
            "pipeline_id": pipeline_id,
            "message": "Pipeline execution initiated"
        }
        
    except Exception as e:
        logger.error(f"Error executing pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/pipelines/{pipeline_id}/jobs", response_model=List[Dict[str, Any]])
async def list_pipeline_jobs(
    pipeline_id: str,
    status: Optional[JobStatus] = None,
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """List jobs for a pipeline"""
    try:
        job_repo = PipelineJobRepository(db)
        
        jobs = job_repo.get_pipeline_jobs(pipeline_id, status, limit)
        
        return [
            {
                "job_id": j.job_id,
                "job_name": j.job_name,
                "status": j.status.value,
                "execution_mode": j.execution_mode,
                "trigger_type": j.trigger_type,
                "progress": j.progress,
                "records_read": j.records_read,
                "records_written": j.records_written,
                "started_at": j.started_at.isoformat() if j.started_at else None,
                "completed_at": j.completed_at.isoformat() if j.completed_at else None,
                "duration_seconds": j.duration_seconds,
                "error_message": j.error_message
            }
            for j in jobs
        ]
        
    except Exception as e:
        logger.error(f"Error listing pipeline jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/jobs/{job_id}", response_model=Dict[str, Any])
async def get_job_details(
    job_id: str,
    db: Session = Depends(get_db)
):
    """Get job execution details"""
    try:
        job_repo = PipelineJobRepository(db)
        
        job = job_repo.get_by_job_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "job_id": job.job_id,
            "pipeline_id": job.pipeline_id,
            "job_name": job.job_name,
            "status": job.status.value,
            "execution_mode": job.execution_mode,
            "trigger_type": job.trigger_type,
            "trigger_details": job.trigger_details,
            "progress": job.progress,
            "metrics": {
                "records_read": job.records_read,
                "records_written": job.records_written,
                "records_failed": job.records_failed,
                "bytes_read": job.bytes_read,
                "bytes_written": job.bytes_written
            },
            "timing": {
                "scheduled_at": job.scheduled_at.isoformat() if job.scheduled_at else None,
                "started_at": job.started_at.isoformat() if job.started_at else None,
                "completed_at": job.completed_at.isoformat() if job.completed_at else None,
                "duration_seconds": job.duration_seconds
            },
            "error": {
                "message": job.error_message,
                "details": job.error_details
            } if job.error_message else None,
            "retry_info": {
                "retry_count": job.retry_count,
                "max_retries": job.max_retries
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/sources", response_model=Dict[str, Any])
async def register_data_source(
    request: DataSourceRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Register a new data source"""
    try:
        source_repo = DataSourceRepository(db)
        
        # Check if source name already exists
        existing = source_repo.query().filter_by(
            tenant_id=tenant_id,
            name=request.name
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=400,
                detail="Data source with this name already exists"
            )
        
        # Create source
        source = source_repo.create({
            "source_id": f"source-{datetime.utcnow().timestamp()}",
            "tenant_id": tenant_id,
            "name": request.name,
            "source_type": request.source_type,
            "description": request.description,
            "connection_config": request.connection_config,
            "test_query": request.test_query,
            "tags": request.tags
        })
        
        # Test connection
        test_result = await pipeline_executor.test_source_connection(source)
        
        if test_result["success"]:
            source_repo.update_health_status(
                source_id=source.source_id,
                health_status="healthy",
                health_details=test_result
            )
        else:
            source_repo.update_health_status(
                source_id=source.source_id,
                health_status="unhealthy",
                health_details=test_result
            )
        
        return {
            "source_id": source.source_id,
            "name": source.name,
            "status": "created",
            "health_status": source.health_status,
            "test_result": test_result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering data source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/sources", response_model=List[Dict[str, Any]])
async def list_data_sources(
    source_type: Optional[SourceType] = None,
    tags: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """List registered data sources"""
    try:
        source_repo = DataSourceRepository(db)
        
        sources = source_repo.search_sources(
            tenant_id=tenant_id,
            source_type=source_type,
            tags=tags
        )
        
        return [
            {
                "source_id": s.source_id,
                "name": s.name,
                "source_type": s.source_type.value,
                "description": s.description,
                "tags": s.tags,
                "health_status": s.health_status,
                "last_health_check": s.last_health_check.isoformat() if s.last_health_check else None,
                "is_active": s.is_active,
                "created_at": s.created_at.isoformat()
            }
            for s in sources
        ]
        
    except Exception as e:
        logger.error(f"Error listing data sources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/sinks", response_model=Dict[str, Any])
async def register_data_sink(
    request: DataSinkRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Register a new data sink"""
    try:
        sink_repo = DataSinkRepository(db)
        
        # Check if sink name already exists
        existing = sink_repo.query().filter_by(
            tenant_id=tenant_id,
            name=request.name
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=400,
                detail="Data sink with this name already exists"
            )
        
        # Create sink
        sink = sink_repo.create({
            "sink_id": f"sink-{datetime.utcnow().timestamp()}",
            "tenant_id": tenant_id,
            "name": request.name,
            "sink_type": request.sink_type,
            "description": request.description,
            "connection_config": request.connection_config,
            "write_mode": request.write_mode,
            "tags": request.tags
        })
        
        return {
            "sink_id": sink.sink_id,
            "name": sink.name,
            "status": "created"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering data sink: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/sync", response_model=Dict[str, Any])
async def create_data_sync(
    request: DataSyncRequest,
    background_tasks: BackgroundTasks,
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Create a data synchronization job"""
    try:
        # Publish sync request event
        event_publisher = EventPublisher()
        
        await event_publisher.publish_event(
            {
                "source_id": request.source_id,
                "sink_id": request.sink_id,
                "sync_mode": request.sync_mode,
                "transform_configs": request.transform_configs,
                "filter_condition": request.filter_condition,
                "tenant_id": tenant_id
            },
            "persistent://public/default/data-sync-events"
        )
        
        return {
            "status": "sync_initiated",
            "source_id": request.source_id,
            "sink_id": request.sink_id,
            "sync_mode": request.sync_mode
        }
        
    except Exception as e:
        logger.error(f"Error creating data sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/templates", response_model=List[Dict[str, Any]])
async def list_pipeline_templates(
    category: Optional[str] = None,
    public_only: bool = False,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """List available pipeline templates"""
    try:
        template_repo = PipelineTemplateRepository(db)
        
        if public_only:
            templates = template_repo.get_public_templates(category)
        else:
            templates = template_repo.get_tenant_templates(tenant_id, category)
        
        return [
            {
                "template_id": t.template_id,
                "name": t.name,
                "description": t.description,
                "category": t.category,
                "icon": t.icon,
                "parameters": t.parameters,
                "usage_count": t.usage_count,
                "is_public": t.is_public,
                "version": t.version
            }
            for t in templates
        ]
        
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/pipelines/from-template", response_model=Dict[str, Any])
async def create_pipeline_from_template(
    template_id: str,
    name: str,
    parameter_values: Dict[str, Any],
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant"),
    user_id: str = Depends(lambda: "user")
):
    """Create a pipeline from a template"""
    try:
        template_repo = PipelineTemplateRepository(db)
        
        template = template_repo.get_by_template_id(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        # Generate pipeline config from template
        config = pipeline_generator.generate_from_template(
            template=template,
            parameter_values=parameter_values
        )
        
        # Create pipeline
        event_publisher = EventPublisher()
        pipeline_id = f"pipeline-{datetime.utcnow().timestamp()}"
        
        await event_publisher.publish_event(
            {
                "pipeline_id": pipeline_id,
                "tenant_id": tenant_id,
                "name": name,
                "description": f"Created from template: {template.name}",
                "category": template.category,
                "config": config,
                "source_configs": config.get("sources", []),
                "sink_configs": config.get("sinks", []),
                "transform_configs": config.get("transforms", []),
                "created_by": user_id
            },
            "persistent://public/default/pipeline-events"
        )
        
        # Increment template usage
        template_repo.increment_usage(template_id)
        
        return {
            "pipeline_id": pipeline_id,
            "status": "creating",
            "template_used": template.name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating pipeline from template: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/statistics", response_model=Dict[str, Any])
async def get_service_statistics(
    days: int = Query(7, ge=1, le=90),
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Get service statistics and metrics"""
    try:
        job_repo = PipelineJobRepository(db)
        pipeline_repo = DataPipelineRepository(db)
        
        # Get job statistics
        job_stats = job_repo.get_job_statistics(tenant_id, days)
        
        # Get pipeline counts
        total_pipelines = pipeline_repo.query().filter_by(tenant_id=tenant_id).count()
        active_pipelines = len(pipeline_repo.get_active_pipelines(tenant_id))
        
        # Get recent jobs
        recent_jobs = job_repo.get_recent_jobs(tenant_id, hours=24)
        
        return {
            "period_days": days,
            "pipelines": {
                "total": total_pipelines,
                "active": active_pipelines
            },
            "jobs": job_stats,
            "recent_activity": {
                "jobs_last_24h": len(recent_jobs),
                "active_jobs": len([j for j in recent_jobs if j.status in [JobStatus.PENDING, JobStatus.RUNNING]])
            },
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check handled by base service 