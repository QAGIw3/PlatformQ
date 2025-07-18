"""
SeaTunnel Event Processors

Handles events for data pipeline orchestration and job management.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import uuid
import json
import asyncio

from platformq_shared.event_framework import EventProcessor, event_handler
from platformq_shared.events import (
    DataPipelineCreated,
    DataPipelineUpdated,
    DataPipelineDeleted,
    PipelineJobStarted,
    PipelineJobCompleted,
    PipelineJobFailed,
    DataSourceCreated,
    DataSinkCreated,
    DataIngestionRequested,
    DataExportRequested,
    DataTransformRequested,
    DataSyncRequested,
    DataLineageUpdated,
    DataQualityCheckRequested,
    ScheduledJobTriggered
)
from platformq_shared.database import get_db

from .repository import (
    DataPipelineRepository,
    PipelineJobRepository,
    DataSourceRepository,
    DataSinkRepository,
    PipelineTemplateRepository,
    PipelineMetricsRepository
)
from .models import (
    DataPipeline,
    PipelineJob,
    PipelineStatus,
    JobStatus,
    ScheduleType
)
from .pipeline_executor import PipelineExecutor
from .pipeline_generator import PipelineGenerator
from .monitoring import PipelineMonitor

logger = logging.getLogger(__name__)


class SeaTunnelEventProcessor(EventProcessor):
    """Event processor for SeaTunnel data integration operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline_repo = None
        self.job_repo = None
        self.source_repo = None
        self.sink_repo = None
        self.template_repo = None
        self.metrics_repo = None
        
        # Initialize service components
        self.pipeline_executor = None
        self.pipeline_generator = None
        self.pipeline_monitor = None
    
    def initialize_resources(self):
        """Initialize repositories and service components"""
        if not self.pipeline_repo:
            db = next(get_db())
            self.pipeline_repo = DataPipelineRepository(db)
            self.job_repo = PipelineJobRepository(db)
            self.source_repo = DataSourceRepository(db)
            self.sink_repo = DataSinkRepository(db)
            self.template_repo = PipelineTemplateRepository(db)
            self.metrics_repo = PipelineMetricsRepository(db)
            
            # Initialize service components
            self.pipeline_executor = PipelineExecutor()
            self.pipeline_generator = PipelineGenerator()
            self.pipeline_monitor = PipelineMonitor()
    
    @event_handler("persistent://public/default/pipeline-events")
    async def handle_pipeline_created(self, event: DataPipelineCreated):
        """Handle new pipeline creation"""
        self.initialize_resources()
        
        try:
            # Create pipeline record
            pipeline = self.pipeline_repo.create({
                "pipeline_id": event.pipeline_id,
                "tenant_id": event.tenant_id,
                "name": event.name,
                "description": event.description,
                "category": event.category,
                "config": event.config,
                "source_configs": event.source_configs,
                "sink_configs": event.sink_configs,
                "transform_configs": event.transform_configs,
                "schedule_type": event.schedule_type,
                "schedule_config": event.schedule_config,
                "status": PipelineStatus.DRAFT,
                "created_by": event.created_by
            })
            
            # Validate pipeline configuration
            validation_result = await self.pipeline_executor.validate_pipeline(pipeline)
            
            if validation_result["valid"]:
                self.pipeline_repo.update_status(pipeline.pipeline_id, PipelineStatus.READY)
                
                # If scheduled, register with scheduler
                if pipeline.schedule_type != ScheduleType.ONCE:
                    await self._register_scheduled_pipeline(pipeline)
            else:
                logger.warning(f"Pipeline {pipeline.pipeline_id} validation failed: {validation_result['errors']}")
            
            # Record metric
            self.metrics_repo.record_metric(
                tenant_id=pipeline.tenant_id,
                pipeline_id=pipeline.pipeline_id,
                metric_name="pipeline_created",
                metric_value=1,
                tags={"category": pipeline.category}
            )
            
            logger.info(f"Created pipeline {pipeline.pipeline_id}")
            
        except Exception as e:
            logger.error(f"Error handling pipeline created: {e}")
    
    @event_handler("persistent://public/default/job-events")
    async def handle_job_start_request(self, event: DataIngestionRequested):
        """Handle data ingestion/job start request"""
        self.initialize_resources()
        
        try:
            # Get pipeline
            pipeline = self.pipeline_repo.get_by_pipeline_id(event.pipeline_id)
            if not pipeline:
                logger.error(f"Pipeline {event.pipeline_id} not found")
                return
            
            # Create job record
            job = self.job_repo.create({
                "job_id": f"job-{datetime.utcnow().timestamp()}",
                "tenant_id": event.tenant_id,
                "pipeline_id": pipeline.pipeline_id,
                "job_name": f"{pipeline.name} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
                "execution_mode": event.execution_mode or "batch",
                "trigger_type": event.trigger_type or "manual",
                "trigger_details": event.trigger_details,
                "runtime_config": event.runtime_config,
                "status": JobStatus.PENDING
            })
            
            # Submit job to executor
            execution_result = await self.pipeline_executor.execute_pipeline(
                pipeline=pipeline,
                job=job,
                runtime_config=event.runtime_config
            )
            
            if execution_result["success"]:
                # Update job with execution details
                self.job_repo.update_job_status(
                    job_id=job.job_id,
                    status=JobStatus.RUNNING,
                    metrics={
                        "k8s_job_name": execution_result.get("k8s_job_name"),
                        "seatunnel_job_id": execution_result.get("seatunnel_job_id")
                    }
                )
                
                # Start monitoring job
                asyncio.create_task(self._monitor_job(job))
                
                # Publish job started event
                await self.publish_event(
                    PipelineJobStarted(
                        job_id=job.job_id,
                        pipeline_id=pipeline.pipeline_id,
                        tenant_id=job.tenant_id,
                        started_at=datetime.utcnow()
                    ),
                    "persistent://public/default/job-events"
                )
            else:
                self.job_repo.update_job_status(
                    job_id=job.job_id,
                    status=JobStatus.FAILED,
                    error_message=execution_result.get("error")
                )
            
            logger.info(f"Started job {job.job_id} for pipeline {pipeline.pipeline_id}")
            
        except Exception as e:
            logger.error(f"Error handling job start request: {e}")
    
    @event_handler("persistent://public/default/data-sync-events")
    async def handle_data_sync_request(self, event: DataSyncRequested):
        """Handle data synchronization request"""
        self.initialize_resources()
        
        try:
            # Generate sync pipeline if not exists
            if not event.pipeline_id:
                # Create pipeline from source and sink
                source = self.source_repo.get_by_source_id(event.source_id)
                sink = self.sink_repo.get_by_sink_id(event.sink_id)
                
                if not source or not sink:
                    logger.error("Source or sink not found")
                    return
                
                # Generate pipeline configuration
                pipeline_config = self.pipeline_generator.generate_sync_pipeline(
                    source=source,
                    sink=sink,
                    transform_configs=event.transform_configs,
                    sync_mode=event.sync_mode
                )
                
                # Create pipeline
                pipeline = self.pipeline_repo.create({
                    "pipeline_id": f"sync-{source.source_id}-{sink.sink_id}-{datetime.utcnow().timestamp()}",
                    "tenant_id": event.tenant_id,
                    "name": f"Sync {source.name} to {sink.name}",
                    "category": "sync",
                    "config": pipeline_config,
                    "source_configs": [source.connection_config],
                    "sink_configs": [sink.connection_config],
                    "transform_configs": event.transform_configs or [],
                    "schedule_type": ScheduleType.ONCE if event.sync_mode == "full" else ScheduleType.CONTINUOUS,
                    "status": PipelineStatus.READY
                })
                
                event.pipeline_id = pipeline.pipeline_id
            
            # Start sync job
            await self.handle_job_start_request(
                DataIngestionRequested(
                    pipeline_id=event.pipeline_id,
                    tenant_id=event.tenant_id,
                    execution_mode="streaming" if event.sync_mode == "cdc" else "batch",
                    trigger_type="sync_request",
                    trigger_details={
                        "sync_mode": event.sync_mode,
                        "source_id": event.source_id,
                        "sink_id": event.sink_id
                    }
                )
            )
            
        except Exception as e:
            logger.error(f"Error handling data sync request: {e}")
    
    @event_handler("persistent://public/default/job-monitor-events")
    async def handle_job_status_update(self, event: Dict[str, Any]):
        """Handle job status updates from monitor"""
        self.initialize_resources()
        
        try:
            job_id = event.get("job_id")
            status = event.get("status")
            metrics = event.get("metrics", {})
            
            # Update job status
            if status == "completed":
                self.job_repo.update_job_status(
                    job_id=job_id,
                    status=JobStatus.SUCCESS,
                    metrics=metrics
                )
                
                # Publish completion event
                job = self.job_repo.get_by_job_id(job_id)
                if job:
                    await self.publish_event(
                        PipelineJobCompleted(
                            job_id=job_id,
                            pipeline_id=job.pipeline_id,
                            tenant_id=job.tenant_id,
                            completed_at=datetime.utcnow(),
                            records_processed=metrics.get("records_written", 0),
                            duration_seconds=job.duration_seconds
                        ),
                        "persistent://public/default/job-events"
                    )
                    
                    # Update data lineage
                    if job.source_datasets and job.target_datasets:
                        for source in job.source_datasets:
                            for target in job.target_datasets:
                                await self.publish_event(
                                    DataLineageUpdated(
                                        source_asset_id=source,
                                        target_asset_id=target,
                                        transformation_type="seatunnel_sync",
                                        pipeline_id=job.pipeline_id,
                                        job_id=job_id,
                                        tenant_id=job.tenant_id,
                                        execution_time=job.completed_at,
                                        duration_seconds=job.duration_seconds,
                                        records_processed=metrics.get("records_written", 0)
                                    ),
                                    "persistent://public/default/lineage-events"
                                )
            
            elif status == "failed":
                self.job_repo.update_job_status(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    error_message=event.get("error_message"),
                    metrics=metrics
                )
                
                # Check if should retry
                job = self.job_repo.get_by_job_id(job_id)
                if job and job.retry_count < job.max_retries:
                    await self._retry_job(job)
            
            # Record metrics
            for metric_name, metric_value in metrics.items():
                self.metrics_repo.record_metric(
                    tenant_id=job.tenant_id if job else "unknown",
                    pipeline_id=job.pipeline_id if job else "unknown",
                    job_id=job_id,
                    metric_name=metric_name,
                    metric_value=metric_value
                )
            
        except Exception as e:
            logger.error(f"Error handling job status update: {e}")
    
    @event_handler("persistent://public/default/scheduler-events")
    async def handle_scheduled_job_trigger(self, event: ScheduledJobTriggered):
        """Handle scheduled job triggers"""
        self.initialize_resources()
        
        try:
            pipeline = self.pipeline_repo.get_by_pipeline_id(event.pipeline_id)
            if not pipeline or not pipeline.is_active:
                logger.warning(f"Pipeline {event.pipeline_id} not found or inactive")
                return
            
            # Create scheduled job
            await self.handle_job_start_request(
                DataIngestionRequested(
                    pipeline_id=pipeline.pipeline_id,
                    tenant_id=pipeline.tenant_id,
                    execution_mode="batch",
                    trigger_type="scheduled",
                    trigger_details={
                        "schedule_time": event.schedule_time,
                        "schedule_type": pipeline.schedule_type.value
                    }
                )
            )
            
        except Exception as e:
            logger.error(f"Error handling scheduled job trigger: {e}")
    
    async def _monitor_job(self, job: PipelineJob):
        """Monitor job execution"""
        try:
            monitor_result = await self.pipeline_monitor.monitor_job(
                job_id=job.job_id,
                k8s_job_name=job.k8s_job_name,
                seatunnel_job_id=job.seatunnel_job_id
            )
            
            # Update job with final status
            await self.handle_job_status_update({
                "job_id": job.job_id,
                "status": monitor_result["status"],
                "metrics": monitor_result.get("metrics", {}),
                "error_message": monitor_result.get("error")
            })
            
        except Exception as e:
            logger.error(f"Error monitoring job {job.job_id}: {e}")
            
            # Mark job as failed
            await self.handle_job_status_update({
                "job_id": job.job_id,
                "status": "failed",
                "error_message": str(e)
            })
    
    async def _retry_job(self, job: PipelineJob):
        """Retry a failed job"""
        try:
            # Increment retry count
            job.retry_count += 1
            job.status = JobStatus.RETRYING
            self.job_repo.db.commit()
            
            # Wait before retry (exponential backoff)
            wait_time = min(300, 30 * (2 ** (job.retry_count - 1)))  # Max 5 minutes
            await asyncio.sleep(wait_time)
            
            # Get pipeline and retry
            pipeline = self.pipeline_repo.get_by_pipeline_id(job.pipeline_id)
            if pipeline:
                execution_result = await self.pipeline_executor.execute_pipeline(
                    pipeline=pipeline,
                    job=job,
                    runtime_config=job.runtime_config
                )
                
                if execution_result["success"]:
                    self.job_repo.update_job_status(
                        job_id=job.job_id,
                        status=JobStatus.RUNNING
                    )
                    
                    # Resume monitoring
                    asyncio.create_task(self._monitor_job(job))
                else:
                    self.job_repo.update_job_status(
                        job_id=job.job_id,
                        status=JobStatus.FAILED,
                        error_message=f"Retry failed: {execution_result.get('error')}"
                    )
            
        except Exception as e:
            logger.error(f"Error retrying job {job.job_id}: {e}")
    
    async def _register_scheduled_pipeline(self, pipeline: DataPipeline):
        """Register pipeline with scheduler"""
        try:
            # This would integrate with a scheduler service
            # For now, just log
            logger.info(f"Registered scheduled pipeline {pipeline.pipeline_id} with schedule: {pipeline.schedule_config}")
            
        except Exception as e:
            logger.error(f"Error registering scheduled pipeline: {e}") 