"""
SeaTunnel Repository

Handles database operations for data pipelines and integration jobs.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.orm import Session
import json

from platformq_shared.repository import BaseRepository, AsyncBaseRepository
from .models import (
    DataPipeline,
    PipelineJob,
    JobCheckpoint,
    DataSource,
    DataSink,
    PipelineTemplate,
    TransformFunction,
    PipelineMetrics,
    PipelineStatus,
    JobStatus,
    SourceType,
    SinkType,
    ScheduleType
)


class DataPipelineRepository(BaseRepository[DataPipeline]):
    """Repository for data pipeline management"""
    
    def __init__(self, db: Session):
        super().__init__(DataPipeline, db)
    
    def get_by_pipeline_id(self, pipeline_id: str) -> Optional[DataPipeline]:
        """Get pipeline by pipeline_id"""
        return self.db.query(DataPipeline).filter(
            DataPipeline.pipeline_id == pipeline_id
        ).first()
    
    def get_active_pipelines(self, tenant_id: str) -> List[DataPipeline]:
        """Get all active pipelines for a tenant"""
        return self.db.query(DataPipeline).filter(
            and_(
                DataPipeline.tenant_id == tenant_id,
                DataPipeline.is_active == True,
                DataPipeline.status != PipelineStatus.DRAFT
            )
        ).all()
    
    def search_pipelines(
        self,
        tenant_id: str,
        query: Optional[str] = None,
        category: Optional[str] = None,
        status: Optional[PipelineStatus] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DataPipeline]:
        """Search pipelines with filters"""
        q = self.db.query(DataPipeline).filter(DataPipeline.tenant_id == tenant_id)
        
        if query:
            q = q.filter(
                or_(
                    DataPipeline.name.ilike(f"%{query}%"),
                    DataPipeline.description.ilike(f"%{query}%")
                )
            )
        
        if category:
            q = q.filter(DataPipeline.category == category)
        
        if status:
            q = q.filter(DataPipeline.status == status)
        
        if tags:
            for tag in tags:
                q = q.filter(DataPipeline.tags.contains([tag]))
        
        return q.offset(offset).limit(limit).all()
    
    def get_scheduled_pipelines(self, tenant_id: str) -> List[DataPipeline]:
        """Get pipelines that need scheduling"""
        return self.db.query(DataPipeline).filter(
            and_(
                DataPipeline.tenant_id == tenant_id,
                DataPipeline.is_active == True,
                DataPipeline.status == PipelineStatus.SCHEDULED,
                DataPipeline.schedule_type != ScheduleType.ONCE
            )
        ).all()
    
    def clone_pipeline(self, pipeline_id: str, new_name: str, user_id: str) -> DataPipeline:
        """Clone an existing pipeline"""
        original = self.get_by_pipeline_id(pipeline_id)
        if not original:
            raise ValueError(f"Pipeline {pipeline_id} not found")
        
        # Create new pipeline with copied config
        new_pipeline = DataPipeline(
            pipeline_id=f"pipeline-{datetime.utcnow().timestamp()}",
            tenant_id=original.tenant_id,
            name=new_name,
            description=f"Cloned from {original.name}",
            category=original.category,
            config=original.config.copy(),
            source_configs=original.source_configs.copy(),
            sink_configs=original.sink_configs.copy(),
            transform_configs=original.transform_configs.copy() if original.transform_configs else [],
            input_schema=original.input_schema,
            output_schema=original.output_schema,
            parallelism=original.parallelism,
            status=PipelineStatus.DRAFT,
            created_by=user_id
        )
        
        self.db.add(new_pipeline)
        self.db.commit()
        return new_pipeline
    
    def update_status(self, pipeline_id: str, status: PipelineStatus) -> bool:
        """Update pipeline status"""
        pipeline = self.get_by_pipeline_id(pipeline_id)
        if pipeline:
            pipeline.status = status
            pipeline.updated_at = datetime.utcnow()
            if status == PipelineStatus.READY:
                pipeline.published_at = datetime.utcnow()
            self.db.commit()
            return True
        return False


class PipelineJobRepository(BaseRepository[PipelineJob]):
    """Repository for pipeline job management"""
    
    def __init__(self, db: Session):
        super().__init__(PipelineJob, db)
    
    def get_by_job_id(self, job_id: str) -> Optional[PipelineJob]:
        """Get job by job_id"""
        return self.db.query(PipelineJob).filter(
            PipelineJob.job_id == job_id
        ).first()
    
    def get_active_jobs(self, tenant_id: str) -> List[PipelineJob]:
        """Get all active jobs"""
        return self.db.query(PipelineJob).filter(
            and_(
                PipelineJob.tenant_id == tenant_id,
                PipelineJob.status.in_([JobStatus.PENDING, JobStatus.RUNNING])
            )
        ).all()
    
    def get_pipeline_jobs(
        self,
        pipeline_id: str,
        status: Optional[JobStatus] = None,
        limit: int = 100
    ) -> List[PipelineJob]:
        """Get jobs for a pipeline"""
        q = self.db.query(PipelineJob).filter(
            PipelineJob.pipeline_id == pipeline_id
        )
        
        if status:
            q = q.filter(PipelineJob.status == status)
        
        return q.order_by(desc(PipelineJob.created_at)).limit(limit).all()
    
    def get_recent_jobs(
        self,
        tenant_id: str,
        hours: int = 24,
        status: Optional[JobStatus] = None
    ) -> List[PipelineJob]:
        """Get recent jobs within specified hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        q = self.db.query(PipelineJob).filter(
            and_(
                PipelineJob.tenant_id == tenant_id,
                PipelineJob.created_at >= cutoff_time
            )
        )
        
        if status:
            q = q.filter(PipelineJob.status == status)
        
        return q.order_by(desc(PipelineJob.created_at)).all()
    
    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update job status and metrics"""
        job = self.get_by_job_id(job_id)
        if not job:
            return False
        
        job.status = status
        
        if status == JobStatus.RUNNING and not job.started_at:
            job.started_at = datetime.utcnow()
        
        if status in [JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED]:
            job.completed_at = datetime.utcnow()
            if job.started_at:
                job.duration_seconds = int((job.completed_at - job.started_at).total_seconds())
        
        if error_message:
            job.error_message = error_message
        
        if metrics:
            for key, value in metrics.items():
                if hasattr(job, key):
                    setattr(job, key, value)
        
        self.db.commit()
        return True
    
    def get_job_statistics(self, tenant_id: str, days: int = 7) -> Dict[str, Any]:
        """Get job execution statistics"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get job counts by status
        status_counts = self.db.query(
            PipelineJob.status,
            func.count(PipelineJob.id)
        ).filter(
            and_(
                PipelineJob.tenant_id == tenant_id,
                PipelineJob.created_at >= cutoff_date
            )
        ).group_by(PipelineJob.status).all()
        
        # Get average duration
        avg_duration = self.db.query(
            func.avg(PipelineJob.duration_seconds)
        ).filter(
            and_(
                PipelineJob.tenant_id == tenant_id,
                PipelineJob.status == JobStatus.SUCCESS,
                PipelineJob.completed_at >= cutoff_date
            )
        ).scalar()
        
        # Get total records processed
        total_records = self.db.query(
            func.sum(PipelineJob.records_written)
        ).filter(
            and_(
                PipelineJob.tenant_id == tenant_id,
                PipelineJob.status == JobStatus.SUCCESS,
                PipelineJob.completed_at >= cutoff_date
            )
        ).scalar()
        
        return {
            "status_distribution": dict(status_counts),
            "average_duration_seconds": float(avg_duration) if avg_duration else 0,
            "total_records_processed": int(total_records) if total_records else 0,
            "period_days": days
        }


class DataSourceRepository(BaseRepository[DataSource]):
    """Repository for data source management"""
    
    def __init__(self, db: Session):
        super().__init__(DataSource, db)
    
    def get_by_source_id(self, source_id: str) -> Optional[DataSource]:
        """Get source by source_id"""
        return self.db.query(DataSource).filter(
            DataSource.source_id == source_id
        ).first()
    
    def get_active_sources(
        self,
        tenant_id: str,
        source_type: Optional[SourceType] = None
    ) -> List[DataSource]:
        """Get active data sources"""
        q = self.db.query(DataSource).filter(
            and_(
                DataSource.tenant_id == tenant_id,
                DataSource.is_active == True
            )
        )
        
        if source_type:
            q = q.filter(DataSource.source_type == source_type)
        
        return q.all()
    
    def search_sources(
        self,
        tenant_id: str,
        query: Optional[str] = None,
        source_type: Optional[SourceType] = None,
        tags: Optional[List[str]] = None
    ) -> List[DataSource]:
        """Search data sources"""
        q = self.db.query(DataSource).filter(DataSource.tenant_id == tenant_id)
        
        if query:
            q = q.filter(
                or_(
                    DataSource.name.ilike(f"%{query}%"),
                    DataSource.description.ilike(f"%{query}%")
                )
            )
        
        if source_type:
            q = q.filter(DataSource.source_type == source_type)
        
        if tags:
            for tag in tags:
                q = q.filter(DataSource.tags.contains([tag]))
        
        return q.all()
    
    def update_health_status(
        self,
        source_id: str,
        health_status: str,
        health_details: Optional[Dict[str, Any]] = None
    ):
        """Update source health status"""
        source = self.get_by_source_id(source_id)
        if source:
            source.health_status = health_status
            source.health_details = health_details
            source.last_health_check = datetime.utcnow()
            self.db.commit()


class DataSinkRepository(BaseRepository[DataSink]):
    """Repository for data sink management"""
    
    def __init__(self, db: Session):
        super().__init__(DataSink, db)
    
    def get_by_sink_id(self, sink_id: str) -> Optional[DataSink]:
        """Get sink by sink_id"""
        return self.db.query(DataSink).filter(
            DataSink.sink_id == sink_id
        ).first()
    
    def get_active_sinks(
        self,
        tenant_id: str,
        sink_type: Optional[SinkType] = None
    ) -> List[DataSink]:
        """Get active data sinks"""
        q = self.db.query(DataSink).filter(
            and_(
                DataSink.tenant_id == tenant_id,
                DataSink.is_active == True
            )
        )
        
        if sink_type:
            q = q.filter(DataSink.sink_type == sink_type)
        
        return q.all()
    
    def search_sinks(
        self,
        tenant_id: str,
        query: Optional[str] = None,
        sink_type: Optional[SinkType] = None,
        tags: Optional[List[str]] = None
    ) -> List[DataSink]:
        """Search data sinks"""
        q = self.db.query(DataSink).filter(DataSink.tenant_id == tenant_id)
        
        if query:
            q = q.filter(
                or_(
                    DataSink.name.ilike(f"%{query}%"),
                    DataSink.description.ilike(f"%{query}%")
                )
            )
        
        if sink_type:
            q = q.filter(DataSink.sink_type == sink_type)
        
        if tags:
            for tag in tags:
                q = q.filter(DataSink.tags.contains([tag]))
        
        return q.all()


class PipelineTemplateRepository(BaseRepository[PipelineTemplate]):
    """Repository for pipeline template management"""
    
    def __init__(self, db: Session):
        super().__init__(PipelineTemplate, db)
    
    def get_by_template_id(self, template_id: str) -> Optional[PipelineTemplate]:
        """Get template by template_id"""
        return self.db.query(PipelineTemplate).filter(
            PipelineTemplate.template_id == template_id
        ).first()
    
    def get_public_templates(self, category: Optional[str] = None) -> List[PipelineTemplate]:
        """Get public templates"""
        q = self.db.query(PipelineTemplate).filter(
            and_(
                PipelineTemplate.is_public == True,
                PipelineTemplate.is_latest == True
            )
        )
        
        if category:
            q = q.filter(PipelineTemplate.category == category)
        
        return q.order_by(desc(PipelineTemplate.usage_count)).all()
    
    def get_tenant_templates(
        self,
        tenant_id: str,
        category: Optional[str] = None
    ) -> List[PipelineTemplate]:
        """Get templates for a tenant"""
        q = self.db.query(PipelineTemplate).filter(
            and_(
                PipelineTemplate.tenant_id == tenant_id,
                PipelineTemplate.is_latest == True
            )
        )
        
        if category:
            q = q.filter(PipelineTemplate.category == category)
        
        return q.order_by(PipelineTemplate.name).all()
    
    def increment_usage(self, template_id: str):
        """Increment template usage count"""
        template = self.get_by_template_id(template_id)
        if template:
            template.usage_count += 1
            self.db.commit()


class TransformFunctionRepository(BaseRepository[TransformFunction]):
    """Repository for transform function management"""
    
    def __init__(self, db: Session):
        super().__init__(TransformFunction, db)
    
    def get_by_function_id(self, function_id: str) -> Optional[TransformFunction]:
        """Get function by function_id"""
        return self.db.query(TransformFunction).filter(
            TransformFunction.function_id == function_id
        ).first()
    
    def get_functions(
        self,
        tenant_id: str,
        transform_type: Optional[str] = None,
        include_public: bool = True
    ) -> List[TransformFunction]:
        """Get transform functions"""
        conditions = []
        
        if include_public:
            conditions.append(TransformFunction.is_public == True)
        
        conditions.append(
            and_(
                TransformFunction.tenant_id == tenant_id,
                TransformFunction.is_public == False
            )
        )
        
        q = self.db.query(TransformFunction).filter(or_(*conditions))
        
        if transform_type:
            q = q.filter(TransformFunction.transform_type == transform_type)
        
        return q.order_by(TransformFunction.name).all()


class PipelineMetricsRepository(BaseRepository[PipelineMetrics]):
    """Repository for pipeline metrics"""
    
    def __init__(self, db: Session):
        super().__init__(PipelineMetrics, db)
    
    def record_metric(
        self,
        tenant_id: str,
        pipeline_id: str,
        metric_name: str,
        metric_value: float,
        job_id: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ):
        """Record a pipeline metric"""
        metric = PipelineMetrics(
            tenant_id=tenant_id,
            pipeline_id=pipeline_id,
            job_id=job_id,
            metric_name=metric_name,
            metric_value=metric_value,
            tags=tags
        )
        self.db.add(metric)
        self.db.commit()
    
    def get_metrics(
        self,
        pipeline_id: str,
        metric_name: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[PipelineMetrics]:
        """Get pipeline metrics"""
        q = self.db.query(PipelineMetrics).filter(
            PipelineMetrics.pipeline_id == pipeline_id
        )
        
        if metric_name:
            q = q.filter(PipelineMetrics.metric_name == metric_name)
        
        if start_time:
            q = q.filter(PipelineMetrics.timestamp >= start_time)
        
        if end_time:
            q = q.filter(PipelineMetrics.timestamp <= end_time)
        
        return q.order_by(PipelineMetrics.timestamp).all()
    
    def get_aggregated_metrics(
        self,
        pipeline_id: str,
        metric_name: str,
        aggregation: str = "avg",  # avg, sum, min, max
        interval: str = "hour",  # hour, day, week
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """Get aggregated metrics over time"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Select appropriate aggregation function
        agg_func = {
            "avg": func.avg,
            "sum": func.sum,
            "min": func.min,
            "max": func.max
        }.get(aggregation, func.avg)
        
        # Build time truncation based on interval
        if interval == "hour":
            time_trunc = func.date_trunc('hour', PipelineMetrics.timestamp)
        elif interval == "day":
            time_trunc = func.date_trunc('day', PipelineMetrics.timestamp)
        else:  # week
            time_trunc = func.date_trunc('week', PipelineMetrics.timestamp)
        
        results = self.db.query(
            time_trunc.label('time_bucket'),
            agg_func(PipelineMetrics.metric_value).label('value'),
            func.count(PipelineMetrics.id).label('count')
        ).filter(
            and_(
                PipelineMetrics.pipeline_id == pipeline_id,
                PipelineMetrics.metric_name == metric_name,
                PipelineMetrics.timestamp >= cutoff_date
            )
        ).group_by('time_bucket').order_by('time_bucket').all()
        
        return [
            {
                "timestamp": r.time_bucket,
                "value": float(r.value),
                "count": r.count
            }
            for r in results
        ] 