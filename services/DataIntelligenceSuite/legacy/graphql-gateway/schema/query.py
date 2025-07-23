"""
GraphQL Query Type

Root query type for all DataIntelligenceSuite services.
"""

from typing import List, Optional, Dict, Any
import strawberry
from strawberry.types import Info

from .types import (
    Pipeline, PipelineExecution, PipelineMetrics,
    DataQualityProfile, QualityIssue, QualityRule,
    CacheRegion, CacheEntry, SyncTask,
    ServiceHealth, Alert, DataLineage,
    SearchResult, PaginationInput, SortInput,
    PipelineFilter, QualityFilter,
    # New types for expanded services
    CatalogEntity, Schema, Classification, GlossaryTerm,
    MLModel, ModelVersion, TrainingJob, ModelDeployment,
    StreamJob, BatchJob, GraphAnalysis,
    Workflow, WorkflowExecution, WorkflowSchedule,
    IngestionSource, IngestionJob
)

from .connector_types import (
    ConnectorConfig, ConnectorStatus, ProcessorInfo,
    ProcessingJob, BatchProcessingResult
)


@strawberry.type
class Query:
    """Root query type"""
    
    # Data Catalog Queries
    @strawberry.field
    async def search_catalog(
        self,
        info: Info,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        pagination: Optional[PaginationInput] = None
    ) -> SearchResult:
        """Search the data catalog"""
        resolver = info.context["service_resolver"]
        return await resolver.search_catalog(query, filters, pagination)
    
    @strawberry.field
    async def get_entity(self, info: Info, id: str) -> Optional[CatalogEntity]:
        """Get a catalog entity by ID"""
        resolver = info.context["service_resolver"]
        return await resolver.get_catalog_entity(id)
    
    @strawberry.field
    async def get_schemas(
        self,
        info: Info,
        subject: Optional[str] = None
    ) -> List[Schema]:
        """List schemas from the registry"""
        resolver = info.context["service_resolver"]
        return await resolver.get_schemas(subject)
    
    @strawberry.field
    async def get_classifications(self, info: Info) -> List[Classification]:
        """List all data classifications"""
        resolver = info.context["service_resolver"]
        return await resolver.get_classifications()
    
    @strawberry.field
    async def get_glossary_terms(
        self,
        info: Info,
        glossary_id: Optional[str] = None
    ) -> List[GlossaryTerm]:
        """List glossary terms"""
        resolver = info.context["service_resolver"]
        return await resolver.get_glossary_terms(glossary_id)
    
    # ML/AI Queries
    @strawberry.field
    async def ml_models(
        self,
        info: Info,
        filter: Optional[Dict[str, Any]] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[MLModel]:
        """List ML models"""
        resolver = info.context["service_resolver"]
        return await resolver.get_ml_models(filter, pagination)
    
    @strawberry.field
    async def ml_model(self, info: Info, id: str) -> Optional[MLModel]:
        """Get a specific ML model"""
        dataloader = info.context["dataloader_registry"].get_model_loader()
        return await dataloader.load(id)
    
    @strawberry.field
    async def model_versions(
        self,
        info: Info,
        model_id: str
    ) -> List[ModelVersion]:
        """Get versions of a model"""
        resolver = info.context["service_resolver"]
        return await resolver.get_model_versions(model_id)
    
    @strawberry.field
    async def training_jobs(
        self,
        info: Info,
        status: Optional[str] = None,
        limit: int = 20
    ) -> List[TrainingJob]:
        """List training jobs"""
        resolver = info.context["service_resolver"]
        return await resolver.get_training_jobs(status, limit)
    
    @strawberry.field
    async def model_deployments(
        self,
        info: Info,
        model_id: Optional[str] = None
    ) -> List[ModelDeployment]:
        """List model deployments"""
        resolver = info.context["service_resolver"]
        return await resolver.get_model_deployments(model_id)
    
    # Stream Processing Queries
    @strawberry.field
    async def stream_jobs(
        self,
        info: Info,
        status: Optional[str] = None
    ) -> List[StreamJob]:
        """List stream processing jobs"""
        resolver = info.context["service_resolver"]
        return await resolver.get_stream_jobs(status)
    
    @strawberry.field
    async def stream_job(self, info: Info, id: str) -> Optional[StreamJob]:
        """Get a specific stream job"""
        resolver = info.context["service_resolver"]
        return await resolver.get_stream_job(id)
    
    # Batch Processing Queries
    @strawberry.field
    async def batch_jobs(
        self,
        info: Info,
        status: Optional[str] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[BatchJob]:
        """List batch processing jobs"""
        resolver = info.context["service_resolver"]
        return await resolver.get_batch_jobs(status, pagination)
    
    @strawberry.field
    async def batch_job(self, info: Info, id: str) -> Optional[BatchJob]:
        """Get a specific batch job"""
        resolver = info.context["service_resolver"]
        return await resolver.get_batch_job(id)
    
    # Graph Processing Queries
    @strawberry.field
    async def graph_analysis(
        self,
        info: Info,
        graph_id: str,
        analysis_type: str
    ) -> GraphAnalysis:
        """Run graph analysis"""
        resolver = info.context["service_resolver"]
        return await resolver.run_graph_analysis(graph_id, analysis_type)
    
    # Workflow/Orchestration Queries
    @strawberry.field
    async def workflows(
        self,
        info: Info,
        filter: Optional[Dict[str, Any]] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[Workflow]:
        """List workflows"""
        resolver = info.context["service_resolver"]
        return await resolver.get_workflows(filter, pagination)
    
    @strawberry.field
    async def workflow(self, info: Info, id: str) -> Optional[Workflow]:
        """Get a specific workflow"""
        resolver = info.context["service_resolver"]
        return await resolver.get_workflow(id)
    
    @strawberry.field
    async def workflow_executions(
        self,
        info: Info,
        workflow_id: str,
        limit: int = 10
    ) -> List[WorkflowExecution]:
        """Get workflow executions"""
        resolver = info.context["service_resolver"]
        return await resolver.get_workflow_executions(workflow_id, limit)
    
    @strawberry.field
    async def workflow_schedules(self, info: Info) -> List[WorkflowSchedule]:
        """List all workflow schedules"""
        resolver = info.context["service_resolver"]
        return await resolver.get_workflow_schedules()
    
    # Data Ingestion Queries
    @strawberry.field
    async def ingestion_sources(self, info: Info) -> List[IngestionSource]:
        """List ingestion sources"""
        resolver = info.context["service_resolver"]
        return await resolver.get_ingestion_sources()
    
    @strawberry.field
    async def ingestion_jobs(
        self,
        info: Info,
        source_id: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[IngestionJob]:
        """List ingestion jobs"""
        resolver = info.context["service_resolver"]
        return await resolver.get_ingestion_jobs(source_id, status)
    
    # Existing queries remain unchanged
    # Pipeline Queries
    @strawberry.field
    async def pipelines(
        self,
        info: Info,
        filter: Optional[PipelineFilter] = None,
        pagination: Optional[PaginationInput] = None,
        sort: Optional[SortInput] = None
    ) -> List[Pipeline]:
        """List pipelines with optional filtering and pagination"""
        resolver = info.context["service_resolver"]
        return await resolver.get_pipelines(filter, pagination, sort)
    
    @strawberry.field
    async def pipeline(self, info: Info, id: str) -> Optional[Pipeline]:
        """Get a specific pipeline by ID"""
        dataloader = info.context["dataloader_registry"].get_pipeline_loader()
        return await dataloader.load(id)
    
    @strawberry.field
    async def pipeline_execution(
        self,
        info: Info,
        execution_id: str
    ) -> Optional[PipelineExecution]:
        """Get a specific pipeline execution"""
        resolver = info.context["service_resolver"]
        return await resolver.get_execution(execution_id)
    
    @strawberry.field
    async def pipeline_metrics_summary(
        self,
        info: Info,
        pipeline_ids: Optional[List[str]] = None
    ) -> List[PipelineMetrics]:
        """Get metrics summary for pipelines"""
        resolver = info.context["service_resolver"]
        return await resolver.get_pipeline_metrics_summary(pipeline_ids)
    
    # Data Quality Queries
    @strawberry.field
    async def data_quality_profile(
        self,
        info: Info,
        dataset: str
    ) -> Optional[DataQualityProfile]:
        """Get data quality profile for a dataset"""
        resolver = info.context["service_resolver"]
        return await resolver.get_quality_profile(dataset)
    
    @strawberry.field
    async def quality_issues(
        self,
        info: Info,
        filter: Optional[QualityFilter] = None,
        pagination: Optional[PaginationInput] = None
    ) -> List[QualityIssue]:
        """List quality issues with filtering"""
        resolver = info.context["service_resolver"]
        return await resolver.get_quality_issues(filter, pagination)
    
    @strawberry.field
    async def quality_rules(
        self,
        info: Info,
        enabled_only: bool = True,
        tags: Optional[List[str]] = None
    ) -> List[QualityRule]:
        """List quality rules"""
        resolver = info.context["service_resolver"]
        return await resolver.get_quality_rules(enabled_only, tags)
    
    # Cache/DIH Queries
    @strawberry.field
    async def cache_regions(self, info: Info) -> List[CacheRegion]:
        """List all cache regions"""
        resolver = info.context["service_resolver"]
        return await resolver.get_cache_regions()
    
    @strawberry.field
    async def cache_entry(
        self,
        info: Info,
        region: str,
        key: str
    ) -> Optional[CacheEntry]:
        """Get a specific cache entry"""
        resolver = info.context["service_resolver"]
        return await resolver.get_cache_entry(region, key)
    
    @strawberry.field
    async def sync_tasks(
        self,
        info: Info,
        status: Optional[str] = None
    ) -> List[SyncTask]:
        """List synchronization tasks"""
        resolver = info.context["service_resolver"]
        return await resolver.get_sync_tasks(status)
    
    # Monitoring Queries
    @strawberry.field
    async def service_health(self, info: Info) -> List[ServiceHealth]:
        """Get health status of all services"""
        resolver = info.context["service_resolver"]
        return await resolver.get_all_service_health()
    
    @strawberry.field
    async def alerts(
        self,
        info: Info,
        service: Optional[str] = None,
        acknowledged: Optional[bool] = None,
        limit: int = 100
    ) -> List[Alert]:
        """Get system alerts"""
        resolver = info.context["service_resolver"]
        return await resolver.get_alerts(service, acknowledged, limit)
    
    # Data Lineage Queries
    @strawberry.field
    async def data_lineage(
        self,
        info: Info,
        dataset: str,
        depth: int = 3
    ) -> Optional[DataLineage]:
        """Get data lineage for a dataset"""
        resolver = info.context["service_resolver"]
        return await resolver.get_data_lineage(dataset, depth)
    
    # Search Queries
    @strawberry.field
    async def search(
        self,
        info: Info,
        query: str,
        types: Optional[List[str]] = None,
        pagination: Optional[PaginationInput] = None
    ) -> SearchResult:
        """Global search across all entities"""
        resolver = info.context["service_resolver"]
        return await resolver.search(query, types, pagination)
    
    # Analytics Queries
    @strawberry.field
    async def system_metrics(
        self,
        info: Info,
        metric_names: List[str],
        time_range_hours: int = 24
    ) -> Dict[str, List[float]]:
        """Get system metrics time series"""
        resolver = info.context["service_resolver"]
        return await resolver.get_system_metrics(metric_names, time_range_hours)
    
    @strawberry.field
    async def cost_analysis(
        self,
        info: Info,
        service: Optional[str] = None,
        time_range_days: int = 30
    ) -> Dict[str, Any]:
        """Get cost analysis for services"""
        resolver = info.context["service_resolver"]
        return await resolver.get_cost_analysis(service, time_range_days)
    
    # Connector and Processor Queries
    @strawberry.field
    async def connectors(self, info: Info) -> List[ConnectorStatus]:
        """List all configured connectors"""
        resolver = info.context["service_resolver"]
        if not resolver.connector_resolver:
            return []
        
        connectors = await resolver.connector_resolver.get_connectors()
        return [
            ConnectorStatus(
                connector_id=c["connector_id"],
                type=ConnectorType[c["type"].upper()],
                enabled=c.get("enabled", True),
                last_run=c.get("last_run"),
                next_run=c.get("next_run"),
                status=c.get("status", "active"),
                config=ConnectorConfig(
                    type=ConnectorType[c["type"].upper()],
                    schedule=c.get("schedule"),
                    config=c.get("config", {})
                )
            )
            for c in connectors
        ]
    
    @strawberry.field
    async def connector(self, info: Info, connector_id: str) -> Optional[ConnectorStatus]:
        """Get a specific connector status"""
        resolver = info.context["service_resolver"]
        if not resolver.connector_resolver:
            return None
        
        c = await resolver.connector_resolver.get_connector(connector_id)
        if not c:
            return None
            
        return ConnectorStatus(
            connector_id=c["connector_id"],
            type=ConnectorType[c["type"].upper()],
            enabled=c.get("enabled", True),
            last_run=c.get("last_run"),
            next_run=c.get("next_run"),
            status=c.get("status", "active"),
            config=ConnectorConfig(
                type=ConnectorType[c["type"].upper()],
                schedule=c.get("schedule"),
                config=c.get("config", {})
            )
        )
    
    @strawberry.field
    async def supported_processors(self, info: Info) -> List[ProcessorInfo]:
        """List all supported file processors"""
        resolver = info.context["service_resolver"]
        if not resolver.connector_resolver:
            return []
        
        processors = await resolver.connector_resolver.get_supported_processors()
        return [
            ProcessorInfo(
                processor_type=ProcessorType[p["type"].upper()],
                supported_formats=p["formats"],
                description=p.get("description", ""),
                requires_gpu=p.get("requires_gpu", False),
                max_file_size=p.get("max_file_size", 0)
            )
            for p in processors
        ]
    
    @strawberry.field
    async def processor_info(self, info: Info, processor_type: str) -> Optional[ProcessorInfo]:
        """Get information about a specific processor"""
        resolver = info.context["service_resolver"]
        if not resolver.connector_resolver:
            return None
        
        p = await resolver.connector_resolver.get_processor_info(processor_type)
        if not p:
            return None
            
        return ProcessorInfo(
            processor_type=ProcessorType[p["type"].upper()],
            supported_formats=p["formats"],
            description=p.get("description", ""),
            requires_gpu=p.get("requires_gpu", False),
            max_file_size=p.get("max_file_size", 0)
        )
    
    @strawberry.field
    async def processing_job(self, info: Info, job_id: str) -> Optional[ProcessingJob]:
        """Get a specific processing job"""
        resolver = info.context["service_resolver"]
        if not resolver.connector_resolver:
            return None
        
        job = await resolver.connector_resolver.get_processing_job(job_id)
        if not job:
            return None
            
        return ProcessingJob(
            job_id=job["id"],
            processor_type=ProcessorType[job["processor_type"].upper()],
            status=JobStatus[job["status"].upper()],
            input_file=job.get("input_path", ""),
            output_path=job.get("output_path"),
            started_at=job.get("started_at"),
            completed_at=job.get("completed_at"),
            error=job.get("error"),
            metadata=job.get("metadata", {})
        )
    
    @strawberry.field
    async def processing_jobs(
        self,
        info: Info,
        processor_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20
    ) -> List[ProcessingJob]:
        """List processing jobs"""
        resolver = info.context["service_resolver"]
        if not resolver.connector_resolver:
            return []
        
        jobs = await resolver.connector_resolver.get_processing_jobs(status)
        
        # Filter by processor type if specified
        if processor_type:
            jobs = [j for j in jobs if j["processor_type"].lower() == processor_type.lower()]
        
        # Apply limit
        jobs = jobs[:limit]
        
        return [
            ProcessingJob(
                job_id=job["id"],
                processor_type=ProcessorType[job["processor_type"].upper()],
                status=JobStatus[job["status"].upper()],
                input_file=job.get("input_path", ""),
                output_path=job.get("output_path"),
                started_at=job.get("started_at"),
                completed_at=job.get("completed_at"),
                error=job.get("error"),
                metadata=job.get("metadata", {})
            )
            for job in jobs
        ] 