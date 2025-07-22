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
    PipelineFilter, QualityFilter
)


@strawberry.type
class Query:
    """Root query type"""
    
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