"""
GraphQL Type Definitions

Unified types for all DataIntelligenceSuite services.
"""

from typing import List, Optional, Any, Dict
from datetime import datetime
from enum import Enum
import strawberry
from strawberry.types import Info


# Enums
@strawberry.enum
class PipelineStatus(Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"
    ARCHIVED = "archived"


@strawberry.enum
class ExecutionStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@strawberry.enum
class QualityMetric(Enum):
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"


@strawberry.enum
class CachePolicy(Enum):
    READ_THROUGH = "read_through"
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    REFRESH_AHEAD = "refresh_ahead"


# Pipeline Types
@strawberry.type
class Pipeline:
    id: str
    name: str
    type: str
    description: str
    status: PipelineStatus
    owner: Optional[str]
    created_at: datetime
    updated_at: datetime
    tags: List[str]
    
    @strawberry.field
    async def executions(
        self, 
        info: Info,
        limit: int = 10,
        status: Optional[ExecutionStatus] = None
    ) -> List['PipelineExecution']:
        """Get pipeline executions"""
        resolver = info.context["service_resolver"]
        return await resolver.get_pipeline_executions(
            self.id, 
            limit=limit,
            status=status
        )
    
    @strawberry.field
    async def metrics(self, info: Info) -> Optional['PipelineMetrics']:
        """Get pipeline metrics"""
        resolver = info.context["service_resolver"]
        return await resolver.get_pipeline_metrics(self.id)


@strawberry.type
class PipelineExecution:
    execution_id: str
    pipeline_id: str
    pipeline_name: str
    status: ExecutionStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    current_step: Optional[str]
    error_count: int
    
    @strawberry.field
    async def pipeline(self, info: Info) -> Optional[Pipeline]:
        """Get pipeline details"""
        dataloader = info.context["dataloader_registry"].get_pipeline_loader()
        return await dataloader.load(self.pipeline_id)
    
    @strawberry.field
    async def steps(self, info: Info) -> List['PipelineStep']:
        """Get execution steps"""
        resolver = info.context["service_resolver"]
        return await resolver.get_execution_steps(self.execution_id)


@strawberry.type
class PipelineStep:
    name: str
    type: str
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    result: Optional[Dict[str, Any]]
    error: Optional[str]


@strawberry.type
class PipelineMetrics:
    pipeline_id: str
    total_executions: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    average_duration_seconds: float
    min_duration_seconds: Optional[float]
    max_duration_seconds: Optional[float]
    last_execution: Optional[datetime]


# Data Quality Types
@strawberry.type
class DataQualityProfile:
    dataset: str
    profiled_at: datetime
    row_count: int
    column_count: int
    quality_score: float
    issues_found: List['QualityIssue']
    
    @strawberry.field
    async def columns(self, info: Info) -> List['ColumnProfile']:
        """Get column profiles"""
        resolver = info.context["service_resolver"]
        return await resolver.get_column_profiles(self.dataset)
    
    @strawberry.field
    async def trends(
        self, 
        info: Info,
        hours: int = 24
    ) -> List['QualityTrend']:
        """Get quality trends"""
        resolver = info.context["service_resolver"]
        return await resolver.get_quality_trends(
            self.dataset,
            hours=hours
        )


@strawberry.type
class ColumnProfile:
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float
    sample_values: List[str]


@strawberry.type
class QualityIssue:
    id: str
    dataset: str
    issue_type: str
    severity: str
    description: str
    detected_at: datetime
    remediation_status: Optional[str]
    
    @strawberry.field
    async def remediation_history(self, info: Info) -> List['RemediationAction']:
        """Get remediation history"""
        resolver = info.context["service_resolver"]
        return await resolver.get_remediation_history(self.id)


@strawberry.type
class RemediationAction:
    action_id: str
    issue_id: str
    action_type: str
    status: str
    executed_at: datetime
    result: Optional[Dict[str, Any]]


@strawberry.type
class QualityTrend:
    dataset: str
    metric: QualityMetric
    timestamp: datetime
    value: float
    trend_direction: str
    change_rate: float


@strawberry.type
class QualityRule:
    id: str
    name: str
    description: str
    type: str
    enabled: bool
    priority: int
    tags: List[str]
    created_at: datetime
    updated_at: datetime
    
    @strawberry.field
    async def execution_stats(self, info: Info) -> 'RuleExecutionStats':
        """Get rule execution statistics"""
        resolver = info.context["service_resolver"]
        return await resolver.get_rule_execution_stats(self.id)


@strawberry.type
class RuleExecutionStats:
    rule_id: str
    total_executions: int
    passed: int
    failed: int
    average_duration_ms: float
    last_executed: Optional[datetime]


# Cache/DIH Types
@strawberry.type
class CacheRegion:
    name: str
    cache_name: str
    policy: CachePolicy
    ttl_seconds: int
    max_entries: int
    current_entries: int
    created_at: datetime
    
    @strawberry.field
    async def stats(self, info: Info) -> 'CacheStats':
        """Get cache statistics"""
        resolver = info.context["service_resolver"]
        return await resolver.get_cache_stats(self.name)


@strawberry.type
class CacheStats:
    region: str
    hit_count: int
    miss_count: int
    eviction_count: int
    hit_rate: float
    average_get_time_ms: float
    average_put_time_ms: float
    memory_usage_bytes: int


@strawberry.type
class CacheEntry:
    key: str
    value: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    access_count: int
    ttl_remaining_seconds: int


@strawberry.type
class SyncTask:
    id: str
    name: str
    source: str
    target: str
    status: str
    mode: str
    last_sync: Optional[datetime]
    next_sync: Optional[datetime]
    records_synced: int
    errors: int


# Monitoring Types
@strawberry.type
class ServiceHealth:
    service_name: str
    status: str
    version: str
    uptime_seconds: int
    checks: List['HealthCheck']


@strawberry.type
class HealthCheck:
    name: str
    status: str
    message: Optional[str]
    last_checked: datetime


@strawberry.type
class Alert:
    id: str
    service: str
    alert_type: str
    severity: str
    message: str
    timestamp: datetime
    acknowledged: bool
    metadata: Dict[str, Any]


# Analytics Types
@strawberry.type
class DataLineage:
    dataset: str
    upstream: List['LineageNode']
    downstream: List['LineageNode']
    transformations: List['Transformation']


@strawberry.type
class LineageNode:
    id: str
    name: str
    type: str
    layer: str
    last_updated: datetime


@strawberry.type
class Transformation:
    id: str
    name: str
    type: str
    input_datasets: List[str]
    output_datasets: List[str]
    sql: Optional[str]
    created_at: datetime


# Search Result Types
@strawberry.type
class SearchResult:
    total_count: int
    items: List[Any]
    facets: Optional[Dict[str, List['Facet']]]


@strawberry.type
class Facet:
    value: str
    count: int


# Pagination Types
@strawberry.input
class PaginationInput:
    offset: int = 0
    limit: int = 20


@strawberry.input
class SortInput:
    field: str
    direction: str = "ASC"


# Filter Types
@strawberry.input
class PipelineFilter:
    status: Optional[PipelineStatus] = None
    type: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None


@strawberry.input
class QualityFilter:
    dataset: Optional[str] = None
    severity: Optional[str] = None
    issue_type: Optional[str] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None 