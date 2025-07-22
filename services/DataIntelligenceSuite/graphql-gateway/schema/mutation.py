"""
GraphQL Mutation Type

Root mutation type for all DataIntelligenceSuite services.
"""

from typing import List, Optional, Dict, Any
import strawberry
from strawberry.types import Info

from .types import (
    Pipeline, PipelineExecution, QualityRule,
    CacheRegion, Alert
)


# Input Types for Mutations
@strawberry.input
class PipelineCreateInput:
    name: str
    type: str
    description: str
    config: Dict[str, Any]
    schedule: Optional[Dict[str, Any]] = None
    dependencies: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    owner: Optional[str] = None


@strawberry.input
class PipelineUpdateInput:
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    schedule: Optional[Dict[str, Any]] = None
    dependencies: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    status: Optional[str] = None


@strawberry.input
class ExecutePipelineInput:
    pipeline_id: str
    parameters: Optional[Dict[str, Any]] = None
    trigger_type: str = "manual"


@strawberry.input
class QualityCheckInput:
    dataset: str
    check_type: str = "full"
    auto_remediate: bool = False
    rules: Optional[List[str]] = None


@strawberry.input
class QualityRuleCreateInput:
    name: str
    description: str
    type: str
    conditions: List[Dict[str, Any]]
    actions: List[Dict[str, Any]]
    enabled: bool = True
    priority: int = 0
    tags: Optional[List[str]] = None


@strawberry.input
class CacheInvalidateInput:
    region: str
    keys: Optional[List[str]] = None
    pattern: Optional[str] = None


@strawberry.input
class CacheWarmupInput:
    region: str
    dataset: str
    query: Optional[str] = None


# Result Types
@strawberry.type
class MutationResult:
    success: bool
    message: Optional[str]
    errors: Optional[List[str]]


@strawberry.type
class PipelineResult(MutationResult):
    pipeline: Optional[Pipeline]


@strawberry.type
class ExecutionResult(MutationResult):
    execution: Optional[PipelineExecution]


@strawberry.type
class QualityCheckResult(MutationResult):
    dataset: str
    quality_score: Optional[float]
    issues_found: Optional[int]
    issues_remediated: Optional[int]


@strawberry.type
class CacheResult(MutationResult):
    region: str
    keys_affected: int


# Mutation Type
@strawberry.type
class Mutation:
    """Root mutation type"""
    
    # Pipeline Mutations
    @strawberry.mutation
    async def create_pipeline(
        self,
        info: Info,
        input: PipelineCreateInput
    ) -> PipelineResult:
        """Create a new pipeline"""
        resolver = info.context["service_resolver"]
        try:
            pipeline = await resolver.create_pipeline(input)
            return PipelineResult(
                success=True,
                pipeline=pipeline
            )
        except Exception as e:
            return PipelineResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def update_pipeline(
        self,
        info: Info,
        id: str,
        input: PipelineUpdateInput
    ) -> PipelineResult:
        """Update an existing pipeline"""
        resolver = info.context["service_resolver"]
        try:
            pipeline = await resolver.update_pipeline(id, input)
            return PipelineResult(
                success=True,
                pipeline=pipeline
            )
        except Exception as e:
            return PipelineResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def delete_pipeline(
        self,
        info: Info,
        id: str
    ) -> MutationResult:
        """Delete a pipeline"""
        resolver = info.context["service_resolver"]
        try:
            await resolver.delete_pipeline(id)
            return MutationResult(
                success=True,
                message=f"Pipeline {id} deleted successfully"
            )
        except Exception as e:
            return MutationResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def execute_pipeline(
        self,
        info: Info,
        input: ExecutePipelineInput
    ) -> ExecutionResult:
        """Execute a pipeline"""
        resolver = info.context["service_resolver"]
        try:
            execution = await resolver.execute_pipeline(input)
            return ExecutionResult(
                success=True,
                execution=execution
            )
        except Exception as e:
            return ExecutionResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def cancel_pipeline_execution(
        self,
        info: Info,
        execution_id: str
    ) -> MutationResult:
        """Cancel a running pipeline execution"""
        resolver = info.context["service_resolver"]
        try:
            await resolver.cancel_execution(execution_id)
            return MutationResult(
                success=True,
                message=f"Execution {execution_id} cancelled"
            )
        except Exception as e:
            return MutationResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    # Data Quality Mutations
    @strawberry.mutation
    async def run_quality_check(
        self,
        info: Info,
        input: QualityCheckInput
    ) -> QualityCheckResult:
        """Run data quality check on a dataset"""
        resolver = info.context["service_resolver"]
        try:
            result = await resolver.run_quality_check(input)
            return QualityCheckResult(
                success=True,
                dataset=input.dataset,
                quality_score=result.get("quality_score"),
                issues_found=result.get("issues_found"),
                issues_remediated=result.get("issues_remediated")
            )
        except Exception as e:
            return QualityCheckResult(
                success=False,
                dataset=input.dataset,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def create_quality_rule(
        self,
        info: Info,
        input: QualityRuleCreateInput
    ) -> MutationResult:
        """Create a new quality rule"""
        resolver = info.context["service_resolver"]
        try:
            await resolver.create_quality_rule(input)
            return MutationResult(
                success=True,
                message="Quality rule created successfully"
            )
        except Exception as e:
            return MutationResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    # Cache Mutations
    @strawberry.mutation
    async def invalidate_cache(
        self,
        info: Info,
        input: CacheInvalidateInput
    ) -> CacheResult:
        """Invalidate cache entries"""
        resolver = info.context["service_resolver"]
        try:
            keys_affected = await resolver.invalidate_cache(input)
            return CacheResult(
                success=True,
                region=input.region,
                keys_affected=keys_affected,
                message=f"Invalidated {keys_affected} keys"
            )
        except Exception as e:
            return CacheResult(
                success=False,
                region=input.region,
                keys_affected=0,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def warmup_cache(
        self,
        info: Info,
        input: CacheWarmupInput
    ) -> CacheResult:
        """Warm up cache with dataset"""
        resolver = info.context["service_resolver"]
        try:
            keys_loaded = await resolver.warmup_cache(input)
            return CacheResult(
                success=True,
                region=input.region,
                keys_affected=keys_loaded,
                message=f"Loaded {keys_loaded} keys into cache"
            )
        except Exception as e:
            return CacheResult(
                success=False,
                region=input.region,
                keys_affected=0,
                message=str(e),
                errors=[str(e)]
            )
    
    # Monitoring Mutations
    @strawberry.mutation
    async def acknowledge_alert(
        self,
        info: Info,
        alert_id: str
    ) -> MutationResult:
        """Acknowledge an alert"""
        resolver = info.context["service_resolver"]
        try:
            await resolver.acknowledge_alert(alert_id)
            return MutationResult(
                success=True,
                message=f"Alert {alert_id} acknowledged"
            )
        except Exception as e:
            return MutationResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            )
    
    @strawberry.mutation
    async def trigger_sync(
        self,
        info: Info,
        task_id: str
    ) -> MutationResult:
        """Manually trigger a sync task"""
        resolver = info.context["service_resolver"]
        try:
            await resolver.trigger_sync(task_id)
            return MutationResult(
                success=True,
                message=f"Sync task {task_id} triggered"
            )
        except Exception as e:
            return MutationResult(
                success=False,
                message=str(e),
                errors=[str(e)]
            ) 