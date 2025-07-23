"""
GraphQL Subscription Type

Real-time subscriptions for DataIntelligenceSuite services.
"""

from typing import AsyncGenerator, Optional
import strawberry
from strawberry.types import Info
import asyncio

from .types import (
    PipelineExecution, QualityIssue, Alert,
    CacheStats, ServiceHealth
)


@strawberry.type
class Subscription:
    """Root subscription type"""
    
    @strawberry.subscription
    async def pipeline_execution_updates(
        self,
        info: Info,
        pipeline_id: Optional[str] = None
    ) -> AsyncGenerator[PipelineExecution, None]:
        """Subscribe to pipeline execution updates"""
        resolver = info.context["service_resolver"]
        
        async for execution in resolver.subscribe_pipeline_executions(pipeline_id):
            yield execution
    
    @strawberry.subscription
    async def quality_issues(
        self,
        info: Info,
        dataset: Optional[str] = None,
        severity: Optional[str] = None
    ) -> AsyncGenerator[QualityIssue, None]:
        """Subscribe to data quality issues"""
        resolver = info.context["service_resolver"]
        
        async for issue in resolver.subscribe_quality_issues(dataset, severity):
            yield issue
    
    @strawberry.subscription
    async def system_alerts(
        self,
        info: Info,
        service: Optional[str] = None,
        severity: Optional[str] = None
    ) -> AsyncGenerator[Alert, None]:
        """Subscribe to system alerts"""
        resolver = info.context["service_resolver"]
        
        async for alert in resolver.subscribe_alerts(service, severity):
            yield alert
    
    @strawberry.subscription
    async def cache_statistics(
        self,
        info: Info,
        region: str,
        interval_seconds: int = 10
    ) -> AsyncGenerator[CacheStats, None]:
        """Subscribe to cache statistics updates"""
        resolver = info.context["service_resolver"]
        
        while True:
            stats = await resolver.get_cache_stats(region)
            if stats:
                yield stats
            await asyncio.sleep(interval_seconds)
    
    @strawberry.subscription
    async def service_health_updates(
        self,
        info: Info,
        interval_seconds: int = 30
    ) -> AsyncGenerator[ServiceHealth, None]:
        """Subscribe to service health updates"""
        resolver = info.context["service_resolver"]
        
        while True:
            health_statuses = await resolver.get_all_service_health()
            for health in health_statuses:
                yield health
            await asyncio.sleep(interval_seconds) 