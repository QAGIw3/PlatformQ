"""
Service Resolver

Communicates with backend services to resolve GraphQL queries.
"""

from typing import List, Optional, Dict, Any, AsyncGenerator
import httpx
import asyncio
from datetime import datetime

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration
from .connector_resolver import ConnectorResolver

logger = StructuredLogger.get_logger(__name__)


class ServiceResolver:
    """
    Resolves GraphQL queries by communicating with backend services
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration):
        self.vault_consul = vault_consul
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.service_urls = {}
        self.event_subscribers = {}
        self.connector_resolver = None
    
    async def initialize(self):
        """Initialize service resolver"""
        logger.info("initializing_service_resolver")
        
        # Discover services via Consul
        await self._discover_services()
        
        # Initialize connector resolver
        self.connector_resolver = ConnectorResolver(self.service_urls)
        
        # Setup event subscriptions
        await self._setup_event_subscriptions()
        
        logger.info("service_resolver_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.http_client.aclose()
        
        # Cleanup connector resolver
        if self.connector_resolver:
            await self.connector_resolver.cleanup()
        
        # Cleanup event subscriptions
        for subscriber in self.event_subscribers.values():
            await subscriber.close()
    
    async def is_healthy(self) -> bool:
        """Check resolver health"""
        # Check if we can reach at least one service
        for service_name, url in self.service_urls.items():
            try:
                response = await self.http_client.get(f"{url}/health")
                if response.status_code == 200:
                    return True
            except Exception:
                continue
        return False
    
    async def _discover_services(self):
        """Discover services via Consul"""
        services = [
            ("data-catalog-service", 8001),
            ("data-ingestion-service", 8002),
            ("stream-processing-service", 8003),
            ("batch-processing-service", 8004),
            ("graph-processing-service", 8005),
            ("quality-engine-service", 8006),
            ("mlops-service", 8007),
            ("workflow-engine-service", 8008),
        ]
        
        for service_name, default_port in services:
            try:
                # Try to discover via Consul
                service_info = await self.vault_consul.consul.catalog.service(service_name)
                if service_info:
                    # Use first instance
                    instance = service_info[0]
                    host = instance["ServiceAddress"] or instance["Address"]
                    port = instance["ServicePort"]
                    self.service_urls[service_name] = f"http://{host}:{port}"
                else:
                    # Fallback to localhost
                    self.service_urls[service_name] = f"http://localhost:{default_port}"
            except Exception as e:
                logger.warning(f"Failed to discover {service_name}: {e}")
                self.service_urls[service_name] = f"http://localhost:{default_port}"
    
    async def _setup_event_subscriptions(self):
        """Setup event subscriptions for real-time updates"""
        # This would setup Pulsar subscriptions for real-time data
        pass
    
    # Pipeline Operations
    async def get_pipelines(
        self,
        filter: Optional[Any] = None,
        pagination: Optional[Any] = None,
        sort: Optional[Any] = None
    ) -> List[Any]:
        """Get pipelines from workflow engine service"""
        url = f"{self.service_urls['workflow-engine-service']}/api/v1/workflows"
        
        params = {}
        if filter:
            if filter.status:
                params["status"] = filter.status.value
            if filter.type:
                params["type"] = filter.type
            if filter.owner:
                params["owner"] = filter.owner
            if filter.tags:
                params["tags"] = ",".join(filter.tags)
        
        if pagination:
            params["offset"] = pagination.offset
            params["limit"] = pagination.limit
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        
        pipelines = response.json()
        return [self._convert_pipeline(p) for p in pipelines]
    
    async def get_pipeline(self, pipeline_id: str) -> Optional[Any]:
        """Get a specific pipeline"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/pipelines/{pipeline_id}"
        
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return self._convert_pipeline(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def create_pipeline(self, input: Any) -> Any:
        """Create a new pipeline"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/pipelines"
        
        data = {
            "name": input.name,
            "type": input.type,
            "description": input.description,
            "config": input.config,
            "schedule": input.schedule,
            "dependencies": input.dependencies,
            "tags": input.tags,
            "owner": input.owner
        }
        
        response = await self.http_client.post(url, json=data)
        response.raise_for_status()
        
        return self._convert_pipeline(response.json())
    
    async def update_pipeline(self, pipeline_id: str, input: Any) -> Any:
        """Update a pipeline"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/pipelines/{pipeline_id}"
        
        data = {}
        if input.name is not None:
            data["name"] = input.name
        if input.description is not None:
            data["description"] = input.description
        if input.config is not None:
            data["config"] = input.config
        if input.schedule is not None:
            data["schedule"] = input.schedule
        if input.dependencies is not None:
            data["dependencies"] = input.dependencies
        if input.tags is not None:
            data["tags"] = input.tags
        if input.status is not None:
            data["status"] = input.status
        
        response = await self.http_client.put(url, json=data)
        response.raise_for_status()
        
        return self._convert_pipeline(response.json())
    
    async def delete_pipeline(self, pipeline_id: str):
        """Delete a pipeline"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/pipelines/{pipeline_id}"
        
        response = await self.http_client.delete(url)
        response.raise_for_status()
    
    async def execute_pipeline(self, input: Any) -> Any:
        """Execute a pipeline"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/executions"
        
        data = {
            "pipeline_id": input.pipeline_id,
            "parameters": input.parameters,
            "trigger_type": input.trigger_type
        }
        
        response = await self.http_client.post(url, json=data)
        response.raise_for_status()
        
        execution_id = response.json()["execution_id"]
        return await self.get_execution(execution_id)
    
    async def get_execution(self, execution_id: str) -> Optional[Any]:
        """Get pipeline execution details"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/executions/{execution_id}"
        
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return self._convert_execution(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def cancel_execution(self, execution_id: str):
        """Cancel a pipeline execution"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/executions/{execution_id}/cancel"
        
        response = await self.http_client.post(url)
        response.raise_for_status()
    
    async def get_pipeline_executions(
        self,
        pipeline_id: str,
        limit: int = 10,
        status: Optional[Any] = None
    ) -> List[Any]:
        """Get executions for a pipeline"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/executions"
        
        params = {
            "pipeline_id": pipeline_id,
            "limit": limit
        }
        if status:
            params["status"] = status.value
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        
        executions = response.json()
        return [self._convert_execution(e) for e in executions]
    
    async def get_pipeline_metrics(self, pipeline_id: str) -> Optional[Any]:
        """Get pipeline metrics"""
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/monitoring/metrics/{pipeline_id}"
        
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return self._convert_metrics(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def get_execution_steps(self, execution_id: str) -> List[Any]:
        """Get execution steps"""
        # This would be part of the execution details
        execution = await self.get_execution(execution_id)
        if execution and hasattr(execution, 'steps'):
            return execution.steps
        return []
    
    # Data Quality Operations
    async def get_quality_profile(self, dataset: str) -> Optional[Any]:
        """Get data quality profile"""
        url = f"{self.service_urls['data-quality-service']}/api/v1/profiling/profile/{dataset}"
        
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return self._convert_quality_profile(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def get_quality_issues(
        self,
        filter: Optional[Any] = None,
        pagination: Optional[Any] = None
    ) -> List[Any]:
        """Get quality issues"""
        url = f"{self.service_urls['data-quality-service']}/api/v1/quality/issues"
        
        params = {}
        if filter:
            if filter.dataset:
                params["dataset"] = filter.dataset
            if filter.severity:
                params["severity"] = filter.severity
            if filter.issue_type:
                params["issue_type"] = filter.issue_type
        
        if pagination:
            params["offset"] = pagination.offset
            params["limit"] = pagination.limit
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        
        issues = response.json()
        return [self._convert_quality_issue(i) for i in issues]
    
    async def run_quality_check(self, input: Any) -> Dict[str, Any]:
        """Run quality check"""
        url = f"{self.service_urls['data-quality-service']}/api/v1/quality/check"
        
        data = {
            "dataset": input.dataset,
            "check_type": input.check_type,
            "auto_remediate": input.auto_remediate,
            "rules": input.rules
        }
        
        response = await self.http_client.post(url, json=data)
        response.raise_for_status()
        
        return response.json()
    
    # Cache Operations
    async def get_cache_regions(self) -> List[Any]:
        """Get cache regions"""
        url = f"{self.service_urls['dih-service']}/api/v1/cache/regions"
        
        response = await self.http_client.get(url)
        response.raise_for_status()
        
        regions = response.json()
        return [self._convert_cache_region(r) for r in regions]
    
    async def get_cache_stats(self, region: str) -> Optional[Any]:
        """Get cache statistics"""
        url = f"{self.service_urls['dih-service']}/api/v1/cache/regions/{region}/stats"
        
        try:
            response = await self.http_client.get(url)
            response.raise_for_status()
            return self._convert_cache_stats(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def invalidate_cache(self, input: Any) -> int:
        """Invalidate cache entries"""
        url = f"{self.service_urls['dih-service']}/api/v1/cache/regions/{input.region}/invalidate"
        
        data = {}
        if input.keys:
            data["keys"] = input.keys
        if input.pattern:
            data["pattern"] = input.pattern
        
        response = await self.http_client.post(url, json=data)
        response.raise_for_status()
        
        return response.json().get("keys_affected", 0)
    
    # Monitoring Operations
    async def get_all_service_health(self) -> List[Any]:
        """Get health status of all services"""
        health_statuses = []
        
        for service_name, url in self.service_urls.items():
            try:
                response = await self.http_client.get(f"{url}/health")
                response.raise_for_status()
                health_data = response.json()
                
                health_statuses.append(self._convert_service_health({
                    "service_name": service_name,
                    "status": health_data.get("status", "unknown"),
                    "version": health_data.get("version", "1.0.0"),
                    "checks": health_data.get("checks", {})
                }))
            except Exception as e:
                logger.error(f"Failed to get health for {service_name}: {e}")
                health_statuses.append(self._convert_service_health({
                    "service_name": service_name,
                    "status": "unhealthy",
                    "version": "unknown",
                    "checks": {}
                }))
        
        return health_statuses
    
    async def get_alerts(
        self,
        service: Optional[str] = None,
        acknowledged: Optional[bool] = None,
        limit: int = 100
    ) -> List[Any]:
        """Get system alerts"""
        # Aggregate alerts from multiple services
        all_alerts = []
        
        # Get pipeline alerts
        url = f"{self.service_urls['pipeline-orchestration-service']}/api/v1/monitoring/alerts"
        params = {"limit": limit}
        if acknowledged is not None:
            params["acknowledged"] = acknowledged
        
        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            alerts = response.json().get("alerts", [])
            for alert in alerts:
                alert["service"] = "pipeline-orchestration"
                all_alerts.extend([self._convert_alert(a) for a in alerts])
        except Exception as e:
            logger.error(f"Failed to get pipeline alerts: {e}")
        
        # Get quality alerts
        url = f"{self.service_urls['data-quality-service']}/api/v1/monitoring/alerts"
        try:
            response = await self.http_client.get(url, params=params)
            response.raise_for_status()
            alerts = response.json().get("alerts", [])
            for alert in alerts:
                alert["service"] = "data-quality"
                all_alerts.extend([self._convert_alert(a) for a in alerts])
        except Exception as e:
            logger.error(f"Failed to get quality alerts: {e}")
        
        # Filter by service if specified
        if service:
            all_alerts = [a for a in all_alerts if a.service == service]
        
        return all_alerts[:limit]
    
    # Subscriptions
    async def subscribe_pipeline_executions(
        self,
        pipeline_id: Optional[str] = None
    ) -> AsyncGenerator[Any, None]:
        """Subscribe to pipeline execution updates"""
        # This would connect to Pulsar and stream updates
        # For now, simulate with polling
        seen_executions = set()
        
        while True:
            try:
                executions = await self.get_pipeline_executions(
                    pipeline_id,
                    limit=10
                ) if pipeline_id else []
                
                for execution in executions:
                    if execution.execution_id not in seen_executions:
                        seen_executions.add(execution.execution_id)
                        yield execution
                
                await asyncio.sleep(5)  # Poll every 5 seconds
            except Exception as e:
                logger.error(f"Error in pipeline subscription: {e}")
                await asyncio.sleep(10)
    
    # Conversion methods
    def _convert_pipeline(self, data: Dict[str, Any]) -> Any:
        """Convert API response to GraphQL type"""
        from ..schema.types import Pipeline, PipelineStatus
        
        return Pipeline(
            id=data["id"],
            name=data["name"],
            type=data["type"],
            description=data["description"],
            status=PipelineStatus(data["status"]),
            owner=data.get("owner"),
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            tags=data.get("tags", [])
        )
    
    def _convert_execution(self, data: Dict[str, Any]) -> Any:
        """Convert execution data"""
        from ..schema.types import PipelineExecution, ExecutionStatus
        
        return PipelineExecution(
            execution_id=data["execution_id"],
            pipeline_id=data["pipeline_id"],
            pipeline_name=data["pipeline_name"],
            status=ExecutionStatus(data["status"]),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            current_step=data.get("current_step"),
            error_count=len(data.get("errors", []))
        )
    
    def _convert_metrics(self, data: Dict[str, Any]) -> Any:
        """Convert metrics data"""
        from ..schema.types import PipelineMetrics
        
        return PipelineMetrics(
            pipeline_id=data["pipeline_id"],
            total_executions=data["total_executions"],
            successful_executions=data["successful_executions"],
            failed_executions=data["failed_executions"],
            success_rate=data["success_rate"],
            average_duration_seconds=data["average_duration_seconds"],
            min_duration_seconds=data.get("min_duration_seconds"),
            max_duration_seconds=data.get("max_duration_seconds"),
            last_execution=datetime.fromisoformat(data["last_execution"]) if data.get("last_execution") else None
        )
    
    def _convert_quality_profile(self, data: Dict[str, Any]) -> Any:
        """Convert quality profile data"""
        from ..schema.types import DataQualityProfile
        
        return DataQualityProfile(
            dataset=data["dataset"],
            profiled_at=datetime.fromisoformat(data["profiled_at"]),
            row_count=data["row_count"],
            column_count=data["column_count"],
            quality_score=data["quality_score"],
            issues_found=[]  # Would convert issues
        )
    
    def _convert_quality_issue(self, data: Dict[str, Any]) -> Any:
        """Convert quality issue data"""
        from ..schema.types import QualityIssue
        
        return QualityIssue(
            id=data["id"],
            dataset=data["dataset"],
            issue_type=data["issue_type"],
            severity=data["severity"],
            description=data["description"],
            detected_at=datetime.fromisoformat(data["detected_at"]),
            remediation_status=data.get("remediation_status")
        )
    
    def _convert_cache_region(self, data: Dict[str, Any]) -> Any:
        """Convert cache region data"""
        from ..schema.types import CacheRegion, CachePolicy
        
        return CacheRegion(
            name=data["name"],
            cache_name=data["cache_name"],
            policy=CachePolicy(data["policy"]),
            ttl_seconds=data["ttl_seconds"],
            max_entries=data["max_entries"],
            current_entries=data["current_entries"],
            created_at=datetime.fromisoformat(data["created_at"])
        )
    
    def _convert_cache_stats(self, data: Dict[str, Any]) -> Any:
        """Convert cache stats data"""
        from ..schema.types import CacheStats
        
        return CacheStats(
            region=data["region"],
            hit_count=data["hit_count"],
            miss_count=data["miss_count"],
            eviction_count=data["eviction_count"],
            hit_rate=data["hit_rate"],
            average_get_time_ms=data["average_get_time_ms"],
            average_put_time_ms=data["average_put_time_ms"],
            memory_usage_bytes=data["memory_usage_bytes"]
        )
    
    def _convert_service_health(self, data: Dict[str, Any]) -> Any:
        """Convert service health data"""
        from ..schema.types import ServiceHealth, HealthCheck
        
        checks = []
        for name, check_data in data.get("checks", {}).items():
            checks.append(HealthCheck(
                name=name,
                status=check_data.get("status", "unknown"),
                message=check_data.get("message"),
                last_checked=datetime.utcnow()
            ))
        
        return ServiceHealth(
            service_name=data["service_name"],
            status=data["status"],
            version=data["version"],
            uptime_seconds=0,  # Would calculate from start time
            checks=checks
        )
    
    def _convert_alert(self, data: Dict[str, Any]) -> Any:
        """Convert alert data"""
        from ..schema.types import Alert
        
        return Alert(
            id=data["id"],
            service=data["service"],
            alert_type=data["alert_type"],
            severity=data["severity"],
            message=data["message"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            acknowledged=data["acknowledged"],
            metadata=data.get("metadata", {})
        ) 