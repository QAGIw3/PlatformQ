"""
Resolver Manager

Manages GraphQL resolvers and communication with backend services.
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional, AsyncGenerator
from datetime import datetime

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class ResolverManager:
    """
    Manages resolvers that communicate with backend services
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration):
        self.vault_consul = vault_consul
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.service_urls: Dict[str, str] = {}
        self.service_health: Dict[str, bool] = {}
        self._health_check_task = None
    
    async def initialize(self):
        """Initialize resolver manager"""
        logger.info("initializing_resolver_manager")
        
        # Discover services
        await self._discover_services()
        
        # Start health monitoring
        self._health_check_task = asyncio.create_task(self._monitor_health())
        
        logger.info("resolver_manager_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        
        await self.http_client.aclose()
    
    async def is_healthy(self) -> bool:
        """Check if resolver manager is healthy"""
        # At least one service should be healthy
        return any(self.service_health.values())
    
    async def _discover_services(self):
        """Discover services via Consul"""
        services = [
            "analytics-engine-service",
            "data-governance-service", 
            "data-platform-service",
            "ml-platform-service",
            "orchestration-service",
            "auth-service",
            "blockchain-gateway-service"
        ]
        
        for service_name in services:
            try:
                # Try to discover via Consul
                _, service_instances = await self.vault_consul.consul.health.service(
                    service_name, passing=True
                )
                
                if service_instances:
                    # Use first healthy instance
                    instance = service_instances[0]
                    service = instance["Service"]
                    host = service["Address"] or "localhost"
                    port = service["Port"]
                    self.service_urls[service_name] = f"http://{host}:{port}"
                    self.service_health[service_name] = True
                else:
                    # Fallback to default ports
                    default_ports = {
                        "analytics-engine-service": 8011,
                        "data-governance-service": 8012,
                        "data-platform-service": 8013,
                        "ml-platform-service": 8014,
                        "orchestration-service": 8015,
                        "auth-service": 8001,
                        "blockchain-gateway-service": 8002
                    }
                    port = default_ports.get(service_name, 8000)
                    self.service_urls[service_name] = f"http://localhost:{port}"
                    self.service_health[service_name] = False
                    
            except Exception as e:
                logger.warning(f"Failed to discover {service_name}: {e}")
                self.service_health[service_name] = False
    
    async def _monitor_health(self):
        """Monitor service health"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for service_name, url in self.service_urls.items():
                    try:
                        response = await self.http_client.get(f"{url}/health")
                        self.service_health[service_name] = response.status_code == 200
                    except Exception:
                        self.service_health[service_name] = False
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitoring error: {e}")
    
    # Data Catalog resolvers
    async def search_catalog(self, query: str, filters: Optional[Dict] = None, 
                           limit: int = 10, offset: int = 0) -> List[Dict[str, Any]]:
        """Search the data catalog"""
        url = f"{self.service_urls.get('data-platform-service')}/api/v1/catalog/search"
        
        params = {
            "q": query,
            "limit": limit,
            "offset": offset
        }
        if filters:
            params.update(filters)
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Get a catalog entity"""
        url = f"{self.service_urls.get('data-platform-service')}/api/v1/catalog/entities/{entity_id}"
        
        response = await self.http_client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    
    async def get_lineage(self, entity_id: str, depth: int = 3, 
                         direction: str = "both") -> Dict[str, Any]:
        """Get data lineage"""
        url = f"{self.service_urls.get('data-platform-service')}/api/v1/catalog/lineage/{entity_id}"
        
        params = {
            "depth": depth,
            "direction": direction
        }
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    # Pipeline resolvers
    async def list_pipelines(self, filter: Optional[Dict] = None, 
                           limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        """List pipelines"""
        url = f"{self.service_urls.get('orchestration-service')}/api/v1/pipelines"
        
        params = {
            "limit": limit,
            "offset": offset
        }
        if filter:
            params.update(filter)
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_pipeline(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get a pipeline"""
        url = f"{self.service_urls.get('orchestration-service')}/api/v1/pipelines/{pipeline_id}"
        
        response = await self.http_client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    
    async def get_pipeline_executions(self, pipeline_id: str, 
                                    limit: int = 10) -> List[Dict[str, Any]]:
        """Get pipeline executions"""
        url = f"{self.service_urls.get('orchestration-service')}/api/v1/pipelines/{pipeline_id}/executions"
        
        params = {"limit": limit}
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def create_pipeline(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """Create a pipeline"""
        url = f"{self.service_urls.get('orchestration-service')}/api/v1/pipelines"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    async def update_pipeline(self, pipeline_id: str, input: Dict[str, Any]) -> Dict[str, Any]:
        """Update a pipeline"""
        url = f"{self.service_urls.get('orchestration-service')}/api/v1/pipelines/{pipeline_id}"
        
        response = await self.http_client.put(url, json=input)
        response.raise_for_status()
        return response.json()
    
    async def execute_pipeline(self, pipeline_id: str, 
                             params: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a pipeline"""
        url = f"{self.service_urls.get('orchestration-service')}/api/v1/pipelines/{pipeline_id}/execute"
        
        response = await self.http_client.post(url, json=params or {})
        response.raise_for_status()
        return response.json()
    
    # Data Quality resolvers
    async def get_quality_profile(self, dataset: str) -> Dict[str, Any]:
        """Get quality profile"""
        url = f"{self.service_urls.get('data-governance-service')}/api/v1/quality/profile"
        
        params = {"dataset": dataset}
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def list_quality_issues(self, filter: Optional[Dict] = None,
                                limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """List quality issues"""
        url = f"{self.service_urls.get('data-governance-service')}/api/v1/quality/issues"
        
        params = {
            "limit": limit,
            "offset": offset
        }
        if filter:
            params.update(filter)
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def list_quality_rules(self) -> List[Dict[str, Any]]:
        """List quality rules"""
        url = f"{self.service_urls.get('data-governance-service')}/api/v1/quality/rules"
        
        response = await self.http_client.get(url)
        response.raise_for_status()
        return response.json()
    
    async def run_quality_check(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """Run quality check"""
        url = f"{self.service_urls.get('data-governance-service')}/api/v1/quality/check"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    async def create_quality_rule(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """Create quality rule"""
        url = f"{self.service_urls.get('data-governance-service')}/api/v1/quality/rules"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    # ML Model resolvers
    async def list_models(self, filter: Optional[Dict] = None,
                        limit: int = 20, offset: int = 0) -> List[Dict[str, Any]]:
        """List ML models"""
        url = f"{self.service_urls.get('ml-platform-service')}/api/v1/models"
        
        params = {
            "limit": limit,
            "offset": offset
        }
        if filter:
            params.update(filter)
        
        response = await self.http_client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def get_model(self, model_id: str) -> Optional[Dict[str, Any]]:
        """Get ML model"""
        url = f"{self.service_urls.get('ml-platform-service')}/api/v1/models/{model_id}"
        
        response = await self.http_client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()
    
    async def get_model_versions(self, model_id: str) -> List[Dict[str, Any]]:
        """Get model versions"""
        url = f"{self.service_urls.get('ml-platform-service')}/api/v1/models/{model_id}/versions"
        
        response = await self.http_client.get(url)
        response.raise_for_status()
        return response.json()
    
    async def train_model(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """Train model"""
        url = f"{self.service_urls.get('ml-platform-service')}/api/v1/models/train"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    async def deploy_model(self, model_id: str, input: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy model"""
        url = f"{self.service_urls.get('ml-platform-service')}/api/v1/models/{model_id}/deploy"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    # Graph resolvers
    async def query_graph(self, query: str, bindings: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute graph query"""
        url = f"{self.service_urls.get('integration-hub-service')}/api/v1/graph/query"
        
        payload = {
            "query": query,
            "bindings": bindings or {}
        }
        
        response = await self.http_client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    async def get_graph_analytics(self, graph_id: str, algorithm: str,
                                params: Optional[Dict] = None) -> Dict[str, Any]:
        """Run graph analytics"""
        url = f"{self.service_urls.get('integration-hub-service')}/api/v1/graph/analytics"
        
        payload = {
            "graph_id": graph_id,
            "algorithm": algorithm,
            "params": params or {}
        }
        
        response = await self.http_client.post(url, json=payload)
        response.raise_for_status()
        return response.json()
    
    async def create_graph_entity(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """Create graph entity"""
        url = f"{self.service_urls.get('integration-hub-service')}/api/v1/graph/entities"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    async def create_graph_relationship(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """Create graph relationship"""
        url = f"{self.service_urls.get('integration-hub-service')}/api/v1/graph/relationships"
        
        response = await self.http_client.post(url, json=input)
        response.raise_for_status()
        return response.json()
    
    # System resolvers
    async def get_service_health(self) -> List[Dict[str, Any]]:
        """Get service health status"""
        health_status = []
        
        for service_name, is_healthy in self.service_health.items():
            health_status.append({
                "service": service_name,
                "healthy": is_healthy,
                "url": self.service_urls.get(service_name),
                "checked_at": datetime.utcnow().isoformat()
            })
        
        return health_status
    
    async def get_system_metrics(self, metric_names: List[str]) -> Dict[str, Any]:
        """Get system metrics"""
        # This would aggregate metrics from various services
        metrics = {}
        
        for metric_name in metric_names:
            # Placeholder - would fetch actual metrics
            metrics[metric_name] = {
                "value": 0,
                "unit": "count",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        return metrics
    
    async def invalidate_cache(self, region: str, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """Invalidate cache"""
        # This would coordinate cache invalidation across services
        return {
            "region": region,
            "keys_invalidated": len(keys) if keys else 0,
            "success": True
        }
    
    async def trigger_lineage_update(self, entity_id: str) -> Dict[str, Any]:
        """Trigger lineage update"""
        url = f"{self.service_urls.get('data-platform-service')}/api/v1/catalog/lineage/{entity_id}/refresh"
        
        response = await self.http_client.post(url)
        response.raise_for_status()
        return response.json()
    
    # Subscription resolvers (would use WebSockets/SSE in production)
    async def subscribe_pipeline_status(self, pipeline_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to pipeline status"""
        # Placeholder - would implement actual subscription
        while True:
            await asyncio.sleep(5)
            yield {
                "pipeline_id": pipeline_id,
                "status": "running",
                "progress": 50,
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def subscribe_quality_alerts(self, severity: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to quality alerts"""
        # Placeholder - would implement actual subscription
        while True:
            await asyncio.sleep(10)
            yield {
                "alert_id": "alert_123",
                "severity": severity or "warning",
                "message": "Data quality issue detected",
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def subscribe_model_metrics(self, model_id: str) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to model metrics"""
        # Placeholder - would implement actual subscription
        while True:
            await asyncio.sleep(3)
            yield {
                "model_id": model_id,
                "accuracy": 0.95,
                "latency_ms": 23,
                "requests_per_sec": 100,
                "timestamp": datetime.utcnow().isoformat()
            }
    
    async def subscribe_system_events(self, services: Optional[List[str]] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Subscribe to system events"""
        # Placeholder - would implement actual subscription
        while True:
            await asyncio.sleep(5)
            yield {
                "event_type": "service_update",
                "service": services[0] if services else "all",
                "message": "Service configuration updated",
                "timestamp": datetime.utcnow().isoformat()
            } 