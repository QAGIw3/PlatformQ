"""Resource Monitor for Dynamic Provisioning

Monitors resource usage across all services and infrastructure components.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

import httpx
from prometheus_client.parser import text_string_to_metric_families
from pyignite import Client as IgniteClient
import pulsar
from pulsar.schema import AvroSchema

from platformq_shared.events import BaseEvent

logger = logging.getLogger(__name__)


@dataclass
class ResourceMetrics:
    """Container for resource metrics"""
    service_name: str
    namespace: str
    timestamp: datetime
    cpu_usage: float  # Percentage
    memory_usage: float  # Percentage
    memory_bytes: int
    network_in_bytes: int
    network_out_bytes: int
    request_rate: float  # Requests per second
    error_rate: float  # Errors per second
    response_time_p99: float  # 99th percentile response time in ms
    active_connections: int
    pod_count: int
    gpu_usage: Optional[float] = None  # For ML workloads
    storage_usage_bytes: Optional[int] = None


@dataclass
class ClusterMetrics:
    """Container for cluster-wide metrics"""
    timestamp: datetime
    total_cpu_cores: int
    used_cpu_cores: float
    total_memory_bytes: int
    used_memory_bytes: int
    total_gpu_count: int
    used_gpu_count: int
    node_count: int
    pod_count: int
    namespace_count: int


class ResourceMonitor:
    """Monitors resource usage across the platform
    
    Collects metrics from:
    - Prometheus for Kubernetes metrics
    - Service-specific endpoints
    - Ignite cache for real-time data
    - Cloud provider APIs for cost data
    """
    
    def __init__(
        self,
        prometheus_url: str,
        ignite_client: IgniteClient,
        pulsar_client: pulsar.Client,
        kubernetes_api_url: str = "https://kubernetes.default.svc",
        collection_interval: int = 30  # seconds
    ):
        self.prometheus_url = prometheus_url
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        self.kubernetes_api_url = kubernetes_api_url
        self.collection_interval = collection_interval
        
        # Create caches
        self.metrics_cache = ignite_client.get_or_create_cache('resource_metrics')
        self.cluster_metrics_cache = ignite_client.get_or_create_cache('cluster_metrics')
        self.historical_cache = ignite_client.get_or_create_cache('historical_metrics')
        
        # HTTP client for API calls
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Event publisher for anomalies
        self.anomaly_publisher = pulsar_client.create_producer(
            'persistent://public/default/resource-anomalies',
            schema=AvroSchema(ResourceAnomalyEvent)
        )
        
        self._running = False
        self._tasks = []
    
    async def start(self):
        """Start monitoring"""
        self._running = True
        logger.info("Starting resource monitoring")
        
        # Start collection tasks
        self._tasks = [
            asyncio.create_task(self._collect_metrics_loop()),
            asyncio.create_task(self._analyze_anomalies_loop()),
            asyncio.create_task(self._cleanup_old_metrics_loop())
        ]
    
    async def stop(self):
        """Stop monitoring"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.http_client.aclose()
        self.anomaly_publisher.close()
        logger.info("Resource monitoring stopped")
    
    async def _collect_metrics_loop(self):
        """Main collection loop"""
        while self._running:
            try:
                # Collect metrics from various sources
                await asyncio.gather(
                    self._collect_kubernetes_metrics(),
                    self._collect_service_metrics(),
                    self._collect_infrastructure_metrics(),
                    return_exceptions=True
                )
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")
            
            await asyncio.sleep(self.collection_interval)
    
    async def _collect_kubernetes_metrics(self):
        """Collect Kubernetes metrics from Prometheus"""
        queries = {
            # CPU usage by service
            'cpu_usage': '''
                avg by (namespace, pod) (
                    rate(container_cpu_usage_seconds_total{container!=""}[5m])
                ) * 100
            ''',
            # Memory usage by service  
            'memory_usage': '''
                avg by (namespace, pod) (
                    container_memory_working_set_bytes{container!=""} 
                    / container_spec_memory_limit_bytes
                ) * 100
            ''',
            # Request rate
            'request_rate': '''
                sum by (namespace, service) (
                    rate(http_requests_total[5m])
                )
            ''',
            # Error rate
            'error_rate': '''
                sum by (namespace, service) (
                    rate(http_requests_total{status=~"5.."}[5m])
                )
            ''',
            # Response time
            'response_time': '''
                histogram_quantile(0.99,
                    sum by (namespace, service, le) (
                        rate(http_request_duration_seconds_bucket[5m])
                    )
                ) * 1000
            ''',
            # Active connections
            'connections': '''
                sum by (namespace, service) (
                    http_connections_active
                )
            ''',
            # Pod count
            'pod_count': '''
                count by (namespace, deployment) (
                    up{job="kubernetes-pods"}
                )
            '''
        }
        
        metrics_by_service = {}
        
        for metric_name, query in queries.items():
            try:
                result = await self._query_prometheus(query)
                
                for item in result:
                    labels = item['metric']
                    namespace = labels.get('namespace', 'default')
                    service = labels.get('service') or labels.get('deployment') or labels.get('pod', '').split('-')[0]
                    value = float(item['value'][1])
                    
                    key = f"{namespace}/{service}"
                    if key not in metrics_by_service:
                        metrics_by_service[key] = {
                            'namespace': namespace,
                            'service_name': service,
                            'timestamp': datetime.utcnow()
                        }
                    
                    # Map metric to field
                    field_map = {
                        'cpu_usage': 'cpu_usage',
                        'memory_usage': 'memory_usage',
                        'request_rate': 'request_rate',
                        'error_rate': 'error_rate',
                        'response_time': 'response_time_p99',
                        'connections': 'active_connections',
                        'pod_count': 'pod_count'
                    }
                    
                    if metric_name in field_map:
                        metrics_by_service[key][field_map[metric_name]] = value
                        
            except Exception as e:
                logger.error(f"Error querying {metric_name}: {e}")
        
        # Store metrics in cache
        for key, metrics_dict in metrics_by_service.items():
            metrics = ResourceMetrics(**metrics_dict)
            self.metrics_cache.put(key, metrics)
            
            # Store historical data
            historical_key = f"{key}:{metrics.timestamp.isoformat()}"
            self.historical_cache.put(historical_key, metrics)
    
    async def _collect_service_metrics(self):
        """Collect service-specific metrics"""
        # Get list of services from Kubernetes
        services = await self._get_kubernetes_services()
        
        for service in services:
            try:
                # Call service health endpoint
                url = f"http://{service['name']}.{service['namespace']}.svc.cluster.local/metrics"
                response = await self.http_client.get(url, timeout=5.0)
                
                if response.status_code == 200:
                    # Parse Prometheus format metrics
                    for family in text_string_to_metric_families(response.text):
                        for sample in family.samples:
                            # Process custom metrics
                            if sample.name == "gpu_utilization_percent":
                                key = f"{service['namespace']}/{service['name']}"
                                if self.metrics_cache.contains_key(key):
                                    metrics = self.metrics_cache.get(key)
                                    metrics.gpu_usage = sample.value
                                    self.metrics_cache.put(key, metrics)
                                    
            except Exception as e:
                logger.debug(f"Could not collect metrics from {service['name']}: {e}")
    
    async def _collect_infrastructure_metrics(self):
        """Collect cluster-wide infrastructure metrics"""
        queries = {
            'total_cpu': 'sum(machine_cpu_cores)',
            'used_cpu': 'sum(rate(container_cpu_usage_seconds_total[5m]))',
            'total_memory': 'sum(machine_memory_bytes)',
            'used_memory': 'sum(container_memory_working_set_bytes)',
            'node_count': 'count(up{job="kubernetes-nodes"})',
            'pod_count': 'count(up{job="kubernetes-pods"})',
            'namespace_count': 'count(count by (namespace)(up))'
        }
        
        cluster_metrics = ClusterMetrics(
            timestamp=datetime.utcnow(),
            total_cpu_cores=0,
            used_cpu_cores=0,
            total_memory_bytes=0,
            used_memory_bytes=0,
            total_gpu_count=0,
            used_gpu_count=0,
            node_count=0,
            pod_count=0,
            namespace_count=0
        )
        
        for metric_name, query in queries.items():
            try:
                result = await self._query_prometheus(query)
                if result:
                    value = float(result[0]['value'][1])
                    setattr(cluster_metrics, metric_name.replace('total_', 'total_').replace('used_', 'used_'), value)
            except Exception as e:
                logger.error(f"Error collecting {metric_name}: {e}")
        
        # Store cluster metrics
        self.cluster_metrics_cache.put('current', cluster_metrics)
        self.historical_cache.put(f"cluster:{cluster_metrics.timestamp.isoformat()}", cluster_metrics)
    
    async def _query_prometheus(self, query: str) -> List[Dict[str, Any]]:
        """Query Prometheus"""
        params = {'query': query}
        response = await self.http_client.get(
            f"{self.prometheus_url}/api/v1/query",
            params=params
        )
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success':
                return data['data']['result']
        
        return []
    
    async def _get_kubernetes_services(self) -> List[Dict[str, str]]:
        """Get list of Kubernetes services"""
        # This would use the Kubernetes API
        # For now, return a hardcoded list
        return [
            {'name': 'auth-service', 'namespace': 'platformq'},
            {'name': 'digital-asset-service', 'namespace': 'platformq'},
            {'name': 'projects-service', 'namespace': 'platformq'},
            {'name': 'simulation-service', 'namespace': 'platformq'},
            {'name': 'federated-learning-service', 'namespace': 'platformq'},
            {'name': 'workflow-service', 'namespace': 'platformq'},
            {'name': 'notification-service', 'namespace': 'platformq'}
        ]
    
    async def _analyze_anomalies_loop(self):
        """Analyze metrics for anomalies"""
        while self._running:
            try:
                await self._detect_anomalies()
            except Exception as e:
                logger.error(f"Error in anomaly detection: {e}")
            
            await asyncio.sleep(60)  # Check every minute
    
    async def _detect_anomalies(self):
        """Detect resource anomalies"""
        # Get all current metrics
        all_metrics = []
        for key in self.metrics_cache.keys():
            metrics = self.metrics_cache.get(key)
            all_metrics.append(metrics)
        
        for metrics in all_metrics:
            anomalies = []
            
            # Check for high CPU usage
            if metrics.cpu_usage > 80:
                anomalies.append({
                    'type': 'high_cpu',
                    'severity': min(1.0, (metrics.cpu_usage - 80) / 20),
                    'value': metrics.cpu_usage
                })
            
            # Check for high memory usage
            if metrics.memory_usage > 85:
                anomalies.append({
                    'type': 'high_memory',
                    'severity': min(1.0, (metrics.memory_usage - 85) / 15),
                    'value': metrics.memory_usage
                })
            
            # Check for high error rate
            if metrics.request_rate > 0 and metrics.error_rate / metrics.request_rate > 0.05:
                anomalies.append({
                    'type': 'high_error_rate',
                    'severity': min(1.0, metrics.error_rate / metrics.request_rate * 10),
                    'value': metrics.error_rate
                })
            
            # Check for slow response times
            if metrics.response_time_p99 > 1000:  # 1 second
                anomalies.append({
                    'type': 'slow_response',
                    'severity': min(1.0, (metrics.response_time_p99 - 1000) / 4000),
                    'value': metrics.response_time_p99
                })
            
            # Publish anomalies
            for anomaly in anomalies:
                event = ResourceAnomalyEvent(
                    service_name=metrics.service_name,
                    namespace=metrics.namespace,
                    anomaly_type=anomaly['type'],
                    severity=anomaly['severity'],
                    timestamp=datetime.utcnow().isoformat(),
                    details={
                        'value': anomaly['value'],
                        'pod_count': metrics.pod_count,
                        'cpu_usage': metrics.cpu_usage,
                        'memory_usage': metrics.memory_usage
                    }
                )
                self.anomaly_publisher.send(event)
                logger.warning(f"Detected anomaly in {metrics.service_name}: {anomaly['type']}")
    
    async def _cleanup_old_metrics_loop(self):
        """Clean up old historical metrics"""
        while self._running:
            try:
                cutoff = datetime.utcnow() - timedelta(days=30)
                
                # Clean up old historical metrics
                for key in list(self.historical_cache.keys()):
                    if ':' in key:
                        timestamp_str = key.split(':')[-1]
                        timestamp = datetime.fromisoformat(timestamp_str)
                        if timestamp < cutoff:
                            self.historical_cache.remove(key)
                            
            except Exception as e:
                logger.error(f"Error cleaning up old metrics: {e}")
            
            await asyncio.sleep(3600)  # Run hourly
    
    def get_current_metrics(self, service_name: str, namespace: str = 'platformq') -> Optional[ResourceMetrics]:
        """Get current metrics for a service"""
        key = f"{namespace}/{service_name}"
        if self.metrics_cache.contains_key(key):
            return self.metrics_cache.get(key)
        return None
    
    def get_cluster_metrics(self) -> Optional[ClusterMetrics]:
        """Get current cluster metrics"""
        if self.cluster_metrics_cache.contains_key('current'):
            return self.cluster_metrics_cache.get('current')
        return None
    
    def get_historical_metrics(
        self,
        service_name: str,
        namespace: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[ResourceMetrics]:
        """Get historical metrics for a service"""
        metrics = []
        prefix = f"{namespace}/{service_name}:"
        
        for key in self.historical_cache.keys():
            if key.startswith(prefix):
                timestamp_str = key.split(':')[-1]
                timestamp = datetime.fromisoformat(timestamp_str)
                
                if start_time <= timestamp <= end_time:
                    metrics.append(self.historical_cache.get(key))
        
        return sorted(metrics, key=lambda m: m.timestamp)


class ResourceAnomalyEvent(BaseEvent):
    """Event for resource anomalies"""
    service_name: str
    namespace: str
    anomaly_type: str
    severity: float
    timestamp: str
    details: Dict[str, Any] 