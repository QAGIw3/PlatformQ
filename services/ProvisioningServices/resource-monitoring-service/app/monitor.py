"""Resource Monitor Implementation

Monitors resource usage across all services and infrastructure components.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

import httpx
from prometheus_api_client import PrometheusConnect
from pyignite import Client as IgniteClient
import pulsar
from kubernetes import client as k8s_client, config as k8s_config

from platformq_resource_common import (
    IResourceMonitor,
    ResourceMetrics,
    ClusterMetrics,
    ResourceAnomalyEvent
)

from .config import Settings

logger = logging.getLogger(__name__)


class ResourceMonitor(IResourceMonitor):
    """Monitors resource usage across the platform"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.prometheus_client = None
        self.ignite_client = None
        self.pulsar_client = None
        self.anomaly_publisher = None
        self.k8s_v1 = None
        
        self._running = False
        self._tasks = []
    
    async def initialize(self):
        """Initialize connections"""
        # Initialize Prometheus client
        self.prometheus_client = PrometheusConnect(
            url=self.settings.prometheus_url,
            disable_ssl=True
        )
        
        # Initialize Ignite client
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (self.settings.ignite_host, self.settings.ignite_port)
        ])
        
        # Create caches
        self.metrics_cache = self.ignite_client.get_or_create_cache('resource_metrics')
        self.cluster_metrics_cache = self.ignite_client.get_or_create_cache('cluster_metrics')
        self.historical_cache = self.ignite_client.get_or_create_cache('historical_metrics')
        
        # Initialize Pulsar client
        self.pulsar_client = pulsar.Client(self.settings.pulsar_url)
        
        # Create anomaly event publisher
        self.anomaly_publisher = self.pulsar_client.create_producer(
            'persistent://public/default/resource-anomalies'
        )
        
        # Initialize Kubernetes client
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()
        
        self.k8s_v1 = k8s_client.CoreV1Api()
        
        logger.info("Resource monitor initialized")
    
    async def start(self):
        """Start monitoring"""
        self._running = True
        logger.info("Starting resource monitoring")
        
        # Start collection tasks
        self._tasks = [
            asyncio.create_task(self._collect_metrics_loop()),
            asyncio.create_task(self._analyze_anomalies_loop()) if self.settings.anomaly_detection_enabled else None,
            asyncio.create_task(self._cleanup_old_metrics_loop())
        ]
        self._tasks = [t for t in self._tasks if t is not None]
    
    async def stop(self):
        """Stop monitoring"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close connections
        self.anomaly_publisher.close()
        self.pulsar_client.close()
        self.ignite_client.close()
        
        logger.info("Resource monitoring stopped")
    
    async def get_service_metrics(
        self,
        service_name: str,
        namespace: str = "platformq"
    ) -> Optional[ResourceMetrics]:
        """Get current metrics for a service"""
        key = f"{namespace}/{service_name}"
        if self.metrics_cache.contains_key(key):
            metrics_dict = self.metrics_cache.get(key)
            return ResourceMetrics(**metrics_dict)
        return None
    
    async def get_cluster_metrics(self) -> Optional[ClusterMetrics]:
        """Get current cluster-wide metrics"""
        if self.cluster_metrics_cache.contains_key('current'):
            metrics_dict = self.cluster_metrics_cache.get('current')
            return ClusterMetrics(**metrics_dict)
        return None
    
    async def get_historical_metrics(
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
                    metrics_dict = self.historical_cache.get(key)
                    metrics.append(ResourceMetrics(**metrics_dict))
        
        return sorted(metrics, key=lambda m: m.timestamp)
    
    async def detect_anomalies(
        self,
        metrics: ResourceMetrics
    ) -> List[ResourceAnomalyEvent]:
        """Detect anomalies in resource metrics"""
        anomalies = []
        
        # Check for high CPU usage
        if metrics.cpu_usage > self.settings.cpu_threshold_high:
            anomalies.append(ResourceAnomalyEvent(
                service_name=metrics.service_name,
                namespace=metrics.namespace,
                anomaly_type='high_cpu',
                severity=min(1.0, (metrics.cpu_usage - self.settings.cpu_threshold_high) / 20),
                current_value=metrics.cpu_usage,
                threshold_value=self.settings.cpu_threshold_high,
                details={'pod_count': metrics.pod_count}
            ))
        
        # Check for high memory usage
        if metrics.memory_usage > self.settings.memory_threshold_high:
            anomalies.append(ResourceAnomalyEvent(
                service_name=metrics.service_name,
                namespace=metrics.namespace,
                anomaly_type='high_memory',
                severity=min(1.0, (metrics.memory_usage - self.settings.memory_threshold_high) / 15),
                current_value=metrics.memory_usage,
                threshold_value=self.settings.memory_threshold_high,
                details={'memory_bytes': metrics.memory_bytes}
            ))
        
        # Check for high error rate
        if metrics.request_rate > 0:
            error_rate_ratio = metrics.error_rate / metrics.request_rate
            if error_rate_ratio > self.settings.error_rate_threshold:
                anomalies.append(ResourceAnomalyEvent(
                    service_name=metrics.service_name,
                    namespace=metrics.namespace,
                    anomaly_type='high_error_rate',
                    severity=min(1.0, error_rate_ratio * 10),
                    current_value=metrics.error_rate,
                    threshold_value=self.settings.error_rate_threshold * metrics.request_rate,
                    details={'request_rate': metrics.request_rate}
                ))
        
        # Check for slow response times
        if metrics.response_time_p99 > self.settings.response_time_threshold_ms:
            anomalies.append(ResourceAnomalyEvent(
                service_name=metrics.service_name,
                namespace=metrics.namespace,
                anomaly_type='slow_response',
                severity=min(1.0, (metrics.response_time_p99 - self.settings.response_time_threshold_ms) / 4000),
                current_value=metrics.response_time_p99,
                threshold_value=self.settings.response_time_threshold_ms,
                details={'active_connections': metrics.active_connections}
            ))
        
        return anomalies
    
    async def _collect_metrics_loop(self):
        """Main collection loop"""
        while self._running:
            try:
                await asyncio.gather(
                    self._collect_kubernetes_metrics(),
                    self._collect_infrastructure_metrics(),
                    return_exceptions=True
                )
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")
            
            await asyncio.sleep(self.settings.collection_interval)
    
    async def _collect_kubernetes_metrics(self):
        """Collect Kubernetes metrics from Prometheus"""
        queries = {
            'cpu_usage': '''
                avg by (namespace, pod) (
                    rate(container_cpu_usage_seconds_total{container!=""}[5m])
                ) * 100
            ''',
            'memory_usage': '''
                avg by (namespace, pod) (
                    container_memory_working_set_bytes{container!=""} 
                    / container_spec_memory_limit_bytes
                ) * 100
            ''',
            'memory_bytes': '''
                avg by (namespace, pod) (
                    container_memory_working_set_bytes{container!=""}
                )
            ''',
            'request_rate': '''
                sum by (namespace, service) (
                    rate(http_requests_total[5m])
                )
            ''',
            'error_rate': '''
                sum by (namespace, service) (
                    rate(http_requests_total{status=~"5.."}[5m])
                )
            ''',
            'response_time': '''
                histogram_quantile(0.99,
                    sum by (namespace, service, le) (
                        rate(http_request_duration_seconds_bucket[5m])
                    )
                ) * 1000
            ''',
            'connections': '''
                sum by (namespace, service) (
                    http_connections_active
                )
            '''
        }
        
        metrics_by_service = {}
        
        for metric_name, query in queries.items():
            try:
                result = self.prometheus_client.custom_query(query)
                
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
                        'memory_bytes': 'memory_bytes',
                        'request_rate': 'request_rate',
                        'error_rate': 'error_rate',
                        'response_time': 'response_time_p99',
                        'connections': 'active_connections'
                    }
                    
                    if metric_name in field_map:
                        metrics_by_service[key][field_map[metric_name]] = value
                        
            except Exception as e:
                logger.error(f"Error querying {metric_name}: {e}")
        
        # Get pod counts
        try:
            pods = self.k8s_v1.list_pod_for_all_namespaces()
            pod_counts = {}
            
            for pod in pods.items:
                if pod.status.phase == 'Running':
                    namespace = pod.metadata.namespace
                    # Extract service name from pod name
                    service = pod.metadata.labels.get('app', pod.metadata.name.split('-')[0])
                    key = f"{namespace}/{service}"
                    pod_counts[key] = pod_counts.get(key, 0) + 1
            
            # Add pod counts to metrics
            for key, count in pod_counts.items():
                if key in metrics_by_service:
                    metrics_by_service[key]['pod_count'] = count
                    
        except Exception as e:
            logger.error(f"Error getting pod counts: {e}")
        
        # Store metrics in cache
        for key, metrics_dict in metrics_by_service.items():
            # Ensure all required fields have default values
            metrics_dict.setdefault('cpu_usage', 0.0)
            metrics_dict.setdefault('memory_usage', 0.0)
            metrics_dict.setdefault('memory_bytes', 0)
            metrics_dict.setdefault('network_in_bytes', 0)
            metrics_dict.setdefault('network_out_bytes', 0)
            metrics_dict.setdefault('request_rate', 0.0)
            metrics_dict.setdefault('error_rate', 0.0)
            metrics_dict.setdefault('response_time_p99', 0.0)
            metrics_dict.setdefault('active_connections', 0)
            metrics_dict.setdefault('pod_count', 1)
            
            metrics = ResourceMetrics(**metrics_dict)
            self.metrics_cache.put(key, metrics.dict())
            
            # Store historical data
            historical_key = f"{key}:{metrics.timestamp.isoformat()}"
            self.historical_cache.put(historical_key, metrics.dict())
    
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
        
        cluster_metrics = {
            'timestamp': datetime.utcnow(),
            'total_cpu_cores': 0,
            'used_cpu_cores': 0,
            'total_memory_bytes': 0,
            'used_memory_bytes': 0,
            'total_gpu_count': 0,
            'used_gpu_count': 0,
            'node_count': 0,
            'pod_count': 0,
            'namespace_count': 0
        }
        
        for metric_name, query in queries.items():
            try:
                result = self.prometheus_client.custom_query(query)
                if result:
                    value = float(result[0]['value'][1])
                    
                    # Map to cluster metrics fields
                    if metric_name == 'total_cpu':
                        cluster_metrics['total_cpu_cores'] = int(value)
                    elif metric_name == 'used_cpu':
                        cluster_metrics['used_cpu_cores'] = value
                    elif metric_name == 'total_memory':
                        cluster_metrics['total_memory_bytes'] = int(value)
                    elif metric_name == 'used_memory':
                        cluster_metrics['used_memory_bytes'] = int(value)
                    elif metric_name == 'node_count':
                        cluster_metrics['node_count'] = int(value)
                    elif metric_name == 'pod_count':
                        cluster_metrics['pod_count'] = int(value)
                    elif metric_name == 'namespace_count':
                        cluster_metrics['namespace_count'] = int(value)
                        
            except Exception as e:
                logger.error(f"Error collecting {metric_name}: {e}")
        
        # Store cluster metrics
        metrics = ClusterMetrics(**cluster_metrics)
        self.cluster_metrics_cache.put('current', metrics.dict())
        self.historical_cache.put(f"cluster:{metrics.timestamp.isoformat()}", metrics.dict())
    
    async def _analyze_anomalies_loop(self):
        """Analyze metrics for anomalies"""
        while self._running:
            try:
                await self._detect_and_publish_anomalies()
            except Exception as e:
                logger.error(f"Error in anomaly detection: {e}")
            
            await asyncio.sleep(60)  # Check every minute
    
    async def _detect_and_publish_anomalies(self):
        """Detect and publish resource anomalies"""
        # Get all current metrics
        for key in self.metrics_cache.keys():
            metrics_dict = self.metrics_cache.get(key)
            metrics = ResourceMetrics(**metrics_dict)
            
            # Detect anomalies
            anomalies = await self.detect_anomalies(metrics)
            
            # Publish anomalies
            for anomaly in anomalies:
                try:
                    self.anomaly_publisher.send(
                        anomaly.json().encode('utf-8')
                    )
                    logger.warning(
                        f"Detected anomaly in {metrics.service_name}: "
                        f"{anomaly.anomaly_type} (severity: {anomaly.severity:.2f})"
                    )
                except Exception as e:
                    logger.error(f"Failed to publish anomaly event: {e}")
    
    async def _cleanup_old_metrics_loop(self):
        """Clean up old historical metrics"""
        while self._running:
            try:
                cutoff = datetime.utcnow() - timedelta(days=self.settings.metrics_retention_days)
                
                # Clean up old historical metrics
                for key in list(self.historical_cache.keys()):
                    if ':' in key:
                        timestamp_str = key.split(':')[-1]
                        try:
                            timestamp = datetime.fromisoformat(timestamp_str)
                            if timestamp < cutoff:
                                self.historical_cache.remove(key)
                        except:
                            pass
                            
            except Exception as e:
                logger.error(f"Error cleaning up old metrics: {e}")
            
            await asyncio.sleep(3600)  # Run hourly 