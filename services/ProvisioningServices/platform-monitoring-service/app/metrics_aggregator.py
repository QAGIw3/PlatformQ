"""Metrics Aggregator for tenant-specific metrics"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import aiohttp
from prometheus_client import Counter, Histogram

from config import settings
from models import (
    TenantMetrics,
    ServiceMetrics,
    ResourceMetrics,
    ServiceType,
    TimeRange,
    Alert
)
from thanos_manager import ThanosManager

logger = logging.getLogger(__name__)

# Metrics
aggregation_counter = Counter(
    'metrics_aggregation_total',
    'Total number of metric aggregations',
    ['tenant_id', 'status']
)
aggregation_duration = Histogram(
    'metrics_aggregation_duration_seconds',
    'Duration of metric aggregations',
    ['tenant_id']
)


class MetricsAggregator:
    """Aggregates metrics for tenants across regions"""
    
    def __init__(self):
        self.thanos_manager = None
        self.session = None
        self.cache: Dict[str, TenantMetrics] = {}
        
        # Define queries for different metrics
        self.metric_queries = {
            'cpu_usage': 'avg(rate(container_cpu_usage_seconds_total{tenant_id="__TENANT__"}[5m])) * 100',
            'memory_usage': 'avg(container_memory_usage_bytes{tenant_id="__TENANT__"}) / avg(container_spec_memory_limit_bytes{tenant_id="__TENANT__"}) * 100',
            'disk_usage': 'sum(container_fs_usage_bytes{tenant_id="__TENANT__"}) / sum(container_fs_limit_bytes{tenant_id="__TENANT__"}) * 100',
            'network_ingress': 'sum(rate(container_network_receive_bytes_total{tenant_id="__TENANT__"}[5m]))',
            'network_egress': 'sum(rate(container_network_transmit_bytes_total{tenant_id="__TENANT__"}[5m]))',
            'iops': 'sum(rate(container_fs_reads_total{tenant_id="__TENANT__"}[5m]) + rate(container_fs_writes_total{tenant_id="__TENANT__"}[5m]))'
        }
        
        # Service-specific metric queries
        self.service_queries = {
            ServiceType.CASSANDRA: {
                'availability': 'avg(up{job="platform-cassandra",tenant_id="__TENANT__"})',
                'latency_p50': 'histogram_quantile(0.5, sum(rate(cassandra_client_request_latency_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p95': 'histogram_quantile(0.95, sum(rate(cassandra_client_request_latency_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p99': 'histogram_quantile(0.99, sum(rate(cassandra_client_request_latency_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'error_rate': 'sum(rate(cassandra_client_request_failures_total{tenant_id="__TENANT__"}[5m])) / sum(rate(cassandra_client_request_latency_count{tenant_id="__TENANT__"}[5m]))',
                'throughput': 'sum(rate(cassandra_client_request_latency_count{tenant_id="__TENANT__"}[5m]))',
                'active_connections': 'sum(cassandra_client_connected_native_clients{tenant_id="__TENANT__"})'
            },
            ServiceType.IGNITE: {
                'availability': 'avg(up{job="platform-ignite",tenant_id="__TENANT__"})',
                'latency_p50': 'histogram_quantile(0.5, sum(rate(ignite_cache_gets_time_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p95': 'histogram_quantile(0.95, sum(rate(ignite_cache_gets_time_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p99': 'histogram_quantile(0.99, sum(rate(ignite_cache_gets_time_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'error_rate': 'sum(rate(ignite_cache_misses_total{tenant_id="__TENANT__"}[5m])) / sum(rate(ignite_cache_gets_total{tenant_id="__TENANT__"}[5m]))',
                'throughput': 'sum(rate(ignite_cache_gets_total{tenant_id="__TENANT__"}[5m]) + rate(ignite_cache_puts_total{tenant_id="__TENANT__"}[5m]))',
                'active_connections': 'sum(ignite_client_active_sessions{tenant_id="__TENANT__"})'
            },
            ServiceType.PULSAR: {
                'availability': 'avg(up{job="platform-pulsar",tenant_id="__TENANT__"})',
                'latency_p50': 'histogram_quantile(0.5, sum(rate(pulsar_broker_publish_latency_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p95': 'histogram_quantile(0.95, sum(rate(pulsar_broker_publish_latency_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p99': 'histogram_quantile(0.99, sum(rate(pulsar_broker_publish_latency_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'error_rate': 'sum(rate(pulsar_broker_publish_failed_total{tenant_id="__TENANT__"}[5m])) / sum(rate(pulsar_broker_publish_total{tenant_id="__TENANT__"}[5m]))',
                'throughput': 'sum(rate(pulsar_broker_in_bytes_total{tenant_id="__TENANT__"}[5m]) + rate(pulsar_broker_out_bytes_total{tenant_id="__TENANT__"}[5m]))',
                'active_connections': 'sum(pulsar_broker_active_connections{tenant_id="__TENANT__"})'
            },
            ServiceType.MINIO: {
                'availability': 'avg(up{job="platform-minio",tenant_id="__TENANT__"})',
                'latency_p50': 'histogram_quantile(0.5, sum(rate(minio_http_requests_duration_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p95': 'histogram_quantile(0.95, sum(rate(minio_http_requests_duration_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p99': 'histogram_quantile(0.99, sum(rate(minio_http_requests_duration_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'error_rate': 'sum(rate(minio_http_requests_errors_total{tenant_id="__TENANT__"}[5m])) / sum(rate(minio_http_requests_total{tenant_id="__TENANT__"}[5m]))',
                'throughput': 'sum(rate(minio_network_sent_bytes_total{tenant_id="__TENANT__"}[5m]) + rate(minio_network_received_bytes_total{tenant_id="__TENANT__"}[5m]))',
                'active_connections': 'sum(minio_http_requests_inflight{tenant_id="__TENANT__"})'
            },
            ServiceType.ELASTICSEARCH: {
                'availability': 'avg(up{job="platform-elasticsearch",tenant_id="__TENANT__"})',
                'latency_p50': 'histogram_quantile(0.5, sum(rate(elasticsearch_indices_search_query_time_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p95': 'histogram_quantile(0.95, sum(rate(elasticsearch_indices_search_query_time_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'latency_p99': 'histogram_quantile(0.99, sum(rate(elasticsearch_indices_search_query_time_bucket{tenant_id="__TENANT__"}[5m])) by (le))',
                'error_rate': 'sum(rate(elasticsearch_indices_search_failed_total{tenant_id="__TENANT__"}[5m])) / sum(rate(elasticsearch_indices_search_total{tenant_id="__TENANT__"}[5m]))',
                'throughput': 'sum(rate(elasticsearch_indices_search_total{tenant_id="__TENANT__"}[5m]) + rate(elasticsearch_indices_indexing_index_total{tenant_id="__TENANT__"}[5m]))',
                'active_connections': 'sum(elasticsearch_http_connections_current{tenant_id="__TENANT__"})'
            }
        }
        
    async def start(self):
        """Start the metrics aggregator"""
        self.session = aiohttp.ClientSession()
        self.thanos_manager = ThanosManager()
        await self.thanos_manager.start()
        
    async def stop(self):
        """Stop the metrics aggregator"""
        if self.thanos_manager:
            await self.thanos_manager.stop()
        if self.session:
            await self.session.close()
            
    async def get_tenant_metrics(
        self,
        tenant_id: str,
        time_range: str = "1h"
    ) -> TenantMetrics:
        """Get aggregated metrics for a tenant"""
        with aggregation_duration.labels(tenant_id=tenant_id).time():
            try:
                # Check cache
                cache_key = f"{tenant_id}:{time_range}"
                if cache_key in self.cache:
                    cached = self.cache[cache_key]
                    if (datetime.utcnow() - cached.time_range.end).total_seconds() < settings.CACHE_TTL:
                        return cached
                        
                # Parse time range
                parsed_range = self._parse_time_range(time_range)
                
                # Get resource metrics
                resource_metrics = await self._get_resource_metrics(tenant_id, parsed_range)
                
                # Get service metrics
                service_metrics = await self._get_service_metrics(tenant_id, parsed_range)
                
                # Get active alerts
                alerts = await self._get_tenant_alerts(tenant_id)
                
                # Calculate cost estimate
                cost_estimate = self._calculate_cost_estimate(resource_metrics, service_metrics)
                
                # Get active regions
                regions = await self._get_active_regions(tenant_id, parsed_range)
                
                # Build result
                result = TenantMetrics(
                    tenant_id=tenant_id,
                    time_range=parsed_range,
                    regions=regions,
                    total_resources=self._calculate_total_resources(resource_metrics),
                    service_metrics=service_metrics,
                    cost_estimate=cost_estimate,
                    alerts=alerts
                )
                
                # Update cache
                self.cache[cache_key] = result
                
                aggregation_counter.labels(tenant_id=tenant_id, status='success').inc()
                return result
                
            except Exception as e:
                logger.error(f"Failed to get metrics for tenant {tenant_id}: {e}")
                aggregation_counter.labels(tenant_id=tenant_id, status='failure').inc()
                raise
                
    async def _get_resource_metrics(
        self,
        tenant_id: str,
        time_range: TimeRange
    ) -> ResourceMetrics:
        """Get resource usage metrics for a tenant"""
        metrics = {}
        
        # Query each resource metric
        for metric_name, query_template in self.metric_queries.items():
            query = query_template.replace('__TENANT__', tenant_id)
            
            try:
                result = await self.thanos_manager.query(
                    promql=query,
                    time_range=time_range
                )
                
                if result.data and result.data[0].values:
                    metrics[metric_name] = result.data[0].values[0].value
                else:
                    metrics[metric_name] = 0.0
                    
            except Exception as e:
                logger.error(f"Failed to get {metric_name} for {tenant_id}: {e}")
                metrics[metric_name] = 0.0
                
        return ResourceMetrics(
            cpu_usage=metrics.get('cpu_usage', 0.0),
            memory_usage=metrics.get('memory_usage', 0.0),
            disk_usage=metrics.get('disk_usage', 0.0),
            network_ingress=metrics.get('network_ingress', 0.0),
            network_egress=metrics.get('network_egress', 0.0),
            iops=metrics.get('iops', 0.0),
            timestamp=datetime.utcnow()
        )
        
    async def _get_service_metrics(
        self,
        tenant_id: str,
        time_range: TimeRange
    ) -> Dict[ServiceType, ServiceMetrics]:
        """Get service-specific metrics for a tenant"""
        service_metrics = {}
        
        for service_type, queries in self.service_queries.items():
            metrics = {}
            
            # Get resource metrics for the service
            resource_query = f'''
                {{__name__=~"container_.*", 
                  job="platform-{service_type.value}",
                  tenant_id="{tenant_id}"}}
            '''
            resource_metrics = await self._get_resource_metrics_for_service(
                resource_query,
                time_range
            )
            
            # Query each service metric
            for metric_name, query_template in queries.items():
                query = query_template.replace('__TENANT__', tenant_id)
                
                try:
                    result = await self.thanos_manager.query(
                        promql=query,
                        time_range=time_range
                    )
                    
                    if result.data and result.data[0].values:
                        metrics[metric_name] = result.data[0].values[0].value
                    else:
                        metrics[metric_name] = 0.0 if metric_name != 'availability' else 1.0
                        
                except Exception as e:
                    logger.error(f"Failed to get {metric_name} for {service_type}: {e}")
                    metrics[metric_name] = 0.0 if metric_name != 'availability' else 1.0
                    
            service_metrics[service_type] = ServiceMetrics(
                service_type=service_type,
                availability=metrics.get('availability', 1.0),
                latency_p50=metrics.get('latency_p50', 0.0),
                latency_p95=metrics.get('latency_p95', 0.0),
                latency_p99=metrics.get('latency_p99', 0.0),
                error_rate=metrics.get('error_rate', 0.0),
                throughput=metrics.get('throughput', 0.0),
                active_connections=int(metrics.get('active_connections', 0)),
                resource_metrics=resource_metrics
            )
            
        return service_metrics
        
    async def _get_resource_metrics_for_service(
        self,
        resource_query: str,
        time_range: TimeRange
    ) -> ResourceMetrics:
        """Get resource metrics for a specific service"""
        # This would query container metrics for the service
        # For now, return estimated values
        return ResourceMetrics(
            cpu_usage=10.0,
            memory_usage=25.0,
            disk_usage=30.0,
            network_ingress=1000000,  # 1MB/s
            network_egress=500000,    # 500KB/s
            iops=100,
            timestamp=datetime.utcnow()
        )
        
    async def _get_tenant_alerts(self, tenant_id: str) -> List[Alert]:
        """Get active alerts for a tenant"""
        alerts = []
        
        try:
            # Query Alertmanager via Thanos
            query = f'ALERTS{{tenant_id="{tenant_id}"}}'
            result = await self.thanos_manager.query(promql=query)
            
            for series in result.data:
                alert = Alert(
                    name=series.labels.get('alertname', 'unknown'),
                    state=series.labels.get('alertstate', 'firing'),
                    labels=series.labels,
                    annotations={},  # Would need separate query
                    active_at=datetime.fromtimestamp(series.values[0].timestamp),
                    value=series.values[0].value,
                    region=series.labels.get('region', 'unknown')
                )
                alerts.append(alert)
                
        except Exception as e:
            logger.error(f"Failed to get alerts for tenant {tenant_id}: {e}")
            
        return alerts
        
    async def _get_active_regions(
        self,
        tenant_id: str,
        time_range: TimeRange
    ) -> List[str]:
        """Get regions where tenant has active resources"""
        try:
            # Query for unique regions
            result = await self.thanos_manager.get_label_values(
                label_name='region',
                start=time_range.start,
                end=time_range.end
            )
            
            # Filter by tenant activity
            active_regions = []
            for region in result:
                query = f'up{{tenant_id="{tenant_id}",region="{region}"}}'
                region_result = await self.thanos_manager.query(
                    promql=query,
                    time_range=time_range
                )
                if region_result.data:
                    active_regions.append(region)
                    
            return active_regions
            
        except Exception as e:
            logger.error(f"Failed to get active regions: {e}")
            return []
            
    def _parse_time_range(self, time_range_str: str) -> TimeRange:
        """Parse time range string (e.g., '1h', '24h', '7d')"""
        now = datetime.utcnow()
        
        # Parse duration
        if time_range_str.endswith('m'):
            delta = timedelta(minutes=int(time_range_str[:-1]))
        elif time_range_str.endswith('h'):
            delta = timedelta(hours=int(time_range_str[:-1]))
        elif time_range_str.endswith('d'):
            delta = timedelta(days=int(time_range_str[:-1]))
        else:
            delta = timedelta(hours=1)  # Default to 1 hour
            
        return TimeRange(
            start=now - delta,
            end=now
        )
        
    def _calculate_total_resources(
        self,
        resource_metrics: ResourceMetrics
    ) -> Dict[str, float]:
        """Calculate total resource usage"""
        return {
            'cpu_cores': resource_metrics.cpu_usage / 100.0 * 8,  # Assuming 8 cores
            'memory_gb': resource_metrics.memory_usage / 100.0 * 32,  # Assuming 32GB
            'disk_gb': resource_metrics.disk_usage / 100.0 * 500,  # Assuming 500GB
            'network_mbps': (resource_metrics.network_ingress + resource_metrics.network_egress) / 1000000
        }
        
    def _calculate_cost_estimate(
        self,
        resource_metrics: ResourceMetrics,
        service_metrics: Dict[ServiceType, ServiceMetrics]
    ) -> float:
        """Calculate estimated cost based on resource usage"""
        # Simple cost model (would be more complex in reality)
        cost = 0.0
        
        # Compute cost: $0.05 per CPU core hour
        cost += (resource_metrics.cpu_usage / 100.0 * 8) * 0.05
        
        # Memory cost: $0.01 per GB hour
        cost += (resource_metrics.memory_usage / 100.0 * 32) * 0.01
        
        # Storage cost: $0.10 per GB month (converted to hourly)
        cost += (resource_metrics.disk_usage / 100.0 * 500) * 0.10 / 730
        
        # Network cost: $0.01 per GB
        gb_transferred = (resource_metrics.network_ingress + resource_metrics.network_egress) / 1e9
        cost += gb_transferred * 0.01
        
        # Service-specific costs
        service_costs = {
            ServiceType.CASSANDRA: 0.20,
            ServiceType.IGNITE: 0.15,
            ServiceType.PULSAR: 0.25,
            ServiceType.MINIO: 0.10,
            ServiceType.ELASTICSEARCH: 0.30,
            ServiceType.JANUSGRAPH: 0.20,
            ServiceType.KUBERNETES: 0.05,
            ServiceType.VAULT: 0.10,
            ServiceType.CONSUL: 0.05
        }
        
        for service_type, metrics in service_metrics.items():
            if metrics.availability > 0:
                cost += service_costs.get(service_type, 0.10)
                
        return round(cost, 2) 