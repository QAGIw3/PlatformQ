"""Data models for Platform Monitoring Service"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from enum import Enum


class RegionStatus(str, Enum):
    """Region health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ServiceType(str, Enum):
    """Platform Q service types"""
    CASSANDRA = "cassandra"
    IGNITE = "ignite"
    PULSAR = "pulsar"
    MINIO = "minio"
    ELASTICSEARCH = "elasticsearch"
    JANUSGRAPH = "janusgraph"
    KUBERNETES = "kubernetes"
    VAULT = "vault"
    CONSUL = "consul"


class MetricType(str, Enum):
    """Types of metrics"""
    GAUGE = "gauge"
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class RegionConfig(BaseModel):
    """Configuration for a region"""
    name: str
    prometheus_url: str
    thanos_sidecar_url: str
    alertmanager_url: Optional[str] = None
    consul_datacenter: str
    availability_zones: List[str] = []
    labels: Dict[str, str] = {}
    enabled: bool = True


class PrometheusTarget(BaseModel):
    """Prometheus scrape target"""
    job_name: str
    targets: List[str]
    labels: Dict[str, str] = {}
    scrape_interval: Optional[str] = None
    scrape_timeout: Optional[str] = None
    metrics_path: str = "/metrics"


class FederationStatus(BaseModel):
    """Federation status across all regions"""
    global_status: RegionStatus
    regions: Dict[str, Dict[str, Any]]
    last_sync: datetime
    total_targets: int
    active_alerts: int
    total_series: int


class TimeRange(BaseModel):
    """Time range for queries"""
    start: datetime
    end: datetime
    step: Optional[str] = None


class MetricsQuery(BaseModel):
    """Prometheus query request"""
    promql: str
    time_range: Optional[TimeRange] = None
    regions: Optional[List[str]] = None
    tenant_id: Optional[str] = None
    step: Optional[str] = "1m"
    timeout: Optional[int] = 30


class MetricValue(BaseModel):
    """Single metric value"""
    timestamp: float
    value: float


class MetricSeries(BaseModel):
    """Time series data"""
    labels: Dict[str, str]
    values: List[MetricValue]


class QueryResult(BaseModel):
    """Query result from Thanos"""
    status: str
    data: List[MetricSeries]
    warnings: List[str] = []
    execution_time: float


class AlertRule(BaseModel):
    """Prometheus alert rule"""
    name: str
    expr: str
    for_duration: str = "5m"
    labels: Dict[str, str] = {}
    annotations: Dict[str, str] = {}
    severity: str = "warning"
    tenant_id: Optional[str] = None


class Alert(BaseModel):
    """Active alert"""
    name: str
    state: str
    labels: Dict[str, str]
    annotations: Dict[str, str]
    active_at: datetime
    value: float
    region: str


class ResourceMetrics(BaseModel):
    """Resource usage metrics"""
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_ingress: float
    network_egress: float
    iops: float
    timestamp: datetime


class ServiceMetrics(BaseModel):
    """Service-specific metrics"""
    service_type: ServiceType
    availability: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    error_rate: float
    throughput: float
    active_connections: int
    resource_metrics: ResourceMetrics


class TenantMetrics(BaseModel):
    """Aggregated metrics for a tenant"""
    tenant_id: str
    time_range: TimeRange
    regions: List[str]
    total_resources: Dict[str, float]
    service_metrics: Dict[ServiceType, ServiceMetrics]
    cost_estimate: float
    alerts: List[Alert] = []


class ServiceEndpoint(BaseModel):
    """Service endpoint information"""
    service_name: str
    region: str
    address: str
    port: int
    protocol: str = "http"
    health_check_path: str = "/health"
    metrics_path: str = "/metrics"
    labels: Dict[str, str] = {}


class GrafanaDashboard(BaseModel):
    """Grafana dashboard configuration"""
    uid: str
    title: str
    tags: List[str]
    templating: Dict[str, Any]
    panels: List[Dict[str, Any]]
    time_range: Dict[str, str] = {"from": "now-6h", "to": "now"}
    refresh: str = "30s"


class RecordingRule(BaseModel):
    """Prometheus recording rule"""
    record: str
    expr: str
    labels: Dict[str, str] = {}
    interval: Optional[str] = None


class RuleGroup(BaseModel):
    """Group of Prometheus rules"""
    name: str
    interval: str = "30s"
    rules: List[Any]  # Can be AlertRule or RecordingRule


class CompactionGroup(BaseModel):
    """Thanos compaction configuration"""
    resolution: str  # 0 (raw), 5m, 1h
    retention: str
    deletion_delay: str = "48h"


class ThanosConfig(BaseModel):
    """Thanos component configuration"""
    version: str = "v0.32.0"
    object_store_config: Dict[str, Any]
    compaction_groups: List[CompactionGroup]
    query_frontend_config: Dict[str, Any] = {}
    store_config: Dict[str, Any] = {}
    ruler_config: Dict[str, Any] = {} 