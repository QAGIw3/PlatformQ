"""Metrics collection for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import time
import logging
from contextlib import contextmanager

from prometheus_client import Counter, Histogram, Gauge, Summary, Info
from prometheus_client import CollectorRegistry, push_to_gateway

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    INFO = "info"


@dataclass
class MetricDefinition:
    """Definition of a metric."""
    
    name: str
    description: str
    metric_type: MetricType
    labels: List[str] = None
    buckets: List[float] = None  # For histograms
    
    def __post_init__(self):
        if self.labels is None:
            self.labels = []
        if self.buckets is None and self.metric_type == MetricType.HISTOGRAM:
            # Default buckets for histograms
            self.buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]


class MetricsCollector:
    """
    Centralized metrics collection for DataIntelligenceSuite services.
    
    Features:
    - Standard metrics for all services
    - Custom metric registration
    - Push gateway support
    - Metric aggregation
    - Performance tracking
    """
    
    def __init__(self, service_name: str, push_gateway_url: Optional[str] = None):
        self.service_name = service_name
        self.push_gateway_url = push_gateway_url
        self.registry = CollectorRegistry()
        self.metrics: Dict[str, Any] = {}
        
        # Initialize standard metrics
        self._init_standard_metrics()
        
    def _init_standard_metrics(self):
        """Initialize standard metrics for all services."""
        # Service info
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_service_info",
                description="Service information",
                metric_type=MetricType.INFO
            )
        )
        
        # Request metrics
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_requests_total",
                description="Total number of requests",
                metric_type=MetricType.COUNTER,
                labels=["method", "endpoint", "status"]
            )
        )
        
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_request_duration_seconds",
                description="Request duration in seconds",
                metric_type=MetricType.HISTOGRAM,
                labels=["method", "endpoint"]
            )
        )
        
        # Database metrics
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_db_queries_total",
                description="Total database queries",
                metric_type=MetricType.COUNTER,
                labels=["database", "operation", "status"]
            )
        )
        
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_db_query_duration_seconds",
                description="Database query duration",
                metric_type=MetricType.HISTOGRAM,
                labels=["database", "operation"]
            )
        )
        
        # Cache metrics
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_cache_hits_total",
                description="Cache hits",
                metric_type=MetricType.COUNTER,
                labels=["cache_name"]
            )
        )
        
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_cache_misses_total",
                description="Cache misses",
                metric_type=MetricType.COUNTER,
                labels=["cache_name"]
            )
        )
        
        # Event processing metrics
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_events_processed_total",
                description="Total events processed",
                metric_type=MetricType.COUNTER,
                labels=["event_type", "status"]
            )
        )
        
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_event_processing_duration_seconds",
                description="Event processing duration",
                metric_type=MetricType.HISTOGRAM,
                labels=["event_type"]
            )
        )
        
        # Resource metrics
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_active_connections",
                description="Number of active connections",
                metric_type=MetricType.GAUGE,
                labels=["connection_type"]
            )
        )
        
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_memory_usage_bytes",
                description="Memory usage in bytes",
                metric_type=MetricType.GAUGE
            )
        )
        
        # Error metrics
        self.register_metric(
            MetricDefinition(
                name="data_intelligence_errors_total",
                description="Total errors",
                metric_type=MetricType.COUNTER,
                labels=["error_type", "component"]
            )
        )
        
    def register_metric(self, definition: MetricDefinition) -> Any:
        """Register a custom metric."""
        metric_name = f"{definition.name}_{self.service_name}".replace("-", "_")
        
        if metric_name in self.metrics:
            logger.warning(f"Metric {metric_name} already registered")
            return self.metrics[metric_name]
            
        # Create metric based on type
        if definition.metric_type == MetricType.COUNTER:
            metric = Counter(
                metric_name,
                definition.description,
                definition.labels,
                registry=self.registry
            )
        elif definition.metric_type == MetricType.GAUGE:
            metric = Gauge(
                metric_name,
                definition.description,
                definition.labels,
                registry=self.registry
            )
        elif definition.metric_type == MetricType.HISTOGRAM:
            metric = Histogram(
                metric_name,
                definition.description,
                definition.labels,
                buckets=definition.buckets,
                registry=self.registry
            )
        elif definition.metric_type == MetricType.SUMMARY:
            metric = Summary(
                metric_name,
                definition.description,
                definition.labels,
                registry=self.registry
            )
        elif definition.metric_type == MetricType.INFO:
            metric = Info(
                metric_name,
                definition.description,
                registry=self.registry
            )
        else:
            raise ValueError(f"Unknown metric type: {definition.metric_type}")
            
        self.metrics[definition.name] = metric
        logger.info(f"Registered metric: {metric_name}")
        
        return metric
        
    def get_metric(self, name: str) -> Any:
        """Get a registered metric."""
        return self.metrics.get(name)
        
    def increment_counter(self, name: str, labels: Dict[str, str] = None, amount: float = 1):
        """Increment a counter metric."""
        metric = self.get_metric(name)
        if not metric:
            logger.warning(f"Metric {name} not found")
            return
            
        if labels:
            metric.labels(**labels).inc(amount)
        else:
            metric.inc(amount)
            
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric."""
        metric = self.get_metric(name)
        if not metric:
            logger.warning(f"Metric {name} not found")
            return
            
        if labels:
            metric.labels(**labels).set(value)
        else:
            metric.set(value)
            
    def observe_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Observe a histogram metric."""
        metric = self.get_metric(name)
        if not metric:
            logger.warning(f"Metric {name} not found")
            return
            
        if labels:
            metric.labels(**labels).observe(value)
        else:
            metric.observe(value)
            
    @contextmanager
    def timer(self, name: str, labels: Dict[str, str] = None):
        """Context manager for timing operations."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.observe_histogram(name, duration, labels)
            
    def track_request(self, method: str, endpoint: str, status: int, duration: float):
        """Track HTTP request metrics."""
        labels = {
            "method": method,
            "endpoint": endpoint,
            "status": str(status)
        }
        
        self.increment_counter("data_intelligence_requests_total", labels)
        self.observe_histogram(
            "data_intelligence_request_duration_seconds",
            duration,
            {"method": method, "endpoint": endpoint}
        )
        
    def track_database_query(self, database: str, operation: str, duration: float, success: bool):
        """Track database query metrics."""
        status = "success" if success else "failure"
        labels = {
            "database": database,
            "operation": operation,
            "status": status
        }
        
        self.increment_counter("data_intelligence_db_queries_total", labels)
        self.observe_histogram(
            "data_intelligence_db_query_duration_seconds",
            duration,
            {"database": database, "operation": operation}
        )
        
    def track_cache_access(self, cache_name: str, hit: bool):
        """Track cache access metrics."""
        if hit:
            self.increment_counter("data_intelligence_cache_hits_total", {"cache_name": cache_name})
        else:
            self.increment_counter("data_intelligence_cache_misses_total", {"cache_name": cache_name})
            
    def track_event(self, event_type: str, duration: float, success: bool):
        """Track event processing metrics."""
        status = "success" if success else "failure"
        
        self.increment_counter(
            "data_intelligence_events_processed_total",
            {"event_type": event_type, "status": status}
        )
        self.observe_histogram(
            "data_intelligence_event_processing_duration_seconds",
            duration,
            {"event_type": event_type}
        )
        
    def track_error(self, error_type: str, component: str):
        """Track error metrics."""
        self.increment_counter(
            "data_intelligence_errors_total",
            {"error_type": error_type, "component": component}
        )
        
    def update_active_connections(self, connection_type: str, count: int):
        """Update active connections gauge."""
        self.set_gauge(
            "data_intelligence_active_connections",
            count,
            {"connection_type": connection_type}
        )
        
    def update_memory_usage(self, bytes_used: int):
        """Update memory usage gauge."""
        self.set_gauge("data_intelligence_memory_usage_bytes", bytes_used)
        
    def push_metrics(self):
        """Push metrics to push gateway if configured."""
        if not self.push_gateway_url:
            return
            
        try:
            push_to_gateway(
                self.push_gateway_url,
                job=self.service_name,
                registry=self.registry
            )
            logger.debug("Pushed metrics to gateway")
        except Exception as e:
            logger.error(f"Failed to push metrics: {e}")
            
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of current metrics."""
        summary = {
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {}
        }
        
        # Add metric values (simplified)
        for name, metric in self.metrics.items():
            try:
                # Get sample value for demonstration
                if hasattr(metric, '_value'):
                    summary["metrics"][name] = metric._value.get()
            except:
                pass
                
        return summary 