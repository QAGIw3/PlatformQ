"""
Unified Monitoring Module for PlatformQ Services

Provides standardized metrics, alerting, and observability.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Callable, Set
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
from decimal import Decimal
import json

from prometheus_client import Counter, Histogram, Gauge, Summary, Info
from prometheus_client.core import CollectorRegistry
import opentelemetry
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
import pulsar

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Standard metric types"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class StandardMetrics:
    """Standard metrics for all PlatformQ services"""
    
    def __init__(self, service_name: str, registry: Optional[CollectorRegistry] = None):
        self.service_name = service_name
        self.registry = registry or CollectorRegistry()
        
        # HTTP metrics
        self.http_requests_total = Counter(
            f'{service_name}_http_requests_total',
            'Total HTTP requests',
            ['method', 'endpoint', 'status'],
            registry=self.registry
        )
        
        self.http_request_duration_seconds = Histogram(
            f'{service_name}_http_request_duration_seconds',
            'HTTP request duration',
            ['method', 'endpoint'],
            buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
            registry=self.registry
        )
        
        self.http_requests_in_flight = Gauge(
            f'{service_name}_http_requests_in_flight',
            'HTTP requests currently being processed',
            registry=self.registry
        )
        
        # Business metrics
        self.business_operations_total = Counter(
            f'{service_name}_business_operations_total',
            'Total business operations',
            ['operation', 'status'],
            registry=self.registry
        )
        
        self.business_operation_duration_seconds = Histogram(
            f'{service_name}_business_operation_duration_seconds',
            'Business operation duration',
            ['operation'],
            registry=self.registry
        )
        
        # Trading-specific metrics (if applicable)
        self.trading_volume_total = Counter(
            f'{service_name}_trading_volume_total',
            'Total trading volume in USD',
            ['market', 'asset'],
            registry=self.registry
        )
        
        self.orders_total = Counter(
            f'{service_name}_orders_total',
            'Total orders',
            ['type', 'status'],
            registry=self.registry
        )
        
        self.order_latency_seconds = Histogram(
            f'{service_name}_order_latency_seconds',
            'Order processing latency',
            ['order_type'],
            buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
            registry=self.registry
        )
        
        # System metrics
        self.dependency_health = Gauge(
            f'{service_name}_dependency_health',
            'Health status of dependencies (1=healthy, 0=unhealthy)',
            ['dependency'],
            registry=self.registry
        )
        
        self.error_rate = Gauge(
            f'{service_name}_error_rate',
            'Current error rate',
            registry=self.registry
        )
        
        self.circuit_breaker_state = Gauge(
            f'{service_name}_circuit_breaker_state',
            'Circuit breaker state (0=closed, 1=open, 2=half-open)',
            ['service'],
            registry=self.registry
        )
        
        # Direct communication metrics
        self.direct_comm_messages_sent = Counter(
            f'{service_name}_direct_comm_messages_sent_total',
            'Total messages sent via direct communication',
            ['target_service', 'message_type'],
            registry=self.registry
        )
        
        self.direct_comm_latency_seconds = Histogram(
            f'{service_name}_direct_comm_latency_seconds',
            'Direct communication latency',
            ['target_service'],
            buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05],
            registry=self.registry
        )
        
        # Service info
        self.service_info = Info(
            f'{service_name}_info',
            'Service information',
            registry=self.registry
        )


class MetricSnapshot:
    """Point-in-time metric value"""
    
    def __init__(self, value: float, timestamp: Optional[datetime] = None):
        self.value = value
        self.timestamp = timestamp or datetime.utcnow()
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "timestamp": self.timestamp.isoformat()
        }


class Alert:
    """Alert definition"""
    
    def __init__(
        self,
        alert_id: str,
        name: str,
        condition: Callable[[float], bool],
        severity: AlertSeverity,
        message_template: str,
        cooldown_seconds: int = 300
    ):
        self.alert_id = alert_id
        self.name = name
        self.condition = condition
        self.severity = severity
        self.message_template = message_template
        self.cooldown_seconds = cooldown_seconds
        self.last_triggered: Optional[datetime] = None
        self.is_active = False
        
    def check(self, value: float) -> Optional[Dict[str, Any]]:
        """Check if alert should be triggered"""
        should_alert = self.condition(value)
        
        if should_alert and not self.is_active:
            # Check cooldown
            if self.last_triggered:
                elapsed = (datetime.utcnow() - self.last_triggered).total_seconds()
                if elapsed < self.cooldown_seconds:
                    return None
                    
            # Trigger alert
            self.is_active = True
            self.last_triggered = datetime.utcnow()
            
            return {
                "alert_id": self.alert_id,
                "name": self.name,
                "severity": self.severity.value,
                "message": self.message_template.format(value=value),
                "value": value,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        elif not should_alert and self.is_active:
            # Clear alert
            self.is_active = False
            
            return {
                "alert_id": self.alert_id,
                "name": self.name,
                "severity": "info",
                "message": f"Alert cleared: {self.name}",
                "timestamp": datetime.utcnow().isoformat()
            }
            
        return None


class UnifiedMonitoring:
    """Unified monitoring system for all PlatformQ services"""
    
    def __init__(
        self,
        service_name: str,
        pulsar_client: Optional[pulsar.Client] = None,
        enable_tracing: bool = True,
        enable_metrics_export: bool = True,
        otlp_endpoint: Optional[str] = None
    ):
        self.service_name = service_name
        self.pulsar_client = pulsar_client
        
        # Initialize standard metrics
        self.metrics = StandardMetrics(service_name)
        
        # Custom metrics registry
        self.custom_metrics: Dict[str, Any] = {}
        
        # Time series data
        self.time_series: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        
        # Alerts
        self.alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        
        # Initialize OpenTelemetry if enabled
        if enable_tracing:
            self._init_tracing(otlp_endpoint)
            
        if enable_metrics_export:
            self._init_metrics_export(otlp_endpoint)
            
        # Alert publisher
        self._alert_producer: Optional[pulsar.Producer] = None
        if self.pulsar_client:
            self._init_alert_publisher()
            
        # Background monitoring task
        self._monitoring_task: Optional[asyncio.Task] = None
        
    def _init_tracing(self, otlp_endpoint: Optional[str]):
        """Initialize OpenTelemetry tracing"""
        # Set up the tracer provider
        trace.set_tracer_provider(TracerProvider())
        
        if otlp_endpoint:
            # Configure OTLP exporter
            otlp_exporter = OTLPSpanExporter(
                endpoint=otlp_endpoint,
                insecure=True
            )
            
            # Add span processor
            span_processor = BatchSpanProcessor(otlp_exporter)
            trace.get_tracer_provider().add_span_processor(span_processor)
            
        # Get tracer
        self.tracer = trace.get_tracer(__name__)
        
        # Auto-instrument FastAPI
        FastAPIInstrumentor.instrument(tracer_provider=trace.get_tracer_provider())
        
        # Auto-instrument HTTPX
        HTTPXClientInstrumentor.instrument()
        
    def _init_metrics_export(self, otlp_endpoint: Optional[str]):
        """Initialize OpenTelemetry metrics export"""
        if otlp_endpoint:
            # Configure OTLP metric exporter
            metric_exporter = OTLPMetricExporter(
                endpoint=otlp_endpoint,
                insecure=True
            )
            
            # Set up metric reader
            metric_reader = PeriodicExportingMetricReader(
                exporter=metric_exporter,
                export_interval_millis=10000  # Export every 10 seconds
            )
            
            # Set up meter provider
            provider = MeterProvider(metric_readers=[metric_reader])
            metrics.set_meter_provider(provider)
            
        # Get meter
        self.meter = metrics.get_meter(__name__)
        
    def _init_alert_publisher(self):
        """Initialize Pulsar alert publisher"""
        try:
            self._alert_producer = self.pulsar_client.create_producer(
                'persistent://public/monitoring/alerts',
                producer_name=f"{self.service_name}-alerts"
            )
            logger.info("Alert publisher initialized")
        except Exception as e:
            logger.error(f"Failed to initialize alert publisher: {e}")
            
    async def start(self):
        """Start monitoring background tasks"""
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"Monitoring started for {self.service_name}")
        
    async def stop(self):
        """Stop monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
                
        if self._alert_producer:
            self._alert_producer.close()
            
    def record_metric(
        self,
        metric_name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ):
        """Record a metric value"""
        # Update time series
        snapshot = MetricSnapshot(value)
        self.time_series[metric_name].append(snapshot)
        
        # Check alerts
        alert_key = f"{metric_name}:{json.dumps(labels or {}, sort_keys=True)}"
        if alert_key in self.alerts:
            alert = self.alerts[alert_key]
            alert_event = alert.check(value)
            if alert_event:
                asyncio.create_task(self._publish_alert(alert_event))
                
    def create_counter(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ) -> Counter:
        """Create a custom counter metric"""
        counter = Counter(
            f'{self.service_name}_{name}',
            description,
            labels or [],
            registry=self.metrics.registry
        )
        self.custom_metrics[name] = counter
        return counter
        
    def create_gauge(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ) -> Gauge:
        """Create a custom gauge metric"""
        gauge = Gauge(
            f'{self.service_name}_{name}',
            description,
            labels or [],
            registry=self.metrics.registry
        )
        self.custom_metrics[name] = gauge
        return gauge
        
    def create_histogram(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        buckets: Optional[List[float]] = None
    ) -> Histogram:
        """Create a custom histogram metric"""
        histogram = Histogram(
            f'{self.service_name}_{name}',
            description,
            labels or [],
            buckets=buckets,
            registry=self.metrics.registry
        )
        self.custom_metrics[name] = histogram
        return histogram
        
    def add_alert(
        self,
        metric_name: str,
        alert_name: str,
        condition: Callable[[float], bool],
        severity: AlertSeverity = AlertSeverity.WARNING,
        message_template: str = "{name} alert: value={value}",
        labels: Optional[Dict[str, str]] = None
    ):
        """Add an alert for a metric"""
        alert_key = f"{metric_name}:{json.dumps(labels or {}, sort_keys=True)}"
        alert_id = f"{self.service_name}:{alert_key}:{alert_name}"
        
        self.alerts[alert_key] = Alert(
            alert_id=alert_id,
            name=alert_name,
            condition=condition,
            severity=severity,
            message_template=message_template
        )
        
    async def _publish_alert(self, alert_event: Dict[str, Any]):
        """Publish alert to Pulsar"""
        if self._alert_producer:
            try:
                alert_event["service_name"] = self.service_name
                self._alert_producer.send_async(
                    json.dumps(alert_event).encode('utf-8'),
                    callback=lambda res, msg: logger.debug(f"Alert published: {alert_event['alert_id']}")
                )
                
                # Store in history
                self.alert_history.append(alert_event)
            except Exception as e:
                logger.error(f"Failed to publish alert: {e}")
                
    async def _monitoring_loop(self):
        """Background monitoring loop"""
        while True:
            try:
                # Calculate and update error rate
                await self._update_error_rate()
                
                # Check system health
                await self._check_system_health()
                
                # Clean old time series data
                self._clean_old_data()
                
                await asyncio.sleep(10)  # Run every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10)
                
    async def _update_error_rate(self):
        """Calculate current error rate"""
        # This is a simplified implementation
        # In practice, would calculate from actual error metrics
        pass
        
    async def _check_system_health(self):
        """Check overall system health"""
        # This is a simplified implementation
        # In practice, would check various health indicators
        pass
        
    def _clean_old_data(self):
        """Clean old time series data"""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        
        for metric_name, series in self.time_series.items():
            # Remove old entries
            while series and series[0].timestamp < cutoff_time:
                series.popleft()
                
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for monitoring dashboard"""
        return {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                name: [s.to_dict() for s in series]
                for name, series in self.time_series.items()
            },
            "active_alerts": [
                alert.alert_id for alert in self.alerts.values() if alert.is_active
            ],
            "alert_history": list(self.alert_history)
        }
        
    def create_span(self, name: str) -> Any:
        """Create a new trace span"""
        if hasattr(self, 'tracer'):
            return self.tracer.start_as_current_span(name)
        return None


# Convenience decorators
def monitor_operation(
    monitoring: UnifiedMonitoring,
    operation_name: str,
    record_duration: bool = True,
    record_errors: bool = True
):
    """Decorator to monitor a function/operation"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            
            # Start trace span
            with monitoring.create_span(operation_name):
                try:
                    result = await func(*args, **kwargs)
                    
                    # Record success
                    monitoring.metrics.business_operations_total.labels(
                        operation=operation_name,
                        status="success"
                    ).inc()
                    
                    return result
                    
                except Exception as e:
                    # Record error
                    if record_errors:
                        monitoring.metrics.business_operations_total.labels(
                            operation=operation_name,
                            status="error"
                        ).inc()
                    raise
                    
                finally:
                    # Record duration
                    if record_duration:
                        duration = time.time() - start_time
                        monitoring.metrics.business_operation_duration_seconds.labels(
                            operation=operation_name
                        ).observe(duration)
                        
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            
            # Start trace span
            with monitoring.create_span(operation_name):
                try:
                    result = func(*args, **kwargs)
                    
                    # Record success
                    monitoring.metrics.business_operations_total.labels(
                        operation=operation_name,
                        status="success"
                    ).inc()
                    
                    return result
                    
                except Exception as e:
                    # Record error
                    if record_errors:
                        monitoring.metrics.business_operations_total.labels(
                            operation=operation_name,
                            status="error"
                        ).inc()
                    raise
                    
                finally:
                    # Record duration
                    if record_duration:
                        duration = time.time() - start_time
                        monitoring.metrics.business_operation_duration_seconds.labels(
                            operation=operation_name
                        ).observe(duration)
                        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator 