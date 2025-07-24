"""
Unified Telemetry Collection

Provides centralized telemetry collection with distributed tracing support.
"""

from typing import Dict, Any, Optional, List, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
import asyncio
import time
import uuid
import json
from functools import wraps

# OpenTelemetry imports
from opentelemetry import trace, metrics, baggage
from opentelemetry.trace import Tracer, Span, Status, StatusCode
from opentelemetry.metrics import Meter, Counter, Histogram, UpDownCounter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
from opentelemetry.propagate import set_global_textmap

from .base import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class TelemetryLevel(str, Enum):
    """Telemetry collection levels"""
    OFF = "off"
    BASIC = "basic"        # Only errors and high-level metrics
    STANDARD = "standard"  # Standard metrics and sampling
    DETAILED = "detailed"  # Full metrics and traces
    DEBUG = "debug"        # Everything including debug spans


@dataclass
class TelemetryConfig:
    """Telemetry configuration"""
    service_name: str
    service_version: str = "1.0.0"
    environment: str = "development"
    
    # Collection settings
    telemetry_level: TelemetryLevel = TelemetryLevel.STANDARD
    enable_traces: bool = True
    enable_metrics: bool = True
    enable_logs: bool = True
    
    # Sampling
    trace_sample_rate: float = 0.1  # 10% sampling by default
    metric_interval: timedelta = field(default_factory=lambda: timedelta(seconds=60))
    
    # Exporters
    otlp_endpoint: str = "localhost:4317"
    otlp_headers: Dict[str, str] = field(default_factory=dict)
    export_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Resource attributes
    resource_attributes: Dict[str, Any] = field(default_factory=dict)
    
    # Performance
    max_export_batch_size: int = 512
    max_queue_size: int = 2048
    
    # Metric naming
    metric_prefix: str = "platformq"
    metric_namespace: str = ""


class MetricType(str, Enum):
    """Types of metrics"""
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"


@dataclass
class MetricDefinition:
    """Definition of a metric"""
    name: str
    type: MetricType
    description: str
    unit: str = ""
    tags: List[str] = field(default_factory=list)


class TelemetryCollector:
    """
    Unified telemetry collector with OpenTelemetry.
    
    Features:
    - Distributed tracing with context propagation
    - Metrics collection (counters, histograms, gauges)
    - Automatic instrumentation
    - Sampling and filtering
    - Multiple export targets
    - Performance optimization
    """
    
    _instance: Optional['TelemetryCollector'] = None
    _initialized: bool = False
    
    def __new__(cls, config: Optional[TelemetryConfig] = None):
        """Singleton pattern for global telemetry"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
        
    def __init__(self, config: Optional[TelemetryConfig] = None):
        if self._initialized:
            return
            
        self.config = config or TelemetryConfig(
            service_name="unknown",
            service_version="0.0.0"
        )
        
        # Initialize providers
        self._tracer_provider: Optional[TracerProvider] = None
        self._meter_provider: Optional[MeterProvider] = None
        self._tracer: Optional[Tracer] = None
        self._meter: Optional[Meter] = None
        
        # Metric instruments
        self._counters: Dict[str, Counter] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._gauges: Dict[str, UpDownCounter] = {}
        
        # Metric definitions
        self._metric_definitions: Dict[str, MetricDefinition] = {}
        
        # Initialize
        self._setup_telemetry()
        self._initialized = True
        
    def _setup_telemetry(self):
        """Setup OpenTelemetry providers"""
        # Create resource
        resource = Resource.create({
            "service.name": self.config.service_name,
            "service.version": self.config.service_version,
            "deployment.environment": self.config.environment,
            **self.config.resource_attributes
        })
        
        # Setup tracing
        if self.config.enable_traces:
            self._setup_tracing(resource)
            
        # Setup metrics
        if self.config.enable_metrics:
            self._setup_metrics(resource)
            
        # Auto-instrumentation
        self._setup_instrumentation()
        
        logger.info(
            "Telemetry initialized",
            service=self.config.service_name,
            level=self.config.telemetry_level.value
        )
        
    def _setup_tracing(self, resource: Resource):
        """Setup distributed tracing"""
        # Create tracer provider
        self._tracer_provider = TracerProvider(resource=resource)
        
        # Add OTLP exporter
        otlp_exporter = OTLPSpanExporter(
            endpoint=self.config.otlp_endpoint,
            headers=self.config.otlp_headers,
            timeout=int(self.config.export_timeout.total_seconds())
        )
        
        span_processor = BatchSpanProcessor(
            otlp_exporter,
            max_queue_size=self.config.max_queue_size,
            max_export_batch_size=self.config.max_export_batch_size
        )
        
        self._tracer_provider.add_span_processor(span_processor)
        
        # Set global tracer provider
        trace.set_tracer_provider(self._tracer_provider)
        
        # Get tracer
        self._tracer = trace.get_tracer(
            self.config.service_name,
            self.config.service_version
        )
        
    def _setup_metrics(self, resource: Resource):
        """Setup metrics collection"""
        # Create metric reader
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=self.config.otlp_endpoint,
                headers=self.config.otlp_headers,
                timeout=int(self.config.export_timeout.total_seconds())
            ),
            export_interval_millis=int(self.config.metric_interval.total_seconds() * 1000)
        )
        
        # Create meter provider
        self._meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader]
        )
        
        # Set global meter provider
        metrics.set_meter_provider(self._meter_provider)
        
        # Get meter
        self._meter = metrics.get_meter(
            self.config.service_name,
            self.config.service_version
        )
        
        # Register standard metrics
        self._register_standard_metrics()
        
    def _setup_instrumentation(self):
        """Setup automatic instrumentation"""
        # Instrument aiohttp client
        AioHttpClientInstrumentor().instrument()
        
        # Instrument asyncpg
        AsyncPGInstrumentor().instrument()
        
        # Add more instrumentations as needed
        
    def _register_standard_metrics(self):
        """Register standard platform metrics"""
        # Request metrics
        self.register_metric(
            name="request_total",
            type=MetricType.COUNTER,
            description="Total number of requests",
            tags=["method", "endpoint", "status"]
        )
        
        self.register_metric(
            name="request_duration",
            type=MetricType.HISTOGRAM,
            description="Request duration in milliseconds",
            unit="ms",
            tags=["method", "endpoint"]
        )
        
        # Error metrics
        self.register_metric(
            name="error_total",
            type=MetricType.COUNTER,
            description="Total number of errors",
            tags=["type", "severity"]
        )
        
        # Resource metrics
        self.register_metric(
            name="active_connections",
            type=MetricType.GAUGE,
            description="Number of active connections",
            tags=["service", "pool"]
        )
        
    def register_metric(
        self,
        name: str,
        type: MetricType,
        description: str,
        unit: str = "",
        tags: Optional[List[str]] = None
    ) -> MetricDefinition:
        """Register a metric definition"""
        metric_def = MetricDefinition(
            name=name,
            type=type,
            description=description,
            unit=unit,
            tags=tags or []
        )
        
        self._metric_definitions[name] = metric_def
        
        # Create instrument
        full_name = self._get_metric_name(name)
        
        if type == MetricType.COUNTER:
            self._counters[name] = self._meter.create_counter(
                full_name,
                description=description,
                unit=unit
            )
        elif type == MetricType.HISTOGRAM:
            self._histograms[name] = self._meter.create_histogram(
                full_name,
                description=description,
                unit=unit
            )
        elif type == MetricType.GAUGE:
            self._gauges[name] = self._meter.create_up_down_counter(
                full_name,
                description=description,
                unit=unit
            )
            
        return metric_def
        
    def _get_metric_name(self, name: str) -> str:
        """Get full metric name with prefix and namespace"""
        parts = [self.config.metric_prefix]
        
        if self.config.metric_namespace:
            parts.append(self.config.metric_namespace)
            
        parts.append(name)
        
        return "_".join(parts)
        
    # Tracing methods
    
    @contextmanager
    def span(
        self,
        name: str,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Create a trace span"""
        if not self._should_trace():
            yield None
            return
            
        with self._tracer.start_as_current_span(
            name,
            kind=kind,
            attributes=attributes
        ) as span:
            try:
                yield span
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise
                
    @asynccontextmanager
    async def async_span(
        self,
        name: str,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Create an async trace span"""
        if not self._should_trace():
            yield None
            return
            
        with self._tracer.start_as_current_span(
            name,
            kind=kind,
            attributes=attributes
        ) as span:
            try:
                yield span
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise
                
    def trace(
        self,
        name: Optional[str] = None,
        kind: trace.SpanKind = trace.SpanKind.INTERNAL,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Decorator for tracing functions"""
        def decorator(func):
            span_name = name or f"{func.__module__}.{func.__name__}"
            
            if asyncio.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    async with self.async_span(span_name, kind, attributes):
                        return await func(*args, **kwargs)
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    with self.span(span_name, kind, attributes):
                        return func(*args, **kwargs)
                return sync_wrapper
                
        return decorator
        
    def _should_trace(self) -> bool:
        """Check if tracing should be enabled"""
        if not self.config.enable_traces:
            return False
            
        if self.config.telemetry_level == TelemetryLevel.OFF:
            return False
            
        # Apply sampling
        if self.config.trace_sample_rate < 1.0:
            import random
            return random.random() < self.config.trace_sample_rate
            
        return True
        
    # Metric methods
    
    def increment(
        self,
        metric: str,
        value: int = 1,
        tags: Optional[Dict[str, Any]] = None
    ):
        """Increment a counter metric"""
        if metric not in self._counters:
            logger.warning(f"Metric {metric} not registered")
            return
            
        counter = self._counters[metric]
        counter.add(value, attributes=tags or {})
        
    def record(
        self,
        metric: str,
        value: Union[int, float],
        tags: Optional[Dict[str, Any]] = None
    ):
        """Record a histogram metric"""
        if metric not in self._histograms:
            logger.warning(f"Metric {metric} not registered")
            return
            
        histogram = self._histograms[metric]
        histogram.record(value, attributes=tags or {})
        
    def gauge(
        self,
        metric: str,
        value: Union[int, float],
        tags: Optional[Dict[str, Any]] = None
    ):
        """Set a gauge metric"""
        if metric not in self._gauges:
            logger.warning(f"Metric {metric} not registered")
            return
            
        gauge = self._gauges[metric]
        # For gauge, we need to track current value and adjust
        # This is simplified - in production, track previous values
        gauge.add(value, attributes=tags or {})
        
    @contextmanager
    def timer(
        self,
        metric: str,
        tags: Optional[Dict[str, Any]] = None
    ):
        """Time a code block and record as histogram"""
        start_time = time.time()
        try:
            yield
        finally:
            duration = (time.time() - start_time) * 1000  # Convert to ms
            self.record(metric, duration, tags)
            
    # Correlation and context
    
    def get_trace_id(self) -> Optional[str]:
        """Get current trace ID"""
        span = trace.get_current_span()
        if span and span.is_recording():
            context = span.get_span_context()
            return format(context.trace_id, '032x')
        return None
        
    def get_span_id(self) -> Optional[str]:
        """Get current span ID"""
        span = trace.get_current_span()
        if span and span.is_recording():
            context = span.get_span_context()
            return format(context.span_id, '016x')
        return None
        
    def set_baggage(self, key: str, value: str):
        """Set baggage item for context propagation"""
        baggage.set_baggage(key, value)
        
    def get_baggage(self, key: str) -> Optional[str]:
        """Get baggage item"""
        return baggage.get_baggage(key)
        
    # Shutdown
    
    def shutdown(self):
        """Shutdown telemetry collection"""
        if self._tracer_provider:
            self._tracer_provider.shutdown()
            
        if self._meter_provider:
            self._meter_provider.shutdown()
            
        logger.info("Telemetry shutdown complete")


# Global instance
_telemetry: Optional[TelemetryCollector] = None


def init_telemetry(config: TelemetryConfig) -> TelemetryCollector:
    """Initialize global telemetry collector"""
    global _telemetry
    _telemetry = TelemetryCollector(config)
    return _telemetry


def get_telemetry() -> TelemetryCollector:
    """Get global telemetry collector"""
    if _telemetry is None:
        raise RuntimeError("Telemetry not initialized")
    return _telemetry


# Convenience decorators

def traced(
    name: Optional[str] = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    attributes: Optional[Dict[str, Any]] = None
):
    """Decorator for tracing functions using global telemetry"""
    def decorator(func):
        telemetry = get_telemetry()
        return telemetry.trace(name, kind, attributes)(func)
    return decorator


def timed(metric: str, tags: Optional[Dict[str, Any]] = None):
    """Decorator for timing functions"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            telemetry = get_telemetry()
            with telemetry.timer(metric, tags):
                return func(*args, **kwargs)
        return wrapper
    return decorator


# Export main components
__all__ = [
    'TelemetryCollector',
    'TelemetryConfig',
    'TelemetryLevel',
    'MetricType',
    'MetricDefinition',
    'init_telemetry',
    'get_telemetry',
    'traced',
    'timed'
] 