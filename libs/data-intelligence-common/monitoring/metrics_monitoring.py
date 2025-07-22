"""
Comprehensive Metrics and Monitoring for DataIntelligence Services
Provides detailed metrics collection, monitoring, and observability
"""

import asyncio
import logging
import time
import os
from typing import Dict, Any, Optional, List, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import json
from prometheus_client import (
    Counter, Histogram, Gauge, Summary, Info,
    CollectorRegistry, generate_latest, push_to_gateway,
    CONTENT_TYPE_LATEST
)
from prometheus_client.openmetrics.exposition import generate_latest as openmetrics_generate_latest
import aiohttp
from functools import wraps

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    INFO = "info"


@dataclass
class MetricConfig:
    """Configuration for a metric"""
    name: str
    description: str
    metric_type: MetricType
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None  # For histograms
    quantiles: Optional[List[float]] = None  # For summaries
    namespace: str = "dataintelligence"
    subsystem: str = ""


class MetricsCollector:
    """Main metrics collection system"""
    
    def __init__(self, service_name: str, 
                 registry: Optional[CollectorRegistry] = None,
                 push_gateway: Optional[str] = None,
                 push_interval: int = 60):
        """
        Initialize metrics collector
        
        Args:
            service_name: Name of the service
            registry: Prometheus registry (creates new if None)
            push_gateway: URL of Prometheus push gateway
            push_interval: Interval for pushing metrics
        """
        self.service_name = service_name
        self.registry = registry or CollectorRegistry()
        self.push_gateway = push_gateway
        self.push_interval = push_interval
        
        self._metrics: Dict[str, Any] = {}
        self._custom_collectors: List[Callable] = []
        self._running = False
        
        # Initialize standard metrics
        self._init_standard_metrics()
        
    def _init_standard_metrics(self):
        """Initialize standard service metrics"""
        # Request metrics
        self.request_counter = self.create_metric(MetricConfig(
            name="requests_total",
            description="Total number of requests",
            metric_type=MetricType.COUNTER,
            labels=["method", "endpoint", "status"],
            subsystem=self.service_name
        ))
        
        self.request_duration = self.create_metric(MetricConfig(
            name="request_duration_seconds",
            description="Request duration in seconds",
            metric_type=MetricType.HISTOGRAM,
            labels=["method", "endpoint"],
            subsystem=self.service_name,
            buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
        ))
        
        self.request_size = self.create_metric(MetricConfig(
            name="request_size_bytes",
            description="Request size in bytes",
            metric_type=MetricType.HISTOGRAM,
            labels=["method", "endpoint"],
            subsystem=self.service_name,
            buckets=[100, 1000, 10000, 100000, 1000000, 10000000]
        ))
        
        self.response_size = self.create_metric(MetricConfig(
            name="response_size_bytes",
            description="Response size in bytes",
            metric_type=MetricType.HISTOGRAM,
            labels=["method", "endpoint"],
            subsystem=self.service_name,
            buckets=[100, 1000, 10000, 100000, 1000000, 10000000]
        ))
        
        # Error metrics
        self.error_counter = self.create_metric(MetricConfig(
            name="errors_total",
            description="Total number of errors",
            metric_type=MetricType.COUNTER,
            labels=["type", "operation"],
            subsystem=self.service_name
        ))
        
        # Performance metrics
        self.active_requests = self.create_metric(MetricConfig(
            name="active_requests",
            description="Number of active requests",
            metric_type=MetricType.GAUGE,
            subsystem=self.service_name
        ))
        
        self.queue_size = self.create_metric(MetricConfig(
            name="queue_size",
            description="Size of processing queue",
            metric_type=MetricType.GAUGE,
            labels=["queue_name"],
            subsystem=self.service_name
        ))
        
        # Data processing metrics
        self.data_processed = self.create_metric(MetricConfig(
            name="data_processed_bytes",
            description="Amount of data processed in bytes",
            metric_type=MetricType.COUNTER,
            labels=["operation", "data_type"],
            subsystem=self.service_name
        ))
        
        self.processing_duration = self.create_metric(MetricConfig(
            name="processing_duration_seconds",
            description="Data processing duration",
            metric_type=MetricType.HISTOGRAM,
            labels=["operation", "data_type"],
            subsystem=self.service_name
        ))
        
        # ML metrics
        self.model_predictions = self.create_metric(MetricConfig(
            name="model_predictions_total",
            description="Total model predictions",
            metric_type=MetricType.COUNTER,
            labels=["model_name", "model_version"],
            subsystem=self.service_name
        ))
        
        self.prediction_latency = self.create_metric(MetricConfig(
            name="prediction_latency_seconds",
            description="Model prediction latency",
            metric_type=MetricType.HISTOGRAM,
            labels=["model_name", "model_version"],
            subsystem=self.service_name
        ))
        
        self.model_accuracy = self.create_metric(MetricConfig(
            name="model_accuracy",
            description="Model accuracy score",
            metric_type=MetricType.GAUGE,
            labels=["model_name", "model_version"],
            subsystem=self.service_name
        ))
        
        # Service info
        self.service_info = self.create_metric(MetricConfig(
            name="service_info",
            description="Service information",
            metric_type=MetricType.INFO,
            subsystem=self.service_name
        ))
        
        # Set service info
        self.service_info.info({
            "service": self.service_name,
            "version": os.getenv("SERVICE_VERSION", "1.0.0"),
            "environment": os.getenv("ENVIRONMENT", "development")
        })
        
    def create_metric(self, config: MetricConfig) -> Any:
        """Create a new metric"""
        metric_name = f"{config.namespace}_{config.subsystem}_{config.name}"
        
        if metric_name in self._metrics:
            return self._metrics[metric_name]
            
        if config.metric_type == MetricType.COUNTER:
            metric = Counter(
                metric_name,
                config.description,
                config.labels,
                registry=self.registry
            )
        elif config.metric_type == MetricType.GAUGE:
            metric = Gauge(
                metric_name,
                config.description,
                config.labels,
                registry=self.registry
            )
        elif config.metric_type == MetricType.HISTOGRAM:
            metric = Histogram(
                metric_name,
                config.description,
                config.labels,
                buckets=config.buckets or Histogram.DEFAULT_BUCKETS,
                registry=self.registry
            )
        elif config.metric_type == MetricType.SUMMARY:
            metric = Summary(
                metric_name,
                config.description,
                config.labels,
                registry=self.registry
            )
        elif config.metric_type == MetricType.INFO:
            metric = Info(
                metric_name,
                config.description,
                registry=self.registry
            )
        else:
            raise ValueError(f"Unknown metric type: {config.metric_type}")
            
        self._metrics[metric_name] = metric
        return metric
        
    def track_request(self, method: str = "GET", endpoint: str = "/"):
        """Decorator to track HTTP requests"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                self.active_requests.inc()
                
                try:
                    result = await func(*args, **kwargs)
                    status = result.status_code if hasattr(result, 'status_code') else 200
                    
                    # Record metrics
                    self.request_counter.labels(
                        method=method,
                        endpoint=endpoint,
                        status=str(status)
                    ).inc()
                    
                    duration = time.time() - start_time
                    self.request_duration.labels(
                        method=method,
                        endpoint=endpoint
                    ).observe(duration)
                    
                    return result
                    
                except Exception as e:
                    self.error_counter.labels(
                        type=type(e).__name__,
                        operation="request"
                    ).inc()
                    raise
                    
                finally:
                    self.active_requests.dec()
                    
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                self.active_requests.inc()
                
                try:
                    result = func(*args, **kwargs)
                    status = result.status_code if hasattr(result, 'status_code') else 200
                    
                    # Record metrics
                    self.request_counter.labels(
                        method=method,
                        endpoint=endpoint,
                        status=str(status)
                    ).inc()
                    
                    duration = time.time() - start_time
                    self.request_duration.labels(
                        method=method,
                        endpoint=endpoint
                    ).observe(duration)
                    
                    return result
                    
                except Exception as e:
                    self.error_counter.labels(
                        type=type(e).__name__,
                        operation="request"
                    ).inc()
                    raise
                    
                finally:
                    self.active_requests.dec()
                    
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return sync_wrapper
            
        return decorator
        
    def track_processing(self, operation: str, data_type: str = "generic"):
        """Decorator to track data processing"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                
                try:
                    result = await func(*args, **kwargs)
                    
                    # Record metrics
                    duration = time.time() - start_time
                    self.processing_duration.labels(
                        operation=operation,
                        data_type=data_type
                    ).observe(duration)
                    
                    # Try to determine data size
                    if hasattr(result, '__len__'):
                        size = len(str(result).encode())
                        self.data_processed.labels(
                            operation=operation,
                            data_type=data_type
                        ).inc(size)
                        
                    return result
                    
                except Exception as e:
                    self.error_counter.labels(
                        type=type(e).__name__,
                        operation=operation
                    ).inc()
                    raise
                    
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    
                    # Record metrics
                    duration = time.time() - start_time
                    self.processing_duration.labels(
                        operation=operation,
                        data_type=data_type
                    ).observe(duration)
                    
                    # Try to determine data size
                    if hasattr(result, '__len__'):
                        size = len(str(result).encode())
                        self.data_processed.labels(
                            operation=operation,
                            data_type=data_type
                        ).inc(size)
                        
                    return result
                    
                except Exception as e:
                    self.error_counter.labels(
                        type=type(e).__name__,
                        operation=operation
                    ).inc()
                    raise
                    
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return sync_wrapper
            
        return decorator
        
    def track_ml_prediction(self, model_name: str, model_version: str = "1.0"):
        """Decorator to track ML predictions"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                
                try:
                    result = await func(*args, **kwargs)
                    
                    # Record metrics
                    self.model_predictions.labels(
                        model_name=model_name,
                        model_version=model_version
                    ).inc()
                    
                    duration = time.time() - start_time
                    self.prediction_latency.labels(
                        model_name=model_name,
                        model_version=model_version
                    ).observe(duration)
                    
                    return result
                    
                except Exception as e:
                    self.error_counter.labels(
                        type=type(e).__name__,
                        operation=f"prediction_{model_name}"
                    ).inc()
                    raise
                    
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    
                    # Record metrics
                    self.model_predictions.labels(
                        model_name=model_name,
                        model_version=model_version
                    ).inc()
                    
                    duration = time.time() - start_time
                    self.prediction_latency.labels(
                        model_name=model_name,
                        model_version=model_version
                    ).observe(duration)
                    
                    return result
                    
                except Exception as e:
                    self.error_counter.labels(
                        type=type(e).__name__,
                        operation=f"prediction_{model_name}"
                    ).inc()
                    raise
                    
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return sync_wrapper
            
        return decorator
        
    def record_model_accuracy(self, model_name: str, model_version: str, 
                            accuracy: float) -> None:
        """Record model accuracy"""
        self.model_accuracy.labels(
            model_name=model_name,
            model_version=model_version
        ).set(accuracy)
        
    def record_queue_size(self, queue_name: str, size: int) -> None:
        """Record queue size"""
        self.queue_size.labels(queue_name=queue_name).set(size)
        
    def record_error(self, error_type: str, operation: str) -> None:
        """Record an error"""
        self.error_counter.labels(
            type=error_type,
            operation=operation
        ).inc()
        
    def add_custom_collector(self, collector: Callable) -> None:
        """Add custom metric collector function"""
        self._custom_collectors.append(collector)
        
    async def collect_custom_metrics(self) -> None:
        """Collect custom metrics"""
        for collector in self._custom_collectors:
            try:
                if asyncio.iscoroutinefunction(collector):
                    await collector(self)
                else:
                    collector(self)
            except Exception as e:
                logger.error(f"Error in custom collector: {e}")
                
    def get_metrics(self, format: str = "prometheus") -> bytes:
        """Get current metrics"""
        if format == "openmetrics":
            return openmetrics_generate_latest(self.registry)
        else:
            return generate_latest(self.registry)
            
    async def start_push_gateway(self) -> None:
        """Start pushing metrics to gateway"""
        if not self.push_gateway:
            return
            
        self._running = True
        
        while self._running:
            try:
                # Collect custom metrics
                await self.collect_custom_metrics()
                
                # Push to gateway
                push_to_gateway(
                    self.push_gateway,
                    job=self.service_name,
                    registry=self.registry
                )
                
                await asyncio.sleep(self.push_interval)
                
            except Exception as e:
                logger.error(f"Error pushing metrics: {e}")
                await asyncio.sleep(self.push_interval)
                
    def stop_push_gateway(self) -> None:
        """Stop pushing metrics"""
        self._running = False


class MetricsMiddleware:
    """FastAPI middleware for automatic metrics collection"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        
    async def __call__(self, request, call_next):
        """Process request and collect metrics"""
        start_time = time.time()
        
        # Track active requests
        self.metrics.active_requests.inc()
        
        # Record request size
        content_length = request.headers.get("content-length")
        if content_length:
            self.metrics.request_size.labels(
                method=request.method,
                endpoint=request.url.path
            ).observe(int(content_length))
            
        try:
            # Process request
            response = await call_next(request)
            
            # Record metrics
            duration = time.time() - start_time
            
            self.metrics.request_counter.labels(
                method=request.method,
                endpoint=request.url.path,
                status=str(response.status_code)
            ).inc()
            
            self.metrics.request_duration.labels(
                method=request.method,
                endpoint=request.url.path
            ).observe(duration)
            
            # Record response size
            if hasattr(response, "headers") and "content-length" in response.headers:
                self.metrics.response_size.labels(
                    method=request.method,
                    endpoint=request.url.path
                ).observe(int(response.headers["content-length"]))
                
            return response
            
        except Exception as e:
            # Record error
            self.metrics.error_counter.labels(
                type=type(e).__name__,
                operation="request"
            ).inc()
            raise
            
        finally:
            self.metrics.active_requests.dec()


class DataIntelligenceMetrics:
    """Specialized metrics for DataIntelligence services"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self._init_specialized_metrics()
        
    def _init_specialized_metrics(self):
        """Initialize DataIntelligence specific metrics"""
        
        # Data quality metrics
        self.data_quality_score = self.metrics.create_metric(MetricConfig(
            name="data_quality_score",
            description="Data quality score (0-1)",
            metric_type=MetricType.GAUGE,
            labels=["dataset", "quality_dimension"],
            subsystem="data_quality"
        ))
        
        self.quality_checks_total = self.metrics.create_metric(MetricConfig(
            name="quality_checks_total",
            description="Total quality checks performed",
            metric_type=MetricType.COUNTER,
            labels=["dataset", "check_type", "result"],
            subsystem="data_quality"
        ))
        
        # Pipeline metrics
        self.pipeline_executions = self.metrics.create_metric(MetricConfig(
            name="pipeline_executions_total",
            description="Total pipeline executions",
            metric_type=MetricType.COUNTER,
            labels=["pipeline_name", "status"],
            subsystem="pipeline"
        ))
        
        self.pipeline_duration = self.metrics.create_metric(MetricConfig(
            name="pipeline_duration_seconds",
            description="Pipeline execution duration",
            metric_type=MetricType.HISTOGRAM,
            labels=["pipeline_name"],
            subsystem="pipeline",
            buckets=[10, 30, 60, 120, 300, 600, 1800, 3600]
        ))
        
        self.pipeline_stage_duration = self.metrics.create_metric(MetricConfig(
            name="pipeline_stage_duration_seconds",
            description="Pipeline stage execution duration",
            metric_type=MetricType.HISTOGRAM,
            labels=["pipeline_name", "stage_name"],
            subsystem="pipeline"
        ))
        
        # Feature store metrics
        self.feature_writes = self.metrics.create_metric(MetricConfig(
            name="feature_writes_total",
            description="Total feature writes",
            metric_type=MetricType.COUNTER,
            labels=["feature_group", "feature_name"],
            subsystem="feature_store"
        ))
        
        self.feature_reads = self.metrics.create_metric(MetricConfig(
            name="feature_reads_total",
            description="Total feature reads",
            metric_type=MetricType.COUNTER,
            labels=["feature_group", "feature_name"],
            subsystem="feature_store"
        ))
        
        self.feature_staleness = self.metrics.create_metric(MetricConfig(
            name="feature_staleness_seconds",
            description="Feature staleness in seconds",
            metric_type=MetricType.GAUGE,
            labels=["feature_group", "feature_name"],
            subsystem="feature_store"
        ))
        
        # ML platform metrics
        self.experiments_total = self.metrics.create_metric(MetricConfig(
            name="experiments_total",
            description="Total ML experiments",
            metric_type=MetricType.COUNTER,
            labels=["project", "status"],
            subsystem="ml_platform"
        ))
        
        self.model_training_duration = self.metrics.create_metric(MetricConfig(
            name="model_training_duration_seconds",
            description="Model training duration",
            metric_type=MetricType.HISTOGRAM,
            labels=["model_type", "dataset_size"],
            subsystem="ml_platform"
        ))
        
        self.model_drift_score = self.metrics.create_metric(MetricConfig(
            name="model_drift_score",
            description="Model drift score",
            metric_type=MetricType.GAUGE,
            labels=["model_name", "model_version", "drift_type"],
            subsystem="ml_platform"
        ))
        
        # Query engine metrics
        self.query_cache_hits = self.metrics.create_metric(MetricConfig(
            name="query_cache_hits_total",
            description="Query cache hits",
            metric_type=MetricType.COUNTER,
            labels=["query_type"],
            subsystem="query_engine"
        ))
        
        self.query_cache_misses = self.metrics.create_metric(MetricConfig(
            name="query_cache_misses_total",
            description="Query cache misses",
            metric_type=MetricType.COUNTER,
            labels=["query_type"],
            subsystem="query_engine"
        ))
        
        self.query_execution_time = self.metrics.create_metric(MetricConfig(
            name="query_execution_time_seconds",
            description="Query execution time",
            metric_type=MetricType.HISTOGRAM,
            labels=["query_type", "complexity"],
            subsystem="query_engine",
            buckets=[0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60]
        ))
        
    def record_data_quality(self, dataset: str, dimension: str, score: float):
        """Record data quality score"""
        self.data_quality_score.labels(
            dataset=dataset,
            quality_dimension=dimension
        ).set(score)
        
    def record_quality_check(self, dataset: str, check_type: str, passed: bool):
        """Record quality check result"""
        self.quality_checks_total.labels(
            dataset=dataset,
            check_type=check_type,
            result="passed" if passed else "failed"
        ).inc()
        
    def record_pipeline_execution(self, pipeline_name: str, duration: float, 
                                success: bool):
        """Record pipeline execution"""
        self.pipeline_executions.labels(
            pipeline_name=pipeline_name,
            status="success" if success else "failure"
        ).inc()
        
        self.pipeline_duration.labels(
            pipeline_name=pipeline_name
        ).observe(duration)
        
    def record_feature_operation(self, operation: str, feature_group: str, 
                               feature_name: str):
        """Record feature store operation"""
        if operation == "write":
            self.feature_writes.labels(
                feature_group=feature_group,
                feature_name=feature_name
            ).inc()
        elif operation == "read":
            self.feature_reads.labels(
                feature_group=feature_group,
                feature_name=feature_name
            ).inc()
            
    def record_model_drift(self, model_name: str, model_version: str,
                         drift_type: str, score: float):
        """Record model drift score"""
        self.model_drift_score.labels(
            model_name=model_name,
            model_version=model_version,
            drift_type=drift_type
        ).set(score) 