"""
Enhanced Instrumentation Utilities for PlatformQ Services

Provides decorators and utilities for easy observability integration
"""

import asyncio
import functools
import time
from typing import Any, Callable, Dict, Optional, TypeVar, Union
from datetime import datetime
import inspect

from opentelemetry import trace, metrics, baggage
from opentelemetry.trace import Span
from prometheus_client import Counter, Histogram, Gauge

from .unified_observability import UnifiedObservability, SpanType, get_observability

T = TypeVar('T')

# Global metrics
request_counter = Counter(
    'platformq_requests_total',
    'Total number of requests',
    ['service', 'endpoint', 'method', 'status']
)

request_duration = Histogram(
    'platformq_request_duration_seconds',
    'Request duration in seconds',
    ['service', 'endpoint', 'method']
)

active_requests = Gauge(
    'platformq_active_requests',
    'Number of active requests',
    ['service', 'endpoint']
)

data_processed = Counter(
    'platformq_data_processed_rows',
    'Total rows of data processed',
    ['service', 'operation', 'dataset']
)


def trace_async(
    operation_name: Optional[str] = None,
    span_type: SpanType = SpanType.API_REQUEST,
    track_args: bool = False,
    track_result: bool = False
):
    """
    Decorator for tracing async functions
    
    Usage:
        @trace_async(span_type=SpanType.DATA_TRANSFORMATION)
        async def transform_data(df: DataFrame) -> DataFrame:
            # transformation logic
            return transformed_df
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get operation name
            op_name = operation_name or f"{func.__module__}.{func.__name__}"
            
            # Get observability instance
            obs = await get_observability()
            
            # Build attributes
            attributes = {
                "function.name": func.__name__,
                "function.module": func.__module__,
            }
            
            # Track arguments if requested
            if track_args:
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                bound_args.apply_defaults()
                
                for param_name, param_value in bound_args.arguments.items():
                    if isinstance(param_value, (str, int, float, bool)):
                        attributes[f"arg.{param_name}"] = param_value
                    else:
                        attributes[f"arg.{param_name}.type"] = type(param_value).__name__
                        
            async with obs.trace_operation(op_name, span_type, attributes) as span:
                try:
                    # Execute function
                    result = await func(*args, **kwargs)
                    
                    # Track result if requested
                    if track_result:
                        if isinstance(result, (str, int, float, bool)):
                            span.set_attribute("result", result)
                        elif hasattr(result, '__len__'):
                            span.set_attribute("result.length", len(result))
                        span.set_attribute("result.type", type(result).__name__)
                        
                    return result
                    
                except Exception as e:
                    # Exception is already recorded by trace_operation
                    raise
                    
        return wrapper
    return decorator


def trace_sync(
    operation_name: Optional[str] = None,
    span_type: SpanType = SpanType.API_REQUEST,
    track_args: bool = False,
    track_result: bool = False
):
    """Decorator for tracing synchronous functions"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Convert sync to async for tracing
            async def async_wrapper():
                traced_func = trace_async(
                    operation_name, span_type, track_args, track_result
                )(asyncio.coroutine(func))
                return await traced_func(*args, **kwargs)
                
            return asyncio.run(async_wrapper())
            
        return wrapper
    return decorator


def measure_time(metric_name: str = None):
    """
    Decorator to measure execution time
    
    Usage:
        @measure_time("data_processing_duration")
        async def process_data(data):
            # processing logic
    """
    def decorator(func: Callable) -> Callable:
        is_async = asyncio.iscoroutinefunction(func)
        name = metric_name or f"{func.__module__}.{func.__name__}.duration"
        
        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # Record metric
                    request_duration.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="async"
                    ).observe(duration)
                    
                    return result
                except Exception as e:
                    duration = time.time() - start_time
                    request_duration.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="async"
                    ).observe(duration)
                    raise
                    
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    request_duration.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="sync"
                    ).observe(duration)
                    
                    return result
                except Exception as e:
                    duration = time.time() - start_time
                    request_duration.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="sync"
                    ).observe(duration)
                    raise
                    
            return sync_wrapper
            
    return decorator


def count_calls(metric_name: str = None, labels: Dict[str, str] = None):
    """
    Decorator to count function calls
    
    Usage:
        @count_calls(labels={"operation": "user_login"})
        async def login(username: str):
            # login logic
    """
    def decorator(func: Callable) -> Callable:
        is_async = asyncio.iscoroutinefunction(func)
        name = metric_name or f"{func.__module__}.{func.__name__}.calls"
        
        if is_async:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                # Increment counter
                request_counter.labels(
                    service=func.__module__.split('.')[0],
                    endpoint=func.__name__,
                    method="async",
                    status="started"
                ).inc()
                
                try:
                    result = await func(*args, **kwargs)
                    request_counter.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="async",
                        status="success"
                    ).inc()
                    return result
                except Exception as e:
                    request_counter.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="async",
                        status="error"
                    ).inc()
                    raise
                    
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                request_counter.labels(
                    service=func.__module__.split('.')[0],
                    endpoint=func.__name__,
                    method="sync",
                    status="started"
                ).inc()
                
                try:
                    result = func(*args, **kwargs)
                    request_counter.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="sync",
                        status="success"
                    ).inc()
                    return result
                except Exception as e:
                    request_counter.labels(
                        service=func.__module__.split('.')[0],
                        endpoint=func.__name__,
                        method="sync",
                        status="error"
                    ).inc()
                    raise
                    
            return sync_wrapper
            
    return decorator


class DataPipelineTracer:
    """
    Context manager for tracing data pipeline operations
    
    Usage:
        async with DataPipelineTracer("etl_pipeline") as tracer:
            # Read data
            df = await tracer.trace_read("source_table", read_func)
            
            # Transform data
            transformed = await tracer.trace_transform("aggregate", transform_func, df)
            
            # Write data
            await tracer.trace_write("target_table", write_func, transformed)
            
            # Set lineage info
            tracer.set_row_count(transformed.count())
    """
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.source_tables = []
        self.target_tables = []
        self.obs = None
        self.span = None
        self.lineage_ctx = None
        
    async def __aenter__(self):
        self.obs = await get_observability()
        
        # Start pipeline span
        self.span, self.lineage_ctx = await self.obs.trace_data_pipeline(
            self.pipeline_name,
            source_tables=[],  # Will be updated
            target_tables=[],  # Will be updated
            transformation_type="pipeline"
        ).__aenter__()
        
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Update lineage context with final tables
        self.lineage_ctx.source_tables = self.source_tables
        self.lineage_ctx.target_tables = self.target_tables
        
        # Exit span context
        await self.obs.trace_data_pipeline(
            self.pipeline_name,
            self.source_tables,
            self.target_tables
        ).__aexit__(exc_type, exc_val, exc_tb)
        
    async def trace_read(self, table_name: str, read_func: Callable, *args, **kwargs):
        """Trace a read operation"""
        self.source_tables.append(table_name)
        
        async with self.obs.trace_operation(
            f"read_{table_name}",
            SpanType.DATABASE_QUERY,
            {"table": table_name, "operation": "read"}
        ) as span:
            result = await read_func(*args, **kwargs)
            
            if hasattr(result, 'count'):
                count = result.count() if callable(result.count) else result.count
                span.set_attribute("row_count", count)
                
            return result
            
    async def trace_transform(self, transform_name: str, transform_func: Callable, *args, **kwargs):
        """Trace a transformation operation"""
        async with self.obs.trace_operation(
            f"transform_{transform_name}",
            SpanType.DATA_TRANSFORMATION,
            {"transform": transform_name}
        ) as span:
            result = await transform_func(*args, **kwargs)
            
            if hasattr(result, 'count'):
                count = result.count() if callable(result.count) else result.count
                span.set_attribute("output_row_count", count)
                
            return result
            
    async def trace_write(self, table_name: str, write_func: Callable, data, *args, **kwargs):
        """Trace a write operation"""
        self.target_tables.append(table_name)
        
        async with self.obs.trace_operation(
            f"write_{table_name}",
            SpanType.DATABASE_QUERY,
            {"table": table_name, "operation": "write"}
        ) as span:
            if hasattr(data, 'count'):
                count = data.count() if callable(data.count) else data.count
                span.set_attribute("row_count", count)
                
            result = await write_func(data, *args, **kwargs)
            
            # Track Iceberg metadata if applicable
            if table_name.startswith("iceberg."):
                await self.obs.track_iceberg_operation(table_name, "write", span)
                
            return result
            
    def set_row_count(self, count: int):
        """Set the total row count for the pipeline"""
        if self.lineage_ctx:
            self.lineage_ctx.row_count = count
            
    def add_columns_read(self, columns: list):
        """Add columns that were read"""
        if self.lineage_ctx:
            self.lineage_ctx.columns_read.extend(columns)
            
    def add_columns_written(self, columns: list):
        """Add columns that were written"""
        if self.lineage_ctx:
            self.lineage_ctx.columns_written.extend(columns)


def instrument_class(cls: type) -> type:
    """
    Class decorator to automatically instrument all methods
    
    Usage:
        @instrument_class
        class DataProcessor:
            async def process(self, data):
                # Will be automatically traced
                return processed_data
    """
    for name, method in inspect.getmembers(cls, inspect.ismethod):
        if not name.startswith('_'):  # Skip private methods
            if asyncio.iscoroutinefunction(method):
                setattr(cls, name, trace_async()(method))
            else:
                setattr(cls, name, trace_sync()(method))
                
    return cls


# FastAPI specific instrumentation
def instrument_fastapi_app(app):
    """
    Instrument a FastAPI application with observability
    
    Usage:
        from fastapi import FastAPI
        from platformq_shared.observability import instrument_fastapi_app
        
        app = FastAPI()
        instrument_fastapi_app(app)
    """
    from fastapi import Request
    from starlette.middleware.base import BaseHTTPMiddleware
    
    class ObservabilityMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            # Start timer
            start_time = time.time()
            
            # Track active requests
            active_requests.labels(
                service=request.app.title,
                endpoint=request.url.path
            ).inc()
            
            # Get observability instance
            obs = await get_observability()
            
            # Extract trace context from headers
            trace_context = {}
            if "traceparent" in request.headers:
                trace_context["traceparent"] = request.headers["traceparent"]
                
            # Create span
            async with obs.trace_operation(
                f"{request.method} {request.url.path}",
                SpanType.API_REQUEST,
                {
                    "http.method": request.method,
                    "http.url": str(request.url),
                    "http.path": request.url.path,
                    "http.host": request.headers.get("host", ""),
                    "http.user_agent": request.headers.get("user-agent", ""),
                }
            ) as span:
                try:
                    # Process request
                    response = await call_next(request)
                    
                    # Record response
                    span.set_attribute("http.status_code", response.status_code)
                    
                    # Record metrics
                    duration = time.time() - start_time
                    request_duration.labels(
                        service=request.app.title,
                        endpoint=request.url.path,
                        method=request.method
                    ).observe(duration)
                    
                    request_counter.labels(
                        service=request.app.title,
                        endpoint=request.url.path,
                        method=request.method,
                        status=str(response.status_code)
                    ).inc()
                    
                    return response
                    
                finally:
                    # Decrement active requests
                    active_requests.labels(
                        service=request.app.title,
                        endpoint=request.url.path
                    ).dec()
                    
    app.add_middleware(ObservabilityMiddleware)
    return app 