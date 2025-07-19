"""
PlatformQ Unified Observability Package

Provides comprehensive observability with OpenTelemetry and Iceberg metadata integration
"""

from .unified_observability import (
    UnifiedObservability,
    ObservabilityConfig,
    SpanType,
    LineageLevel,
    DataLineageContext,
    get_observability
)

from .instrumentation import (
    trace_async,
    trace_sync,
    measure_time,
    count_calls,
    DataPipelineTracer,
    instrument_class,
    instrument_fastapi_app,
    request_counter,
    request_duration,
    active_requests,
    data_processed
)

__all__ = [
    # Core classes
    "UnifiedObservability",
    "ObservabilityConfig",
    "SpanType",
    "LineageLevel",
    "DataLineageContext",
    "get_observability",
    
    # Decorators
    "trace_async",
    "trace_sync",
    "measure_time",
    "count_calls",
    
    # Utilities
    "DataPipelineTracer",
    "instrument_class",
    "instrument_fastapi_app",
    
    # Metrics
    "request_counter",
    "request_duration",
    "active_requests",
    "data_processed"
] 