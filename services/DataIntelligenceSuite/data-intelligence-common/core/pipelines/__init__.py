"""
Unified Pipeline Framework for DataIntelligenceSuite

Consolidates pipeline building and orchestration capabilities.
"""

from .base import (
    PipelineStage,
    StageType,
    StageStatus,
    ExecutionMode,
    PipelineConfig,
    StageConfig,
    StageResult,
    PipelineResult
)

from .builder import (
    PipelineBuilder,
    Pipeline,
    StageBuilder,
    TransformFunction,
    FilterFunction,
    AggregateFunction
)

# Import from our new orchestrator.py
from .orchestrator import (
    PipelineOrchestrator,
    OrchestrationStrategy,
    ResourceAllocation,
    OrchestrationConfig,
    ExecutionPlan
)

# Import from our new executors.py
from .executors import (
    BaseExecutor,
    AsyncExecutor,
    ThreadExecutor,
    ProcessExecutor,
    RayExecutor,
    DaskExecutor,
    ExecutorFactory,
    ExecutorType,
    ExecutorConfig
)

# Import from our new patterns.py
from .patterns import (
    PipelinePattern,
    ETLPipeline,
    MapReducePipeline,
    ScatterGatherPipeline,
    ForkJoinPipeline,
    RetryPipeline,
    CircuitBreakerPipeline,
    BulkheadPipeline,
    SagaPipeline,
    PipelineTemplates
)

from .monitoring import (
    PipelineMonitor,
    PipelineTracer,
    PipelineMetrics,
    AlertConfig,
    Alert,
    MetricType
)

__all__ = [
    # Base
    "PipelineStage",
    "StageType",
    "StageStatus",
    "ExecutionMode",
    "PipelineConfig",
    "StageConfig",
    "StageResult",
    "PipelineResult",
    
    # Builder
    "PipelineBuilder",
    "Pipeline",
    "StageBuilder",
    "TransformFunction",
    "FilterFunction",
    "AggregateFunction",
    
    # Orchestrator
    "PipelineOrchestrator",
    "OrchestrationStrategy",
    "ResourceAllocation",
    "OrchestrationConfig",
    "ExecutionPlan",
    
    # Executors
    "BaseExecutor",
    "AsyncExecutor",
    "ThreadExecutor",
    "ProcessExecutor",
    "RayExecutor",
    "DaskExecutor",
    "ExecutorFactory",
    "ExecutorType",
    "ExecutorConfig",
    
    # Patterns
    "PipelinePattern",
    "ETLPipeline",
    "MapReducePipeline",
    "ScatterGatherPipeline",
    "ForkJoinPipeline",
    "RetryPipeline",
    "CircuitBreakerPipeline",
    "BulkheadPipeline",
    "SagaPipeline",
    "PipelineTemplates",
    
    # Monitoring
    "PipelineMonitor",
    "PipelineTracer",
    "PipelineMetrics",
    "AlertConfig",
    "Alert",
    "MetricType"
] 