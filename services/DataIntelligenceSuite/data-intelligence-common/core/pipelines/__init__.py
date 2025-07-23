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

from .orchestrator import (
    PipelineOrchestrator,
    PipelineRun,
    PipelineSchedule,
    RetryStrategy,
    TriggerType,
    ScheduleConfig
)

from .executors import (
    StageExecutor,
    SparkStageExecutor,
    FlinkStageExecutor,
    BeamStageExecutor,
    NativeStageExecutor,
    ExecutorRegistry
)

from .patterns import (
    PipelinePattern,
    BatchPattern,
    StreamPattern,
    HybridPattern,
    MLPipelinePattern
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
    "PipelineRun",
    "PipelineSchedule",
    "RetryStrategy",
    "TriggerType",
    "ScheduleConfig",
    
    # Executors
    "StageExecutor",
    "SparkStageExecutor",
    "FlinkStageExecutor",
    "BeamStageExecutor",
    "NativeStageExecutor",
    "ExecutorRegistry",
    
    # Patterns
    "PipelinePattern",
    "BatchPattern",
    "StreamPattern",
    "HybridPattern",
    "MLPipelinePattern"
] 