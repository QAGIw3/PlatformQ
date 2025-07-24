"""
Processing Framework for DataIntelligenceSuite

Unified Processing Architecture:
-------------------------------
UnifiedProcessor - Handles both batch and stream processing
├── DataSource - Abstract source interface
├── DataSink - Abstract sink interface
└── ProcessingStage - Abstract processing stage

Common Sources:
- FileSource (JSON, CSV, Parquet)
- EventBusSource
- DatabaseSource
- PulsarSource
- LambdaSource

Common Sinks:
- FileSink
- EventBusSink
- DatabaseSink
- LambdaSink

Quality Stages:
- QualityCheckStage
- SchemaValidationStage
- DataCleaningStage
- DeduplicationStage
- AnomalyDetectionStage

Legacy Processors (for backward compatibility):
- BaseProcessor
- BatchProcessor
- StreamProcessor
- QualityProcessor
"""

# Unified processing components
from .unified_processor import (
    ProcessingMode,
    ProcessingEngine,
    WindowType,
    ProcessingWindow,
    ProcessingConfig,
    ProcessingContext,
    DataSource,
    DataSink,
    ProcessingStage,
    UnifiedProcessor,
    PipelineBuilder
)

from .sources_sinks import (
    FileSource,
    FileSink,
    EventBusSource,
    EventBusSink,
    DatabaseSource,
    DatabaseSink,
    LambdaSource,
    LambdaSink
)

from .quality_stages import (
    QualityLevel,
    QualityCheckType,
    QualityRule,
    QualityResult,
    QualityCheckStage,
    SchemaValidationStage,
    DataCleaningStage,
    DeduplicationStage,
    AnomalyDetectionStage,
    CommonQualityRules
)

# Legacy components (backward compatibility)
from .base_processor import (
    BaseProcessor,
    ProcessorConfig,
    ProcessingStatus,
    ProcessingMetrics,
    ProcessingResult,
    PartitionStrategy,
    ResourceLimits,
    OptimizationConfig
)

from .batch_processor import (
    BatchProcessor,
    BatchConfig,
    BatchJob,
    BatchResult,
    BatchEngine
)

from .stream_processor import (
    StreamProcessor,
    StreamConfig,
    StreamEngine as LegacyStreamEngine,
    WindowType as LegacyWindowType,
    WindowConfig,
    StreamResult
)

from .quality_processor import (
    QualityProcessor,
    QualityConfig,
    QualityEngine,
    QualityRule as LegacyQualityRule,
    QualityResult as LegacyQualityResult,
    DataQualityDimension,
    RemediationAction
)

from .pipeline_builder import (
    PipelineBuilder as LegacyPipelineBuilder,
    Pipeline,
    PipelineStage,
    StageConfig,
    StageResult,
    TransformFunction,
    FilterFunction,
    AggregateFunction
)

__all__ = [
    # Unified processing
    "ProcessingMode",
    "ProcessingEngine",
    "WindowType",
    "ProcessingWindow",
    "ProcessingConfig",
    "ProcessingContext",
    "DataSource",
    "DataSink",
    "ProcessingStage",
    "UnifiedProcessor",
    "PipelineBuilder",
    
    # Sources and sinks
    "FileSource",
    "FileSink",
    "EventBusSource",
    "EventBusSink",
    "DatabaseSource",
    "DatabaseSink",
    "LambdaSource",
    "LambdaSink",
    
    # Quality stages
    "QualityLevel",
    "QualityCheckType",
    "QualityRule",
    "QualityResult",
    "QualityCheckStage",
    "SchemaValidationStage",
    "DataCleaningStage",
    "DeduplicationStage",
    "AnomalyDetectionStage",
    "CommonQualityRules",
    
    # Legacy (backward compatibility)
    "BaseProcessor",
    "ProcessorConfig",
    "ProcessingStatus",
    "ProcessingMetrics",
    "ProcessingResult",
    "PartitionStrategy",
    "ResourceLimits",
    "OptimizationConfig",
    "BatchProcessor",
    "BatchConfig",
    "BatchJob",
    "BatchResult",
    "BatchEngine",
    "StreamProcessor",
    "StreamConfig",
    "LegacyStreamEngine",
    "LegacyWindowType",
    "WindowConfig",
    "StreamResult",
    "QualityProcessor",
    "QualityConfig",
    "QualityEngine",
    "LegacyQualityRule",
    "LegacyQualityResult",
    "DataQualityDimension",
    "RemediationAction",
    "LegacyPipelineBuilder",
    "Pipeline",
    "PipelineStage",
    "StageConfig",
    "StageResult",
    "TransformFunction",
    "FilterFunction",
    "AggregateFunction"
] 