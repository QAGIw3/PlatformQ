"""
Processing Framework for DataIntelligenceSuite

Processor Hierarchy:
-------------------
BaseProcessor (ABC, Generic[T])
├── BatchProcessor - For batch data processing
├── StreamProcessor - For stream data processing  
├── QualityProcessor - For data quality validation
└── EventProcessor (in core.events) - For event-driven processing

BaseEventProcessor (ABC) - Separate hierarchy for event handling
└── Various event handlers in services

BaseFileProcessor (ABC) - Separate hierarchy for file processing
├── BlenderProcessor
├── FreeCADProcessor
├── MultimediaProcessor
└── Other file processors

BaseCDCProcessor (ABC) - Separate hierarchy for CDC
└── IgniteCDCProcessor

Notes:
- BaseProcessor is the root for general data processing
- BaseEventProcessor is for event-driven architectures
- Different processor types serve different purposes
- Not all processors need to inherit from BaseProcessor
"""

from .base_processor import (
    BaseProcessor,
    ProcessorConfig,
    ProcessingStatus,
    ProcessingMode,
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
    StreamEngine,
    WindowType,
    WindowConfig,
    StreamResult
)

from .quality_processor import (
    QualityProcessor,
    QualityConfig,
    QualityEngine,
    QualityRule,
    QualityResult,
    DataQualityDimension,
    RemediationAction
)

from .pipeline_builder import (
    PipelineBuilder,
    Pipeline,
    PipelineStage,
    StageConfig,
    StageResult,
    TransformFunction,
    FilterFunction,
    AggregateFunction
)

__all__ = [
    # Base
    "BaseProcessor",
    "ProcessorConfig",
    "ProcessingStatus",
    "ProcessingMode",
    "ProcessingMetrics",
    "ProcessingResult",
    "PartitionStrategy",
    "ResourceLimits",
    "OptimizationConfig",
    
    # Batch
    "BatchProcessor",
    "BatchConfig",
    "BatchJob",
    "BatchResult",
    "BatchEngine",
    
    # Stream
    "StreamProcessor",
    "StreamConfig",
    "StreamEngine",
    "WindowType",
    "WindowConfig",
    "StreamResult",
    
    # Quality
    "QualityProcessor",
    "QualityConfig",
    "QualityEngine",
    "QualityRule",
    "QualityResult",
    "DataQualityDimension",
    "RemediationAction",
    
    # Pipeline
    "PipelineBuilder",
    "Pipeline",
    "PipelineStage",
    "StageConfig",
    "StageResult",
    "TransformFunction",
    "FilterFunction",
    "AggregateFunction"
] 