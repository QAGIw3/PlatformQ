"""
Processing Framework for DataIntelligenceSuite

Provides unified processing patterns for batch, stream, and quality processing.
"""

from .base_processor import (
    BaseProcessor,
    ProcessorConfig,
    ProcessingResult,
    ProcessingStatus,
    ProcessingMode
)

from .batch_processor import (
    BatchProcessor,
    BatchConfig,
    BatchJob,
    BatchResult,
    PartitionStrategy
)

from .stream_processor import (
    StreamProcessor,
    StreamConfig,
    StreamSource,
    StreamSink,
    WindowType
)

from .quality_processor import (
    QualityProcessor,
    QualityConfig,
    QualityCheck,
    QualityResult,
    DataQualityDimension
)

from .pipeline_builder import (
    PipelineBuilder,
    Pipeline,
    PipelineStage,
    StageResult,
    TransformFunction
)

__all__ = [
    # Base
    "BaseProcessor",
    "ProcessorConfig",
    "ProcessingResult",
    "ProcessingStatus",
    "ProcessingMode",
    
    # Batch
    "BatchProcessor",
    "BatchConfig",
    "BatchJob",
    "BatchResult",
    "PartitionStrategy",
    
    # Stream
    "StreamProcessor",
    "StreamConfig",
    "StreamSource",
    "StreamSink",
    "WindowType",
    
    # Quality
    "QualityProcessor",
    "QualityConfig",
    "QualityCheck",
    "QualityResult",
    "DataQualityDimension",
    
    # Pipeline
    "PipelineBuilder",
    "Pipeline",
    "PipelineStage",
    "StageResult",
    "TransformFunction"
] 