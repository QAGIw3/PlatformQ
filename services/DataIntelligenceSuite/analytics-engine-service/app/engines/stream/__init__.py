"""
Stream Processing Engine

Provides real-time stream processing capabilities using Apache Flink.
"""

from .stream_processor import (
    StreamProcessor,
    StreamConfig,
    ProcessingMode,
    WindowType,
    StreamMetrics
)
from .pattern_detector import (
    PatternDetector,
    PatternType,
    Pattern,
    PatternMatch
)
from .state_manager import (
    StateManager,
    StateBackend,
    CheckpointConfig
)

__all__ = [
    # Main processor
    "StreamProcessor",
    
    # Configuration
    "StreamConfig",
    "ProcessingMode",
    "WindowType",
    "StreamMetrics",
    
    # Pattern detection
    "PatternDetector",
    "PatternType",
    "Pattern",
    "PatternMatch",
    
    # State management
    "StateManager",
    "StateBackend",
    "CheckpointConfig"
]
