"""
Algorithm Framework

Provides base classes and utilities for implementing algorithms across services.
"""

from .base_algorithm import (
    BaseAlgorithm,
    AlgorithmConfig,
    AlgorithmResult,
    AlgorithmStatus,
    AlgorithmType
)

from .parallel_algorithm import (
    ParallelAlgorithm,
    ParallelConfig,
    PartitionStrategy,
    ParallelResult
)

from .optimization_algorithm import (
    OptimizationAlgorithm,
    OptimizationConfig,
    OptimizationResult,
    OptimizationMethod,
    ConstraintType
)

from .ml_algorithm import (
    MLAlgorithm,
    MLAlgorithmConfig,
    MLAlgorithmResult,
    MLTask
)

__all__ = [
    # Base
    "BaseAlgorithm",
    "AlgorithmConfig",
    "AlgorithmResult",
    "AlgorithmStatus",
    "AlgorithmType",
    
    # Parallel
    "ParallelAlgorithm",
    "ParallelConfig",
    "PartitionStrategy",
    "ParallelResult",
    
    # Optimization
    "OptimizationAlgorithm",
    "OptimizationConfig",
    "OptimizationResult",
    "OptimizationMethod",
    "ConstraintType",
    
    # ML
    "MLAlgorithm",
    "MLAlgorithmConfig",
    "MLAlgorithmResult",
    "MLTask"
] 