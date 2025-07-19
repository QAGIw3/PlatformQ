"""
AutoML Module for Unified ML Platform

Provides automated machine learning capabilities
"""

from .automl_engine import (
    AutoMLEngine,
    AutoMLConfig,
    AutoMLResult,
    ProblemType,
    ModelFramework,
    FeatureEngineer,
    NeuralArchitectureSearch
)

__all__ = [
    "AutoMLEngine",
    "AutoMLConfig",
    "AutoMLResult",
    "ProblemType",
    "ModelFramework",
    "FeatureEngineer",
    "NeuralArchitectureSearch"
] 