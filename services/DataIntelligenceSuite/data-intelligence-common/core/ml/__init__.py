"""
Machine Learning core components.

Provides comprehensive ML functionality for model lifecycle management.
"""

from .base_model import (
    BaseMLModel,
    ModelConfig,
    ModelType,
    ProblemType,
    ModelStatus
)

from .training import (
    ModelTrainer,
    TrainingConfig,
    TrainingResult,
    HyperparameterTuner,
    DistributedTrainer
)

from .inference import (
    InferenceEngine,
    ModelEndpoint,
    BatchRequest,
    InferenceMetrics,
    InferenceMode,
    ModelBackend
)

from .feature_engineering import (
    FeatureEngineering,
    Feature,
    FeatureGroup,
    FeatureVector,
    FeatureType,
    TransformationType
)

from .model_registry import (
    ModelRegistry,
    ModelSignature,
    ModelLineage,
    StageTransition,
    ModelComparison,
    TransitionRule
)

from .monitoring import (
    ModelMonitor,
    DriftResult,
    PerformanceMetric,
    MonitoringAlert,
    MonitoringConfig,
    DriftType,
    AlertSeverity
)

from .automl import (
    AutoMLEngine,
    AutoMLConfig,
    AutoMLResult,
    SearchSpace,
    OptimizationMetric,
    ModelSelector
)

from .explainability import (
    ModelExplainer,
    ExplanationType,
    ExplanationScope,
    FeatureImportance,
    LocalExplanation,
    GlobalExplanation,
    ShapExplainer,
    LimeExplainer
)

__all__ = [
    # Base
    "BaseMLModel",
    "ModelConfig",
    "ModelType",
    "ProblemType",
    "ModelStatus",
    
    # Training
    "ModelTrainer",
    "TrainingConfig",
    "TrainingResult",
    "HyperparameterTuner",
    "DistributedTrainer",
    
    # Inference
    "InferenceEngine",
    "ModelEndpoint",
    "BatchRequest",
    "InferenceMetrics",
    "InferenceMode",
    "ModelBackend",
    
    # Feature Engineering
    "FeatureEngineering",
    "Feature",
    "FeatureGroup",
    "FeatureVector",
    "FeatureType",
    "TransformationType",
    
    # Model Registry
    "ModelRegistry",
    "ModelSignature",
    "ModelLineage",
    "StageTransition",
    "ModelComparison",
    "TransitionRule",
    
    # Monitoring
    "ModelMonitor",
    "DriftResult",
    "PerformanceMetric",
    "MonitoringAlert",
    "MonitoringConfig",
    "DriftType",
    "AlertSeverity",
    
    # AutoML
    "AutoMLEngine",
    "AutoMLConfig",
    "AutoMLResult",
    "SearchSpace",
    "OptimizationMetric",
    "ModelSelector",
    
    # Explainability
    "ModelExplainer",
    "ExplanationType",
    "ExplanationScope",
    "FeatureImportance",
    "LocalExplanation",
    "GlobalExplanation",
    "ShapExplainer",
    "LimeExplainer"
] 