"""
Base ML model classes and interfaces.

Provides common patterns for ML models across services.
"""

import pickle
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from pathlib import Path

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ModelType(str, Enum):
    """ML model types"""
    SKLEARN = "sklearn"
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CUSTOM = "custom"


class ProblemType(str, Enum):
    """ML problem types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"
    TIME_SERIES = "time_series"
    RECOMMENDATION = "recommendation"
    NLP = "nlp"
    COMPUTER_VISION = "computer_vision"


class ModelStatus(str, Enum):
    """Model lifecycle status"""
    DRAFT = "draft"
    TRAINING = "training"
    TRAINED = "trained"
    VALIDATING = "validating"
    VALIDATED = "validated"
    DEPLOYED = "deployed"
    SERVING = "serving"
    DEPRECATED = "deprecated"
    FAILED = "failed"


@dataclass
class ModelMetadata:
    """Model metadata"""
    model_id: str
    name: str
    version: str
    model_type: ModelType
    problem_type: ProblemType
    status: ModelStatus = ModelStatus.DRAFT
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "model_id": self.model_id,
            "name": self.name,
            "version": self.version,
            "model_type": self.model_type,
            "problem_type": self.problem_type,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "description": self.description,
            "tags": self.tags,
            "metrics": self.metrics,
            "parameters": self.parameters,
            "input_schema": self.input_schema,
            "output_schema": self.output_schema
        }


@dataclass
class ModelConfig:
    """Model configuration"""
    model_type: ModelType
    problem_type: ProblemType
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    preprocessing_config: Dict[str, Any] = field(default_factory=dict)
    postprocessing_config: Dict[str, Any] = field(default_factory=dict)
    training_config: Dict[str, Any] = field(default_factory=dict)
    serving_config: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> bool:
        """Validate configuration"""
        # Add validation logic based on model type
        return True


class BaseMLModel(ABC):
    """
    Base class for all ML models.
    
    Provides common functionality:
    - Model serialization/deserialization
    - Prediction interface
    - Metrics tracking
    - Model versioning
    """
    
    def __init__(self, config: ModelConfig, metadata: Optional[ModelMetadata] = None):
        self.config = config
        self.metadata = metadata or self._create_default_metadata()
        self.model = None
        self.preprocessor = None
        self.postprocessor = None
        self._is_fitted = False
        
    def _create_default_metadata(self) -> ModelMetadata:
        """Create default metadata"""
        import uuid
        return ModelMetadata(
            model_id=str(uuid.uuid4()),
            name=f"{self.config.model_type}_{self.config.problem_type}",
            version="1.0.0",
            model_type=self.config.model_type,
            problem_type=self.config.problem_type
        )
        
    @abstractmethod
    def fit(self, X: Union[np.ndarray, pd.DataFrame], y: Optional[Union[np.ndarray, pd.Series]] = None, **kwargs) -> 'BaseMLModel':
        """Train the model"""
        pass
        
    @abstractmethod
    def predict(self, X: Union[np.ndarray, pd.DataFrame], **kwargs) -> Union[np.ndarray, pd.DataFrame]:
        """Make predictions"""
        pass
        
    def predict_proba(self, X: Union[np.ndarray, pd.DataFrame], **kwargs) -> Union[np.ndarray, pd.DataFrame]:
        """Predict probabilities (for classification)"""
        if self.config.problem_type != ProblemType.CLASSIFICATION:
            raise NotImplementedError("predict_proba only available for classification models")
        return self.predict(X, **kwargs)
        
    def preprocess(self, X: Union[np.ndarray, pd.DataFrame]) -> Union[np.ndarray, pd.DataFrame]:
        """Preprocess input data"""
        if self.preprocessor:
            return self.preprocessor.transform(X)
        return X
        
    def postprocess(self, predictions: Union[np.ndarray, pd.DataFrame]) -> Union[np.ndarray, pd.DataFrame]:
        """Postprocess predictions"""
        if self.postprocessor:
            return self.postprocessor.transform(predictions)
        return predictions
        
    @abstractmethod
    def evaluate(self, X: Union[np.ndarray, pd.DataFrame], y: Union[np.ndarray, pd.Series], **kwargs) -> Dict[str, float]:
        """Evaluate model performance"""
        pass
        
    def save(self, path: Union[str, Path]) -> None:
        """Save model to disk"""
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        
        # Save model
        model_path = path / "model.pkl"
        with open(model_path, 'wb') as f:
            pickle.dump(self.model, f)
            
        # Save metadata
        metadata_path = path / "metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(self.metadata.to_dict(), f, indent=2)
            
        # Save config
        config_path = path / "config.json"
        with open(config_path, 'w') as f:
            json.dump(self.config.__dict__, f, indent=2)
            
        # Save preprocessor/postprocessor if exists
        if self.preprocessor:
            preprocessor_path = path / "preprocessor.pkl"
            with open(preprocessor_path, 'wb') as f:
                pickle.dump(self.preprocessor, f)
                
        if self.postprocessor:
            postprocessor_path = path / "postprocessor.pkl"
            with open(postprocessor_path, 'wb') as f:
                pickle.dump(self.postprocessor, f)
                
        logger.info(f"Model saved to {path}")
        
    @classmethod
    def load(cls, path: Union[str, Path]) -> 'BaseMLModel':
        """Load model from disk"""
        path = Path(path)
        
        # Load config
        config_path = path / "config.json"
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        config = ModelConfig(**config_dict)
        
        # Load metadata
        metadata_path = path / "metadata.json"
        with open(metadata_path, 'r') as f:
            metadata_dict = json.load(f)
        metadata = ModelMetadata(**metadata_dict)
        
        # Create instance
        instance = cls(config, metadata)
        
        # Load model
        model_path = path / "model.pkl"
        with open(model_path, 'rb') as f:
            instance.model = pickle.load(f)
            
        # Load preprocessor/postprocessor if exists
        preprocessor_path = path / "preprocessor.pkl"
        if preprocessor_path.exists():
            with open(preprocessor_path, 'rb') as f:
                instance.preprocessor = pickle.load(f)
                
        postprocessor_path = path / "postprocessor.pkl"
        if postprocessor_path.exists():
            with open(postprocessor_path, 'rb') as f:
                instance.postprocessor = pickle.load(f)
                
        instance._is_fitted = True
        logger.info(f"Model loaded from {path}")
        
        return instance
        
    def get_params(self) -> Dict[str, Any]:
        """Get model parameters"""
        return self.config.hyperparameters
        
    def set_params(self, **params) -> 'BaseMLModel':
        """Set model parameters"""
        self.config.hyperparameters.update(params)
        return self
        
    def get_feature_importance(self) -> Optional[Dict[str, float]]:
        """Get feature importance (if available)"""
        if hasattr(self.model, 'feature_importances_'):
            return dict(enumerate(self.model.feature_importances_))
        return None
        
    def update_metadata(self, **kwargs):
        """Update model metadata"""
        for key, value in kwargs.items():
            if hasattr(self.metadata, key):
                setattr(self.metadata, key, value)
        self.metadata.updated_at = datetime.utcnow()
        
    def add_metric(self, name: str, value: float):
        """Add performance metric"""
        self.metadata.metrics[name] = value
        self.metadata.updated_at = datetime.utcnow()
        
    def add_tag(self, tag: str):
        """Add tag to model"""
        if tag not in self.metadata.tags:
            self.metadata.tags.append(tag)
            self.metadata.updated_at = datetime.utcnow()
            
    @property
    def is_fitted(self) -> bool:
        """Check if model is fitted"""
        return self._is_fitted
        
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"model_id={self.metadata.model_id}, "
            f"name={self.metadata.name}, "
            f"version={self.metadata.version}, "
            f"status={self.metadata.status})"
        ) 