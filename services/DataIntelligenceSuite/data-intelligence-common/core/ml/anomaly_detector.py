"""
Anomaly Detection for ML models.

Provides anomaly detection capabilities for data quality and monitoring.
"""

from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pandas as pd
from abc import ABC
import asyncio
import json

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    from sklearn.cluster import DBSCAN
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class AnomalyType(str, Enum):
    """Types of anomalies"""
    OUTLIER = "outlier"
    DRIFT = "drift"
    PATTERN = "pattern"
    STRUCTURAL = "structural"
    TEMPORAL = "temporal"
    MULTIVARIATE = "multivariate"
    CONCEPT_DRIFT = "concept_drift"
    DATA_QUALITY = "data_quality"


class DetectionMethod(str, Enum):
    """Anomaly detection methods"""
    ISOLATION_FOREST = "isolation_forest"
    ZSCORE = "zscore"
    IQR = "iqr"
    DBSCAN = "dbscan"
    AUTOENCODER = "autoencoder"
    PROPHET = "prophet"
    LSTM = "lstm"
    ENSEMBLE = "ensemble"
    CUSTOM = "custom"


@dataclass
class AnomalyScore:
    """Anomaly score for a data point"""
    value: float
    confidence: float
    method: DetectionMethod
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyResult:
    """Result of anomaly detection"""
    is_anomaly: bool
    score: AnomalyScore
    anomaly_type: Optional[AnomalyType] = None
    explanation: Optional[str] = None
    features: Optional[Dict[str, float]] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class AnomalyDetectorConfig:
    """Configuration for anomaly detector"""
    method: DetectionMethod = DetectionMethod.ISOLATION_FOREST
    contamination: float = 0.1
    sensitivity: float = 0.95
    window_size: int = 100
    min_samples: int = 10
    cache_enabled: bool = True
    cache_ttl: int = 3600
    ensemble_methods: List[DetectionMethod] = field(default_factory=list)
    custom_params: Dict[str, Any] = field(default_factory=dict)


class AnomalyDetector:
    """
    Advanced anomaly detection with multiple methods and ensemble support.
    """
    
    def __init__(
        self,
        config: AnomalyDetectorConfig,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.config = config
        self.cache_manager = cache_manager
        self.event_bus = event_bus
        self._models: Dict[str, Any] = {}
        self._scalers: Dict[str, StandardScaler] = {}
        self._history: List[AnomalyResult] = []
        self._initialized = False
        
    async def initialize(self):
        """Initialize the anomaly detector"""
        if not SKLEARN_AVAILABLE and self.config.method != DetectionMethod.CUSTOM:
            raise ImportError("scikit-learn is required for anomaly detection")
            
        # Initialize models based on method
        if self.config.method == DetectionMethod.ISOLATION_FOREST:
            self._models['main'] = IsolationForest(
                contamination=self.config.contamination,
                **self.config.custom_params
            )
        elif self.config.method == DetectionMethod.ENSEMBLE:
            # Initialize ensemble models
            for method in self.config.ensemble_methods:
                if method == DetectionMethod.ISOLATION_FOREST:
                    self._models[method.value] = IsolationForest(
                        contamination=self.config.contamination
                    )
                # Add more methods as needed
                
        self._initialized = True
        logger.info(f"Anomaly detector initialized with method: {self.config.method}")
        
    async def detect(
        self,
        data: Union[np.ndarray, pd.DataFrame, Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None
    ) -> AnomalyResult:
        """
        Detect anomalies in the provided data.
        
        Args:
            data: Input data for anomaly detection
            context: Additional context for detection
            
        Returns:
            AnomalyResult with detection outcome
        """
        if not self._initialized:
            await self.initialize()
            
        # Convert data to numpy array
        X = self._prepare_data(data)
        
        # Check cache
        cache_key = self._generate_cache_key(X, context)
        if self.config.cache_enabled and self.cache_manager:
            cached_result = await self.cache_manager.get(cache_key)
            if cached_result:
                return cached_result
                
        # Perform detection
        if self.config.method == DetectionMethod.ENSEMBLE:
            result = await self._ensemble_detect(X, context)
        else:
            result = await self._single_detect(X, context)
            
        # Cache result
        if self.config.cache_enabled and self.cache_manager:
            await self.cache_manager.set(
                cache_key,
                result,
                ttl=self.config.cache_ttl
            )
            
        # Store in history
        self._history.append(result)
        if len(self._history) > self.config.window_size:
            self._history.pop(0)
            
        # Emit event
        if self.event_bus and result.is_anomaly:
            await self.event_bus.publish(Event(
                type="anomaly_detected",
                data={
                    "result": result,
                    "context": context
                }
            ))
            
        return result
        
    async def train(
        self,
        training_data: Union[np.ndarray, pd.DataFrame],
        labels: Optional[np.ndarray] = None
    ):
        """
        Train the anomaly detector on historical data.
        
        Args:
            training_data: Historical data for training
            labels: Optional labels for semi-supervised learning
        """
        if not self._initialized:
            await self.initialize()
            
        X = self._prepare_data(training_data)
        
        # Scale data
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        self._scalers['main'] = scaler
        
        # Train models
        for name, model in self._models.items():
            if hasattr(model, 'fit'):
                if labels is not None and hasattr(model, 'fit_predict'):
                    model.fit(X_scaled, labels)
                else:
                    model.fit(X_scaled)
                    
        logger.info("Anomaly detector training completed")
        
    async def update(
        self,
        new_data: Union[np.ndarray, pd.DataFrame],
        is_anomaly: bool
    ):
        """
        Update the detector with new labeled data.
        
        Args:
            new_data: New data point
            is_anomaly: Whether the data point is an anomaly
        """
        # Implement online learning if supported by the model
        pass
        
    def _prepare_data(
        self,
        data: Union[np.ndarray, pd.DataFrame, Dict[str, Any]]
    ) -> np.ndarray:
        """Prepare data for processing"""
        if isinstance(data, np.ndarray):
            return data
        elif isinstance(data, pd.DataFrame):
            return data.values
        elif isinstance(data, dict):
            # Convert dict to array
            return np.array(list(data.values())).reshape(1, -1)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
            
    async def _single_detect(
        self,
        X: np.ndarray,
        context: Optional[Dict[str, Any]]
    ) -> AnomalyResult:
        """Perform single method detection"""
        model = self._models.get('main')
        if model is None:
            raise ValueError("Model not initialized")
            
        # Scale data if scaler exists
        if 'main' in self._scalers:
            X = self._scalers['main'].transform(X)
            
        # Predict
        if hasattr(model, 'predict'):
            predictions = model.predict(X)
            scores = model.decision_function(X) if hasattr(model, 'decision_function') else predictions
        else:
            # Custom detection logic
            predictions, scores = await self._custom_detect(X, context)
            
        is_anomaly = predictions[0] == -1 if len(predictions) > 0 else False
        score_value = float(scores[0]) if len(scores) > 0 else 0.0
        
        # Calculate confidence
        confidence = self._calculate_confidence(score_value)
        
        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=AnomalyScore(
                value=score_value,
                confidence=confidence,
                method=self.config.method
            ),
            anomaly_type=self._determine_anomaly_type(X, context),
            explanation=self._generate_explanation(is_anomaly, score_value, context),
            context=context
        )
        
    async def _ensemble_detect(
        self,
        X: np.ndarray,
        context: Optional[Dict[str, Any]]
    ) -> AnomalyResult:
        """Perform ensemble detection"""
        results = []
        
        for method, model in self._models.items():
            if 'main' in self._scalers:
                X_scaled = self._scalers['main'].transform(X)
            else:
                X_scaled = X
                
            predictions = model.predict(X_scaled)
            scores = model.decision_function(X_scaled) if hasattr(model, 'decision_function') else predictions
            
            results.append({
                'method': method,
                'is_anomaly': predictions[0] == -1,
                'score': float(scores[0])
            })
            
        # Aggregate results
        anomaly_votes = sum(1 for r in results if r['is_anomaly'])
        is_anomaly = anomaly_votes > len(results) / 2
        
        # Average scores
        avg_score = np.mean([r['score'] for r in results])
        confidence = anomaly_votes / len(results)
        
        return AnomalyResult(
            is_anomaly=is_anomaly,
            score=AnomalyScore(
                value=avg_score,
                confidence=confidence,
                method=DetectionMethod.ENSEMBLE
            ),
            anomaly_type=self._determine_anomaly_type(X, context),
            explanation=f"Ensemble detection: {anomaly_votes}/{len(results)} models detected anomaly",
            context=context
        )
        
    async def _custom_detect(
        self,
        X: np.ndarray,
        context: Optional[Dict[str, Any]]
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Custom detection logic"""
        # Implement custom detection
        # This is a placeholder - override in subclasses
        predictions = np.array([1])  # Normal by default
        scores = np.array([0.0])
        return predictions, scores
        
    def _calculate_confidence(self, score: float) -> float:
        """Calculate confidence from anomaly score"""
        # Normalize score to 0-1 range
        # This is method-specific and should be overridden
        return min(abs(score), 1.0)
        
    def _determine_anomaly_type(
        self,
        X: np.ndarray,
        context: Optional[Dict[str, Any]]
    ) -> Optional[AnomalyType]:
        """Determine the type of anomaly"""
        # Implement logic to classify anomaly type
        # This could use additional analysis or context
        return AnomalyType.OUTLIER
        
    def _generate_explanation(
        self,
        is_anomaly: bool,
        score: float,
        context: Optional[Dict[str, Any]]
    ) -> str:
        """Generate human-readable explanation"""
        if not is_anomaly:
            return "Data point is within normal range"
            
        severity = "high" if abs(score) > 0.8 else "moderate"
        return f"Anomaly detected with {severity} confidence (score: {score:.3f})"
        
    def _generate_cache_key(
        self,
        X: np.ndarray,
        context: Optional[Dict[str, Any]]
    ) -> str:
        """Generate cache key for detection result"""
        import hashlib
        data_hash = hashlib.md5(X.tobytes()).hexdigest()
        context_hash = hashlib.md5(
            json.dumps(context, sort_keys=True).encode()
        ).hexdigest() if context else "none"
        return f"anomaly:{self.config.method}:{data_hash}:{context_hash}"
        
    async def get_statistics(self) -> Dict[str, Any]:
        """Get detection statistics"""
        if not self._history:
            return {
                "total_detections": 0,
                "anomalies_detected": 0,
                "anomaly_rate": 0.0
            }
            
        total = len(self._history)
        anomalies = sum(1 for r in self._history if r.is_anomaly)
        
        return {
            "total_detections": total,
            "anomalies_detected": anomalies,
            "anomaly_rate": anomalies / total if total > 0 else 0.0,
            "methods_used": list(set(r.score.method.value for r in self._history)),
            "average_confidence": np.mean([r.score.confidence for r in self._history])
        } 