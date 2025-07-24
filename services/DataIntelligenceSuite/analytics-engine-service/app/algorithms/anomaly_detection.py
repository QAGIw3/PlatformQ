"""
Anomaly Detection Algorithm

Implements various anomaly detection methods for the Analytics Engine.
"""

from typing import Dict, Any, List, Optional, Union, Tuple
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from sklearn.ensemble import IsolationForest
from sklearn.covariance import EllipticEnvelope
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

from data_intelligence_common.core.algorithms import BaseAlgorithm, AlgorithmConfig
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class AnomalyMethod(str, Enum):
    """Anomaly detection methods"""
    ISOLATION_FOREST = "isolation_forest"
    ELLIPTIC_ENVELOPE = "elliptic_envelope"
    LOCAL_OUTLIER_FACTOR = "lof"
    STATISTICAL = "statistical"
    PROPHET = "prophet"
    LSTM = "lstm"


@dataclass
class AnomalyDetectionConfig(AlgorithmConfig):
    """Configuration for anomaly detection"""
    method: AnomalyMethod = AnomalyMethod.ISOLATION_FOREST
    contamination: float = 0.1  # Expected proportion of anomalies
    sensitivity: float = 0.95  # Detection sensitivity
    
    # Method-specific parameters
    n_estimators: int = 100  # For Isolation Forest
    n_neighbors: int = 20  # For LOF
    
    # Time series parameters
    seasonality_mode: str = "multiplicative"
    changepoint_prior_scale: float = 0.05
    
    # Feature engineering
    use_rolling_features: bool = True
    rolling_windows: List[int] = None
    use_lag_features: bool = True
    lag_periods: List[int] = None
    
    def __post_init__(self):
        super().__post_init__()
        if self.rolling_windows is None:
            self.rolling_windows = [3, 7, 14, 30]
        if self.lag_periods is None:
            self.lag_periods = [1, 7, 30]


class AnomalyDetectionAlgorithm(BaseAlgorithm):
    """
    Advanced anomaly detection algorithm supporting multiple methods.
    
    Features:
    - Multiple detection methods (Isolation Forest, LOF, Statistical)
    - Time series anomaly detection
    - Multivariate anomaly detection
    - Real-time and batch processing
    - Explainable anomalies
    """
    
    def __init__(self, config: AnomalyDetectionConfig):
        super().__init__(config)
        self.config = config
        self._models = {}
        self._scalers = {}
        self._thresholds = {}
        
    async def initialize(self):
        """Initialize algorithm components"""
        await super().initialize()
        
        # Initialize models based on method
        if self.config.method == AnomalyMethod.ISOLATION_FOREST:
            self._models['detector'] = IsolationForest(
                n_estimators=self.config.n_estimators,
                contamination=self.config.contamination,
                random_state=42,
                n_jobs=-1
            )
        elif self.config.method == AnomalyMethod.ELLIPTIC_ENVELOPE:
            self._models['detector'] = EllipticEnvelope(
                contamination=self.config.contamination,
                random_state=42
            )
        elif self.config.method == AnomalyMethod.LOCAL_OUTLIER_FACTOR:
            self._models['detector'] = LocalOutlierFactor(
                n_neighbors=self.config.n_neighbors,
                contamination=self.config.contamination,
                novelty=True,
                n_jobs=-1
            )
            
        logger.info(f"Anomaly detection algorithm initialized with method: {self.config.method}")
        
    async def train(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """Train anomaly detection model"""
        start_time = datetime.utcnow()
        
        try:
            # Prepare features
            features, feature_names = self._prepare_features(data)
            
            # Scale features
            scaler = StandardScaler()
            scaled_features = scaler.fit_transform(features)
            self._scalers['main'] = scaler
            
            # Train model
            if self.config.method == AnomalyMethod.STATISTICAL:
                # Statistical method doesn't need training
                self._calculate_thresholds(scaled_features, feature_names)
            else:
                # Train ML model
                self._models['detector'].fit(scaled_features)
                
                # Calculate decision threshold
                if hasattr(self._models['detector'], 'decision_function'):
                    scores = self._models['detector'].decision_function(scaled_features)
                    self._thresholds['decision'] = np.percentile(
                        scores, 
                        (1 - self.config.sensitivity) * 100
                    )
                    
            # Store metadata
            self._metadata.update({
                'trained_at': datetime.utcnow().isoformat(),
                'training_samples': len(features),
                'feature_names': feature_names,
                'feature_count': len(feature_names)
            })
            
            # Calculate training metrics
            predictions = await self.predict(data)
            anomaly_rate = sum(predictions['is_anomaly']) / len(predictions)
            
            result = {
                'status': 'success',
                'training_time': (datetime.utcnow() - start_time).total_seconds(),
                'samples_trained': len(features),
                'feature_count': len(feature_names),
                'anomaly_rate': anomaly_rate,
                'model_type': self.config.method.value
            }
            
            logger.info("Anomaly detection model trained", **result)
            return result
            
        except Exception as e:
            logger.error(f"Training failed: {e}")
            raise
            
    async def predict(
        self,
        data: Union[pd.DataFrame, Dict[str, Any]],
        **kwargs
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Detect anomalies in data"""
        single_record = isinstance(data, dict)
        
        # Convert to DataFrame if needed
        if single_record:
            df = pd.DataFrame([data])
        else:
            df = data
            
        try:
            # Prepare features
            features, feature_names = self._prepare_features(df)
            
            # Scale features
            if 'main' in self._scalers:
                scaled_features = self._scalers['main'].transform(features)
            else:
                # Fit scaler if not trained
                scaler = StandardScaler()
                scaled_features = scaler.fit_transform(features)
                self._scalers['main'] = scaler
                
            # Detect anomalies
            if self.config.method == AnomalyMethod.STATISTICAL:
                predictions, scores = self._statistical_detection(scaled_features, feature_names)
            else:
                predictions, scores = self._ml_detection(scaled_features)
                
            # Explain anomalies
            explanations = self._explain_anomalies(
                df, features, feature_names, predictions, scores
            )
            
            # Format results
            results = []
            for i in range(len(df)):
                result = {
                    'timestamp': df.index[i] if hasattr(df.index[i], 'isoformat') else str(df.index[i]),
                    'is_anomaly': bool(predictions[i] == -1),
                    'anomaly_score': float(scores[i]),
                    'confidence': float(abs(scores[i]) / (abs(self._thresholds.get('decision', 1)) + 1e-6))
                }
                
                if predictions[i] == -1:
                    result['explanation'] = explanations.get(i, {})
                    
                # Add original data
                for col in df.columns:
                    if col not in result:
                        result[col] = df.iloc[i][col]
                        
                results.append(result)
                
            # Record metrics
            anomaly_count = sum(1 for r in results if r['is_anomaly'])
            self.record_metric('anomalies_detected', anomaly_count)
            self.record_metric('detection_rate', anomaly_count / len(results) if results else 0)
            
            return results[0] if single_record else results
            
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            raise
            
    def _prepare_features(self, data: pd.DataFrame) -> Tuple[np.ndarray, List[str]]:
        """Prepare features for anomaly detection"""
        features = []
        feature_names = []
        
        # Numeric features
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            features.append(data[col].values)
            feature_names.append(col)
            
            # Rolling features
            if self.config.use_rolling_features:
                for window in self.config.rolling_windows:
                    if len(data) >= window:
                        # Rolling mean
                        rolling_mean = data[col].rolling(window).mean().fillna(data[col].mean())
                        features.append(rolling_mean.values)
                        feature_names.append(f"{col}_rolling_mean_{window}")
                        
                        # Rolling std
                        rolling_std = data[col].rolling(window).std().fillna(0)
                        features.append(rolling_std.values)
                        feature_names.append(f"{col}_rolling_std_{window}")
                        
            # Lag features
            if self.config.use_lag_features:
                for lag in self.config.lag_periods:
                    if len(data) > lag:
                        lagged = data[col].shift(lag).fillna(data[col].mean())
                        features.append(lagged.values)
                        feature_names.append(f"{col}_lag_{lag}")
                        
        # Stack features
        if features:
            feature_matrix = np.column_stack(features)
        else:
            # No numeric features, create dummy
            feature_matrix = np.zeros((len(data), 1))
            feature_names = ['dummy']
            
        return feature_matrix, feature_names
        
    def _ml_detection(self, features: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Detect anomalies using ML model"""
        if 'detector' not in self._models:
            raise ValueError("Model not trained")
            
        # Get predictions
        predictions = self._models['detector'].predict(features)
        
        # Get anomaly scores
        if hasattr(self._models['detector'], 'decision_function'):
            scores = self._models['detector'].decision_function(features)
        elif hasattr(self._models['detector'], 'score_samples'):
            scores = -self._models['detector'].score_samples(features)
        else:
            # Use predictions as scores
            scores = predictions.astype(float)
            
        # Apply threshold if available
        if 'decision' in self._thresholds:
            predictions = np.where(scores < self._thresholds['decision'], -1, 1)
            
        return predictions, scores
        
    def _statistical_detection(
        self,
        features: np.ndarray,
        feature_names: List[str]
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Detect anomalies using statistical methods"""
        predictions = np.ones(len(features))
        scores = np.zeros(len(features))
        
        # Check each feature
        for i, feature_name in enumerate(feature_names):
            feature_values = features[:, i]
            
            # Z-score method
            mean = np.mean(feature_values)
            std = np.std(feature_values)
            
            if std > 0:
                z_scores = np.abs((feature_values - mean) / std)
                threshold = self._thresholds.get(f'z_score_{feature_name}', 3.0)
                
                # Mark anomalies
                anomaly_mask = z_scores > threshold
                predictions[anomaly_mask] = -1
                
                # Update scores
                scores = np.maximum(scores, z_scores)
                
        return predictions, scores
        
    def _calculate_thresholds(self, features: np.ndarray, feature_names: List[str]):
        """Calculate statistical thresholds"""
        for i, feature_name in enumerate(feature_names):
            feature_values = features[:, i]
            
            # Z-score threshold based on sensitivity
            if self.config.sensitivity >= 0.99:
                z_threshold = 3.5
            elif self.config.sensitivity >= 0.95:
                z_threshold = 3.0
            elif self.config.sensitivity >= 0.90:
                z_threshold = 2.5
            else:
                z_threshold = 2.0
                
            self._thresholds[f'z_score_{feature_name}'] = z_threshold
            
    def _explain_anomalies(
        self,
        data: pd.DataFrame,
        features: np.ndarray,
        feature_names: List[str],
        predictions: np.ndarray,
        scores: np.ndarray
    ) -> Dict[int, Dict[str, Any]]:
        """Generate explanations for detected anomalies"""
        explanations = {}
        
        for i in range(len(predictions)):
            if predictions[i] == -1:  # Anomaly
                explanation = {
                    'severity': 'high' if abs(scores[i]) > np.percentile(np.abs(scores), 95) else 'medium',
                    'contributing_features': []
                }
                
                # Find contributing features
                if self.config.method == AnomalyMethod.STATISTICAL:
                    # For statistical method, check each feature
                    for j, feature_name in enumerate(feature_names):
                        feature_value = features[i, j]
                        mean = np.mean(features[:, j])
                        std = np.std(features[:, j])
                        
                        if std > 0:
                            z_score = abs((feature_value - mean) / std)
                            if z_score > 2.0:
                                explanation['contributing_features'].append({
                                    'feature': feature_name,
                                    'value': float(feature_value),
                                    'z_score': float(z_score),
                                    'deviation': f"{z_score:.1f} std from mean"
                                })
                else:
                    # For ML methods, use feature importance approximation
                    # Calculate feature deviations
                    for j, feature_name in enumerate(feature_names):
                        feature_value = features[i, j]
                        feature_median = np.median(features[:, j])
                        feature_mad = np.median(np.abs(features[:, j] - feature_median))
                        
                        if feature_mad > 0:
                            deviation = abs(feature_value - feature_median) / feature_mad
                            if deviation > 2.0:
                                explanation['contributing_features'].append({
                                    'feature': feature_name,
                                    'value': float(feature_value),
                                    'deviation': f"{deviation:.1f}x MAD from median"
                                })
                                
                # Sort by importance
                explanation['contributing_features'].sort(
                    key=lambda x: x.get('z_score', x.get('deviation', 0)),
                    reverse=True
                )
                
                # Keep top features
                explanation['contributing_features'] = explanation['contributing_features'][:5]
                
                explanations[i] = explanation
                
        return explanations
        
    async def update(self, new_data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        """Update model with new data (online learning)"""
        # For now, retrain the model
        # In production, implement incremental learning
        return await self.train(new_data, **kwargs)
        
    def get_params(self) -> Dict[str, Any]:
        """Get algorithm parameters"""
        params = {
            'method': self.config.method.value,
            'contamination': self.config.contamination,
            'sensitivity': self.config.sensitivity
        }
        
        if self.config.method == AnomalyMethod.ISOLATION_FOREST:
            params['n_estimators'] = self.config.n_estimators
        elif self.config.method == AnomalyMethod.LOCAL_OUTLIER_FACTOR:
            params['n_neighbors'] = self.config.n_neighbors
            
        return params
        
    def set_params(self, **params) -> None:
        """Set algorithm parameters"""
        for key, value in params.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                
        # Reinitialize if needed
        # This part needs to be adapted for async initialization
        # For now, it will re-initialize synchronously, which might not be ideal
        # if the algorithm is designed for async initialization.
        # A proper async update mechanism would involve re-calling initialize()
        # or managing a separate async task for re-initialization.
        # For now, keeping it simple as per the original code structure.
        pass


# Algorithm registration
__algorithm_class__ = AnomalyDetectionAlgorithm
__algorithm_name__ = "anomaly_detection" 