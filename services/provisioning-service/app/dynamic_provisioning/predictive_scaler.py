"""Predictive Scaler for Dynamic Resource Provisioning

Uses machine learning models to predict future resource needs.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import joblib

from pyignite import Client as IgniteClient
import mlflow
import mlflow.sklearn

logger = logging.getLogger(__name__)


@dataclass
class PredictedLoad:
    """Container for predicted load metrics"""
    service_name: str
    timestamp: datetime
    horizon_minutes: int
    cpu_usage: float
    memory_usage: float
    request_rate: float
    confidence: float
    features_used: List[str]


class PredictiveScaler:
    """Predictive scaler using ML models
    
    Features:
    - Time series forecasting for resource usage
    - Pattern recognition for workload spikes
    - Seasonal and trend analysis
    - Multi-horizon predictions
    - Model versioning and A/B testing
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite_client = ignite_client
        
        # Create caches
        self.models_cache = ignite_client.get_or_create_cache('predictive_models')
        self.predictions_cache = ignite_client.get_or_create_cache('predictions')
        self.training_data_cache = ignite_client.get_or_create_cache('training_data')
        
        # Model registry
        self.models = {}
        self.scalers = {}
        
        # MLflow configuration
        self.mlflow_tracking_uri = "http://mlflow:5000"
        mlflow.set_tracking_uri(self.mlflow_tracking_uri)
        mlflow.set_experiment("resource_prediction")
        
        # Load models
        self._load_models()
    
    def _load_models(self):
        """Load pre-trained models from MLflow"""
        services = [
            'auth-service',
            'digital-asset-service',
            'simulation-service',
            'federated-learning-service',
            'workflow-service',
            'notification-service'
        ]
        
        for service in services:
            try:
                # Try to load from MLflow
                model_name = f"resource_predictor_{service}"
                try:
                    model_version = mlflow.sklearn.load_model(
                        model_uri=f"models:/{model_name}/Production"
                    )
                    self.models[service] = model_version
                    logger.info(f"Loaded production model for {service}")
                except:
                    # Create default model if not found
                    self.models[service] = self._create_default_model()
                    logger.info(f"Created default model for {service}")
                
                # Load or create scaler
                scaler_key = f"scaler_{service}"
                if self.models_cache.contains_key(scaler_key):
                    self.scalers[service] = self.models_cache.get(scaler_key)
                else:
                    self.scalers[service] = StandardScaler()
                    
            except Exception as e:
                logger.error(f"Failed to load model for {service}: {e}")
                self.models[service] = self._create_default_model()
                self.scalers[service] = StandardScaler()
    
    def _create_default_model(self) -> RandomForestRegressor:
        """Create a default random forest model"""
        return RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )
    
    async def predict_load(
        self,
        service_name: str,
        horizon_minutes: int = 30
    ) -> Optional[PredictedLoad]:
        """Predict future resource load for a service"""
        try:
            # Get historical data
            historical_data = self._get_historical_data(service_name)
            
            if len(historical_data) < 100:  # Not enough data
                logger.warning(f"Insufficient historical data for {service_name}")
                return None
            
            # Prepare features
            features, feature_names = self._prepare_features(
                historical_data,
                horizon_minutes
            )
            
            if service_name not in self.models:
                logger.error(f"No model found for {service_name}")
                return None
            
            model = self.models[service_name]
            scaler = self.scalers[service_name]
            
            # Scale features
            try:
                features_scaled = scaler.transform(features)
            except:
                # Fit scaler if not already fitted
                scaler.fit(features)
                features_scaled = scaler.transform(features)
            
            # Make predictions
            predictions = model.predict(features_scaled)
            
            # Calculate confidence (using out-of-bag score if available)
            confidence = 0.8  # Default confidence
            if hasattr(model, 'oob_score_'):
                confidence = model.oob_score_
            
            # Create prediction object
            prediction = PredictedLoad(
                service_name=service_name,
                timestamp=datetime.utcnow() + timedelta(minutes=horizon_minutes),
                horizon_minutes=horizon_minutes,
                cpu_usage=float(predictions[0][0]) if predictions.shape[1] > 0 else 50.0,
                memory_usage=float(predictions[0][1]) if predictions.shape[1] > 1 else 50.0,
                request_rate=float(predictions[0][2]) if predictions.shape[1] > 2 else 100.0,
                confidence=confidence,
                features_used=feature_names
            )
            
            # Cache prediction
            cache_key = f"{service_name}:{horizon_minutes}:{datetime.utcnow().isoformat()}"
            self.predictions_cache.put(cache_key, prediction)
            
            # Log to MLflow
            with mlflow.start_run(nested=True):
                mlflow.log_params({
                    "service": service_name,
                    "horizon_minutes": horizon_minutes,
                    "features_count": len(feature_names)
                })
                mlflow.log_metrics({
                    "predicted_cpu": prediction.cpu_usage,
                    "predicted_memory": prediction.memory_usage,
                    "predicted_requests": prediction.request_rate,
                    "confidence": confidence
                })
            
            return prediction
            
        except Exception as e:
            logger.error(f"Failed to predict load for {service_name}: {e}")
            return None
    
    def _get_historical_data(self, service_name: str) -> pd.DataFrame:
        """Get historical metrics data"""
        # This would fetch from the historical_metrics cache
        # For now, generate synthetic data
        
        # Generate 7 days of synthetic data
        dates = pd.date_range(
            start=datetime.utcnow() - timedelta(days=7),
            end=datetime.utcnow(),
            freq='5min'
        )
        
        # Create realistic patterns
        base_cpu = 40 + np.random.normal(0, 5, len(dates))
        base_memory = 50 + np.random.normal(0, 3, len(dates))
        base_requests = 100 + np.random.normal(0, 20, len(dates))
        
        # Add daily patterns
        hour_of_day = np.array([d.hour for d in dates])
        daily_pattern = np.sin(2 * np.pi * hour_of_day / 24) * 20
        
        # Add weekly patterns
        day_of_week = np.array([d.weekday() for d in dates])
        weekly_pattern = (day_of_week < 5).astype(float) * 10  # Weekday boost
        
        # Add some spikes
        spike_mask = np.random.random(len(dates)) < 0.05
        spikes = spike_mask * np.random.uniform(20, 50, len(dates))
        
        data = pd.DataFrame({
            'timestamp': dates,
            'cpu_usage': base_cpu + daily_pattern + weekly_pattern + spikes,
            'memory_usage': base_memory + daily_pattern * 0.5 + weekly_pattern * 0.5,
            'request_rate': base_requests + daily_pattern * 2 + weekly_pattern * 2 + spikes * 3,
            'error_rate': np.maximum(0, np.random.normal(1, 0.5, len(dates))),
            'response_time_p99': 100 + daily_pattern + np.random.normal(0, 10, len(dates))
        })
        
        # Ensure non-negative values
        for col in ['cpu_usage', 'memory_usage', 'request_rate', 'error_rate', 'response_time_p99']:
            data[col] = data[col].clip(lower=0)
        
        # Cap percentages at 100
        data['cpu_usage'] = data['cpu_usage'].clip(upper=100)
        data['memory_usage'] = data['memory_usage'].clip(upper=100)
        
        return data
    
    def _prepare_features(
        self,
        data: pd.DataFrame,
        horizon_minutes: int
    ) -> Tuple[np.ndarray, List[str]]:
        """Prepare features for prediction"""
        # Sort by timestamp
        data = data.sort_values('timestamp')
        
        # Create lag features
        lag_windows = [5, 15, 30, 60]  # minutes
        features = []
        feature_names = []
        
        for window in lag_windows:
            window_samples = window // 5  # Assuming 5-minute intervals
            
            # CPU lag features
            data[f'cpu_lag_{window}'] = data['cpu_usage'].shift(window_samples)
            data[f'cpu_ma_{window}'] = data['cpu_usage'].rolling(window_samples).mean()
            data[f'cpu_std_{window}'] = data['cpu_usage'].rolling(window_samples).std()
            
            # Memory lag features
            data[f'memory_lag_{window}'] = data['memory_usage'].shift(window_samples)
            data[f'memory_ma_{window}'] = data['memory_usage'].rolling(window_samples).mean()
            
            # Request rate features
            data[f'requests_lag_{window}'] = data['request_rate'].shift(window_samples)
            data[f'requests_ma_{window}'] = data['request_rate'].rolling(window_samples).mean()
            
            feature_names.extend([
                f'cpu_lag_{window}', f'cpu_ma_{window}', f'cpu_std_{window}',
                f'memory_lag_{window}', f'memory_ma_{window}',
                f'requests_lag_{window}', f'requests_ma_{window}'
            ])
        
        # Time-based features
        data['hour'] = data['timestamp'].dt.hour
        data['day_of_week'] = data['timestamp'].dt.dayofweek
        data['is_weekend'] = (data['day_of_week'] >= 5).astype(int)
        data['hour_sin'] = np.sin(2 * np.pi * data['hour'] / 24)
        data['hour_cos'] = np.cos(2 * np.pi * data['hour'] / 24)
        
        feature_names.extend(['hour', 'day_of_week', 'is_weekend', 'hour_sin', 'hour_cos'])
        
        # Interaction features
        data['cpu_memory_product'] = data['cpu_usage'] * data['memory_usage'] / 100
        data['load_factor'] = (data['cpu_usage'] + data['memory_usage']) / 2
        
        feature_names.extend(['cpu_memory_product', 'load_factor'])
        
        # Drop NaN values
        data = data.dropna()
        
        # Get the latest features for prediction
        if len(data) > 0:
            features = data[feature_names].iloc[-1:].values
        else:
            features = np.zeros((1, len(feature_names)))
        
        return features, feature_names
    
    async def train_model(
        self,
        service_name: str,
        force_retrain: bool = False
    ) -> bool:
        """Train or retrain prediction model"""
        try:
            # Get training data
            data = self._get_historical_data(service_name)
            
            if len(data) < 1000 and not force_retrain:
                logger.warning(f"Insufficient data for training {service_name}")
                return False
            
            # Prepare training data
            X, y = self._prepare_training_data(data)
            
            if len(X) < 100:
                logger.warning(f"Insufficient training samples for {service_name}")
                return False
            
            # Start MLflow run
            with mlflow.start_run(run_name=f"train_{service_name}_{datetime.utcnow().isoformat()}"):
                # Log parameters
                mlflow.log_params({
                    "service": service_name,
                    "n_samples": len(X),
                    "n_features": X.shape[1],
                    "model_type": "RandomForestRegressor"
                })
                
                # Create and train model
                model = RandomForestRegressor(
                    n_estimators=200,
                    max_depth=15,
                    min_samples_split=10,
                    min_samples_leaf=5,
                    random_state=42,
                    n_jobs=-1,
                    oob_score=True
                )
                
                # Fit scaler
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # Train model
                model.fit(X_scaled, y)
                
                # Log metrics
                oob_score = model.oob_score_ if hasattr(model, 'oob_score_') else 0.0
                mlflow.log_metrics({
                    "oob_score": oob_score,
                    "n_features_used": len(model.feature_importances_)
                })
                
                # Log feature importances
                feature_importance = pd.DataFrame({
                    'feature': [f"feature_{i}" for i in range(X.shape[1])],
                    'importance': model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                mlflow.log_text(
                    feature_importance.to_csv(index=False),
                    "feature_importances.csv"
                )
                
                # Save model
                mlflow.sklearn.log_model(
                    model,
                    "model",
                    registered_model_name=f"resource_predictor_{service_name}"
                )
                
                # Update local cache
                self.models[service_name] = model
                self.scalers[service_name] = scaler
                
                # Save scaler to cache
                self.models_cache.put(f"scaler_{service_name}", scaler)
                
                logger.info(f"Successfully trained model for {service_name}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to train model for {service_name}: {e}")
            return False
    
    def _prepare_training_data(self, data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """Prepare data for model training"""
        # Prepare features (similar to prediction features)
        features, feature_names = self._prepare_features(data, horizon_minutes=30)
        
        # Prepare full feature matrix
        X = []
        y = []
        
        # Create samples with different horizons
        horizons = [5, 15, 30]  # minutes ahead
        
        for i in range(len(data) - max(horizons) - 1):
            # Current features
            row_features = []
            
            # Add all lag features
            for fname in feature_names:
                if fname in data.columns:
                    row_features.append(data[fname].iloc[i])
            
            # Add horizon as a feature
            for horizon in horizons:
                X.append(row_features + [horizon])
                
                # Target values at horizon
                target_idx = i + (horizon // 5)
                if target_idx < len(data):
                    y.append([
                        data['cpu_usage'].iloc[target_idx],
                        data['memory_usage'].iloc[target_idx],
                        data['request_rate'].iloc[target_idx]
                    ])
        
        return np.array(X), np.array(y)
    
    def get_model_metrics(self, service_name: str) -> Dict[str, float]:
        """Get model performance metrics"""
        if service_name not in self.models:
            return {}
        
        model = self.models[service_name]
        metrics = {
            "model_type": type(model).__name__,
            "n_estimators": getattr(model, 'n_estimators', 0),
            "oob_score": getattr(model, 'oob_score_', 0.0)
        }
        
        # Get recent prediction accuracy
        recent_predictions = []
        for key in self.predictions_cache.keys():
            if key.startswith(f"{service_name}:"):
                recent_predictions.append(self.predictions_cache.get(key))
        
        if recent_predictions:
            # Calculate average confidence
            avg_confidence = np.mean([p.confidence for p in recent_predictions])
            metrics["avg_confidence"] = avg_confidence
            metrics["n_recent_predictions"] = len(recent_predictions)
        
        return metrics
    
    async def evaluate_models(self) -> Dict[str, Dict[str, float]]:
        """Evaluate all models and return metrics"""
        results = {}
        
        for service_name in self.models:
            metrics = self.get_model_metrics(service_name)
            
            # Test prediction
            test_prediction = await self.predict_load(service_name, 30)
            if test_prediction:
                metrics["test_prediction_cpu"] = test_prediction.cpu_usage
                metrics["test_prediction_memory"] = test_prediction.memory_usage
            
            results[service_name] = metrics
        
        return results 