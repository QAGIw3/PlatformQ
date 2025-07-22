"""
Real-time ML Engine

Provides machine learning capabilities for streaming analytics including
anomaly detection, forecasting, and pattern recognition.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pickle
import httpx
import mlflow
from prophet import Prophet
import torch
import torch.nn as nn

logger = logging.getLogger(__name__)


class RealtimeMLEngine:
    """Machine learning engine for real-time analytics"""
    
    def __init__(self, mlflow_uri: Optional[str] = "http://mlflow:5000"):
        self.mlflow_uri = mlflow_uri
        self.models = {}
        self.scalers = {}
        self.model_cache = {}
        
        # Set MLflow tracking URI
        if mlflow_uri:
            mlflow.set_tracking_uri(mlflow_uri)
            
    async def initialize(self):
        """Initialize ML models and load pre-trained models"""
        # Load pre-trained models
        await self.load_models()
        
        # Initialize default models
        self._initialize_default_models()
        
    def _initialize_default_models(self):
        """Initialize default ML models"""
        # Anomaly detection model
        self.models['anomaly_detection'] = IsolationForest(
            contamination=0.1,
            random_state=42
        )
        
        # Initialize scalers
        self.scalers['default'] = StandardScaler()
        
    async def load_models(self):
        """Load pre-trained models from MLflow"""
        try:
            # Load registered models
            client = mlflow.tracking.MlflowClient()
            
            # Load anomaly detection model
            try:
                model_version = client.get_latest_versions(
                    "realtime-anomaly-detector", 
                    stages=["Production"]
                )[0]
                self.models['anomaly_detection'] = mlflow.sklearn.load_model(
                    f"models:/realtime-anomaly-detector/{model_version.version}"
                )
            except Exception as e:
                logger.info(f"No production anomaly model found: {e}")
                
            # Load forecasting model
            try:
                model_version = client.get_latest_versions(
                    "realtime-forecaster", 
                    stages=["Production"]
                )[0]
                self.models['forecaster'] = mlflow.prophet.load_model(
                    f"models:/realtime-forecaster/{model_version.version}"
                )
            except Exception as e:
                logger.info(f"No production forecasting model found: {e}")
                
        except Exception as e:
            logger.error(f"Error loading models from MLflow: {e}")
            
    async def detect_anomalies(self, data: List[Dict[str, Any]], 
                              features: List[str]) -> List[Dict[str, Any]]:
        """Detect anomalies in streaming data"""
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Extract features
        X = df[features].values
        
        # Scale features
        scaler = self.scalers.get('default', StandardScaler())
        X_scaled = scaler.fit_transform(X)
        
        # Detect anomalies
        model = self.models.get('anomaly_detection')
        predictions = model.predict(X_scaled)
        scores = model.score_samples(X_scaled)
        
        # Add results to data
        results = []
        for i, item in enumerate(data):
            result = item.copy()
            result['is_anomaly'] = predictions[i] == -1
            result['anomaly_score'] = float(scores[i])
            results.append(result)
            
        return results
        
    async def forecast(self, time_series: List[Dict[str, Any]], 
                      target_column: str, 
                      horizon_days: int = 7) -> Dict[str, Any]:
        """Forecast future values for time series data"""
        # Prepare data for Prophet
        df = pd.DataFrame(time_series)
        df = df.rename(columns={
            'timestamp': 'ds',
            target_column: 'y'
        })
        
        # Create or get cached model
        model_key = f"prophet_{target_column}"
        if model_key not in self.model_cache:
            model = Prophet(
                daily_seasonality=True,
                weekly_seasonality=True,
                yearly_seasonality=True
            )
            model.fit(df[['ds', 'y']])
            self.model_cache[model_key] = model
        else:
            model = self.model_cache[model_key]
            
        # Make predictions
        future = model.make_future_dataframe(periods=horizon_days, freq='D')
        forecast = model.predict(future)
        
        # Return forecast results
        return {
            'forecast': forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon_days).to_dict('records'),
            'components': {
                'trend': forecast['trend'].tail(horizon_days).tolist(),
                'weekly': forecast['weekly'].tail(horizon_days).tolist(),
                'yearly': forecast['yearly'].tail(horizon_days).tolist()
            }
        }
        
    async def classify_pattern(self, data: List[Dict[str, Any]], 
                              pattern_type: str = 'trend') -> Dict[str, Any]:
        """Classify patterns in streaming data"""
        df = pd.DataFrame(data)
        
        if pattern_type == 'trend':
            # Simple trend detection
            values = df['value'].values
            trend = np.polyfit(range(len(values)), values, 1)[0]
            
            if trend > 0.1:
                pattern = 'increasing'
            elif trend < -0.1:
                pattern = 'decreasing'
            else:
                pattern = 'stable'
                
            return {
                'pattern': pattern,
                'trend_coefficient': float(trend),
                'confidence': 0.85  # Simplified confidence
            }
            
        elif pattern_type == 'seasonality':
            # Detect seasonality using FFT
            values = df['value'].values
            fft = np.fft.fft(values)
            power = np.abs(fft)**2
            freqs = np.fft.fftfreq(len(values))
            
            # Find dominant frequency
            idx = np.argmax(power[1:len(power)//2]) + 1
            dominant_period = 1/freqs[idx] if freqs[idx] != 0 else len(values)
            
            return {
                'has_seasonality': dominant_period < len(values) / 2,
                'period': abs(dominant_period),
                'strength': float(power[idx] / np.sum(power))
            }
            
    async def train_online_model(self, model_name: str, 
                                data: List[Dict[str, Any]], 
                                features: List[str], 
                                target: str):
        """Train or update model with new streaming data"""
        # Convert to DataFrame
        df = pd.DataFrame(data)
        X = df[features].values
        y = df[target].values
        
        # Get or create model
        if model_name not in self.models:
            self.models[model_name] = self._create_online_model(len(features))
            
        model = self.models[model_name]
        
        # Update model (simplified online learning)
        if hasattr(model, 'partial_fit'):
            model.partial_fit(X, y)
        else:
            # For models without partial_fit, retrain on recent data
            model.fit(X, y)
            
        # Log to MLflow
        with mlflow.start_run():
            mlflow.log_param("model_name", model_name)
            mlflow.log_param("n_samples", len(data))
            mlflow.log_param("features", features)
            mlflow.sklearn.log_model(model, model_name)
            
    def _create_online_model(self, n_features: int):
        """Create a new online learning model"""
        from sklearn.linear_model import SGDRegressor
        return SGDRegressor(
            learning_rate='adaptive',
            eta0=0.01,
            max_iter=1000
        )
        
    async def get_feature_importance(self, model_name: str) -> Dict[str, float]:
        """Get feature importance for a trained model"""
        if model_name not in self.models:
            return {}
            
        model = self.models[model_name]
        
        if hasattr(model, 'feature_importances_'):
            return {f"feature_{i}": float(imp) 
                   for i, imp in enumerate(model.feature_importances_)}
        elif hasattr(model, 'coef_'):
            return {f"feature_{i}": float(abs(coef)) 
                   for i, coef in enumerate(model.coef_)}
        else:
            return {} 