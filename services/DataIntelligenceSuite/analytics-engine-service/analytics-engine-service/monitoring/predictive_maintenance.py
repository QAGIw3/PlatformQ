"""
Predictive Maintenance Model for Simulation Components

Uses time-series analysis and machine learning to predict component failures
and maintenance requirements in multi-physics simulations.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import pickle
from pyignite import Client as IgniteClient
from elasticsearch import AsyncElasticsearch
import asyncio
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from prophet import Prophet
import warnings

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


class PredictiveMaintenanceModel:
    """Predictive maintenance for simulation components"""
    
    def __init__(self, ignite_config: Dict[str, Any], es_config: Dict[str, Any]):
        self.ignite_config = ignite_config
        self.es_config = es_config
        self.ignite_client = None
        self.es_client = None
        self.models = {}
        self._initialize_clients()
        
    def _initialize_clients(self):
        """Initialize storage clients"""
        # Initialize Ignite
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (self.ignite_config.get('host', 'ignite'), 
             self.ignite_config.get('port', 10800))
        ])
        
        # Create caches
        self.model_cache = self.ignite_client.get_or_create_cache('predictive_models')
        self.prediction_cache = self.ignite_client.get_or_create_cache('maintenance_predictions')
        self.health_cache = self.ignite_client.get_or_create_cache('component_health')
        
        # Initialize Elasticsearch
        self.es_client = AsyncElasticsearch(
            hosts=[self.es_config.get('host', 'http://elasticsearch:9200')]
        )
        
    async def train_component_models(self, component_type: str, lookback_days: int = 30):
        """Train predictive models for a component type"""
        # Get historical data
        data = await self._get_component_history(component_type, lookback_days)
        
        if len(data) < 100:
            logger.warning(f"Insufficient data for {component_type}")
            return
            
        # Prepare features
        features_df = self._prepare_features(data)
        
        # Train anomaly detection model
        anomaly_model = self._train_anomaly_detector(features_df)
        
        # Train failure prediction model
        failure_model = self._train_failure_predictor(features_df)
        
        # Train time-series models
        ts_models = self._train_timeseries_models(features_df)
        
        # Store models
        model_data = {
            'component_type': component_type,
            'trained_at': datetime.utcnow().isoformat(),
            'anomaly_model': pickle.dumps(anomaly_model),
            'failure_model': pickle.dumps(failure_model),
            'ts_models': {k: pickle.dumps(v) for k, v in ts_models.items()},
            'feature_columns': list(features_df.columns),
            'training_samples': len(features_df)
        }
        
        self.model_cache.put(f"model_{component_type}", model_data)
        self.models[component_type] = model_data
        
        logger.info(f"Trained models for {component_type}")
        
    async def _get_component_history(self, component_type: str, lookback_days: int) -> List[Dict]:
        """Get historical component data from Elasticsearch"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"component_type": component_type}},
                        {"range": {
                            "timestamp": {
                                "gte": f"now-{lookback_days}d"
                            }
                        }}
                    ]
                }
            },
            "size": 10000,
            "sort": [{"timestamp": "asc"}]
        }
        
        result = await self.es_client.search(
            index="component-metrics-*",
            body=query
        )
        
        return [hit["_source"] for hit in result["hits"]["hits"]]
        
    def _prepare_features(self, data: List[Dict]) -> pd.DataFrame:
        """Prepare features for ML models"""
        df = pd.DataFrame(data)
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        
        # Extract numeric features
        numeric_cols = ['temperature', 'pressure', 'vibration', 'load_factor', 
                       'efficiency', 'runtime_hours', 'error_rate', 'cpu_usage',
                       'memory_usage', 'io_operations']
        
        # Keep only available columns
        available_cols = [col for col in numeric_cols if col in df.columns]
        df = df[available_cols]
        
        # Fill missing values
        df = df.fillna(method='ffill').fillna(0)
        
        # Add engineered features
        if 'temperature' in df.columns:
            df['temp_rolling_mean'] = df['temperature'].rolling(window=24).mean()
            df['temp_rolling_std'] = df['temperature'].rolling(window=24).std()
            df['temp_change_rate'] = df['temperature'].diff()
            
        if 'efficiency' in df.columns:
            df['efficiency_drop'] = df['efficiency'].rolling(window=24).mean() - df['efficiency']
            
        if 'error_rate' in df.columns:
            df['error_spike'] = (df['error_rate'] > df['error_rate'].rolling(window=24).mean() * 2).astype(int)
            
        # Add time-based features
        df['hour'] = df.index.hour
        df['day_of_week'] = df.index.dayofweek
        df['is_weekend'] = (df.index.dayofweek >= 5).astype(int)
        
        # Remove NaN rows from rolling calculations
        df = df.dropna()
        
        return df
        
    def _train_anomaly_detector(self, df: pd.DataFrame) -> IsolationForest:
        """Train anomaly detection model"""
        # Select features for anomaly detection
        features = df.select_dtypes(include=[np.number])
        
        # Scale features
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(features)
        
        # Train Isolation Forest
        model = IsolationForest(
            contamination=0.05,  # Expect 5% anomalies
            random_state=42,
            n_estimators=100
        )
        
        model.fit(scaled_features)
        
        # Store scaler with model
        model.scaler = scaler
        
        return model
        
    def _train_failure_predictor(self, df: pd.DataFrame) -> Optional[RandomForestClassifier]:
        """Train failure prediction model"""
        # Create failure labels (simplified - based on efficiency drop or error spikes)
        failure_indicators = []
        
        if 'efficiency' in df.columns:
            # Mark as failure if efficiency drops below 70%
            failure_indicators.append(df['efficiency'] < 0.7)
            
        if 'error_rate' in df.columns:
            # Mark as failure if error rate is high
            failure_indicators.append(df['error_rate'] > 0.1)
            
        if not failure_indicators:
            return None
            
        # Combine indicators
        df['failure'] = pd.concat(failure_indicators, axis=1).any(axis=1).astype(int)
        
        # Create prediction target (failure in next 24 hours)
        df['failure_24h'] = df['failure'].shift(-24).fillna(0).astype(int)
        
        # Prepare features and target
        feature_cols = [col for col in df.columns if col not in ['failure', 'failure_24h']]
        X = df[feature_cols]
        y = df['failure_24h']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train model
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'  # Handle imbalanced classes
        )
        
        model.fit(X_train, y_train)
        
        # Evaluate
        score = model.score(X_test, y_test)
        logger.info(f"Failure prediction accuracy: {score:.2f}")
        
        return model
        
    def _train_timeseries_models(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Train time-series models for key metrics"""
        models = {}
        
        # Train ARIMA for temperature if available
        if 'temperature' in df.columns:
            try:
                temp_series = df['temperature'].resample('H').mean()
                arima_model = ARIMA(temp_series, order=(2, 1, 2))
                arima_fit = arima_model.fit()
                models['temperature_arima'] = arima_fit
            except Exception as e:
                logger.error(f"ARIMA training failed: {e}")
                
        # Train Prophet for efficiency prediction
        if 'efficiency' in df.columns:
            try:
                prophet_df = df[['efficiency']].reset_index()
                prophet_df.columns = ['ds', 'y']
                
                prophet_model = Prophet(
                    yearly_seasonality=True,
                    weekly_seasonality=True,
                    daily_seasonality=True
                )
                prophet_model.fit(prophet_df)
                models['efficiency_prophet'] = prophet_model
            except Exception as e:
                logger.error(f"Prophet training failed: {e}")
                
        return models
        
    async def predict_maintenance(self, component_id: str, component_type: str) -> Dict[str, Any]:
        """Predict maintenance requirements for a component"""
        # Load models
        model_key = f"model_{component_type}"
        model_data = self.models.get(component_type) or self.model_cache.get(model_key)
        
        if not model_data:
            return {
                'status': 'no_model',
                'message': f'No trained model for {component_type}'
            }
            
        # Get recent component data
        recent_data = await self._get_recent_component_data(component_id)
        
        if not recent_data:
            return {
                'status': 'no_data',
                'message': 'No recent data for component'
            }
            
        # Prepare features
        features_df = self._prepare_features(recent_data)
        
        predictions = {
            'component_id': component_id,
            'component_type': component_type,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Anomaly detection
        if 'anomaly_model' in model_data:
            anomaly_score = self._predict_anomaly(
                features_df, 
                pickle.loads(model_data['anomaly_model'])
            )
            predictions['anomaly_score'] = anomaly_score
            predictions['is_anomalous'] = anomaly_score < -0.1
            
        # Failure prediction
        if 'failure_model' in model_data:
            failure_prob = self._predict_failure(
                features_df,
                pickle.loads(model_data['failure_model'])
            )
            predictions['failure_probability_24h'] = failure_prob
            predictions['maintenance_urgency'] = self._calculate_urgency(failure_prob)
            
        # Time-series forecasts
        if 'ts_models' in model_data:
            forecasts = self._generate_forecasts(features_df, model_data['ts_models'])
            predictions['forecasts'] = forecasts
            
        # Calculate health score
        health_score = self._calculate_health_score(predictions)
        predictions['health_score'] = health_score
        
        # Generate recommendations
        recommendations = self._generate_recommendations(predictions)
        predictions['recommendations'] = recommendations
        
        # Store predictions
        self.prediction_cache.put(f"prediction_{component_id}", predictions)
        
        return predictions
        
    async def _get_recent_component_data(self, component_id: str) -> List[Dict]:
        """Get recent data for a specific component"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"component_id": component_id}},
                        {"range": {
                            "timestamp": {
                                "gte": "now-7d"
                            }
                        }}
                    ]
                }
            },
            "size": 1000,
            "sort": [{"timestamp": "asc"}]
        }
        
        result = await self.es_client.search(
            index="component-metrics-*",
            body=query
        )
        
        return [hit["_source"] for hit in result["hits"]["hits"]]
        
    def _predict_anomaly(self, df: pd.DataFrame, model: IsolationForest) -> float:
        """Predict anomaly score"""
        if len(df) == 0:
            return 0.0
            
        # Use latest data point
        latest_features = df.iloc[-1:].select_dtypes(include=[np.number])
        
        # Scale features
        scaled_features = model.scaler.transform(latest_features)
        
        # Get anomaly score
        score = model.score_samples(scaled_features)[0]
        
        return float(score)
        
    def _predict_failure(self, df: pd.DataFrame, model: RandomForestClassifier) -> float:
        """Predict failure probability"""
        if len(df) == 0 or model is None:
            return 0.0
            
        # Use latest data point
        feature_cols = [col for col in model.feature_names_in_ if col in df.columns]
        latest_features = df[feature_cols].iloc[-1:].fillna(0)
        
        # Get failure probability
        prob = model.predict_proba(latest_features)[0, 1]
        
        return float(prob)
        
    def _generate_forecasts(self, df: pd.DataFrame, ts_models: Dict[str, bytes]) -> Dict[str, Any]:
        """Generate time-series forecasts"""
        forecasts = {}
        
        # Temperature forecast
        if 'temperature_arima' in ts_models and 'temperature' in df.columns:
            try:
                model = pickle.loads(ts_models['temperature_arima'])
                forecast = model.forecast(steps=24)  # 24 hour forecast
                forecasts['temperature_24h'] = {
                    'values': forecast.tolist(),
                    'mean': float(forecast.mean()),
                    'max': float(forecast.max())
                }
            except Exception as e:
                logger.error(f"Temperature forecast failed: {e}")
                
        # Efficiency forecast
        if 'efficiency_prophet' in ts_models and 'efficiency' in df.columns:
            try:
                model = pickle.loads(ts_models['efficiency_prophet'])
                future = model.make_future_dataframe(periods=24, freq='H')
                forecast = model.predict(future)
                
                future_forecast = forecast.iloc[-24:]
                forecasts['efficiency_24h'] = {
                    'values': future_forecast['yhat'].tolist(),
                    'lower_bound': future_forecast['yhat_lower'].tolist(),
                    'upper_bound': future_forecast['yhat_upper'].tolist()
                }
            except Exception as e:
                logger.error(f"Efficiency forecast failed: {e}")
                
        return forecasts
        
    def _calculate_urgency(self, failure_prob: float) -> str:
        """Calculate maintenance urgency level"""
        if failure_prob > 0.8:
            return "critical"
        elif failure_prob > 0.5:
            return "high"
        elif failure_prob > 0.3:
            return "medium"
        elif failure_prob > 0.1:
            return "low"
        else:
            return "none"
            
    def _calculate_health_score(self, predictions: Dict[str, Any]) -> float:
        """Calculate overall component health score"""
        score = 100.0
        
        # Deduct for anomalies
        if predictions.get('is_anomalous'):
            score -= 20
            
        # Deduct based on failure probability
        failure_prob = predictions.get('failure_probability_24h', 0)
        score -= failure_prob * 50
        
        # Deduct for poor forecasts
        if 'forecasts' in predictions:
            if 'efficiency_24h' in predictions['forecasts']:
                min_efficiency = min(predictions['forecasts']['efficiency_24h']['values'])
                if min_efficiency < 0.7:
                    score -= 15
                    
        return max(0, min(100, score))
        
    def _generate_recommendations(self, predictions: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate maintenance recommendations"""
        recommendations = []
        
        urgency = predictions.get('maintenance_urgency', 'none')
        health_score = predictions.get('health_score', 100)
        
        if urgency == 'critical' or health_score < 30:
            recommendations.append({
                'action': 'immediate_maintenance',
                'priority': 'critical',
                'description': 'Component requires immediate attention',
                'estimated_downtime': '2-4 hours'
            })
            
        elif urgency == 'high' or health_score < 50:
            recommendations.append({
                'action': 'schedule_maintenance',
                'priority': 'high',
                'description': 'Schedule maintenance within 24 hours',
                'estimated_downtime': '1-2 hours'
            })
            
        elif urgency == 'medium' or health_score < 70:
            recommendations.append({
                'action': 'plan_maintenance',
                'priority': 'medium',
                'description': 'Plan maintenance within 1 week',
                'estimated_downtime': '30-60 minutes'
            })
            
        # Add specific recommendations based on anomalies
        if predictions.get('is_anomalous'):
            recommendations.append({
                'action': 'investigate_anomaly',
                'priority': 'high',
                'description': 'Unusual behavior detected - investigate immediately'
            })
            
        # Add forecast-based recommendations
        if 'forecasts' in predictions:
            if 'temperature_24h' in predictions['forecasts']:
                max_temp = predictions['forecasts']['temperature_24h']['max']
                if max_temp > 80:  # Celsius
                    recommendations.append({
                        'action': 'cooling_system_check',
                        'priority': 'medium',
                        'description': f'Temperature expected to reach {max_temp:.1f}°C'
                    })
                    
        return recommendations
        
    async def get_fleet_health(self, component_type: Optional[str] = None) -> Dict[str, Any]:
        """Get health status for entire fleet of components"""
        # Get all recent predictions
        all_predictions = []
        
        # Query from cache (in production, would query from database)
        cache_keys = []
        for key in self.prediction_cache.get_keys():
            if key.startswith('prediction_'):
                prediction = self.prediction_cache.get(key)
                if prediction and (not component_type or prediction.get('component_type') == component_type):
                    all_predictions.append(prediction)
                    
        if not all_predictions:
            return {
                'total_components': 0,
                'health_summary': {}
            }
            
        # Calculate fleet statistics
        health_scores = [p['health_score'] for p in all_predictions if 'health_score' in p]
        failure_probs = [p['failure_probability_24h'] for p in all_predictions if 'failure_probability_24h' in p]
        
        # Group by urgency
        urgency_counts = {}
        for p in all_predictions:
            urgency = p.get('maintenance_urgency', 'none')
            urgency_counts[urgency] = urgency_counts.get(urgency, 0) + 1
            
        return {
            'total_components': len(all_predictions),
            'average_health_score': np.mean(health_scores) if health_scores else 0,
            'components_at_risk': sum(1 for p in failure_probs if p > 0.5),
            'urgency_distribution': urgency_counts,
            'maintenance_backlog': sum(1 for p in all_predictions if p.get('maintenance_urgency') in ['critical', 'high']),
            'fleet_status': self._determine_fleet_status(health_scores, urgency_counts)
        }
        
    def _determine_fleet_status(self, health_scores: List[float], urgency_counts: Dict[str, int]) -> str:
        """Determine overall fleet status"""
        if not health_scores:
            return 'unknown'
            
        avg_health = np.mean(health_scores)
        critical_count = urgency_counts.get('critical', 0)
        high_count = urgency_counts.get('high', 0)
        
        if critical_count > 0 or avg_health < 50:
            return 'critical'
        elif high_count > 2 or avg_health < 70:
            return 'warning'
        elif avg_health < 85:
            return 'fair'
        else:
            return 'healthy'
            
    def close(self):
        """Close connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.es_client:
            asyncio.create_task(self.es_client.close()) 