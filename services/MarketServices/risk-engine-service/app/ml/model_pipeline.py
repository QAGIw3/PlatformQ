"""ML Model Pipeline for automated training and deployment of risk models."""

import asyncio
import logging
import pickle
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_score
from sklearn.metrics import accuracy_score, mean_squared_error, mean_absolute_error
import xgboost as xgb
import lightgbm as lgb
from pyignite import Client as IgniteClient
import mlflow
import mlflow.sklearn

logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration for a risk model."""
    model_id: str
    model_type: str  # 'liquidation_predictor', 'volatility_forecast', 'anomaly_detector'
    algorithm: str  # 'random_forest', 'xgboost', 'lightgbm', 'neural_network'
    features: List[str]
    target: str
    hyperparameters: Dict[str, Any]
    training_config: Dict[str, Any] = field(default_factory=dict)
    
    
@dataclass
class ModelVersion:
    """Version information for a trained model."""
    version_id: str
    model_id: str
    trained_at: datetime
    metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    data_hash: str
    is_active: bool = False
    

class MLModelPipeline:
    """
    Automated ML pipeline for risk model training and deployment.
    
    Features:
    - Automated feature engineering
    - Model training with hyperparameter optimization
    - A/B testing for model comparison
    - Automated deployment
    - Model monitoring and retraining
    """
    
    def __init__(self, 
                 ignite_client: IgniteClient,
                 mlflow_uri: str = "http://mlflow:5000"):
        self.ignite = ignite_client
        self.mlflow_uri = mlflow_uri
        
        # Model registry
        self.models: Dict[str, ModelConfig] = {}
        self.active_versions: Dict[str, ModelVersion] = {}
        
        # Cache names
        self.model_cache = "ml_models"
        self.feature_cache = "ml_features"
        self.training_data_cache = "ml_training_data"
        
        # Initialize MLflow
        mlflow.set_tracking_uri(mlflow_uri)
        
        # Feature engineering functions
        self.feature_engineers = {
            'liquidation_predictor': self._engineer_liquidation_features,
            'volatility_forecast': self._engineer_volatility_features,
            'anomaly_detector': self._engineer_anomaly_features,
            'cross_market_correlation': self._engineer_correlation_features
        }
        
    async def initialize(self):
        """Initialize ML pipeline and load models."""
        logger.info("Initializing ML model pipeline")
        
        # Create caches
        self.ignite.get_or_create_cache(self.model_cache)
        self.ignite.get_or_create_cache(self.feature_cache)
        self.ignite.get_or_create_cache(self.training_data_cache)
        
        # Register default models
        await self._register_default_models()
        
        # Load active models
        await self._load_active_models()
        
        logger.info("ML pipeline initialized")
        
    async def _register_default_models(self):
        """Register default risk models."""
        # Liquidation predictor
        self.models['liquidation_predictor'] = ModelConfig(
            model_id='liquidation_predictor',
            model_type='liquidation_predictor',
            algorithm='xgboost',
            features=[
                'margin_ratio', 'leverage', 'position_count', 'avg_position_size',
                'volatility_30d', 'pnl_30d', 'max_drawdown_30d', 'win_rate',
                'avg_hold_time', 'concentration_ratio', 'correlation_exposure'
            ],
            target='liquidated_within_24h',
            hyperparameters={
                'n_estimators': 100,
                'max_depth': 6,
                'learning_rate': 0.1,
                'subsample': 0.8,
                'colsample_bytree': 0.8
            }
        )
        
        # Volatility forecaster
        self.models['volatility_forecast'] = ModelConfig(
            model_id='volatility_forecast',
            model_type='volatility_forecast',
            algorithm='lightgbm',
            features=[
                'returns_1h', 'returns_4h', 'returns_24h', 'volume_ratio',
                'spread_ratio', 'order_imbalance', 'trades_per_minute',
                'large_trade_ratio', 'price_momentum', 'rsi', 'bollinger_position'
            ],
            target='realized_volatility_next_hour',
            hyperparameters={
                'num_leaves': 31,
                'learning_rate': 0.05,
                'n_estimators': 200,
                'subsample': 0.8,
                'colsample_bytree': 0.8
            }
        )
        
        # Anomaly detector
        self.models['anomaly_detector'] = ModelConfig(
            model_id='anomaly_detector',
            model_type='anomaly_detector',
            algorithm='isolation_forest',
            features=[
                'price_zscore', 'volume_zscore', 'volatility_zscore',
                'spread_zscore', 'order_flow_imbalance', 'large_trade_count',
                'price_velocity', 'microstructure_noise'
            ],
            target='is_anomaly',
            hyperparameters={
                'n_estimators': 100,
                'contamination': 0.01,
                'max_features': 1.0
            }
        )
        
    async def train_model(self, 
                         model_id: str,
                         training_data: pd.DataFrame,
                         force_retrain: bool = False) -> ModelVersion:
        """
        Train a model with the given data.
        
        Args:
            model_id: Model identifier
            training_data: Training dataset
            force_retrain: Force retraining even if data hasn't changed
            
        Returns:
            Trained model version
        """
        config = self.models.get(model_id)
        if not config:
            raise ValueError(f"Model {model_id} not registered")
            
        # Check if retraining needed
        data_hash = self._hash_data(training_data)
        current_version = self.active_versions.get(model_id)
        
        if not force_retrain and current_version and current_version.data_hash == data_hash:
            logger.info(f"Model {model_id} already trained on this data")
            return current_version
            
        logger.info(f"Training model {model_id}")
        
        # Start MLflow run
        with mlflow.start_run(run_name=f"{model_id}_{datetime.utcnow().isoformat()}"):
            # Log parameters
            mlflow.log_params(config.hyperparameters)
            mlflow.log_param("algorithm", config.algorithm)
            mlflow.log_param("features", ",".join(config.features))
            
            # Engineer features
            X, y = await self._prepare_training_data(config, training_data)
            
            # Split data
            if config.model_type in ['volatility_forecast', 'liquidation_predictor']:
                # Time series split for temporal data
                tscv = TimeSeriesSplit(n_splits=5)
                splits = list(tscv.split(X))
                X_train, X_test = X.iloc[splits[-1][0]], X.iloc[splits[-1][1]]
                y_train, y_test = y.iloc[splits[-1][0]], y.iloc[splits[-1][1]]
            else:
                # Random split for other models
                X_train, X_test, y_train, y_test = train_test_split(
                    X, y, test_size=0.2, random_state=42
                )
            
            # Train model
            model = self._create_model(config)
            model.fit(X_train, y_train)
            
            # Evaluate
            metrics = self._evaluate_model(model, X_test, y_test, config)
            
            # Log metrics
            for metric_name, value in metrics.items():
                mlflow.log_metric(metric_name, value)
                
            # Feature importance
            feature_importance = self._get_feature_importance(model, config, X.columns)
            
            # Log model
            mlflow.sklearn.log_model(model, model_id)
            
            # Create version
            version = ModelVersion(
                version_id=f"{model_id}_v{datetime.utcnow().timestamp()}",
                model_id=model_id,
                trained_at=datetime.utcnow(),
                metrics=metrics,
                feature_importance=feature_importance,
                data_hash=data_hash
            )
            
            # Save to cache
            await self._save_model(model, version)
            
            logger.info(f"Model {model_id} trained successfully. Metrics: {metrics}")
            
            return version
            
    async def deploy_model(self, 
                          version: ModelVersion,
                          canary_percentage: float = 0.1) -> bool:
        """
        Deploy a model version with optional canary deployment.
        
        Args:
            version: Model version to deploy
            canary_percentage: Percentage of traffic for canary (0-1)
            
        Returns:
            True if deployment successful
        """
        logger.info(f"Deploying model version {version.version_id}")
        
        try:
            # Load model
            model = await self._load_model(version.version_id)
            
            # Validate model
            if not await self._validate_model(model, version):
                logger.error("Model validation failed")
                return False
                
            # Update deployment config
            deployment_config = {
                'version_id': version.version_id,
                'canary_percentage': canary_percentage,
                'deployed_at': datetime.utcnow().isoformat(),
                'status': 'canary' if canary_percentage < 1.0 else 'full'
            }
            
            # Save deployment config
            cache = self.ignite.get_cache(self.model_cache)
            cache.put(f"deployment_{version.model_id}", deployment_config)
            
            # Update active version (with canary if specified)
            if canary_percentage >= 1.0:
                self.active_versions[version.model_id] = version
                version.is_active = True
            else:
                # Keep both versions for A/B testing
                logger.info(f"Canary deployment at {canary_percentage*100}%")
                
            logger.info(f"Model {version.version_id} deployed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            return False
            
    async def predict(self, 
                     model_id: str,
                     features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make prediction using active model.
        
        Args:
            model_id: Model identifier
            features: Feature dictionary
            
        Returns:
            Prediction results
        """
        # Get active version
        version = self.active_versions.get(model_id)
        if not version:
            raise ValueError(f"No active version for model {model_id}")
            
        # Check for canary deployment
        deployment = self._get_deployment_config(model_id)
        if deployment and deployment['status'] == 'canary':
            # Random routing for canary
            if np.random.random() > deployment['canary_percentage']:
                # Use previous version
                version = await self._get_previous_version(model_id)
                
        # Load model
        model = await self._load_model(version.version_id)
        
        # Prepare features
        config = self.models[model_id]
        X = pd.DataFrame([features])[config.features]
        
        # Scale features if needed
        scaler = await self._load_scaler(version.version_id)
        if scaler:
            X = scaler.transform(X)
            
        # Predict
        prediction = model.predict(X)[0]
        
        # Get prediction probability if classifier
        if hasattr(model, 'predict_proba'):
            proba = model.predict_proba(X)[0]
            return {
                'prediction': prediction,
                'probability': float(max(proba)),
                'model_version': version.version_id
            }
        else:
            return {
                'prediction': float(prediction),
                'model_version': version.version_id
            }
            
    async def monitor_and_retrain(self):
        """Monitor model performance and trigger retraining if needed."""
        for model_id, config in self.models.items():
            version = self.active_versions.get(model_id)
            if not version:
                continue
                
            # Check if retraining needed
            if await self._should_retrain(model_id, version):
                logger.info(f"Retraining triggered for {model_id}")
                
                # Get fresh training data
                training_data = await self._get_training_data(model_id)
                
                # Train new version
                new_version = await self.train_model(model_id, training_data)
                
                # Compare with current version
                if await self._compare_models(version, new_version) > 0:
                    # New model is better
                    logger.info(f"New version of {model_id} is better, deploying")
                    await self.deploy_model(new_version, canary_percentage=0.2)
                    
    async def _prepare_training_data(self, 
                                   config: ModelConfig,
                                   raw_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare training data with feature engineering."""
        # Apply feature engineering
        engineer_func = self.feature_engineers.get(config.model_type)
        if engineer_func:
            engineered_data = await engineer_func(raw_data)
        else:
            engineered_data = raw_data
            
        # Select features and target
        X = engineered_data[config.features]
        y = engineered_data[config.target]
        
        # Handle missing values
        X = X.fillna(X.mean())
        
        return X, y
        
    async def _engineer_liquidation_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for liquidation prediction."""
        # Add rolling statistics
        df['volatility_30d'] = df['returns'].rolling(30*24).std()
        df['pnl_30d'] = df['pnl'].rolling(30*24).sum()
        df['max_drawdown_30d'] = df['equity'].rolling(30*24).apply(
            lambda x: (x.max() - x[-1]) / x.max()
        )
        
        # Win rate
        df['win_rate'] = df['pnl'].rolling(30*24).apply(
            lambda x: (x > 0).sum() / len(x)
        )
        
        # Concentration ratio
        df['concentration_ratio'] = df['largest_position'] / df['total_position_value']
        
        return df
        
    async def _engineer_volatility_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for volatility forecasting."""
        # Price returns at different intervals
        df['returns_1h'] = df['price'].pct_change(1)
        df['returns_4h'] = df['price'].pct_change(4)
        df['returns_24h'] = df['price'].pct_change(24)
        
        # Volume metrics
        df['volume_ratio'] = df['volume'] / df['volume'].rolling(24).mean()
        
        # Microstructure features
        df['spread_ratio'] = df['spread'] / df['price']
        df['order_imbalance'] = (df['buy_volume'] - df['sell_volume']) / df['volume']
        
        # Technical indicators
        df['rsi'] = self._calculate_rsi(df['price'])
        df['bollinger_position'] = (df['price'] - df['price'].rolling(20).mean()) / (2 * df['price'].rolling(20).std())
        
        return df
        
    async def _engineer_anomaly_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for anomaly detection."""
        # Z-scores for various metrics
        for col in ['price', 'volume', 'volatility', 'spread']:
            df[f'{col}_zscore'] = (df[col] - df[col].rolling(100).mean()) / df[col].rolling(100).std()
            
        # Price velocity
        df['price_velocity'] = df['price'].diff() / df['time_diff']
        
        # Microstructure noise
        df['microstructure_noise'] = df['price'].rolling(10).std() / df['price'].rolling(100).std()
        
        return df
        
    async def _engineer_correlation_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features for cross-market correlation."""
        # This would include correlation calculations across markets
        # Placeholder for now
        return df
        
    def _create_model(self, config: ModelConfig):
        """Create model instance based on configuration."""
        if config.algorithm == 'xgboost':
            return xgb.XGBClassifier(**config.hyperparameters)
        elif config.algorithm == 'lightgbm':
            return lgb.LGBMRegressor(**config.hyperparameters)
        elif config.algorithm == 'random_forest':
            if config.model_type == 'liquidation_predictor':
                return RandomForestClassifier(**config.hyperparameters)
            else:
                return RandomForestRegressor(**config.hyperparameters)
        elif config.algorithm == 'neural_network':
            return MLPRegressor(**config.hyperparameters)
        elif config.algorithm == 'isolation_forest':
            from sklearn.ensemble import IsolationForest
            return IsolationForest(**config.hyperparameters)
        else:
            raise ValueError(f"Unknown algorithm: {config.algorithm}")
            
    def _evaluate_model(self, model, X_test, y_test, config: ModelConfig) -> Dict[str, float]:
        """Evaluate model performance."""
        predictions = model.predict(X_test)
        
        metrics = {}
        
        if config.model_type == 'liquidation_predictor':
            # Classification metrics
            metrics['accuracy'] = accuracy_score(y_test, predictions)
            if hasattr(model, 'predict_proba'):
                from sklearn.metrics import roc_auc_score
                proba = model.predict_proba(X_test)[:, 1]
                metrics['auc_roc'] = roc_auc_score(y_test, proba)
        else:
            # Regression metrics
            metrics['mse'] = mean_squared_error(y_test, predictions)
            metrics['mae'] = mean_absolute_error(y_test, predictions)
            metrics['rmse'] = np.sqrt(metrics['mse'])
            
        return metrics
        
    def _get_feature_importance(self, model, config: ModelConfig, feature_names) -> Dict[str, float]:
        """Extract feature importance from model."""
        importance_dict = {}
        
        if hasattr(model, 'feature_importances_'):
            importances = model.feature_importances_
            for name, importance in zip(feature_names, importances):
                importance_dict[name] = float(importance)
                
        return importance_dict
        
    async def _save_model(self, model, version: ModelVersion):
        """Save model to cache."""
        cache = self.ignite.get_cache(self.model_cache)
        
        # Serialize model
        model_bytes = pickle.dumps(model)
        
        # Save to cache
        cache.put(version.version_id, model_bytes)
        cache.put(f"version_{version.model_id}", version.__dict__)
        
    async def _load_model(self, version_id: str):
        """Load model from cache."""
        cache = self.ignite.get_cache(self.model_cache)
        
        model_bytes = cache.get(version_id)
        if not model_bytes:
            raise ValueError(f"Model {version_id} not found")
            
        return pickle.loads(model_bytes)
        
    def _hash_data(self, data: pd.DataFrame) -> str:
        """Generate hash of training data."""
        # Use shape and sample of data for hash
        data_str = f"{data.shape}_{data.iloc[:100].to_json()}"
        return hashlib.md5(data_str.encode()).hexdigest()
        
    async def _should_retrain(self, model_id: str, version: ModelVersion) -> bool:
        """Check if model should be retrained."""
        # Retrain if:
        # 1. Model is older than threshold
        # 2. Performance has degraded
        # 3. Data distribution has shifted
        
        age = datetime.utcnow() - version.trained_at
        if age > timedelta(days=7):  # Retrain weekly
            return True
            
        # Check performance degradation (would need monitoring data)
        # Check data drift (would need to implement drift detection)
        
        return False
        
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI technical indicator."""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi 