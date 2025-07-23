"""
AutoML Engine for Unified ML Platform

Provides automated machine learning capabilities including:
- Automated feature engineering
- Model selection and ensemble
- Hyperparameter optimization
- Neural architecture search
- Automated pipeline generation
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, VotingClassifier, VotingRegressor
from sklearn.metrics import accuracy_score, f1_score, mean_squared_error, r2_score
import optuna
from optuna.samplers import TPESampler
import mlflow
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

logger = logging.getLogger(__name__)


class ProblemType(str, Enum):
    """ML problem types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    TIME_SERIES = "time_series"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"
    RECOMMENDATION = "recommendation"


class ModelFramework(str, Enum):
    """Supported ML frameworks"""
    SKLEARN = "sklearn"
    PYTORCH = "pytorch"
    TENSORFLOW = "tensorflow"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CATBOOST = "catboost"


@dataclass
class AutoMLConfig:
    """Configuration for AutoML run"""
    problem_type: ProblemType
    optimization_metric: str
    time_budget: int = 3600  # seconds
    max_trials: int = 100
    cross_validation_folds: int = 5
    test_size: float = 0.2
    ensemble_size: int = 5
    enable_feature_engineering: bool = True
    enable_neural_search: bool = False
    frameworks: List[ModelFramework] = field(default_factory=lambda: [ModelFramework.SKLEARN, ModelFramework.XGBOOST])
    resource_limit: Dict[str, Any] = field(default_factory=lambda: {"cpu": 4, "memory": "8GB", "gpu": 0})


@dataclass
class AutoMLResult:
    """Result of AutoML run"""
    best_model: Any
    best_score: float
    best_params: Dict[str, Any]
    feature_importance: Dict[str, float]
    ensemble_models: List[Any] = field(default_factory=list)
    training_history: List[Dict[str, Any]] = field(default_factory=list)
    pipeline_code: str = ""
    deployment_config: Dict[str, Any] = field(default_factory=dict)


class FeatureEngineer:
    """Automated feature engineering"""
    
    def __init__(self):
        self.generated_features = []
        self.encoders = {}
        self.scalers = {}
        
    async def engineer_features(
        self,
        df: pd.DataFrame,
        target_column: str,
        problem_type: ProblemType
    ) -> pd.DataFrame:
        """Automatically engineer features"""
        df_engineered = df.copy()
        
        # Numeric features
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if target_column in numeric_cols:
            numeric_cols.remove(target_column)
            
        # Categorical features
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        # Time-based features
        datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        
        # Generate polynomial features for numeric columns
        if len(numeric_cols) > 0:
            for col in numeric_cols[:5]:  # Limit to prevent explosion
                df_engineered[f'{col}_squared'] = df[col] ** 2
                df_engineered[f'{col}_cubed'] = df[col] ** 3
                df_engineered[f'{col}_log'] = np.log1p(np.abs(df[col]))
                self.generated_features.extend([f'{col}_squared', f'{col}_cubed', f'{col}_log'])
                
        # Generate interaction features
        if len(numeric_cols) > 1:
            for i, col1 in enumerate(numeric_cols[:5]):
                for col2 in numeric_cols[i+1:6]:
                    df_engineered[f'{col1}_x_{col2}'] = df[col1] * df[col2]
                    self.generated_features.append(f'{col1}_x_{col2}')
                    
        # Encode categorical variables
        for col in categorical_cols:
            if df[col].nunique() < 50:  # One-hot encode low cardinality
                dummies = pd.get_dummies(df[col], prefix=col)
                df_engineered = pd.concat([df_engineered, dummies], axis=1)
                df_engineered.drop(col, axis=1, inplace=True)
            else:  # Label encode high cardinality
                le = LabelEncoder()
                df_engineered[col] = le.fit_transform(df[col].astype(str))
                self.encoders[col] = le
                
        # Extract time features
        for col in datetime_cols:
            df_engineered[f'{col}_year'] = df[col].dt.year
            df_engineered[f'{col}_month'] = df[col].dt.month
            df_engineered[f'{col}_day'] = df[col].dt.day
            df_engineered[f'{col}_dayofweek'] = df[col].dt.dayofweek
            df_engineered[f'{col}_hour'] = df[col].dt.hour
            self.generated_features.extend([
                f'{col}_year', f'{col}_month', f'{col}_day',
                f'{col}_dayofweek', f'{col}_hour'
            ])
            
        # Generate aggregation features
        if problem_type == ProblemType.TIME_SERIES and len(numeric_cols) > 0:
            for col in numeric_cols[:5]:
                df_engineered[f'{col}_rolling_mean_7'] = df[col].rolling(window=7, min_periods=1).mean()
                df_engineered[f'{col}_rolling_std_7'] = df[col].rolling(window=7, min_periods=1).std()
                df_engineered[f'{col}_diff'] = df[col].diff()
                self.generated_features.extend([
                    f'{col}_rolling_mean_7', f'{col}_rolling_std_7', f'{col}_diff'
                ])
                
        logger.info(f"Generated {len(self.generated_features)} new features")
        return df_engineered


class NeuralArchitectureSearch:
    """Automated neural architecture search"""
    
    def __init__(self, input_dim: int, output_dim: int, problem_type: ProblemType):
        self.input_dim = input_dim
        self.output_dim = output_dim
        self.problem_type = problem_type
        
    def create_model(self, trial: optuna.Trial) -> nn.Module:
        """Create a neural network based on Optuna trial"""
        n_layers = trial.suggest_int('n_layers', 2, 5)
        layers = []
        
        in_features = self.input_dim
        for i in range(n_layers):
            out_features = trial.suggest_int(f'n_units_l{i}', 16, 256)
            layers.append(nn.Linear(in_features, out_features))
            
            activation = trial.suggest_categorical(
                f'activation_l{i}',
                ['relu', 'tanh', 'elu', 'leaky_relu']
            )
            if activation == 'relu':
                layers.append(nn.ReLU())
            elif activation == 'tanh':
                layers.append(nn.Tanh())
            elif activation == 'elu':
                layers.append(nn.ELU())
            elif activation == 'leaky_relu':
                layers.append(nn.LeakyReLU())
                
            dropout_rate = trial.suggest_float(f'dropout_l{i}', 0.0, 0.5)
            if dropout_rate > 0:
                layers.append(nn.Dropout(dropout_rate))
                
            in_features = out_features
            
        # Output layer
        if self.problem_type == ProblemType.CLASSIFICATION:
            layers.append(nn.Linear(in_features, self.output_dim))
            if self.output_dim == 1:
                layers.append(nn.Sigmoid())
            else:
                layers.append(nn.Softmax(dim=1))
        else:
            layers.append(nn.Linear(in_features, self.output_dim))
            
        return nn.Sequential(*layers)


class AutoMLEngine:
    """
    Main AutoML engine for automated machine learning
    """
    
    def __init__(
        self,
        training_orchestrator,
        model_registry,
        optimization_metric: str = "accuracy",
        time_budget: int = 3600
    ):
        self.training_orchestrator = training_orchestrator
        self.model_registry = model_registry
        self.optimization_metric = optimization_metric
        self.time_budget = time_budget
        
        self.feature_engineer = FeatureEngineer()
        self.models = {}
        self.study = None
        self.best_model = None
        self.best_score = -np.inf
        
    async def initialize(self):
        """Initialize AutoML engine"""
        logger.info("AutoML Engine initialized")
        
    async def run(
        self,
        dataset: Union[pd.DataFrame, str],
        target_column: str,
        config: AutoMLConfig,
        experiment_name: str = "automl_experiment"
    ) -> AutoMLResult:
        """
        Run AutoML pipeline
        
        Args:
            dataset: DataFrame or path to dataset
            target_column: Name of target column
            config: AutoML configuration
            experiment_name: MLflow experiment name
            
        Returns:
            AutoML result with best model and metadata
        """
        logger.info(f"Starting AutoML run with config: {config}")
        
        # Load dataset if path provided
        if isinstance(dataset, str):
            dataset = await self._load_dataset(dataset)
            
        # Prepare data
        X, y = self._prepare_data(dataset, target_column)
        
        # Feature engineering
        if config.enable_feature_engineering:
            X = await self.feature_engineer.engineer_features(
                X, target_column, config.problem_type
            )
            
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=config.test_size, random_state=42
        )
        
        # Scale features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Create Optuna study
        self.study = optuna.create_study(
            direction="maximize" if self._is_maximize_metric(config.optimization_metric) else "minimize",
            sampler=TPESampler(seed=42)
        )
        
        # Track with MLflow
        mlflow.set_experiment(experiment_name)
        
        # Run optimization
        start_time = datetime.utcnow()
        trial_count = 0
        
        while (datetime.utcnow() - start_time).total_seconds() < config.time_budget and trial_count < config.max_trials:
            # Try different frameworks
            for framework in config.frameworks:
                if (datetime.utcnow() - start_time).total_seconds() >= config.time_budget:
                    break
                    
                trial = self.study.ask()
                
                try:
                    if framework == ModelFramework.SKLEARN:
                        score = await self._optimize_sklearn(
                            trial, X_train_scaled, y_train, X_test_scaled, y_test, config
                        )
                    elif framework == ModelFramework.XGBOOST:
                        score = await self._optimize_xgboost(
                            trial, X_train_scaled, y_train, X_test_scaled, y_test, config
                        )
                    elif framework == ModelFramework.PYTORCH and config.enable_neural_search:
                        score = await self._optimize_pytorch(
                            trial, X_train_scaled, y_train, X_test_scaled, y_test, config
                        )
                    else:
                        continue
                        
                    self.study.tell(trial, score)
                    trial_count += 1
                    
                except Exception as e:
                    logger.error(f"Trial failed: {e}")
                    self.study.tell(trial, -np.inf)
                    
        # Create ensemble
        ensemble_models = await self._create_ensemble(
            X_train_scaled, y_train, X_test_scaled, y_test, config
        )
        
        # Generate pipeline code
        pipeline_code = self._generate_pipeline_code(config)
        
        # Create deployment config
        deployment_config = self._create_deployment_config(config)
        
        # Save best model
        await self._save_best_model(experiment_name)
        
        return AutoMLResult(
            best_model=self.best_model,
            best_score=self.best_score,
            best_params=self.study.best_params,
            feature_importance=self._get_feature_importance(),
            ensemble_models=ensemble_models,
            training_history=self.study.trials_dataframe().to_dict('records'),
            pipeline_code=pipeline_code,
            deployment_config=deployment_config
        )
        
    async def _optimize_sklearn(
        self,
        trial: optuna.Trial,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        config: AutoMLConfig
    ) -> float:
        """Optimize scikit-learn models"""
        model_type = trial.suggest_categorical(
            'sklearn_model',
            ['random_forest', 'gradient_boosting', 'svm', 'neural_net']
        )
        
        if model_type == 'random_forest':
            if config.problem_type == ProblemType.CLASSIFICATION:
                model = RandomForestClassifier(
                    n_estimators=trial.suggest_int('n_estimators', 10, 200),
                    max_depth=trial.suggest_int('max_depth', 3, 20),
                    min_samples_split=trial.suggest_int('min_samples_split', 2, 20),
                    min_samples_leaf=trial.suggest_int('min_samples_leaf', 1, 10),
                    random_state=42
                )
            else:
                model = RandomForestRegressor(
                    n_estimators=trial.suggest_int('n_estimators', 10, 200),
                    max_depth=trial.suggest_int('max_depth', 3, 20),
                    min_samples_split=trial.suggest_int('min_samples_split', 2, 20),
                    min_samples_leaf=trial.suggest_int('min_samples_leaf', 1, 10),
                    random_state=42
                )
                
        # Train model
        model.fit(X_train, y_train)
        
        # Evaluate
        predictions = model.predict(X_test)
        score = self._calculate_score(y_test, predictions, config)
        
        # Track best model
        if score > self.best_score:
            self.best_score = score
            self.best_model = model
            
        # Log to MLflow
        with mlflow.start_run(nested=True):
            mlflow.log_params(trial.params)
            mlflow.log_metric(config.optimization_metric, score)
            mlflow.sklearn.log_model(model, "model")
            
        return score
        
    async def _optimize_xgboost(
        self,
        trial: optuna.Trial,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        config: AutoMLConfig
    ) -> float:
        """Optimize XGBoost models"""
        import xgboost as xgb
        
        params = {
            'objective': 'binary:logistic' if config.problem_type == ProblemType.CLASSIFICATION else 'reg:squarederror',
            'max_depth': trial.suggest_int('max_depth', 3, 10),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
            'n_estimators': trial.suggest_int('n_estimators', 50, 300),
            'subsample': trial.suggest_float('subsample', 0.6, 1.0),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
            'gamma': trial.suggest_float('gamma', 0, 5),
            'reg_alpha': trial.suggest_float('reg_alpha', 0, 2),
            'reg_lambda': trial.suggest_float('reg_lambda', 0, 2),
            'random_state': 42
        }
        
        if config.problem_type == ProblemType.CLASSIFICATION:
            model = xgb.XGBClassifier(**params)
        else:
            model = xgb.XGBRegressor(**params)
            
        model.fit(X_train, y_train)
        
        predictions = model.predict(X_test)
        score = self._calculate_score(y_test, predictions, config)
        
        if score > self.best_score:
            self.best_score = score
            self.best_model = model
            
        return score
        
    async def _optimize_pytorch(
        self,
        trial: optuna.Trial,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        config: AutoMLConfig
    ) -> float:
        """Optimize PyTorch neural networks"""
        # Convert to tensors
        X_train_tensor = torch.FloatTensor(X_train)
        y_train_tensor = torch.FloatTensor(y_train.reshape(-1, 1))
        X_test_tensor = torch.FloatTensor(X_test)
        y_test_tensor = torch.FloatTensor(y_test.reshape(-1, 1))
        
        # Create data loaders
        train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
        train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)
        
        # Create model
        input_dim = X_train.shape[1]
        output_dim = 1 if config.problem_type == ProblemType.REGRESSION else len(np.unique(y_train))
        
        nas = NeuralArchitectureSearch(input_dim, output_dim, config.problem_type)
        model = nas.create_model(trial)
        
        # Training parameters
        learning_rate = trial.suggest_float('learning_rate', 1e-4, 1e-2, log=True)
        optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
        
        if config.problem_type == ProblemType.CLASSIFICATION:
            criterion = nn.BCELoss() if output_dim == 1 else nn.CrossEntropyLoss()
        else:
            criterion = nn.MSELoss()
            
        # Train model
        epochs = trial.suggest_int('epochs', 10, 100)
        for epoch in range(epochs):
            model.train()
            for batch_X, batch_y in train_loader:
                optimizer.zero_grad()
                outputs = model(batch_X)
                loss = criterion(outputs, batch_y)
                loss.backward()
                optimizer.step()
                
        # Evaluate
        model.eval()
        with torch.no_grad():
            predictions = model(X_test_tensor).numpy()
            
        score = self._calculate_score(y_test, predictions, config)
        
        if score > self.best_score:
            self.best_score = score
            self.best_model = model
            
        return score
        
    async def _create_ensemble(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        config: AutoMLConfig
    ) -> List[Any]:
        """Create ensemble of top models"""
        # Get top trials
        top_trials = sorted(
            self.study.trials,
            key=lambda t: t.value,
            reverse=self._is_maximize_metric(config.optimization_metric)
        )[:config.ensemble_size]
        
        ensemble_models = []
        for trial in top_trials:
            # Recreate model with best params
            # This is simplified - in production, load from MLflow
            ensemble_models.append(self.best_model)
            
        return ensemble_models
        
    def _calculate_score(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        config: AutoMLConfig
    ) -> float:
        """Calculate score based on optimization metric"""
        if config.problem_type == ProblemType.CLASSIFICATION:
            if config.optimization_metric == 'accuracy':
                return accuracy_score(y_true, y_pred)
            elif config.optimization_metric == 'f1':
                return f1_score(y_true, y_pred, average='weighted')
        else:
            if config.optimization_metric == 'mse':
                return -mean_squared_error(y_true, y_pred)
            elif config.optimization_metric == 'r2':
                return r2_score(y_true, y_pred)
                
        return 0.0
        
    def _is_maximize_metric(self, metric: str) -> bool:
        """Check if metric should be maximized"""
        return metric in ['accuracy', 'f1', 'r2', 'auc']
        
    def _get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance from best model"""
        if hasattr(self.best_model, 'feature_importances_'):
            importances = self.best_model.feature_importances_
            feature_names = [f"feature_{i}" for i in range(len(importances))]
            return dict(zip(feature_names, importances))
        return {}
        
    def _generate_pipeline_code(self, config: AutoMLConfig) -> str:
        """Generate deployable pipeline code"""
        code = f"""
# AutoML Generated Pipeline
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import joblib

# Load model
model = joblib.load('best_model.pkl')
scaler = joblib.load('scaler.pkl')

def predict(data):
    # Feature engineering
    # {self.feature_engineer.generated_features}
    
    # Scale features
    data_scaled = scaler.transform(data)
    
    # Make predictions
    predictions = model.predict(data_scaled)
    
    return predictions
"""
        return code
        
    def _create_deployment_config(self, config: AutoMLConfig) -> Dict[str, Any]:
        """Create deployment configuration"""
        return {
            "model_name": f"automl_{config.problem_type.value}",
            "model_version": "1.0.0",
            "runtime": "python:3.9",
            "dependencies": [
                "scikit-learn>=1.3.0",
                "xgboost>=1.7.0",
                "pandas>=2.0.0",
                "numpy>=1.24.0"
            ],
            "api_schema": {
                "input": {"type": "array", "items": {"type": "number"}},
                "output": {"type": "number" if config.problem_type == ProblemType.REGRESSION else "string"}
            },
            "resources": config.resource_limit
        }
        
    async def _save_best_model(self, experiment_name: str):
        """Save best model to model registry"""
        if self.best_model:
            await self.model_registry.register_model(
                name=f"{experiment_name}_best_model",
                model=self.best_model,
                metrics={"score": self.best_score},
                tags={"automl": "true", "framework": str(type(self.best_model).__name__)}
            )
            
    async def _load_dataset(self, path: str) -> pd.DataFrame:
        """Load dataset from path"""
        if path.endswith('.csv'):
            return pd.read_csv(path)
        elif path.endswith('.parquet'):
            return pd.read_parquet(path)
        else:
            raise ValueError(f"Unsupported file format: {path}")
            
    def _prepare_data(
        self,
        df: pd.DataFrame,
        target_column: str
    ) -> Tuple[pd.DataFrame, np.ndarray]:
        """Prepare data for training"""
        X = df.drop(columns=[target_column])
        y = df[target_column].values
        return X, y 