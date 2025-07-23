"""
AutoML engine for automated machine learning.

Provides automated model selection, hyperparameter optimization, and pipeline generation.
"""

import uuid
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.svm import SVC, SVR
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, mean_squared_error, mean_absolute_error, r2_score
import optuna
from optuna.samplers import TPESampler
import joblib

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger
from ...models.ml_models import ModelType, ProblemType

logger = StructuredLogger.get_logger(__name__)


class OptimizationMetric(str, Enum):
    """Optimization metrics"""
    # Classification
    ACCURACY = "accuracy"
    PRECISION = "precision"
    RECALL = "recall"
    F1_SCORE = "f1_score"
    AUC_ROC = "auc_roc"
    
    # Regression
    MSE = "mse"
    MAE = "mae"
    RMSE = "rmse"
    R2 = "r2"
    MAPE = "mape"


class SearchStrategy(str, Enum):
    """Hyperparameter search strategies"""
    GRID_SEARCH = "grid_search"
    RANDOM_SEARCH = "random_search"
    BAYESIAN = "bayesian"
    EVOLUTIONARY = "evolutionary"
    HYPERBAND = "hyperband"


class FeatureEngineering(str, Enum):
    """Feature engineering strategies"""
    NONE = "none"
    BASIC = "basic"
    POLYNOMIAL = "polynomial"
    INTERACTIONS = "interactions"
    AUTOMATED = "automated"


@dataclass
class SearchSpace:
    """Hyperparameter search space definition"""
    algorithm: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "algorithm": self.algorithm,
            "parameters": self.parameters
        }


@dataclass
class AutoMLConfig:
    """AutoML configuration"""
    problem_type: ProblemType = ProblemType.CLASSIFICATION
    
    # Optimization
    optimization_metric: OptimizationMetric = OptimizationMetric.ACCURACY
    search_strategy: SearchStrategy = SearchStrategy.BAYESIAN
    
    # Time and resource limits
    time_limit_minutes: Optional[int] = 60
    max_trials: Optional[int] = 100
    n_jobs: int = -1
    
    # Model selection
    algorithms: Optional[List[str]] = None
    exclude_algorithms: Optional[List[str]] = None
    
    # Feature engineering
    feature_engineering: FeatureEngineering = FeatureEngineering.BASIC
    handle_missing: bool = True
    handle_categorical: bool = True
    
    # Validation
    cv_folds: int = 5
    validation_size: float = 0.2
    
    # Early stopping
    early_stopping: bool = True
    patience: int = 10
    
    # Ensemble
    ensemble: bool = True
    ensemble_size: int = 5


@dataclass
class ModelCandidate:
    """Model candidate with results"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    algorithm: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Scores
    cv_score: Optional[float] = None
    validation_score: Optional[float] = None
    training_time: Optional[float] = None
    
    # Model
    model: Optional[Any] = None
    preprocessor: Optional[Any] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "algorithm": self.algorithm,
            "parameters": self.parameters,
            "cv_score": self.cv_score,
            "validation_score": self.validation_score,
            "training_time": self.training_time
        }


@dataclass
class AutoMLResult:
    """AutoML execution result"""
    best_model: Optional[ModelCandidate] = None
    all_models: List[ModelCandidate] = field(default_factory=list)
    
    # Feature importance
    feature_importance: Optional[Dict[str, float]] = None
    
    # Metadata
    total_time_seconds: Optional[float] = None
    trials_completed: int = 0
    
    # Configuration used
    config: Optional[AutoMLConfig] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "best_model": self.best_model.to_dict() if self.best_model else None,
            "models_evaluated": len(self.all_models),
            "feature_importance": self.feature_importance,
            "total_time_seconds": self.total_time_seconds,
            "trials_completed": self.trials_completed
        }


class ModelSelector:
    """Model selection and evaluation"""
    
    # Available algorithms by problem type
    CLASSIFICATION_ALGORITHMS = {
        "logistic_regression": LogisticRegression,
        "random_forest": RandomForestClassifier,
        "gradient_boosting": GradientBoostingClassifier,
        "svm": SVC,
        "knn": KNeighborsClassifier,
        "decision_tree": DecisionTreeClassifier
    }
    
    REGRESSION_ALGORITHMS = {
        "linear_regression": LinearRegression,
        "ridge": Ridge,
        "lasso": Lasso,
        "elastic_net": ElasticNet,
        "random_forest": RandomForestRegressor,
        "gradient_boosting": GradientBoostingRegressor,
        "svm": SVR,
        "knn": KNeighborsRegressor,
        "decision_tree": DecisionTreeRegressor
    }
    
    @staticmethod
    def get_algorithms(
        problem_type: ProblemType,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get algorithms for problem type"""
        if problem_type == ProblemType.CLASSIFICATION:
            algorithms = ModelSelector.CLASSIFICATION_ALGORITHMS.copy()
        else:
            algorithms = ModelSelector.REGRESSION_ALGORITHMS.copy()
            
        # Filter algorithms
        if include:
            algorithms = {k: v for k, v in algorithms.items() if k in include}
        if exclude:
            algorithms = {k: v for k, v in algorithms.items() if k not in exclude}
            
        return algorithms
        
    @staticmethod
    def get_search_space(algorithm: str, problem_type: ProblemType) -> Dict[str, Any]:
        """Get hyperparameter search space for algorithm"""
        spaces = {
            # Classification
            "logistic_regression": {
                "C": ("float", 0.001, 100.0, "log"),
                "penalty": ("categorical", ["l1", "l2", "elasticnet"]),
                "solver": ("categorical", ["liblinear", "saga"]),
                "max_iter": ("int", 100, 1000)
            },
            "random_forest": {
                "n_estimators": ("int", 10, 500),
                "max_depth": ("int", 3, 50),
                "min_samples_split": ("int", 2, 20),
                "min_samples_leaf": ("int", 1, 10),
                "max_features": ("categorical", ["auto", "sqrt", "log2"])
            },
            "gradient_boosting": {
                "n_estimators": ("int", 50, 500),
                "learning_rate": ("float", 0.01, 0.3, "log"),
                "max_depth": ("int", 3, 10),
                "min_samples_split": ("int", 2, 20),
                "subsample": ("float", 0.5, 1.0)
            },
            "svm": {
                "C": ("float", 0.001, 100.0, "log"),
                "kernel": ("categorical", ["linear", "rbf", "poly"]),
                "gamma": ("categorical", ["scale", "auto"])
            },
            "knn": {
                "n_neighbors": ("int", 3, 50),
                "weights": ("categorical", ["uniform", "distance"]),
                "metric": ("categorical", ["euclidean", "manhattan", "minkowski"])
            },
            "decision_tree": {
                "max_depth": ("int", 3, 50),
                "min_samples_split": ("int", 2, 20),
                "min_samples_leaf": ("int", 1, 10),
                "criterion": ("categorical", ["gini", "entropy"] if problem_type == ProblemType.CLASSIFICATION else ["mse", "mae"])
            }
        }
        
        # Regression-specific
        if problem_type == ProblemType.REGRESSION:
            spaces["linear_regression"] = {}
            spaces["ridge"] = {
                "alpha": ("float", 0.001, 100.0, "log")
            }
            spaces["lasso"] = {
                "alpha": ("float", 0.001, 100.0, "log")
            }
            spaces["elastic_net"] = {
                "alpha": ("float", 0.001, 100.0, "log"),
                "l1_ratio": ("float", 0.0, 1.0)
            }
            
        return spaces.get(algorithm, {})
        
    @staticmethod
    def evaluate_model(
        model: Any,
        X: np.ndarray,
        y: np.ndarray,
        metric: OptimizationMetric,
        cv_folds: int = 5
    ) -> float:
        """Evaluate model using cross-validation"""
        if metric == OptimizationMetric.ACCURACY:
            scores = cross_val_score(model, X, y, cv=cv_folds, scoring='accuracy')
        elif metric == OptimizationMetric.PRECISION:
            scores = cross_val_score(model, X, y, cv=cv_folds, scoring='precision_weighted')
        elif metric == OptimizationMetric.RECALL:
            scores = cross_val_score(model, X, y, cv=cv_folds, scoring='recall_weighted')
        elif metric == OptimizationMetric.F1_SCORE:
            scores = cross_val_score(model, X, y, cv=cv_folds, scoring='f1_weighted')
        elif metric == OptimizationMetric.MSE:
            scores = -cross_val_score(model, X, y, cv=cv_folds, scoring='neg_mean_squared_error')
        elif metric == OptimizationMetric.MAE:
            scores = -cross_val_score(model, X, y, cv=cv_folds, scoring='neg_mean_absolute_error')
        elif metric == OptimizationMetric.R2:
            scores = cross_val_score(model, X, y, cv=cv_folds, scoring='r2')
        else:
            raise ValueError(f"Unsupported metric: {metric}")
            
        return np.mean(scores)


class AutoMLEngine:
    """
    Automated machine learning engine.
    
    Features:
    - Automated model selection
    - Hyperparameter optimization
    - Feature engineering
    - Model evaluation
    - Ensemble creation
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._results: Dict[str, AutoMLResult] = {}
        self._studies: Dict[str, optuna.Study] = {}
        
    def run(
        self,
        X: Union[pd.DataFrame, np.ndarray],
        y: Union[pd.Series, np.ndarray],
        config: Optional[AutoMLConfig] = None,
        feature_names: Optional[List[str]] = None
    ) -> AutoMLResult:
        """Run AutoML pipeline"""
        config = config or AutoMLConfig()
        start_time = datetime.utcnow()
        
        # Convert to numpy if needed
        if isinstance(X, pd.DataFrame):
            feature_names = feature_names or X.columns.tolist()
            X = X.values
        if isinstance(y, pd.Series):
            y = y.values
            
        # Split data
        X_train, X_val, y_train, y_val = train_test_split(
            X, y, test_size=config.validation_size, random_state=42
        )
        
        # Feature engineering
        if config.feature_engineering != FeatureEngineering.NONE:
            X_train, X_val = self._engineer_features(
                X_train, X_val, config.feature_engineering
            )
            
        # Get algorithms
        algorithms = ModelSelector.get_algorithms(
            config.problem_type,
            config.algorithms,
            config.exclude_algorithms
        )
        
        # Create result
        result = AutoMLResult(config=config)
        
        # Optimize each algorithm
        for algo_name, algo_class in algorithms.items():
            logger.info(f"Optimizing {algo_name}")
            
            # Create optimization study
            study = self._create_study(config)
            
            # Define objective
            def objective(trial):
                # Sample hyperparameters
                params = self._sample_hyperparameters(
                    trial,
                    algo_name,
                    config.problem_type
                )
                
                # Create model
                model = algo_class(**params)
                
                # Evaluate
                try:
                    score = ModelSelector.evaluate_model(
                        model,
                        X_train,
                        y_train,
                        config.optimization_metric,
                        config.cv_folds
                    )
                    
                    # Create candidate
                    candidate = ModelCandidate(
                        algorithm=algo_name,
                        parameters=params,
                        cv_score=score
                    )
                    
                    # Train on full training set
                    model.fit(X_train, y_train)
                    candidate.model = model
                    
                    # Validate
                    if config.problem_type == ProblemType.CLASSIFICATION:
                        y_pred = model.predict(X_val)
                        val_score = accuracy_score(y_val, y_pred)
                    else:
                        y_pred = model.predict(X_val)
                        val_score = -mean_squared_error(y_val, y_pred)
                        
                    candidate.validation_score = val_score
                    
                    # Store candidate
                    result.all_models.append(candidate)
                    
                    return score
                    
                except Exception as e:
                    logger.error(f"Error in trial: {e}")
                    return float('-inf')
                    
            # Optimize
            study.optimize(
                objective,
                n_trials=min(20, config.max_trials // len(algorithms)) if config.max_trials else 20,
                timeout=config.time_limit_minutes * 60 // len(algorithms) if config.time_limit_minutes else None
            )
            
            # Store study
            self._studies[f"{algo_name}_{id(study)}"] = study
            
            # Check time limit
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            if config.time_limit_minutes and elapsed > config.time_limit_minutes * 60:
                logger.info("Time limit reached")
                break
                
        # Select best model
        if result.all_models:
            result.best_model = max(
                result.all_models,
                key=lambda m: m.cv_score or float('-inf')
            )
            
            # Calculate feature importance
            if hasattr(result.best_model.model, 'feature_importances_'):
                importances = result.best_model.model.feature_importances_
                if feature_names and len(feature_names) == len(importances):
                    result.feature_importance = dict(zip(feature_names, importances))
                    
        # Calculate total time
        result.total_time_seconds = (datetime.utcnow() - start_time).total_seconds()
        result.trials_completed = len(result.all_models)
        
        # Create ensemble if enabled
        if config.ensemble and len(result.all_models) >= config.ensemble_size:
            ensemble_model = self._create_ensemble(
                result.all_models[:config.ensemble_size],
                config.problem_type
            )
            
            # Evaluate ensemble
            ensemble_score = ModelSelector.evaluate_model(
                ensemble_model,
                X_train,
                y_train,
                config.optimization_metric,
                config.cv_folds
            )
            
            ensemble_candidate = ModelCandidate(
                algorithm="ensemble",
                parameters={"models": config.ensemble_size},
                cv_score=ensemble_score,
                model=ensemble_model
            )
            
            result.all_models.append(ensemble_candidate)
            
            # Update best model if ensemble is better
            if ensemble_score > (result.best_model.cv_score or float('-inf')):
                result.best_model = ensemble_candidate
                
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="automl.completed",
                source="automl_engine",
                data=result.to_dict()
            ))
            
        logger.info(f"AutoML completed: {result.trials_completed} models evaluated")
        return result
        
    def _create_study(self, config: AutoMLConfig) -> optuna.Study:
        """Create Optuna study"""
        sampler = TPESampler(seed=42)
        
        # Determine direction
        if config.optimization_metric in [
            OptimizationMetric.MSE,
            OptimizationMetric.MAE,
            OptimizationMetric.RMSE
        ]:
            direction = "minimize"
        else:
            direction = "maximize"
            
        return optuna.create_study(
            direction=direction,
            sampler=sampler
        )
        
    def _sample_hyperparameters(
        self,
        trial: optuna.Trial,
        algorithm: str,
        problem_type: ProblemType
    ) -> Dict[str, Any]:
        """Sample hyperparameters for algorithm"""
        search_space = ModelSelector.get_search_space(algorithm, problem_type)
        params = {}
        
        for param_name, param_spec in search_space.items():
            param_type = param_spec[0]
            
            if param_type == "int":
                params[param_name] = trial.suggest_int(
                    param_name,
                    param_spec[1],
                    param_spec[2]
                )
            elif param_type == "float":
                log = len(param_spec) > 3 and param_spec[3] == "log"
                params[param_name] = trial.suggest_float(
                    param_name,
                    param_spec[1],
                    param_spec[2],
                    log=log
                )
            elif param_type == "categorical":
                params[param_name] = trial.suggest_categorical(
                    param_name,
                    param_spec[1]
                )
                
        return params
        
    def _engineer_features(
        self,
        X_train: np.ndarray,
        X_val: np.ndarray,
        strategy: FeatureEngineering
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Apply feature engineering"""
        if strategy == FeatureEngineering.BASIC:
            # Standardize features
            scaler = StandardScaler()
            X_train = scaler.fit_transform(X_train)
            X_val = scaler.transform(X_val)
            
        elif strategy == FeatureEngineering.POLYNOMIAL:
            # Add polynomial features
            from sklearn.preprocessing import PolynomialFeatures
            poly = PolynomialFeatures(degree=2, include_bias=False)
            X_train = poly.fit_transform(X_train)
            X_val = poly.transform(X_val)
            
            # Standardize
            scaler = StandardScaler()
            X_train = scaler.fit_transform(X_train)
            X_val = scaler.transform(X_val)
            
        # Add more strategies as needed
        
        return X_train, X_val
        
    def _create_ensemble(
        self,
        models: List[ModelCandidate],
        problem_type: ProblemType
    ) -> Any:
        """Create ensemble model"""
        from sklearn.ensemble import VotingClassifier, VotingRegressor
        
        # Extract models and names
        estimators = [
            (f"{m.algorithm}_{i}", m.model)
            for i, m in enumerate(models)
            if m.model is not None
        ]
        
        if problem_type == ProblemType.CLASSIFICATION:
            return VotingClassifier(estimators=estimators, voting='soft')
        else:
            return VotingRegressor(estimators=estimators)
            
    def get_result(self, result_id: str) -> Optional[AutoMLResult]:
        """Get AutoML result"""
        return self._results.get(result_id)
        
    def save_model(
        self,
        result: AutoMLResult,
        path: str,
        include_all: bool = False
    ):
        """Save AutoML model(s)"""
        if result.best_model and result.best_model.model:
            # Save best model
            joblib.dump(result.best_model.model, f"{path}_best_model.pkl")
            
            # Save metadata
            metadata = {
                "algorithm": result.best_model.algorithm,
                "parameters": result.best_model.parameters,
                "cv_score": result.best_model.cv_score,
                "feature_importance": result.feature_importance
            }
            
            import json
            with open(f"{path}_metadata.json", 'w') as f:
                json.dump(metadata, f, indent=2)
                
        if include_all:
            # Save all models
            for i, candidate in enumerate(result.all_models):
                if candidate.model:
                    joblib.dump(
                        candidate.model,
                        f"{path}_model_{i}_{candidate.algorithm}.pkl"
                    )
                    
        logger.info(f"Saved AutoML models to {path}") 