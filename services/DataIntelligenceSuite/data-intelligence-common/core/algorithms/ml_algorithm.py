"""
Machine Learning Algorithm Implementation

Provides base class for ML algorithms with support for training, inference, and evaluation.
"""


from typing import Any, Dict, List, Optional, Union, Callable, TypeVar, Generic, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from datetime import datetime

from .base_algorithm import BaseAlgorithm, AlgorithmConfig, AlgorithmResult, AlgorithmType
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

# Type variables
X = TypeVar('X')  # Feature type
Y = TypeVar('Y')  # Label type
M = TypeVar('M')  # Model type


class MLTask(str, Enum):
    """Machine learning task types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    DIMENSIONALITY_REDUCTION = "dimensionality_reduction"
    ANOMALY_DETECTION = "anomaly_detection"
    RECOMMENDATION = "recommendation"
    TIME_SERIES = "time_series"
    REINFORCEMENT_LEARNING = "reinforcement_learning"


class ModelFramework(str, Enum):
    """ML frameworks"""
    SCIKIT_LEARN = "scikit_learn"
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CATBOOST = "catboost"
    CUSTOM = "custom"


class DataSplitStrategy(str, Enum):
    """Data splitting strategies"""
    RANDOM = "random"
    STRATIFIED = "stratified"
    TIME_SERIES = "time_series"
    GROUP = "group"
    CUSTOM = "custom"


@dataclass
class MLAlgorithmConfig(AlgorithmConfig):
    """Configuration for ML algorithms"""
    task: MLTask = MLTask.CLASSIFICATION
    framework: ModelFramework = ModelFramework.SCIKIT_LEARN
    
    # Model parameters
    model_params: Dict[str, Any] = field(default_factory=dict)
    
    # Training parameters
    batch_size: int = 32
    epochs: int = 100
    learning_rate: float = 0.001
    validation_split: float = 0.2
    test_split: float = 0.1
    random_state: int = 42
    
    # Data handling
    split_strategy: DataSplitStrategy = DataSplitStrategy.RANDOM
    shuffle_data: bool = True
    normalize_features: bool = True
    handle_missing: str = "drop"  # drop, mean, median, mode, forward_fill
    
    # Model selection
    enable_cross_validation: bool = False
    cv_folds: int = 5
    scoring_metric: Optional[str] = None
    
    # Early stopping
    enable_early_stopping: bool = True
    early_stopping_patience: int = 10
    early_stopping_metric: Optional[str] = None
    
    # Model persistence
    save_model: bool = True
    model_path: Optional[str] = None
    
    # Feature engineering
    enable_feature_selection: bool = False
    feature_selection_method: str = "variance"  # variance, mutual_info, recursive
    n_features_to_select: Optional[int] = None
    
    # Hyperparameter tuning
    enable_hyperparameter_tuning: bool = False
    tuning_method: str = "grid"  # grid, random, bayesian
    param_grid: Optional[Dict[str, List[Any]]] = None
    
    def __post_init__(self):
        self.type = AlgorithmType.MACHINE_LEARNING


@dataclass
class DataSplit:
    """Data split container"""
    X_train: X
    X_val: Optional[X]
    X_test: Optional[X]
    y_train: Optional[Y] = None
    y_val: Optional[Y] = None
    y_test: Optional[Y] = None
    
    # Additional metadata
    train_indices: Optional[np.ndarray] = None
    val_indices: Optional[np.ndarray] = None
    test_indices: Optional[np.ndarray] = None


@dataclass
class ModelMetrics:
    """Model performance metrics"""
    # Classification metrics
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    auc_roc: Optional[float] = None
    confusion_matrix: Optional[np.ndarray] = None
    
    # Regression metrics
    mse: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    
    # Clustering metrics
    silhouette_score: Optional[float] = None
    davies_bouldin_score: Optional[float] = None
    calinski_harabasz_score: Optional[float] = None
    
    # Custom metrics
    custom_metrics: Dict[str, float] = field(default_factory=dict)


@dataclass
class MLAlgorithmResult(AlgorithmResult[M]):
    """Result from ML algorithm execution"""
    model: Optional[M] = None
    
    # Performance metrics
    train_metrics: Optional[ModelMetrics] = None
    val_metrics: Optional[ModelMetrics] = None
    test_metrics: Optional[ModelMetrics] = None
    
    # Training history
    training_history: Dict[str, List[float]] = field(default_factory=dict)
    
    # Feature importance
    feature_importance: Optional[Dict[str, float]] = None
    selected_features: Optional[List[str]] = None
    
    # Model info
    model_size_bytes: Optional[int] = None
    training_time_seconds: Optional[float] = None
    inference_time_ms: Optional[float] = None
    
    # Hyperparameters
    best_params: Optional[Dict[str, Any]] = None
    cv_scores: Optional[List[float]] = None


class MLAlgorithm(BaseAlgorithm[Tuple[X, Optional[Y]], M], Generic[X, Y, M]):
    """
    Base class for machine learning algorithms.
    
    Provides:
    - Data preprocessing and splitting
    - Model training and evaluation
    - Cross-validation
    - Hyperparameter tuning
    - Feature selection
    - Model persistence
    """
    
    def __init__(self, config: MLAlgorithmConfig, **kwargs):
        super().__init__(config, **kwargs)
        self.config: MLAlgorithmConfig = config
        self._model: Optional[M] = None
        self._scaler = None
        self._feature_selector = None
        self._best_params = None
        
    async def _execute_algorithm(self, data: Tuple[X, Optional[Y]], **kwargs) -> M:
        """Execute ML algorithm"""
        X, y = data
        
        # Preprocess data
        X_processed, y_processed = await self._preprocess_data(X, y)
        
        # Split data
        data_split = await self._split_data(X_processed, y_processed)
        
        # Feature selection if enabled
        if self.config.enable_feature_selection:
            data_split = await self._select_features(data_split)
        
        # Hyperparameter tuning if enabled
        if self.config.enable_hyperparameter_tuning:
            self._best_params = await self._tune_hyperparameters(data_split)
            self.config.model_params.update(self._best_params)
        
        # Create model
        self._model = await self.create_model()
        
        # Train model
        if self.config.task != MLTask.CLUSTERING:
            await self._train_supervised(data_split)
        else:
            await self._train_unsupervised(data_split)
        
        # Evaluate model
        result = await self._evaluate_model(data_split)
        
        # Save model if configured
        if self.config.save_model and self.config.model_path:
            await self.save_model(self._model, self.config.model_path)
        
        return self._model
    
    async def _preprocess_data(self, X: X, y: Optional[Y]) -> Tuple[X, Optional[Y]]:
        """Preprocess data"""
        # Handle missing values
        X_processed = await self._handle_missing_values(X)
        
        # Normalize features if configured
        if self.config.normalize_features:
            X_processed, self._scaler = await self._normalize_features(X_processed)
        
        return X_processed, y
    
    async def _handle_missing_values(self, X: X) -> X:
        """Handle missing values based on strategy"""
        import pandas as pd
        import numpy as np
        
        if isinstance(X, pd.DataFrame):
            if self.config.handle_missing == "drop":
                return X.dropna()
            elif self.config.handle_missing == "mean":
                return X.fillna(X.mean())
            elif self.config.handle_missing == "median":
                return X.fillna(X.median())
            elif self.config.handle_missing == "mode":
                return X.fillna(X.mode().iloc[0])
            elif self.config.handle_missing == "forward_fill":
                return X.fillna(method='ffill')
            elif self.config.handle_missing == "backward_fill":
                return X.fillna(method='bfill')
            elif self.config.handle_missing == "interpolate":
                return X.interpolate()
            elif self.config.handle_missing == "zero":
                return X.fillna(0)
        elif isinstance(X, np.ndarray):
            if self.config.handle_missing == "drop":
                return X[~np.isnan(X).any(axis=1)]
            elif self.config.handle_missing == "mean":
                col_mean = np.nanmean(X, axis=0)
                inds = np.where(np.isnan(X))
                X[inds] = np.take(col_mean, inds[1])
                return X
            elif self.config.handle_missing == "median":
                col_median = np.nanmedian(X, axis=0)
                inds = np.where(np.isnan(X))
                X[inds] = np.take(col_median, inds[1])
                return X
            elif self.config.handle_missing == "zero":
                X[np.isnan(X)] = 0
                return X
        
        return X
    
    async def _normalize_features(self, X: X) -> Tuple[X, Any]:
        """Normalize features"""
        # Default implementation - override in subclass
        return X, None
    
    async def _split_data(self, X: X, y: Optional[Y]) -> DataSplit:
        """Split data into train/val/test sets"""
        n_samples = len(X) if hasattr(X, '__len__') else X.shape[0]
        
        # Calculate split sizes
        test_size = int(n_samples * self.config.test_split)
        val_size = int(n_samples * self.config.validation_split)
        train_size = n_samples - test_size - val_size
        
        # Generate indices
        indices = np.arange(n_samples)
        if self.config.shuffle_data:
            np.random.seed(self.config.random_state)
            np.random.shuffle(indices)
        
        # Split indices
        train_indices = indices[:train_size]
        val_indices = indices[train_size:train_size + val_size]
        test_indices = indices[train_size + val_size:]
        
        # Create splits
        split = DataSplit(
            X_train=self._index_data(X, train_indices),
            X_val=self._index_data(X, val_indices) if val_size > 0 else None,
            X_test=self._index_data(X, test_indices) if test_size > 0 else None,
            train_indices=train_indices,
            val_indices=val_indices if val_size > 0 else None,
            test_indices=test_indices if test_size > 0 else None
        )
        
        if y is not None:
            split.y_train = self._index_data(y, train_indices)
            split.y_val = self._index_data(y, val_indices) if val_size > 0 else None
            split.y_test = self._index_data(y, test_indices) if test_size > 0 else None
        
        return split
    
    def _index_data(self, data: Union[X, Y], indices: np.ndarray) -> Union[X, Y]:
        """Index data by indices - override for custom data types"""
        if hasattr(data, 'iloc'):
            return data.iloc[indices]
        elif hasattr(data, '__getitem__'):
            return data[indices]
        else:
            raise NotImplementedError("Data indexing not implemented for this data type")
    
    async def _select_features(self, data_split: DataSplit) -> DataSplit:
        """Select features based on importance"""
        # Default implementation - override in subclass
        return data_split
    
    async def _tune_hyperparameters(self, data_split: DataSplit) -> Dict[str, Any]:
        """Tune hyperparameters"""
        logger.info(f"Starting hyperparameter tuning with {self.config.tuning_method} method")
        
        if self.config.tuning_method == "grid":
            return await self._grid_search(data_split)
        elif self.config.tuning_method == "random":
            return await self._random_search(data_split)
        elif self.config.tuning_method == "bayesian":
            return await self._bayesian_optimization(data_split)
        else:
            return {}
    
    async def _grid_search(self, data_split: DataSplit) -> Dict[str, Any]:
        """Grid search for hyperparameters"""
        # Default implementation - override in subclass
        return {}
    
    async def _random_search(self, data_split: DataSplit) -> Dict[str, Any]:
        """Random search for hyperparameters"""
        # Default implementation - override in subclass
        return {}
    
    async def _bayesian_optimization(self, data_split: DataSplit) -> Dict[str, Any]:
        """Bayesian optimization for hyperparameters"""
        # Default implementation - override in subclass
        return {}
    
    async def _train_supervised(self, data_split: DataSplit):
        """Train supervised learning model"""
        import time
        start_time = time.time()
        
        # Training loop
        history = {
            'train_loss': [],
            'val_loss': [],
            'train_metric': [],
            'val_metric': []
        }
        
        best_val_metric = None
        patience_counter = 0
        
        for epoch in range(self.config.epochs):
            # Train epoch
            train_loss = await self.train_epoch(
                data_split.X_train,
                data_split.y_train,
                epoch
            )
            
            # Evaluate on training set
            train_metrics = await self.evaluate(
                data_split.X_train,
                data_split.y_train
            )
            
            # Evaluate on validation set
            val_loss = None
            val_metrics = None
            if data_split.X_val is not None:
                val_loss = await self.evaluate_loss(
                    data_split.X_val,
                    data_split.y_val
                )
                val_metrics = await self.evaluate(
                    data_split.X_val,
                    data_split.y_val
                )
            
            # Update history
            history['train_loss'].append(train_loss)
            if val_loss is not None:
                history['val_loss'].append(val_loss)
            
            # Early stopping check
            if self.config.enable_early_stopping and val_metrics:
                metric_value = self._get_metric_value(
                    val_metrics,
                    self.config.early_stopping_metric or self.config.scoring_metric
                )
                
                if best_val_metric is None or self._is_better_metric(metric_value, best_val_metric):
                    best_val_metric = metric_value
                    patience_counter = 0
                    # Save best model
                    self._best_model_state = await self.get_model_state()
                else:
                    patience_counter += 1
                    
                if patience_counter >= self.config.early_stopping_patience:
                    logger.info(f"Early stopping triggered at epoch {epoch}")
                    break
            
            # Log progress
            if epoch % 10 == 0:
                logger.info(f"Epoch {epoch}: train_loss={train_loss:.4f}")
        
        # Restore best model if early stopping was used
        if self.config.enable_early_stopping and hasattr(self, '_best_model_state'):
            await self.set_model_state(self._best_model_state)
        
        self._training_time = time.time() - start_time
    
    async def _train_unsupervised(self, data_split: DataSplit):
        """Train unsupervised learning model"""
        import time
        start_time = time.time()
        
        # Fit model
        await self.fit(data_split.X_train)
        
        self._training_time = time.time() - start_time
    
    async def _evaluate_model(self, data_split: DataSplit) -> MLAlgorithmResult[M]:
        """Evaluate model performance"""
        result = MLAlgorithmResult[M](
            algorithm_name=self.config.name,
            status=AlgorithmStatus.COMPLETED,
            model=self._model
        )
        
        # Evaluate on each split
        if data_split.y_train is not None:
            result.train_metrics = await self.evaluate(
                data_split.X_train,
                data_split.y_train
            )
        
        if data_split.X_val is not None and data_split.y_val is not None:
            result.val_metrics = await self.evaluate(
                data_split.X_val,
                data_split.y_val
            )
        
        if data_split.X_test is not None and data_split.y_test is not None:
            result.test_metrics = await self.evaluate(
                data_split.X_test,
                data_split.y_test
            )
        
        # Add additional info
        result.training_time_seconds = getattr(self, '_training_time', None)
        result.best_params = self._best_params
        
        # Measure inference time
        if data_split.X_test is not None:
            import time
            start = time.time()
            await self.predict(data_split.X_test[:100])  # Predict on subset
            result.inference_time_ms = (time.time() - start) * 1000 / 100
        
        return result
    
    def _get_metric_value(self, metrics: ModelMetrics, metric_name: str) -> float:
        """Get specific metric value"""
        if hasattr(metrics, metric_name):
            return getattr(metrics, metric_name)
        elif metric_name in metrics.custom_metrics:
            return metrics.custom_metrics[metric_name]
        else:
            # Default to first available metric
            for attr in ['accuracy', 'f1_score', 'auc_roc', 'r2_score']:
                if hasattr(metrics, attr) and getattr(metrics, attr) is not None:
                    return getattr(metrics, attr)
            return 0.0
    
    def _is_better_metric(self, new_value: float, old_value: float) -> bool:
        """Check if new metric value is better"""
        # For most metrics, higher is better
        # Override for metrics where lower is better (e.g., loss, error)
        return new_value > old_value
    
    # Abstract methods to be implemented by subclasses
    
    async def create_model(self) -> M:
        """
        Create the ML model.
        
        Returns:
            Model instance
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement create_model method"
        )
    
    async def train_epoch(self, X: X, y: Y, epoch: int) -> float:
        """
        Train one epoch.
        
        Args:
            X: Features
            y: Labels
            epoch: Current epoch number
            
        Returns:
            Loss value
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement train_epoch method"
        )
    
    async def predict_batch(self, X: X) -> Y:
        """
        Make predictions on a batch.
        
        Args:
            X: Features
            
        Returns:
            Predictions
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement predict_batch method"
        )
    
    async def evaluate_metrics(self, y_true: Y, y_pred: Y) -> ModelMetrics:
        """
        Evaluate model metrics.
        
        Args:
            y_true: True labels
            y_pred: Predictions
            
        Returns:
            Model metrics
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement evaluate_metrics method"
        )
    
    async def save_model(self, path: str) -> None:
        """
        Save model to disk.
        
        Args:
            path: Save path
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement save_model method"
        )
    
    async def load_model(self, path: str) -> M:
        """
        Load model from disk.
        
        Args:
            path: Load path
            
        Returns:
            Loaded model
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement load_model method"
        )
    
    def _prepare_features(self, X: X) -> X:
        """Prepare features for model"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _prepare_features method"
        )
    
    def _prepare_labels(self, y: Y) -> Y:
        """Prepare labels for model"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _prepare_labels method"
        )
    
    def _get_model_size(self) -> int:
        """Get model size in bytes"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_model_size method"
        )
    
    def _get_feature_importance(self) -> Optional[Dict[str, float]]:
        """Get feature importance scores"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_feature_importance method"
        )
    
    # Cross-validation support
    
    async def cross_validate(self, X: X, y: Y) -> List[float]:
        """Perform cross-validation"""
        from sklearn.model_selection import KFold
        
        scores = []
        kf = KFold(n_splits=self.config.cv_folds, shuffle=True, random_state=self.config.random_state)
        
        for fold, (train_idx, val_idx) in enumerate(kf.split(X)):
            logger.info(f"Cross-validation fold {fold + 1}/{self.config.cv_folds}")
            
            # Create fold data
            X_train_fold = self._index_data(X, train_idx)
            X_val_fold = self._index_data(X, val_idx)
            y_train_fold = self._index_data(y, train_idx)
            y_val_fold = self._index_data(y, val_idx)
            
            # Create and train model
            fold_model = await self.create_model()
            self._model = fold_model
            
            # Train on fold
            await self.fit(X_train_fold, y_train_fold)
            
            # Evaluate on fold
            metrics = await self.evaluate(X_val_fold, y_val_fold)
            score = self._get_metric_value(
                metrics,
                self.config.scoring_metric or 'accuracy'
            )
            scores.append(score)
        
        return scores


__all__ = [
    "MLAlgorithm",
    "MLAlgorithmConfig",
    "MLAlgorithmResult",
    "MLTask",
    "ModelFramework",
    "DataSplitStrategy",
    "DataSplit",
    "ModelMetrics"
] 