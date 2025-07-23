"""
ML training components and utilities.

Provides training orchestration, distributed training, and hyperparameter optimization.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from pathlib import Path
import optuna
from optuna.samplers import TPESampler
import mlflow

from .base_model import BaseMLModel, ModelStatus
from ..caching import CacheManager
from ...monitoring import MetricsCollector, StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class TrainingStatus(str, Enum):
    """Training job status"""
    PENDING = "pending"
    PREPARING = "preparing"
    TRAINING = "training"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TrainingConfig:
    """Training configuration"""
    batch_size: int = 32
    epochs: int = 100
    learning_rate: float = 0.001
    validation_split: float = 0.2
    early_stopping_patience: int = 10
    checkpoint_interval: int = 5
    distributed: bool = False
    num_workers: int = 1
    device: str = "cpu"  # cpu, cuda, tpu
    mixed_precision: bool = False
    gradient_accumulation_steps: int = 1
    max_grad_norm: float = 1.0
    seed: int = 42
    experiment_name: Optional[str] = None
    run_name: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    callbacks: List[str] = field(default_factory=list)
    custom_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TrainingResult:
    """Training result"""
    job_id: str
    status: TrainingStatus
    model: Optional[BaseMLModel] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    history: Dict[str, List[float]] = field(default_factory=dict)
    best_epoch: Optional[int] = None
    total_epochs: int = 0
    training_time: Optional[timedelta] = None
    validation_metrics: Dict[str, float] = field(default_factory=dict)
    test_metrics: Dict[str, float] = field(default_factory=dict)
    artifacts: Dict[str, str] = field(default_factory=dict)
    error: Optional[str] = None
    

class BaseTrainer(ABC):
    """
    Base trainer for ML models.
    
    Provides:
    - Training orchestration
    - Metric tracking
    - Checkpointing
    - Early stopping
    - MLflow integration
    """
    
    def __init__(
        self,
        model: BaseMLModel,
        config: TrainingConfig,
        cache_manager: Optional[CacheManager] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.model = model
        self.config = config
        self.cache = cache_manager
        self.metrics = metrics_collector or MetricsCollector()
        
        self.job_id = self._generate_job_id()
        self.result = TrainingResult(
            job_id=self.job_id,
            status=TrainingStatus.PENDING
        )
        
        # Training state
        self.current_epoch = 0
        self.best_metric = None
        self.patience_counter = 0
        self.start_time = None
        
        # MLflow
        self.mlflow_run = None
        
    def _generate_job_id(self) -> str:
        """Generate unique job ID"""
        import uuid
        return f"train_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
        
    async def train(
        self,
        X_train: Union[np.ndarray, pd.DataFrame],
        y_train: Union[np.ndarray, pd.Series],
        X_val: Optional[Union[np.ndarray, pd.DataFrame]] = None,
        y_val: Optional[Union[np.ndarray, pd.Series]] = None,
        **kwargs
    ) -> TrainingResult:
        """Train the model"""
        logger.info(f"Starting training job {self.job_id}")
        self.start_time = datetime.utcnow()
        self.result.status = TrainingStatus.PREPARING
        
        try:
            # Setup MLflow
            if self.config.experiment_name:
                mlflow.set_experiment(self.config.experiment_name)
                
            with mlflow.start_run(run_name=self.config.run_name) as run:
                self.mlflow_run = run
                
                # Log parameters
                mlflow.log_params(self._get_params_to_log())
                
                # Log tags
                for key, value in self.config.tags.items():
                    mlflow.set_tag(key, value)
                    
                # Prepare data
                X_train, y_train, X_val, y_val = await self._prepare_data(
                    X_train, y_train, X_val, y_val
                )
                
                # Initialize training
                await self._initialize_training()
                
                # Training loop
                self.result.status = TrainingStatus.TRAINING
                await self._training_loop(X_train, y_train, X_val, y_val)
                
                # Validation
                if X_val is not None:
                    self.result.status = TrainingStatus.VALIDATING
                    val_metrics = await self._validate(X_val, y_val)
                    self.result.validation_metrics = val_metrics
                    
                # Finalize
                await self._finalize_training()
                
                # Save model
                model_path = await self._save_model()
                self.result.artifacts["model"] = model_path
                
                # Log model to MLflow
                mlflow.sklearn.log_model(self.model.model, "model")
                
                self.result.status = TrainingStatus.COMPLETED
                self.result.model = self.model
                self.result.training_time = datetime.utcnow() - self.start_time
                
                logger.info(f"Training job {self.job_id} completed successfully")
                
        except Exception as e:
            self.result.status = TrainingStatus.FAILED
            self.result.error = str(e)
            logger.error(f"Training job {self.job_id} failed: {e}")
            raise
            
        return self.result
        
    @abstractmethod
    async def _prepare_data(
        self,
        X_train: Union[np.ndarray, pd.DataFrame],
        y_train: Union[np.ndarray, pd.Series],
        X_val: Optional[Union[np.ndarray, pd.DataFrame]],
        y_val: Optional[Union[np.ndarray, pd.Series]]
    ) -> Tuple:
        """Prepare training data"""
        pass
        
    @abstractmethod
    async def _initialize_training(self):
        """Initialize training process"""
        pass
        
    @abstractmethod
    async def _training_loop(
        self,
        X_train: Union[np.ndarray, pd.DataFrame],
        y_train: Union[np.ndarray, pd.Series],
        X_val: Optional[Union[np.ndarray, pd.DataFrame]],
        y_val: Optional[Union[np.ndarray, pd.Series]]
    ):
        """Main training loop"""
        pass
        
    @abstractmethod
    async def _validate(
        self,
        X_val: Union[np.ndarray, pd.DataFrame],
        y_val: Union[np.ndarray, pd.Series]
    ) -> Dict[str, float]:
        """Validate model"""
        pass
        
    async def _finalize_training(self):
        """Finalize training process"""
        # Update model metadata
        self.model.update_metadata(
            status=ModelStatus.TRAINED,
            metrics=self.result.metrics
        )
        
    async def _save_model(self) -> str:
        """Save trained model"""
        model_path = f"/tmp/models/{self.job_id}"
        self.model.save(model_path)
        return model_path
        
    def _get_params_to_log(self) -> Dict[str, Any]:
        """Get parameters to log to MLflow"""
        params = {
            "batch_size": self.config.batch_size,
            "epochs": self.config.epochs,
            "learning_rate": self.config.learning_rate,
            "validation_split": self.config.validation_split,
            "device": self.config.device
        }
        params.update(self.model.get_params())
        return params
        
    async def _check_early_stopping(self, metric: float, mode: str = "min") -> bool:
        """Check early stopping condition"""
        if self.best_metric is None:
            self.best_metric = metric
            self.patience_counter = 0
            return False
            
        improved = (metric < self.best_metric) if mode == "min" else (metric > self.best_metric)
        
        if improved:
            self.best_metric = metric
            self.patience_counter = 0
            return False
        else:
            self.patience_counter += 1
            return self.patience_counter >= self.config.early_stopping_patience
            
    async def _save_checkpoint(self, epoch: int):
        """Save training checkpoint"""
        if self.cache and epoch % self.config.checkpoint_interval == 0:
            checkpoint = {
                "epoch": epoch,
                "model_state": self.model.model,
                "metrics": self.result.metrics,
                "history": self.result.history,
                "best_metric": self.best_metric
            }
            
            cache_key = f"training:checkpoint:{self.job_id}:epoch_{epoch}"
            await self.cache.set(cache_key, checkpoint, ttl=86400)  # 24 hours
            
            logger.debug(f"Saved checkpoint for epoch {epoch}")
            
    def log_metric(self, name: str, value: float, step: Optional[int] = None):
        """Log metric"""
        # Log to result
        self.result.metrics[name] = value
        
        # Add to history
        if name not in self.result.history:
            self.result.history[name] = []
        self.result.history[name].append(value)
        
        # Log to MLflow
        if self.mlflow_run:
            mlflow.log_metric(name, value, step=step)
            
        # Log to metrics collector
        self.metrics.gauge(
            f"training_{name}",
            value,
            labels={"job_id": self.job_id}
        )


class HyperparameterTuner:
    """
    Hyperparameter optimization using Optuna.
    
    Features:
    - Bayesian optimization
    - Parallel trials
    - Early stopping
    - Best model tracking
    """
    
    def __init__(
        self,
        objective_func: Callable,
        search_space: Dict[str, Any],
        n_trials: int = 100,
        direction: str = "minimize",
        n_jobs: int = 1,
        study_name: Optional[str] = None
    ):
        self.objective_func = objective_func
        self.search_space = search_space
        self.n_trials = n_trials
        self.direction = direction
        self.n_jobs = n_jobs
        self.study_name = study_name or f"study_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        self.study = None
        self.best_params = None
        self.best_value = None
        
    async def optimize(self, **kwargs) -> Dict[str, Any]:
        """Run hyperparameter optimization"""
        logger.info(f"Starting hyperparameter optimization: {self.study_name}")
        
        # Create study
        self.study = optuna.create_study(
            study_name=self.study_name,
            direction=self.direction,
            sampler=TPESampler(seed=42)
        )
        
        # Define objective wrapper
        def objective(trial):
            # Sample hyperparameters
            params = self._sample_params(trial)
            
            # Run objective function
            return asyncio.run(self.objective_func(params, trial, **kwargs))
            
        # Run optimization
        self.study.optimize(
            objective,
            n_trials=self.n_trials,
            n_jobs=self.n_jobs,
            show_progress_bar=True
        )
        
        # Get best results
        self.best_params = self.study.best_params
        self.best_value = self.study.best_value
        
        logger.info(f"Optimization completed. Best value: {self.best_value}")
        logger.info(f"Best params: {self.best_params}")
        
        return {
            "best_params": self.best_params,
            "best_value": self.best_value,
            "n_trials": len(self.study.trials),
            "study_name": self.study_name
        }
        
    def _sample_params(self, trial) -> Dict[str, Any]:
        """Sample hyperparameters from search space"""
        params = {}
        
        for param_name, param_config in self.search_space.items():
            param_type = param_config["type"]
            
            if param_type == "int":
                params[param_name] = trial.suggest_int(
                    param_name,
                    param_config["low"],
                    param_config["high"],
                    step=param_config.get("step", 1)
                )
            elif param_type == "float":
                if param_config.get("log", False):
                    params[param_name] = trial.suggest_loguniform(
                        param_name,
                        param_config["low"],
                        param_config["high"]
                    )
                else:
                    params[param_name] = trial.suggest_float(
                        param_name,
                        param_config["low"],
                        param_config["high"]
                    )
            elif param_type == "categorical":
                params[param_name] = trial.suggest_categorical(
                    param_name,
                    param_config["choices"]
                )
                
        return params
        
    def get_importance(self) -> Dict[str, float]:
        """Get parameter importance"""
        if not self.study:
            return {}
            
        importance = optuna.importance.get_param_importances(self.study)
        return dict(importance)
        
    def visualize(self, plot_type: str = "optimization_history") -> None:
        """Visualize optimization results"""
        if not self.study:
            return
            
        if plot_type == "optimization_history":
            optuna.visualization.plot_optimization_history(self.study).show()
        elif plot_type == "param_importances":
            optuna.visualization.plot_param_importances(self.study).show()
        elif plot_type == "parallel_coordinate":
            optuna.visualization.plot_parallel_coordinate(self.study).show()


class DistributedTrainer:
    """
    Distributed training coordinator.
    
    Supports:
    - Data parallel training
    - Model parallel training
    - Horovod integration
    - Multi-node training
    """
    
    def __init__(
        self,
        trainer: BaseTrainer,
        backend: str = "nccl",  # nccl, gloo, mpi
        world_size: int = 1,
        rank: int = 0
    ):
        self.trainer = trainer
        self.backend = backend
        self.world_size = world_size
        self.rank = rank
        
    async def train(self, *args, **kwargs) -> TrainingResult:
        """Run distributed training"""
        if self.world_size == 1:
            # Single node training
            return await self.trainer.train(*args, **kwargs)
            
        # Initialize distributed backend
        await self._init_distributed()
        
        try:
            # Distributed training
            result = await self._distributed_train(*args, **kwargs)
            
            # Gather results from all nodes
            if self.rank == 0:
                all_results = await self._gather_results(result)
                return self._aggregate_results(all_results)
            else:
                await self._send_results(result)
                return result
                
        finally:
            await self._cleanup_distributed()
            
    async def _init_distributed(self):
        """Initialize distributed training backend"""
        logger.info(f"Initializing distributed training: backend={self.backend}, world_size={self.world_size}, rank={self.rank}")
        # Implementation depends on backend
        pass
        
    async def _distributed_train(self, *args, **kwargs) -> TrainingResult:
        """Run training on this node"""
        # Shard data based on rank
        # Run training
        return await self.trainer.train(*args, **kwargs)
        
    async def _gather_results(self, result: TrainingResult) -> List[TrainingResult]:
        """Gather results from all nodes (rank 0 only)"""
        # Implementation depends on backend
        return [result]
        
    async def _send_results(self, result: TrainingResult):
        """Send results to rank 0"""
        # Implementation depends on backend
        pass
        
    def _aggregate_results(self, results: List[TrainingResult]) -> TrainingResult:
        """Aggregate results from all nodes"""
        # Average metrics
        aggregated = results[0]
        
        for metric_name in aggregated.metrics:
            values = [r.metrics.get(metric_name, 0) for r in results]
            aggregated.metrics[metric_name] = np.mean(values)
            
        return aggregated
        
    async def _cleanup_distributed(self):
        """Cleanup distributed resources"""
        logger.info("Cleaning up distributed training")
        # Implementation depends on backend
        pass 