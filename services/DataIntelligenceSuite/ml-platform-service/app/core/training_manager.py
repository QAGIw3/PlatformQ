"""
Training Manager for ML Platform
"""
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from uuid import UUID
import asyncio
import json
from collections import deque

from ..domain.models.training import (
    TrainingJob, TrainingStatus, TrainingConfig, DatasetConfig,
    TrainingMetrics, Framework, DistributedStrategy,
    HyperparameterTuning, ExperimentRun
)
from ..domain.models.model import ModelFormat
from .model_registry import ModelRegistryManager
from ..infrastructure.spark import SparkClient
from ..infrastructure.minio import MinIOClient

logger = logging.getLogger(__name__)


class TrainingManager:
    """
    Manages ML training job orchestration
    """
    
    def __init__(self,
                 model_registry: ModelRegistryManager,
                 spark_client: SparkClient,
                 minio_client: MinIOClient,
                 max_jobs: int = 10,
                 default_timeout: int = 3600,
                 checkpoint_interval: int = 300):
        self.model_registry = model_registry
        self.spark = spark_client
        self.minio = minio_client
        self.max_jobs = max_jobs
        self.default_timeout = default_timeout
        self.checkpoint_interval = checkpoint_interval
        
        # Job tracking
        self.active_jobs: Dict[UUID, TrainingJob] = {}
        self.job_queue: deque = deque()
        self.job_futures: Dict[UUID, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize training manager"""
        logger.info("Initializing training manager")
        
        # Create training artifacts bucket
        await self.minio.create_bucket("training-artifacts")
        
        # Start job processor
        asyncio.create_task(self._process_job_queue())
        
        logger.info("Training manager initialized")
    
    async def submit_job(self,
                        name: str,
                        experiment_id: str,
                        user_id: str,
                        project_id: str,
                        training_config: TrainingConfig,
                        dataset_config: DatasetConfig,
                        description: Optional[str] = None,
                        tags: Optional[Dict[str, str]] = None) -> TrainingJob:
        """Submit a new training job"""
        # Create job
        job = TrainingJob(
            name=name,
            description=description,
            experiment_id=experiment_id,
            user_id=user_id,
            project_id=project_id,
            training_config=training_config,
            dataset_config=dataset_config,
            tags=tags or {}
        )
        
        # Add to queue
        self.job_queue.append(job)
        logger.info(f"Training job submitted: {job.id} - {job.name}")
        
        return job
    
    async def _process_job_queue(self):
        """Process queued training jobs"""
        while True:
            try:
                # Check if we can run more jobs
                if len(self.active_jobs) >= self.max_jobs or not self.job_queue:
                    await asyncio.sleep(5)
                    continue
                
                # Get next job
                job = self.job_queue.popleft()
                
                # Start job
                task = asyncio.create_task(self._run_training_job(job))
                self.job_futures[job.id] = task
                self.active_jobs[job.id] = job
                
            except Exception as e:
                logger.error(f"Error processing job queue: {str(e)}")
                await asyncio.sleep(5)
    
    async def _run_training_job(self, job: TrainingJob):
        """Run a training job"""
        try:
            logger.info(f"Starting training job: {job.id}")
            job.status = TrainingStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            # Load dataset
            train_df = await self.spark.read_data(
                job.dataset_config.train_path,
                format=job.dataset_config.data_format
            )
            
            # Apply sampling if needed
            if job.dataset_config.sample_fraction < 1.0:
                train_df = train_df.sample(
                    fraction=job.dataset_config.sample_fraction,
                    seed=42
                )
            
            # Train based on framework
            if job.training_config.framework == Framework.SKLEARN:
                model = await self._train_sklearn(job, train_df)
            elif job.training_config.framework in [Framework.XGBOOST, Framework.LIGHTGBM]:
                model = await self._train_spark_ml(job, train_df)
            elif job.training_config.framework == Framework.PYTORCH:
                model = await self._train_pytorch(job, train_df)
            elif job.training_config.framework == Framework.TENSORFLOW:
                model = await self._train_tensorflow(job, train_df)
            else:
                raise ValueError(f"Unsupported framework: {job.training_config.framework}")
            
            # Save model
            model_path = f"/tmp/model_{job.id}"
            if hasattr(model, 'save'):
                model.save(model_path)
            else:
                import joblib
                joblib.dump(model, f"{model_path}.pkl")
                model_path = f"{model_path}.pkl"
            
            # Upload to MinIO
            model_key = f"models/{job.name}/{job.id}"
            model_uri = await self.minio.upload_file(
                "training-artifacts",
                model_key,
                model_path
            )
            
            # Register model
            registered_model = await self.model_registry.register_model(
                name=job.name,
                model_path=model_path,
                framework=job.training_config.framework.value,
                model_format=ModelFormat.JOBLIB if job.training_config.framework == Framework.SKLEARN else ModelFormat.CUSTOM,
                training_job_id=job.id,
                experiment_id=job.experiment_id,
                metrics=job.metrics,
                parameters=job.training_config.hyperparameters,
                tags=job.tags,
                created_by=job.user_id
            )
            
            # Update job
            job.status = TrainingStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.model_uri = model_uri
            job.model_version = registered_model.version
            
            logger.info(f"Training job completed: {job.id}")
            
        except Exception as e:
            logger.error(f"Training job failed: {job.id} - {str(e)}")
            job.status = TrainingStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            
        finally:
            # Clean up
            self.active_jobs.pop(job.id, None)
            self.job_futures.pop(job.id, None)
    
    async def _train_spark_ml(self, job: TrainingJob, train_df) -> Any:
        """Train using Spark ML"""
        # Determine model type
        model_type = "random_forest_classifier"  # Default
        if "model_type" in job.training_config.hyperparameters:
            model_type = job.training_config.hyperparameters.pop("model_type")
        
        # Train model
        model = await self.spark.train_model(
            train_df=train_df,
            features_col=job.dataset_config.features[0] if job.dataset_config.features else "features",
            label_col=job.dataset_config.target or "label",
            model_type=model_type,
            hyperparameters=job.training_config.hyperparameters
        )
        
        # Evaluate if test data provided
        if job.dataset_config.test_path:
            test_df = await self.spark.read_data(job.dataset_config.test_path)
            metrics = await self.spark.evaluate_model(
                model=model,
                test_df=test_df,
                label_col=job.dataset_config.target or "label",
                is_classification="classifier" in model_type
            )
            job.metrics.update(metrics)
        
        return model
    
    async def _train_sklearn(self, job: TrainingJob, train_df) -> Any:
        """Train using scikit-learn"""
        # Convert Spark DataFrame to Pandas
        loop = asyncio.get_event_loop()
        
        def _train():
            import pandas as pd
            from sklearn.model_selection import train_test_split
            from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
            from sklearn.linear_model import LogisticRegression, LinearRegression
            from sklearn.metrics import accuracy_score, mean_squared_error
            
            # Convert to pandas
            pdf = train_df.toPandas()
            
            # Prepare data
            X = pdf[job.dataset_config.features] if job.dataset_config.features else pdf.drop(columns=[job.dataset_config.target])
            y = pdf[job.dataset_config.target]
            
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Select model
            model_class = job.training_config.hyperparameters.pop("model_class", "RandomForestClassifier")
            model_classes = {
                "RandomForestClassifier": RandomForestClassifier,
                "RandomForestRegressor": RandomForestRegressor,
                "LogisticRegression": LogisticRegression,
                "LinearRegression": LinearRegression
            }
            
            ModelClass = model_classes.get(model_class, RandomForestClassifier)
            model = ModelClass(**job.training_config.hyperparameters)
            
            # Train
            model.fit(X_train, y_train)
            
            # Evaluate
            predictions = model.predict(X_test)
            if hasattr(model, "predict_proba"):
                job.metrics["accuracy"] = accuracy_score(y_test, predictions)
            else:
                job.metrics["mse"] = mean_squared_error(y_test, predictions)
            
            return model
        
        return await loop.run_in_executor(None, _train)
    
    async def _train_pytorch(self, job: TrainingJob, train_df) -> Any:
        """Train using PyTorch"""
        # Placeholder for PyTorch training
        raise NotImplementedError("PyTorch training not yet implemented")
    
    async def _train_tensorflow(self, job: TrainingJob, train_df) -> Any:
        """Train using TensorFlow"""
        # Placeholder for TensorFlow training
        raise NotImplementedError("TensorFlow training not yet implemented")
    
    async def get_job_status(self, job_id: UUID) -> Optional[TrainingJob]:
        """Get training job status"""
        # Check active jobs
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        
        # Check queue
        for job in self.job_queue:
            if job.id == job_id:
                return job
        
        # TODO: Check completed jobs in storage
        return None
    
    async def cancel_job(self, job_id: UUID) -> bool:
        """Cancel a training job"""
        # Check if job is in queue
        for i, job in enumerate(self.job_queue):
            if job.id == job_id:
                self.job_queue.remove(job)
                job.status = TrainingStatus.CANCELLED
                logger.info(f"Cancelled queued job: {job_id}")
                return True
        
        # Check if job is running
        if job_id in self.job_futures:
            self.job_futures[job_id].cancel()
            if job_id in self.active_jobs:
                self.active_jobs[job_id].status = TrainingStatus.CANCELLED
            logger.info(f"Cancelled running job: {job_id}")
            return True
        
        return False
    
    async def list_jobs(self,
                       user_id: Optional[str] = None,
                       project_id: Optional[str] = None,
                       status: Optional[TrainingStatus] = None,
                       limit: int = 100) -> List[TrainingJob]:
        """List training jobs"""
        jobs = []
        
        # Add active jobs
        for job in self.active_jobs.values():
            if user_id and job.user_id != user_id:
                continue
            if project_id and job.project_id != project_id:
                continue
            if status and job.status != status:
                continue
            jobs.append(job)
        
        # Add queued jobs
        for job in self.job_queue:
            if user_id and job.user_id != user_id:
                continue
            if project_id and job.project_id != project_id:
                continue
            if status and job.status != status:
                continue
            jobs.append(job)
        
        # TODO: Add completed jobs from storage
        
        return jobs[:limit]
    
    async def get_job_metrics(self, job_id: UUID) -> List[TrainingMetrics]:
        """Get training metrics for a job"""
        # TODO: Implement metrics tracking during training
        return []
    
    async def hyperparameter_tuning(self,
                                   name: str,
                                   experiment_id: str,
                                   user_id: str,
                                   project_id: str,
                                   base_config: TrainingConfig,
                                   dataset_config: DatasetConfig,
                                   tuning_config: HyperparameterTuning) -> List[TrainingJob]:
        """Run hyperparameter tuning"""
        # TODO: Implement using Optuna or similar
        raise NotImplementedError("Hyperparameter tuning not yet implemented")
    
    async def shutdown(self):
        """Shutdown training manager"""
        logger.info("Shutting down training manager")
        
        # Cancel all running jobs
        for task in self.job_futures.values():
            task.cancel()
        
        # Wait for tasks to complete
        if self.job_futures:
            await asyncio.gather(*self.job_futures.values(), return_exceptions=True) 