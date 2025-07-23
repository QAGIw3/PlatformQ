"""
AutoML Engine

Automates model selection, hyperparameter tuning, and feature engineering.
"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from enum import Enum
import uuid

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class AutoMLStatus(Enum):
    """AutoML job status"""
    INITIALIZING = "initializing"
    ANALYZING = "analyzing"
    SEARCHING = "searching"
    EVALUATING = "evaluating"
    COMPLETED = "completed"
    FAILED = "failed"


class ProblemType(Enum):
    """ML problem types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    TIME_SERIES = "time_series"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"


class AutoMLEngine:
    """
    Automates machine learning workflow
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 model_search: Any, hyperparameter_tuner: Any, feature_engineer: Any,
                 training_orchestrator: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.model_search = model_search
        self.hyperparameter_tuner = hyperparameter_tuner
        self.feature_engineer = feature_engineer
        self.training_orchestrator = training_orchestrator
        
        # AutoML jobs
        self.jobs: Dict[str, Dict[str, Any]] = {}
        
        # Configuration
        self.config = {
            "search": {
                "max_trials": 100,
                "time_limit_minutes": 60,
                "parallel_trials": 5,
                "early_stopping_rounds": 10
            },
            "models": {
                "classification": [
                    "logistic_regression",
                    "random_forest",
                    "xgboost",
                    "lightgbm",
                    "neural_network",
                    "svm"
                ],
                "regression": [
                    "linear_regression",
                    "random_forest",
                    "xgboost",
                    "lightgbm",
                    "neural_network",
                    "elastic_net"
                ],
                "time_series": [
                    "arima",
                    "prophet",
                    "lstm",
                    "xgboost",
                    "exponential_smoothing"
                ]
            },
            "feature_engineering": {
                "auto_generate": True,
                "max_features": 100,
                "selection_method": "mutual_info",
                "handle_missing": "auto",
                "encode_categorical": "auto"
            },
            "evaluation": {
                "cv_folds": 5,
                "test_size": 0.2,
                "stratify": True,
                "metrics": {
                    "classification": ["accuracy", "f1", "auc_roc", "precision", "recall"],
                    "regression": ["rmse", "mae", "r2", "mape"]
                }
            }
        }
        
        # Metrics
        self.metrics = {
            "jobs_submitted": 0,
            "jobs_completed": 0,
            "models_evaluated": 0,
            "best_model_score": 0,
            "avg_job_time": 0
        }
    
    async def initialize(self):
        """Initialize AutoML engine"""
        logger.info("initializing_automl_engine")
        
        # Load configuration
        await self._load_configuration()
        
        # Initialize components
        await self.model_search.initialize()
        await self.hyperparameter_tuner.initialize()
        await self.feature_engineer.initialize()
        
        # Start background tasks
        asyncio.create_task(self._monitor_jobs())
        
        logger.info("automl_engine_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Cancel all active jobs
        for job_id in list(self.jobs.keys()):
            if self.jobs[job_id]["status"] not in [AutoMLStatus.COMPLETED, AutoMLStatus.FAILED]:
                await self.cancel_job(job_id)
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/automl-engine")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def start_automl(self, job_config: Dict[str, Any]) -> str:
        """
        Start an AutoML job
        
        Args:
            job_config: AutoML job configuration including:
                - name: Job name
                - dataset: Dataset configuration
                - problem_type: Type of ML problem
                - target_column: Target variable name
                - time_limit: Time limit in minutes
                - optimization_metric: Metric to optimize
                - constraints: Model constraints
                
        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())
        
        # Validate configuration
        self._validate_job_config(job_config)
        
        # Detect problem type if not specified
        if "problem_type" not in job_config:
            job_config["problem_type"] = await self._detect_problem_type(job_config)
        
        # Create job
        job = {
            "id": job_id,
            "config": job_config,
            "status": AutoMLStatus.INITIALIZING,
            "started_at": datetime.utcnow(),
            "completed_at": None,
            "problem_type": ProblemType(job_config["problem_type"]),
            "trials": [],
            "best_model": None,
            "best_score": float('-inf') if self._is_maximize_metric(job_config) else float('inf'),
            "feature_importance": {},
            "leaderboard": []
        }
        
        # Store job
        self.jobs[job_id] = job
        
        # Update metrics
        self.metrics["jobs_submitted"] += 1
        
        # Start AutoML process
        asyncio.create_task(self._run_automl(job_id))
        
        # Emit event
        await self.event_bus.publish(
            "automl.job.started",
            {
                "job_id": job_id,
                "name": job_config.get("name"),
                "problem_type": job_config["problem_type"],
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"AutoML job started: {job_id}")
        return job_id
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get AutoML job status"""
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        return {
            "id": job_id,
            "status": job["status"].value,
            "problem_type": job["problem_type"].value,
            "trials_completed": len(job["trials"]),
            "best_model": job["best_model"],
            "best_score": job["best_score"],
            "leaderboard": job["leaderboard"][:10],  # Top 10 models
            "started_at": job["started_at"].isoformat(),
            "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None
        }
    
    async def get_best_model(self, job_id: str) -> Dict[str, Any]:
        """Get the best model from AutoML job"""
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        if job["status"] != AutoMLStatus.COMPLETED:
            raise RuntimeError(f"Job not completed: {job['status'].value}")
        
        if not job["best_model"]:
            raise ValueError("No best model found")
        
        return {
            "model_info": job["best_model"],
            "score": job["best_score"],
            "feature_importance": job["feature_importance"],
            "training_config": job["best_model"]["config"]
        }
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel an AutoML job"""
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        if job["status"] in [AutoMLStatus.COMPLETED, AutoMLStatus.FAILED]:
            return False
        
        # Update status
        job["status"] = AutoMLStatus.FAILED
        job["completed_at"] = datetime.utcnow()
        
        # Cancel any running trials
        # This would cancel actual training jobs
        
        # Emit event
        await self.event_bus.publish(
            "automl.job.cancelled",
            {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"AutoML job cancelled: {job_id}")
        return True
    
    async def _run_automl(self, job_id: str):
        """Run AutoML workflow"""
        job = self.jobs.get(job_id)
        if not job:
            return
        
        try:
            # Update status
            job["status"] = AutoMLStatus.ANALYZING
            
            # Load and analyze dataset
            dataset_info = await self._analyze_dataset(job["config"]["dataset"])
            job["dataset_info"] = dataset_info
            
            # Feature engineering
            if self.config["feature_engineering"]["auto_generate"]:
                engineered_features = await self.feature_engineer.engineer_features(
                    dataset_info,
                    job["problem_type"],
                    job["config"]["target_column"]
                )
                job["engineered_features"] = engineered_features
            
            # Update status
            job["status"] = AutoMLStatus.SEARCHING
            
            # Get candidate models
            candidate_models = self._get_candidate_models(job["problem_type"])
            
            # Search for best model
            await self._search_models(job_id, candidate_models)
            
            # Update status
            job["status"] = AutoMLStatus.EVALUATING
            
            # Final evaluation
            await self._final_evaluation(job_id)
            
            # Complete job
            job["status"] = AutoMLStatus.COMPLETED
            job["completed_at"] = datetime.utcnow()
            
            # Update metrics
            self.metrics["jobs_completed"] += 1
            job_time = (job["completed_at"] - job["started_at"]).total_seconds()
            self._update_avg_job_time(job_time)
            
            if job["best_score"] > self.metrics["best_model_score"]:
                self.metrics["best_model_score"] = job["best_score"]
            
            # Emit event
            await self.event_bus.publish(
                "automl.job.completed",
                {
                    "job_id": job_id,
                    "best_model": job["best_model"]["name"] if job["best_model"] else None,
                    "best_score": job["best_score"],
                    "trials_completed": len(job["trials"]),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"AutoML job completed: {job_id}")
            
        except Exception as e:
            logger.error(f"AutoML job failed: {job_id}, error: {e}")
            
            job["status"] = AutoMLStatus.FAILED
            job["completed_at"] = datetime.utcnow()
            job["error"] = str(e)
            
            # Emit event
            await self.event_bus.publish(
                "automl.job.failed",
                {
                    "job_id": job_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def _search_models(self, job_id: str, candidate_models: List[str]):
        """Search for best model and hyperparameters"""
        job = self.jobs.get(job_id)
        if not job:
            return
        
        # Parallel trial execution
        max_parallel = min(
            self.config["search"]["parallel_trials"],
            len(candidate_models)
        )
        
        trial_queue = asyncio.Queue()
        for model in candidate_models:
            await trial_queue.put(model)
        
        # Worker tasks
        workers = []
        for i in range(max_parallel):
            worker = asyncio.create_task(
                self._trial_worker(job_id, trial_queue)
            )
            workers.append(worker)
        
        # Wait for completion or timeout
        start_time = datetime.utcnow()
        time_limit = job["config"].get("time_limit", self.config["search"]["time_limit_minutes"])
        
        while True:
            # Check time limit
            elapsed = (datetime.utcnow() - start_time).seconds / 60
            if elapsed > time_limit:
                logger.info(f"AutoML job {job_id} reached time limit")
                break
            
            # Check trial limit
            if len(job["trials"]) >= self.config["search"]["max_trials"]:
                logger.info(f"AutoML job {job_id} reached trial limit")
                break
            
            # Check if all models tried
            if trial_queue.empty() and all(w.done() for w in workers):
                break
            
            # Check early stopping
            if self._should_early_stop(job):
                logger.info(f"AutoML job {job_id} early stopping triggered")
                break
            
            await asyncio.sleep(5)
        
        # Cancel workers
        for worker in workers:
            if not worker.done():
                worker.cancel()
    
    async def _trial_worker(self, job_id: str, trial_queue: asyncio.Queue):
        """Worker to execute model trials"""
        while True:
            try:
                # Get next model to try
                model_name = await asyncio.wait_for(trial_queue.get(), timeout=1.0)
                
                # Run trial
                await self._run_trial(job_id, model_name)
                
            except asyncio.TimeoutError:
                break
            except Exception as e:
                logger.error(f"Trial worker error: {e}")
    
    async def _run_trial(self, job_id: str, model_name: str):
        """Run a single model trial"""
        job = self.jobs.get(job_id)
        if not job:
            return
        
        trial_id = f"{job_id}_trial_{len(job['trials']) + 1}"
        
        try:
            # Get hyperparameter search space
            search_space = await self.model_search.get_search_space(
                model_name,
                job["problem_type"]
            )
            
            # Optimize hyperparameters
            best_params = await self.hyperparameter_tuner.optimize(
                model_name=model_name,
                search_space=search_space,
                dataset=job["config"]["dataset"],
                target_column=job["config"]["target_column"],
                optimization_metric=job["config"].get("optimization_metric"),
                cv_folds=self.config["evaluation"]["cv_folds"]
            )
            
            # Train model with best parameters
            training_config = {
                "name": f"{job['config']['name']}_{model_name}",
                "framework": self._get_model_framework(model_name),
                "model_type": model_name,
                "dataset": job["config"]["dataset"],
                "hyperparameters": best_params["params"],
                "automl_job_id": job_id
            }
            
            # Submit training job
            training_job_id = await self.training_orchestrator.submit_training_job(training_config)
            
            # Wait for training to complete
            # In real implementation, this would be async
            await asyncio.sleep(1)  # Placeholder
            
            # Get evaluation results
            eval_results = best_params["score"]
            
            # Create trial record
            trial = {
                "id": trial_id,
                "model_name": model_name,
                "hyperparameters": best_params["params"],
                "score": eval_results,
                "training_job_id": training_job_id,
                "duration": best_params.get("duration", 0)
            }
            
            # Update job
            job["trials"].append(trial)
            self.metrics["models_evaluated"] += 1
            
            # Update best model if improved
            if self._is_better_score(eval_results, job["best_score"], job["config"]):
                job["best_score"] = eval_results
                job["best_model"] = {
                    "name": model_name,
                    "config": training_config,
                    "hyperparameters": best_params["params"],
                    "trial_id": trial_id
                }
            
            # Update leaderboard
            self._update_leaderboard(job, trial)
            
            # Emit event
            await self.event_bus.publish(
                "automl.trial.completed",
                {
                    "job_id": job_id,
                    "trial_id": trial_id,
                    "model_name": model_name,
                    "score": eval_results,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Trial completed: {trial_id}, model: {model_name}, score: {eval_results}")
            
        except Exception as e:
            logger.error(f"Trial failed: {trial_id}, model: {model_name}, error: {e}")
    
    async def _final_evaluation(self, job_id: str):
        """Perform final evaluation of best model"""
        job = self.jobs.get(job_id)
        if not job or not job["best_model"]:
            return
        
        # Get feature importance
        job["feature_importance"] = await self._get_feature_importance(
            job["best_model"],
            job["config"]["dataset"]
        )
        
        # Additional evaluation metrics
        # This would evaluate on hold-out test set
    
    async def _analyze_dataset(self, dataset_config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze dataset characteristics"""
        # This would load and analyze the actual dataset
        return {
            "num_samples": 10000,
            "num_features": 50,
            "feature_types": {
                "numeric": 40,
                "categorical": 10
            },
            "missing_values": 0.05,
            "class_balance": "balanced"
        }
    
    async def _detect_problem_type(self, job_config: Dict[str, Any]) -> str:
        """Automatically detect problem type from dataset"""
        # This would analyze the target variable
        # For now, return classification
        return ProblemType.CLASSIFICATION.value
    
    def _get_candidate_models(self, problem_type: ProblemType) -> List[str]:
        """Get candidate models for problem type"""
        return self.config["models"].get(problem_type.value, [])
    
    def _get_model_framework(self, model_name: str) -> str:
        """Get framework for model"""
        framework_mapping = {
            "logistic_regression": "scikit-learn",
            "linear_regression": "scikit-learn",
            "random_forest": "scikit-learn",
            "svm": "scikit-learn",
            "xgboost": "xgboost",
            "lightgbm": "lightgbm",
            "neural_network": "pytorch",
            "lstm": "pytorch",
            "arima": "statsmodels",
            "prophet": "prophet",
            "elastic_net": "scikit-learn",
            "exponential_smoothing": "statsmodels"
        }
        
        return framework_mapping.get(model_name, "custom")
    
    def _validate_job_config(self, config: Dict[str, Any]):
        """Validate AutoML job configuration"""
        required_fields = ["name", "dataset", "target_column"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
    
    def _is_maximize_metric(self, job_config: Dict[str, Any]) -> bool:
        """Check if optimization metric should be maximized"""
        metric = job_config.get("optimization_metric", "accuracy")
        maximize_metrics = ["accuracy", "f1", "auc_roc", "precision", "recall", "r2"]
        return metric in maximize_metrics
    
    def _is_better_score(self, new_score: float, current_best: float, 
                        job_config: Dict[str, Any]) -> bool:
        """Check if new score is better than current best"""
        if self._is_maximize_metric(job_config):
            return new_score > current_best
        else:
            return new_score < current_best
    
    def _should_early_stop(self, job: Dict[str, Any]) -> bool:
        """Check if early stopping should be triggered"""
        if len(job["trials"]) < self.config["search"]["early_stopping_rounds"]:
            return False
        
        # Check if score hasn't improved in last N trials
        recent_trials = job["trials"][-self.config["search"]["early_stopping_rounds"]:]
        recent_scores = [t["score"] for t in recent_trials]
        
        best_recent = max(recent_scores) if self._is_maximize_metric(job["config"]) else min(recent_scores)
        
        return not self._is_better_score(best_recent, job["best_score"], job["config"])
    
    def _update_leaderboard(self, job: Dict[str, Any], trial: Dict[str, Any]):
        """Update model leaderboard"""
        job["leaderboard"].append({
            "rank": len(job["leaderboard"]) + 1,
            "model_name": trial["model_name"],
            "score": trial["score"],
            "trial_id": trial["id"]
        })
        
        # Sort leaderboard
        reverse = self._is_maximize_metric(job["config"])
        job["leaderboard"].sort(key=lambda x: x["score"], reverse=reverse)
        
        # Update ranks
        for i, entry in enumerate(job["leaderboard"]):
            entry["rank"] = i + 1
    
    async def _get_feature_importance(self, model_info: Dict[str, Any], 
                                    dataset_config: Dict[str, Any]) -> Dict[str, float]:
        """Get feature importance from best model"""
        # This would get actual feature importance
        # For now, return mock data
        return {
            f"feature_{i}": float(i) / 100 
            for i in range(10)
        }
    
    def _update_avg_job_time(self, job_time: float):
        """Update average job time metric"""
        completed = self.metrics["jobs_completed"]
        
        if completed == 1:
            self.metrics["avg_job_time"] = job_time
        else:
            current_avg = self.metrics["avg_job_time"]
            self.metrics["avg_job_time"] = (
                (current_avg * (completed - 1) + job_time) / completed
            )
    
    async def _monitor_jobs(self):
        """Monitor AutoML jobs"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Clean up old completed jobs
                current_time = datetime.utcnow()
                
                for job_id, job in list(self.jobs.items()):
                    if job["status"] in [AutoMLStatus.COMPLETED, AutoMLStatus.FAILED]:
                        if job["completed_at"]:
                            age = (current_time - job["completed_at"]).days
                            if age > 7:  # Remove jobs older than 7 days
                                del self.jobs[job_id]
                
            except Exception as e:
                logger.error(f"Error monitoring jobs: {e}")
    
    async def get_automl_metrics(self) -> Dict[str, Any]:
        """Get AutoML engine metrics"""
        return {
            **self.metrics,
            "active_jobs": sum(
                1 for job in self.jobs.values()
                if job["status"] not in [AutoMLStatus.COMPLETED, AutoMLStatus.FAILED]
            ),
            "total_jobs": len(self.jobs)
        } 