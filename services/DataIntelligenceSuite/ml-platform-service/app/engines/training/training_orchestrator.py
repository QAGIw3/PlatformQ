"""
Training Orchestrator

Orchestrates distributed ML model training across multiple frameworks and infrastructures.
"""

import asyncio
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from enum import Enum
import uuid

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class TrainingStatus(Enum):
    """Training job status"""
    PENDING = "pending"
    PREPARING = "preparing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Framework(Enum):
    """Supported ML frameworks"""
    PYTORCH = "pytorch"
    TENSORFLOW = "tensorflow"
    SCIKIT_LEARN = "scikit-learn"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CUSTOM = "custom"


class TrainingOrchestrator:
    """
    Orchestrates ML model training workflows
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 model_registry: Any, distributed_trainer: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.model_registry = model_registry
        self.distributed_trainer = distributed_trainer
        
        # Training job tracking
        self.active_jobs: Dict[str, Dict[str, Any]] = {}
        self.job_queue: asyncio.Queue = asyncio.Queue()
        
        # Configuration
        self.config = {
            "max_concurrent_jobs": 10,
            "default_timeout": 3600,  # 1 hour
            "checkpoint_interval": 300,  # 5 minutes
            "resource_limits": {
                "cpu": 8,
                "memory": "32Gi",
                "gpu": 2
            }
        }
        
        # Metrics
        self.metrics = {
            "jobs_submitted": 0,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "avg_training_time": 0
        }
    
    async def initialize(self):
        """Initialize training orchestrator"""
        logger.info("initializing_training_orchestrator")
        
        # Load configuration from Consul
        await self._load_configuration()
        
        # Initialize distributed trainer
        await self.distributed_trainer.initialize()
        
        # Start background workers
        asyncio.create_task(self._process_job_queue())
        asyncio.create_task(self._monitor_jobs())
        
        logger.info("training_orchestrator_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Cancel all active jobs
        for job_id in list(self.active_jobs.keys()):
            await self.cancel_job(job_id)
        
        await self.distributed_trainer.cleanup()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/training-orchestrator")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def submit_training_job(self, job_config: Dict[str, Any]) -> str:
        """
        Submit a new training job
        
        Args:
            job_config: Training job configuration including:
                - name: Job name
                - framework: ML framework to use
                - model_type: Type of model to train
                - dataset: Dataset configuration
                - hyperparameters: Model hyperparameters
                - resources: Resource requirements
                - callbacks: Training callbacks
                
        Returns:
            Job ID
        """
        job_id = str(uuid.uuid4())
        
        # Create job record
        job = {
            "id": job_id,
            "config": job_config,
            "status": TrainingStatus.PENDING,
            "submitted_at": datetime.utcnow(),
            "started_at": None,
            "completed_at": None,
            "metrics": {},
            "artifacts": {},
            "error": None
        }
        
        # Validate job configuration
        self._validate_job_config(job_config)
        
        # Store job
        self.active_jobs[job_id] = job
        
        # Add to queue
        await self.job_queue.put(job_id)
        
        # Update metrics
        self.metrics["jobs_submitted"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "ml.training.submitted",
            {
                "job_id": job_id,
                "name": job_config.get("name"),
                "framework": job_config.get("framework"),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Training job submitted: {job_id}")
        return job_id
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get training job status"""
        job = self.active_jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        return {
            "id": job_id,
            "status": job["status"].value,
            "submitted_at": job["submitted_at"].isoformat(),
            "started_at": job["started_at"].isoformat() if job["started_at"] else None,
            "completed_at": job["completed_at"].isoformat() if job["completed_at"] else None,
            "metrics": job["metrics"],
            "error": job["error"]
        }
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a training job"""
        job = self.active_jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")
        
        if job["status"] in [TrainingStatus.COMPLETED, TrainingStatus.FAILED, TrainingStatus.CANCELLED]:
            return False
        
        # Cancel distributed training
        if job["status"] == TrainingStatus.RUNNING:
            await self.distributed_trainer.cancel_training(job_id)
        
        # Update job status
        job["status"] = TrainingStatus.CANCELLED
        job["completed_at"] = datetime.utcnow()
        
        # Emit event
        await self.event_bus.publish(
            "ml.training.cancelled",
            {
                "job_id": job_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Training job cancelled: {job_id}")
        return True
    
    async def _process_job_queue(self):
        """Process training job queue"""
        while True:
            try:
                # Check if we can run more jobs
                running_jobs = sum(1 for job in self.active_jobs.values() 
                                 if job["status"] == TrainingStatus.RUNNING)
                
                if running_jobs >= self.config["max_concurrent_jobs"]:
                    await asyncio.sleep(10)
                    continue
                
                # Get next job from queue
                job_id = await self.job_queue.get()
                
                # Start training
                asyncio.create_task(self._run_training_job(job_id))
                
            except Exception as e:
                logger.error(f"Error processing job queue: {e}")
                await asyncio.sleep(5)
    
    async def _run_training_job(self, job_id: str):
        """Run a training job"""
        job = self.active_jobs.get(job_id)
        if not job:
            logger.error(f"Job not found: {job_id}")
            return
        
        try:
            # Update job status
            job["status"] = TrainingStatus.PREPARING
            job["started_at"] = datetime.utcnow()
            
            # Emit event
            await self.event_bus.publish(
                "ml.training.started",
                {
                    "job_id": job_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            # Prepare training environment
            await self._prepare_training_environment(job)
            
            # Update status to running
            job["status"] = TrainingStatus.RUNNING
            
            # Run distributed training
            result = await self.distributed_trainer.train(
                job_id=job_id,
                framework=job["config"]["framework"],
                model_config=job["config"],
                callbacks=[
                    self._create_metrics_callback(job_id),
                    self._create_checkpoint_callback(job_id)
                ]
            )
            
            # Save model to registry
            model_info = await self._save_model_to_registry(job, result)
            
            # Update job with results
            job["status"] = TrainingStatus.COMPLETED
            job["completed_at"] = datetime.utcnow()
            job["artifacts"]["model_id"] = model_info["model_id"]
            job["artifacts"]["model_version"] = model_info["version"]
            job["metrics"]["final"] = result.get("metrics", {})
            
            # Update average training time
            training_time = (job["completed_at"] - job["started_at"]).total_seconds()
            self._update_avg_training_time(training_time)
            
            # Update metrics
            self.metrics["jobs_completed"] += 1
            
            # Emit event
            await self.event_bus.publish(
                "ml.training.completed",
                {
                    "job_id": job_id,
                    "model_id": model_info["model_id"],
                    "metrics": job["metrics"]["final"],
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Training job completed: {job_id}")
            
        except Exception as e:
            logger.error(f"Training job failed: {job_id}, error: {e}")
            
            # Update job status
            job["status"] = TrainingStatus.FAILED
            job["completed_at"] = datetime.utcnow()
            job["error"] = str(e)
            
            # Update metrics
            self.metrics["jobs_failed"] += 1
            
            # Emit event
            await self.event_bus.publish(
                "ml.training.failed",
                {
                    "job_id": job_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def _prepare_training_environment(self, job: Dict[str, Any]):
        """Prepare training environment"""
        # Get credentials from Vault
        credentials = await self.vault_consul.get_secret(
            f"ml/training/{job['config']['framework']}"
        )
        
        # Set up compute resources
        resources = job["config"].get("resources", self.config["resource_limits"])
        
        # Prepare dataset
        dataset_config = job["config"]["dataset"]
        # This would handle dataset preparation, downloading, etc.
        
        logger.info(f"Training environment prepared for job: {job['id']}")
    
    async def _save_model_to_registry(self, job: Dict[str, Any], 
                                    result: Dict[str, Any]) -> Dict[str, Any]:
        """Save trained model to registry"""
        model_info = {
            "name": job["config"]["name"],
            "framework": job["config"]["framework"],
            "model_type": job["config"]["model_type"],
            "hyperparameters": job["config"]["hyperparameters"],
            "metrics": result.get("metrics", {}),
            "artifacts": result.get("artifacts", {}),
            "training_job_id": job["id"]
        }
        
        # Register model
        registered_model = await self.model_registry.register_model(model_info)
        
        return registered_model
    
    def _validate_job_config(self, config: Dict[str, Any]):
        """Validate training job configuration"""
        required_fields = ["name", "framework", "model_type", "dataset", "hyperparameters"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate framework
        framework = config["framework"]
        if framework not in [f.value for f in Framework]:
            raise ValueError(f"Unsupported framework: {framework}")
        
        # Validate resources if specified
        if "resources" in config:
            resources = config["resources"]
            if "cpu" in resources and resources["cpu"] > self.config["resource_limits"]["cpu"]:
                raise ValueError(f"CPU limit exceeded: {resources['cpu']}")
            if "gpu" in resources and resources["gpu"] > self.config["resource_limits"]["gpu"]:
                raise ValueError(f"GPU limit exceeded: {resources['gpu']}")
    
    def _create_metrics_callback(self, job_id: str):
        """Create metrics tracking callback"""
        async def callback(metrics: Dict[str, Any]):
            job = self.active_jobs.get(job_id)
            if job:
                job["metrics"]["current"] = metrics
                
                # Emit metrics event
                await self.event_bus.publish(
                    "ml.training.metrics",
                    {
                        "job_id": job_id,
                        "metrics": metrics,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
        
        return callback
    
    def _create_checkpoint_callback(self, job_id: str):
        """Create checkpoint saving callback"""
        async def callback(checkpoint_path: str):
            job = self.active_jobs.get(job_id)
            if job:
                job["artifacts"]["latest_checkpoint"] = checkpoint_path
                
                logger.info(f"Checkpoint saved for job {job_id}: {checkpoint_path}")
        
        return callback
    
    async def _monitor_jobs(self):
        """Monitor running jobs for timeouts and issues"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                current_time = datetime.utcnow()
                
                for job_id, job in list(self.active_jobs.items()):
                    if job["status"] != TrainingStatus.RUNNING:
                        continue
                    
                    # Check for timeout
                    timeout = job["config"].get("timeout", self.config["default_timeout"])
                    elapsed = (current_time - job["started_at"]).total_seconds()
                    
                    if elapsed > timeout:
                        logger.warning(f"Job {job_id} timed out after {elapsed}s")
                        await self.cancel_job(job_id)
                        job["error"] = "Training timeout"
                
            except Exception as e:
                logger.error(f"Error monitoring jobs: {e}")
    
    def _update_avg_training_time(self, training_time: float):
        """Update average training time metric"""
        completed = self.metrics["jobs_completed"]
        if completed == 1:
            self.metrics["avg_training_time"] = training_time
        else:
            current_avg = self.metrics["avg_training_time"]
            self.metrics["avg_training_time"] = (
                (current_avg * (completed - 1) + training_time) / completed
            )
    
    async def get_training_metrics(self) -> Dict[str, Any]:
        """Get training orchestrator metrics"""
        return {
            **self.metrics,
            "active_jobs": len(self.active_jobs),
            "running_jobs": sum(1 for job in self.active_jobs.values() 
                              if job["status"] == TrainingStatus.RUNNING),
            "queued_jobs": self.job_queue.qsize()
        } 