"""
ML Platform Service Base Classes

Migrated to use the unified data-intelligence-common library.
"""

from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig, ScalableConfig
from data_intelligence_common.core.processing import (
    UnifiedProcessor, ProcessingConfig, ProcessingMode, ProcessingEngine,
    DataSource, DataSink, ProcessingStage, ProcessingContext,
    FileSource, DatabaseSource, EventBusSource,
    FileSink, DatabaseSink, EventBusSink
)
from data_intelligence_common.core.events import Event, EventType, create_model_event
from data_intelligence_common.core.patterns.factory import TypedFactory, PluginFactory
from data_intelligence_common.core.mixins import StateMixin, ResourceMixin
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class ModelStatus(str, Enum):
    """Model lifecycle status"""
    CREATED = "created"
    TRAINING = "training"
    VALIDATING = "validating"
    DEPLOYED = "deployed"
    SERVING = "serving"
    RETIRED = "retired"
    FAILED = "failed"


class TrainingMode(str, Enum):
    """Training execution modes"""
    LOCAL = "local"
    DISTRIBUTED = "distributed"
    FEDERATED = "federated"
    ONLINE = "online"
    AUTO = "auto"


@dataclass
class MLPlatformConfig(UnifiedServiceConfig, ScalableConfig):
    """Configuration for ML platform service"""
    # ML specific settings
    model_registry_url: str = "http://mlflow.local:5000"
    experiment_tracking_enabled: bool = True
    auto_versioning: bool = True
    
    # Training settings
    default_training_mode: TrainingMode = TrainingMode.AUTO
    distributed_framework: str = "ray"  # ray, horovod, spark
    gpu_enabled: bool = True
    
    # Model serving
    enable_model_serving: bool = True
    serving_framework: str = "triton"  # triton, seldon, kserve
    default_replicas: int = 2
    autoscaling_enabled: bool = True
    
    # Feature store
    enable_feature_store: bool = True
    feature_store_backend: str = "feast"
    
    # Monitoring
    enable_model_monitoring: bool = True
    drift_detection_enabled: bool = True
    performance_tracking_enabled: bool = True
    
    # Storage
    model_storage_path: str = "/models"
    artifact_storage: str = "minio"


class MLPlatformService(DataIntelligenceBaseService, StateMixin, ResourceMixin):
    """
    ML Platform service for model lifecycle management.
    
    Provides training, serving, monitoring, and governance for ML models.
    """
    
    def __init__(self, config: MLPlatformConfig):
        super().__init__(config)
        self.config = config
        
        # ML components
        self._model_registry = None
        self._experiment_tracker = None
        self._feature_store = None
        self._serving_engine = None
        self._training_engine = None
        
        # Factories
        self._model_factory = None
        self._trainer_factory = None
        
        # Active resources
        self._active_trainings: Dict[str, asyncio.Task] = {}
        self._deployed_models: Dict[str, Any] = {}
        
    async def _initialize_internal(self):
        """Initialize ML-specific components"""
        await super()._initialize_internal()
        
        # Initialize ML components
        await self._initialize_ml_components()
        
        # Initialize factories
        self._initialize_factories()
        
        # Register health checks
        self.register_health_check(
            "model_registry",
            self._check_model_registry_health,
            critical=True
        )
        
        self.register_health_check(
            "serving_engine",
            self._check_serving_engine_health,
            critical=self.config.enable_model_serving
        )
        
        # Start model monitoring if enabled
        if self.config.enable_model_monitoring:
            self._start_background_task(self._monitor_models_loop())
            
        logger.info("ML platform service initialized")
        
    async def _initialize_ml_components(self):
        """Initialize ML infrastructure components"""
        # Initialize model registry
        from ..registry.mlflow_registry import MLFlowRegistry
        self._model_registry = MLFlowRegistry(self.config.model_registry_url)
        await self._model_registry.initialize()
        
        # Initialize experiment tracker
        if self.config.experiment_tracking_enabled:
            from ..tracking.experiment_tracker import ExperimentTracker
            self._experiment_tracker = ExperimentTracker(self.config.model_registry_url)
            await self._experiment_tracker.initialize()
            
        # Initialize feature store
        if self.config.enable_feature_store:
            from ..features.feature_store import FeatureStore
            self._feature_store = FeatureStore(
                backend=self.config.feature_store_backend
            )
            await self._feature_store.initialize()
            
        # Initialize serving engine
        if self.config.enable_model_serving:
            from ..serving.serving_engine import ServingEngine
            self._serving_engine = ServingEngine(
                framework=self.config.serving_framework,
                default_replicas=self.config.default_replicas
            )
            await self._serving_engine.initialize()
            
        # Initialize training engine
        from ..training.training_engine import TrainingEngine
        self._training_engine = TrainingEngine(
            framework=self.config.distributed_framework,
            gpu_enabled=self.config.gpu_enabled
        )
        await self._training_engine.initialize()
        
    def _initialize_factories(self):
        """Initialize model and trainer factories"""
        # Model factory for creating different model types
        self._model_factory = TypedFactory({
            "sklearn": "sklearn.ensemble.RandomForestClassifier",
            "xgboost": "xgboost.XGBClassifier",
            "lightgbm": "lightgbm.LGBMClassifier",
            "tensorflow": "tensorflow.keras.Sequential",
            "pytorch": "torch.nn.Module"
        })
        
        # Trainer factory for different training strategies
        self._trainer_factory = PluginFactory(
            plugin_dir="trainers",
            base_class="BaseTrainer"
        )
        
    async def train_model(
        self,
        name: str,
        model_type: str,
        dataset_id: str,
        hyperparameters: Dict[str, Any],
        training_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Train a new model.
        
        Uses unified processing for data pipeline and training.
        """
        model_id = str(uuid.uuid4())
        experiment_id = f"exp_{model_id}"
        
        # Update model state
        await self.set_state(f"model:{model_id}:status", ModelStatus.TRAINING)
        
        # Emit training started event
        await self.publish_event(
            event_type="model.training.started",
            data={
                "model_id": model_id,
                "name": name,
                "model_type": model_type,
                "dataset_id": dataset_id
            }
        )
        
        try:
            # Create training pipeline
            pipeline = await self._create_training_pipeline(
                model_id=model_id,
                model_type=model_type,
                dataset_id=dataset_id,
                hyperparameters=hyperparameters,
                training_config=training_config or {}
            )
            
            # Start training task
            training_task = asyncio.create_task(
                self._execute_training(
                    model_id=model_id,
                    pipeline=pipeline,
                    experiment_id=experiment_id
                )
            )
            
            self._active_trainings[model_id] = training_task
            
            # Wait for completion if synchronous
            if training_config and training_config.get("wait_for_completion", True):
                result = await training_task
                
                # Record metrics
                self.record_operation("model_trained", {
                    "model_type": model_type,
                    "duration": result.get("duration", 0),
                    "metrics": result.get("metrics", {})
                })
                
                return result
            else:
                return {
                    "model_id": model_id,
                    "status": "training",
                    "experiment_id": experiment_id
                }
                
        except Exception as e:
            # Update state and emit failure
            await self.set_state(f"model:{model_id}:status", ModelStatus.FAILED)
            await self.publish_event(
                event_type="model.training.failed",
                data={
                    "model_id": model_id,
                    "error": str(e)
                }
            )
            
            self.record_error("model_training_failed", e)
            raise
            
    async def _create_training_pipeline(
        self,
        model_id: str,
        model_type: str,
        dataset_id: str,
        hyperparameters: Dict[str, Any],
        training_config: Dict[str, Any]
    ) -> UnifiedProcessor:
        """Create training data pipeline"""
        # Determine training mode
        mode = training_config.get("mode", self.config.default_training_mode)
        if mode == TrainingMode.AUTO:
            mode = await self._determine_training_mode(dataset_id)
            
        # Create processing config
        processing_config = ProcessingConfig(
            name=f"training_{model_id}",
            mode=ProcessingMode.BATCH,
            engine=ProcessingEngine.SPARK if mode == TrainingMode.DISTRIBUTED else ProcessingEngine.PANDAS,
            batch_size=training_config.get("batch_size", 1000),
            enable_quality_checks=True,
            enable_lineage_tracking=True
        )
        
        # Get dataset source
        source = await self._get_dataset_source(dataset_id)
        
        # Create model artifact sink
        sink = DatabaseSink(
            client=self._model_registry,
            table=f"models/{model_id}",
            mode="overwrite"
        )
        
        # Build pipeline with preprocessing
        pipeline = UnifiedProcessor.pipeline(processing_config)\
            .from_source(source)\
            .transform(self._create_preprocessing_stage(model_type))\
            .transform(self._create_feature_engineering_stage())\
            .transform(self._create_training_stage(
                model_type=model_type,
                hyperparameters=hyperparameters
            ))\
            .transform(self._create_validation_stage())\
            .to_sink(sink)\
            .build(
                metrics_collector=self.metrics,
                event_bus=self.event_bus,
                cache_manager=self.cache
            )
            
        return pipeline
        
    def _create_preprocessing_stage(self, model_type: str) -> ProcessingStage:
        """Create preprocessing stage based on model type"""
        class PreprocessingStage(ProcessingStage):
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                # Model-specific preprocessing
                if model_type in ["sklearn", "xgboost", "lightgbm"]:
                    # Tabular data preprocessing
                    data = self._preprocess_tabular(data)
                elif model_type == "tensorflow":
                    # Deep learning preprocessing
                    data = self._preprocess_deep_learning(data)
                elif model_type == "pytorch":
                    # PyTorch preprocessing
                    data = self._preprocess_pytorch(data)
                    
                return data
                
            def _preprocess_tabular(self, data):
                # Standard tabular preprocessing
                return data
                
            def _preprocess_deep_learning(self, data):
                # Normalize, resize, etc.
                return data
                
            def _preprocess_pytorch(self, data):
                # Convert to tensors, etc.
                return data
                
        return PreprocessingStage()
        
    def _create_feature_engineering_stage(self) -> ProcessingStage:
        """Create feature engineering stage"""
        feature_store = self._feature_store
        
        class FeatureEngineeringStage(ProcessingStage):
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                if feature_store:
                    # Get features from feature store
                    features = await feature_store.get_features(
                        entity_id=data.get("id"),
                        feature_names=context.metadata.get("feature_names", [])
                    )
                    data.update(features)
                    
                # Apply feature transformations
                # This would include scaling, encoding, etc.
                
                return data
                
        return FeatureEngineeringStage()
        
    def _create_training_stage(
        self,
        model_type: str,
        hyperparameters: Dict[str, Any]
    ) -> ProcessingStage:
        """Create model training stage"""
        model_factory = self._model_factory
        trainer_factory = self._trainer_factory
        
        class TrainingStage(ProcessingStage):
            def __init__(self):
                self.model = None
                self.trainer = None
                
            async def process_batch(self, batch: List[Any], context: ProcessingContext) -> List[Any]:
                # Initialize model and trainer on first batch
                if not self.model:
                    self.model = model_factory.create(model_type, **hyperparameters)
                    self.trainer = trainer_factory.create(
                        f"{model_type}_trainer",
                        model=self.model
                    )
                    
                # Train on batch
                self.trainer.train_batch(batch)
                
                # Return training metrics
                return [{
                    "batch_id": context.job_id,
                    "metrics": self.trainer.get_metrics()
                }]
                
        return TrainingStage()
        
    def _create_validation_stage(self) -> ProcessingStage:
        """Create model validation stage"""
        class ValidationStage(ProcessingStage):
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                # Validate model performance
                metrics = data.get("metrics", {})
                
                # Check thresholds
                if metrics.get("accuracy", 0) < 0.8:
                    logger.warning("Model accuracy below threshold")
                    
                # Add validation results
                data["validation"] = {
                    "passed": True,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                return data
                
        return ValidationStage()
        
    async def deploy_model(
        self,
        model_id: str,
        endpoint_name: str,
        deployment_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Deploy a trained model to serving"""
        if not self.config.enable_model_serving:
            raise ValueError("Model serving is not enabled")
            
        # Check model exists and is ready
        model_status = await self.get_state(f"model:{model_id}:status")
        if model_status != ModelStatus.DEPLOYED:
            raise ValueError(f"Model {model_id} is not ready for deployment")
            
        try:
            # Deploy to serving engine
            deployment = await self._serving_engine.deploy(
                model_id=model_id,
                endpoint_name=endpoint_name,
                config=deployment_config or {}
            )
            
            # Update state
            await self.set_state(f"model:{model_id}:status", ModelStatus.SERVING)
            await self.set_state(f"endpoint:{endpoint_name}:model", model_id)
            
            # Store deployment info
            self._deployed_models[endpoint_name] = {
                "model_id": model_id,
                "deployment": deployment,
                "timestamp": datetime.utcnow()
            }
            
            # Emit deployment event
            await self.publish_event(
                event_type="model.deployed",
                data={
                    "model_id": model_id,
                    "endpoint_name": endpoint_name,
                    "url": deployment.get("url")
                }
            )
            
            # Record metrics
            self.record_operation("model_deployed", {
                "endpoint": endpoint_name,
                "replicas": deployment.get("replicas", 1)
            })
            
            return deployment
            
        except Exception as e:
            self.record_error("model_deployment_failed", e)
            raise
            
    async def predict(
        self,
        endpoint_name: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Make predictions using deployed model"""
        # Get endpoint info
        model_id = await self.get_state(f"endpoint:{endpoint_name}:model")
        if not model_id:
            raise ValueError(f"Endpoint {endpoint_name} not found")
            
        # Check cache for predictions
        cache_key = f"prediction:{endpoint_name}:{hash(str(data))}"
        cached_result = await self.get_cached(cache_key)
        if cached_result:
            return cached_result
            
        try:
            # Make prediction
            result = await self._serving_engine.predict(
                endpoint_name=endpoint_name,
                data=data
            )
            
            # Cache result
            await self.cache_result(cache_key, result, ttl=60)
            
            # Record metrics
            self.record_operation("prediction_made", {
                "endpoint": endpoint_name,
                "batch_size": len(data) if isinstance(data, list) else 1
            })
            
            # Check for drift if monitoring enabled
            if self.config.drift_detection_enabled:
                asyncio.create_task(
                    self._check_prediction_drift(model_id, data, result)
                )
                
            return result
            
        except Exception as e:
            self.record_error("prediction_failed", e)
            raise
            
    async def _execute_training(
        self,
        model_id: str,
        pipeline: UnifiedProcessor,
        experiment_id: str
    ) -> Dict[str, Any]:
        """Execute model training"""
        start_time = datetime.utcnow()
        
        try:
            # Start experiment tracking
            if self._experiment_tracker:
                await self._experiment_tracker.start_run(
                    experiment_id=experiment_id,
                    run_name=f"training_{model_id}"
                )
                
            # Execute pipeline
            result = await pipeline.process(job_id=model_id)
            
            # Log metrics
            if self._experiment_tracker:
                await self._experiment_tracker.log_metrics(
                    result.get("metrics", {})
                )
                await self._experiment_tracker.end_run()
                
            # Register model
            model_info = await self._model_registry.register_model(
                model_id=model_id,
                model_name=f"model_{model_id}",
                metrics=result.get("metrics", {}),
                artifacts=result.get("artifacts", {})
            )
            
            # Update state
            await self.set_state(f"model:{model_id}:status", ModelStatus.DEPLOYED)
            
            # Emit completion event
            await self.publish_event(
                event_type="model.training.completed",
                data={
                    "model_id": model_id,
                    "duration": (datetime.utcnow() - start_time).total_seconds(),
                    "metrics": result.get("metrics", {})
                }
            )
            
            return {
                "model_id": model_id,
                "status": "completed",
                "duration": (datetime.utcnow() - start_time).total_seconds(),
                "metrics": result.get("metrics", {}),
                "model_info": model_info
            }
            
        except Exception as e:
            # Update state
            await self.set_state(f"model:{model_id}:status", ModelStatus.FAILED)
            
            # Log failure
            if self._experiment_tracker:
                await self._experiment_tracker.log_error(str(e))
                await self._experiment_tracker.end_run(status="FAILED")
                
            raise
            
    async def _determine_training_mode(self, dataset_id: str) -> TrainingMode:
        """Determine optimal training mode based on dataset"""
        # Get dataset info
        dataset_size = await self._get_dataset_size(dataset_id)
        
        # Simple heuristics
        if dataset_size < 1_000_000:  # < 1M records
            return TrainingMode.LOCAL
        elif dataset_size < 100_000_000:  # < 100M records
            return TrainingMode.DISTRIBUTED
        else:
            return TrainingMode.DISTRIBUTED
            
    async def _get_dataset_source(self, dataset_id: str) -> DataSource:
        """Get data source for dataset"""
        # This would look up dataset location
        # For now, return file source
        return FileSource(f"/datasets/{dataset_id}.parquet", format="parquet")
        
    async def _get_dataset_size(self, dataset_id: str) -> int:
        """Get dataset size in records"""
        # This would query dataset metadata
        return 1_000_000  # Default
        
    async def _check_prediction_drift(
        self,
        model_id: str,
        input_data: Any,
        predictions: Any
    ):
        """Check for model drift"""
        # This would implement drift detection
        # For now, just log
        logger.info(f"Checking drift for model {model_id}")
        
    async def _monitor_models_loop(self):
        """Monitor deployed models"""
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                for endpoint_name, info in self._deployed_models.items():
                    try:
                        # Check model health
                        health = await self._serving_engine.check_health(endpoint_name)
                        
                        if not health.get("healthy"):
                            await self.publish_event(
                                event_type="model.unhealthy",
                                data={
                                    "endpoint": endpoint_name,
                                    "model_id": info["model_id"],
                                    "reason": health.get("reason")
                                }
                            )
                            
                        # Check performance metrics
                        if self.config.performance_tracking_enabled:
                            metrics = await self._serving_engine.get_metrics(endpoint_name)
                            
                            # Check latency
                            if metrics.get("p99_latency", 0) > 1000:  # > 1s
                                logger.warning(f"High latency for endpoint {endpoint_name}")
                                
                    except Exception as e:
                        logger.error(f"Error monitoring endpoint {endpoint_name}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in model monitoring loop: {e}")
                
    async def _check_model_registry_health(self) -> Dict[str, Any]:
        """Check model registry health"""
        try:
            await self._model_registry.list_models(limit=1)
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "reason": str(e)}
            
    async def _check_serving_engine_health(self) -> Dict[str, Any]:
        """Check serving engine health"""
        if not self._serving_engine:
            return {"healthy": False, "reason": "Not initialized"}
            
        try:
            endpoints = await self._serving_engine.list_endpoints()
            return {
                "healthy": True,
                "active_endpoints": len(endpoints)
            }
        except Exception as e:
            return {"healthy": False, "reason": str(e)}
            
    async def _stop_internal(self):
        """Stop ML platform components"""
        # Cancel active trainings
        for task in self._active_trainings.values():
            if not task.done():
                task.cancel()
                
        # Cleanup components
        if self._model_registry:
            await self._model_registry.close()
            
        if self._serving_engine:
            await self._serving_engine.close()
            
        if self._training_engine:
            await self._training_engine.close()
            
        if self._feature_store:
            await self._feature_store.close()
            
        await super()._stop_internal()
        
        logger.info("ML platform service stopped")


# Export main components
__all__ = [
    'ModelStatus',
    'TrainingMode',
    'MLPlatformConfig',
    'MLPlatformService'
] 