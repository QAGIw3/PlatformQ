"""
Dependency injection container for ML Platform Service
"""
import logging
from dependency_injector import containers, providers

from ..infrastructure.mlflow import MLflowClient
from ..infrastructure.minio import MinIOClient
from ..infrastructure.ignite import IgniteClient
from ..infrastructure.spark import SparkClient
from ..infrastructure.triton import TritonClient
from ..core.model_registry import ModelRegistryManager
from ..core.training_manager import TrainingManager
from ..core.serving_manager import ServingManager
from ..core.monitoring_manager import MonitoringManager
from ..core.automl_manager import AutoMLManager
from ..core.federated_manager import FederatedLearningManager
from .config import settings

logger = logging.getLogger(__name__)


class Container(containers.DeclarativeContainer):
    """DI Container for ML Platform Service"""
    
    # Configuration
    config = providers.Configuration()
    config.from_dict(settings.model_dump())
    
    # Infrastructure clients
    mlflow_client = providers.Singleton(
        MLflowClient,
        tracking_uri=config.MLFLOW_TRACKING_URI,
        backend_store_uri=config.MLFLOW_BACKEND_STORE_URI,
        artifact_location=config.MLFLOW_ARTIFACT_LOCATION,
        experiment_name=config.MLFLOW_EXPERIMENT_NAME
    )
    
    minio_client = providers.Singleton(
        MinIOClient,
        endpoint=config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE
    )
    
    ignite_client = providers.Singleton(
        IgniteClient,
        host=config.IGNITE_HOST,
        port=config.IGNITE_PORT
    )
    
    spark_client = providers.Singleton(
        SparkClient,
        master=config.SPARK_MASTER,
        app_name=config.SERVICE_NAME,
        executor_memory=config.SPARK_EXECUTOR_MEMORY,
        executor_cores=config.SPARK_EXECUTOR_CORES
    )
    
    triton_client = providers.Singleton(
        TritonClient,
        server_url=config.TRITON_SERVER_URL,
        model_repository=config.TRITON_MODEL_REPOSITORY
    )
    
    # Core managers
    model_registry_manager = providers.Singleton(
        ModelRegistryManager,
        mlflow_client=mlflow_client,
        minio_client=minio_client,
        ignite_client=ignite_client,
        model_bucket=config.MODEL_BUCKET,
        artifact_bucket=config.ARTIFACT_BUCKET
    )
    
    training_manager = providers.Singleton(
        TrainingManager,
        model_registry=model_registry_manager,
        spark_client=spark_client,
        minio_client=minio_client,
        max_jobs=config.MAX_TRAINING_JOBS,
        default_timeout=config.DEFAULT_TRAINING_TIMEOUT,
        checkpoint_interval=config.CHECKPOINT_INTERVAL
    )
    
    serving_manager = providers.Singleton(
        ServingManager,
        model_registry=model_registry_manager,
        triton_client=triton_client,
        ignite_client=ignite_client,
        model_cache_size=config.MODEL_CACHE_SIZE,
        inference_timeout=config.INFERENCE_TIMEOUT,
        batch_size=config.BATCH_SIZE
    )
    
    monitoring_manager = providers.Singleton(
        MonitoringManager,
        model_registry=model_registry_manager,
        serving_manager=serving_manager,
        drift_enabled=config.DRIFT_DETECTION_ENABLED,
        drift_interval=config.DRIFT_CHECK_INTERVAL,
        performance_threshold=config.PERFORMANCE_THRESHOLD
    )
    
    automl_manager = providers.Singleton(
        AutoMLManager,
        training_manager=training_manager,
        model_registry=model_registry_manager,
        time_limit_minutes=config.AUTOML_TIME_LIMIT_MINUTES,
        max_trials=config.AUTOML_MAX_TRIALS,
        metric=config.AUTOML_METRIC,
        frameworks=config.AUTOML_FRAMEWORKS
    )
    
    federated_manager = providers.Singleton(
        FederatedLearningManager,
        model_registry=model_registry_manager,
        ignite_client=ignite_client,
        rounds=config.FEDERATED_ROUNDS,
        min_clients=config.MIN_CLIENTS_PER_ROUND,
        client_timeout=config.CLIENT_TIMEOUT_SECONDS,
        aggregation_strategy=config.AGGREGATION_STRATEGY,
        differential_privacy_epsilon=config.DIFFERENTIAL_PRIVACY_EPSILON
    )
    
    async def init_resources(self):
        """Initialize all resources"""
        logger.info("Initializing ML Platform Service resources...")
        
        # Initialize infrastructure clients
        await self.mlflow_client().initialize()
        await self.minio_client().initialize()
        await self.ignite_client().initialize()
        await self.spark_client().initialize()
        await self.triton_client().initialize()
        
        # Initialize managers
        await self.model_registry_manager().initialize()
        await self.training_manager().initialize()
        await self.serving_manager().initialize()
        await self.monitoring_manager().initialize()
        await self.automl_manager().initialize()
        await self.federated_manager().initialize()
        
        logger.info("All resources initialized successfully")
    
    async def shutdown_resources(self):
        """Shutdown all resources"""
        logger.info("Shutting down ML Platform Service resources...")
        
        # Shutdown managers
        await self.monitoring_manager().shutdown()
        await self.serving_manager().shutdown()
        await self.training_manager().shutdown()
        await self.federated_manager().shutdown()
        await self.automl_manager().shutdown()
        await self.model_registry_manager().shutdown()
        
        # Shutdown infrastructure clients
        await self.triton_client().close()
        await self.spark_client().close()
        await self.ignite_client().close()
        await self.minio_client().close()
        await self.mlflow_client().close()
        
        logger.info("All resources shut down successfully") 