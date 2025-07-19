"""
Unified ML Platform Service

Comprehensive machine learning platform consolidating:
- Model training, serving, and lifecycle management
- MLOps and model marketplace
- Federated learning capabilities
- Neuromorphic computing
- Feature store and model registry
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from platformq_shared import create_base_app, ConfigLoader
from platformq_event_framework import BaseEventProcessor, EventMetrics

from .core.config import settings
from .core.model_registry import UnifiedModelRegistry
from .core.feature_store import FeatureStore
from .core.training_orchestrator import TrainingOrchestrator
from .core.serving_engine import ModelServingEngine
from .core.monitoring import ModelMonitor
from .core.marketplace import ModelMarketplace

# Import modules
from .modules.federated_learning import FederatedLearningCoordinator
from .modules.neuromorphic import NeuromorphicEngine
from .modules.mlops import MLOpsManager
from .modules.automl import AutoMLEngine

# Import API routers
from .api import (
    models,
    training,
    serving,
    federated,
    neuromorphic,
    marketplace,
    monitoring,
    features,
    experiments
)

# Import event handlers
from .event_handlers import UnifiedMLEventHandler

logger = logging.getLogger(__name__)

# Global instances
model_registry: Optional[UnifiedModelRegistry] = None
feature_store: Optional[FeatureStore] = None
training_orchestrator: Optional[TrainingOrchestrator] = None
serving_engine: Optional[ModelServingEngine] = None
model_monitor: Optional[ModelMonitor] = None
marketplace: Optional[ModelMarketplace] = None
federated_coordinator: Optional[FederatedLearningCoordinator] = None
neuromorphic_engine: Optional[NeuromorphicEngine] = None
mlops_manager: Optional[MLOpsManager] = None
automl_engine: Optional[AutoMLEngine] = None
event_handler: Optional[UnifiedMLEventHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global model_registry, feature_store, training_orchestrator, serving_engine
    global model_monitor, marketplace, federated_coordinator, neuromorphic_engine
    global mlops_manager, automl_engine, event_handler
    
    # Startup
    logger.info("Starting Unified ML Platform Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize unified model registry (combines MLflow + custom features)
    model_registry = UnifiedModelRegistry(
        mlflow_url=config.get("mlflow_url", "http://mlflow-server:5000"),
        ignite_host=config.get("ignite_host", "ignite"),
        ignite_port=int(config.get("ignite_port", 10800))
    )
    await model_registry.initialize()
    app.state.model_registry = model_registry
    
    # Initialize feature store (Feast with Ignite backend)
    feature_store = FeatureStore(
        ignite_host=config.get("ignite_host", "ignite"),
        ignite_port=int(config.get("ignite_port", 10800)),
        feast_repo_path=config.get("feast_repo_path", "/feast"),
        enable_online_store=True,
        enable_offline_store=True
    )
    await feature_store.initialize()
    app.state.feature_store = feature_store
    
    # Initialize training orchestrator (Spark + Kubernetes)
    training_orchestrator = TrainingOrchestrator(
        model_registry=model_registry,
        feature_store=feature_store,
        spark_master=config.get("spark_master", "spark://spark-master:7077"),
        k8s_namespace=config.get("k8s_namespace", "ml-training"),
        max_concurrent_jobs=int(config.get("max_concurrent_training_jobs", 10))
    )
    await training_orchestrator.initialize()
    app.state.training_orchestrator = training_orchestrator
    
    # Initialize model serving engine (multi-framework support)
    serving_engine = ModelServingEngine(
        model_registry=model_registry,
        feature_store=feature_store,
        triton_url=config.get("triton_url", "triton:8000"),
        torch_serve_url=config.get("torch_serve_url", "torchserve:8080"),
        tf_serving_url=config.get("tf_serving_url", "tensorflow-serving:8501"),
        knative_enabled=config.get("knative_enabled", True)
    )
    await serving_engine.initialize()
    app.state.serving_engine = serving_engine
    
    # Initialize model monitor
    model_monitor = ModelMonitor(
        model_registry=model_registry,
        serving_engine=serving_engine,
        monitoring_interval=int(config.get("monitoring_interval", 300)),
        drift_detection_enabled=True,
        performance_tracking_enabled=True
    )
    await model_monitor.initialize()
    app.state.model_monitor = model_monitor
    
    # Initialize model marketplace
    marketplace = ModelMarketplace(
        model_registry=model_registry,
        blockchain_gateway_url=config.get("blockchain_gateway_url", "http://blockchain-gateway-service:8000"),
        derivatives_engine_url=config.get("derivatives_engine_url", "http://derivatives-engine-service:8000")
    )
    await marketplace.initialize()
    app.state.marketplace = marketplace
    
    # Initialize federated learning coordinator
    federated_coordinator = FederatedLearningCoordinator(
        model_registry=model_registry,
        feature_store=feature_store,
        ignite_host=config.get("ignite_host", "ignite"),
        verifiable_credential_service_url=config.get("vc_service_url", "http://verifiable-credential-service:8000")
    )
    await federated_coordinator.initialize()
    app.state.federated_coordinator = federated_coordinator
    
    # Initialize neuromorphic engine
    neuromorphic_engine = NeuromorphicEngine(
        model_registry=model_registry,
        spike_threshold=float(config.get("spike_threshold", 1.0)),
        learning_rate=float(config.get("stdp_learning_rate", 0.01))
    )
    await neuromorphic_engine.initialize()
    app.state.neuromorphic_engine = neuromorphic_engine
    
    # Initialize MLOps manager
    mlops_manager = MLOpsManager(
        model_registry=model_registry,
        training_orchestrator=training_orchestrator,
        serving_engine=serving_engine,
        enable_auto_retraining=config.get("enable_auto_retraining", True),
        enable_ab_testing=config.get("enable_ab_testing", True)
    )
    await mlops_manager.initialize()
    app.state.mlops_manager = mlops_manager
    
    # Initialize AutoML engine
    automl_engine = AutoMLEngine(
        training_orchestrator=training_orchestrator,
        model_registry=model_registry,
        optimization_metric=config.get("automl_metric", "accuracy"),
        time_budget=int(config.get("automl_time_budget", 3600))
    )
    await automl_engine.initialize()
    app.state.automl_engine = automl_engine
    
    # Initialize event handler
    event_handler = UnifiedMLEventHandler(
        service_name="unified-ml-platform-service",
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        metrics=EventMetrics("unified-ml-platform-service"),
        model_registry=model_registry,
        training_orchestrator=training_orchestrator,
        serving_engine=serving_engine,
        federated_coordinator=federated_coordinator,
        mlops_manager=mlops_manager
    )
    await event_handler.initialize()
    
    # Register event handlers for all ML-related events
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/ml-training-requests",
        handler=event_handler.handle_training_request,
        subscription_name="unified-ml-training-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/ml-inference-requests",
        handler=event_handler.handle_inference_request,
        subscription_name="unified-ml-inference-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/federated-learning-events",
        handler=event_handler.handle_federated_event,
        subscription_name="unified-ml-federated-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/model-retraining-requests",
        handler=event_handler.handle_retraining_request,
        subscription_name="unified-ml-retraining-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/anomaly-detected-events",
        handler=event_handler.handle_anomaly_detection,
        subscription_name="unified-ml-anomaly-sub"
    )
    
    # Start background tasks
    await event_handler.start()
    asyncio.create_task(model_monitor.start_monitoring())
    asyncio.create_task(training_orchestrator.process_training_queue())
    asyncio.create_task(federated_coordinator.coordinate_rounds())
    asyncio.create_task(mlops_manager.monitor_deployments())
    asyncio.create_task(neuromorphic_engine.process_spike_events())
    
    logger.info("Unified ML Platform Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Unified ML Platform Service...")
    
    # Stop all components
    if event_handler:
        await event_handler.stop()
        
    if model_monitor:
        await model_monitor.stop()
        
    if training_orchestrator:
        await training_orchestrator.shutdown()
        
    if federated_coordinator:
        await federated_coordinator.shutdown()
        
    if serving_engine:
        await serving_engine.shutdown()
        
    if neuromorphic_engine:
        await neuromorphic_engine.shutdown()
        
    if mlops_manager:
        await mlops_manager.shutdown()
        
    if automl_engine:
        await automl_engine.shutdown()
        
    if feature_store:
        await feature_store.close()
        
    if model_registry:
        await model_registry.close()
        
    if marketplace:
        await marketplace.close()
    
    logger.info("Unified ML Platform Service shutdown complete")


# Create app
app = create_base_app(
    service_name="unified-ml-platform-service",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(models.router, prefix="/api/v1/models", tags=["models"])
app.include_router(training.router, prefix="/api/v1/training", tags=["training"])
app.include_router(serving.router, prefix="/api/v1/serving", tags=["serving"])
app.include_router(federated.router, prefix="/api/v1/federated", tags=["federated"])
app.include_router(neuromorphic.router, prefix="/api/v1/neuromorphic", tags=["neuromorphic"])
app.include_router(marketplace.router, prefix="/api/v1/marketplace", tags=["marketplace"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
app.include_router(features.router, prefix="/api/v1/features", tags=["features"])
app.include_router(experiments.router, prefix="/api/v1/experiments", tags=["experiments"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "unified-ml-platform-service",
        "version": "2.0.0",
        "status": "operational",
        "description": "Unified machine learning platform for PlatformQ",
        "capabilities": {
            "model_registry": {
                "backend": "mlflow",
                "versioning": True,
                "staging": True,
                "production": True
            },
            "feature_store": {
                "backend": "feast-with-ignite",
                "online_store": True,
                "offline_store": True,
                "streaming": True
            },
            "training": {
                "distributed": True,
                "frameworks": ["pytorch", "tensorflow", "scikit-learn", "xgboost"],
                "hyperparameter_optimization": True,
                "automl": True
            },
            "serving": {
                "frameworks": ["triton", "torchserve", "tensorflow-serving", "knative"],
                "ab_testing": True,
                "canary_deployment": True,
                "multi_model": True
            },
            "federated_learning": {
                "privacy_preserving": True,
                "secure_aggregation": True,
                "differential_privacy": True,
                "verifiable_credentials": True
            },
            "neuromorphic": {
                "spiking_networks": True,
                "event_driven": True,
                "online_learning": True,
                "low_latency": True
            },
            "marketplace": {
                "model_trading": True,
                "licensing": True,
                "royalties": True,
                "blockchain_integration": True
            },
            "monitoring": {
                "drift_detection": True,
                "performance_tracking": True,
                "explainability": True,
                "alerts": True
            }
        }
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Comprehensive health check for all ML platform components"""
    health_status = {
        "status": "healthy",
        "components": {}
    }
    
    # Check model registry
    if model_registry:
        try:
            await model_registry.health_check()
            health_status["components"]["model_registry"] = "healthy"
        except Exception as e:
            health_status["components"]["model_registry"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check feature store
    if feature_store:
        try:
            await feature_store.health_check()
            health_status["components"]["feature_store"] = "healthy"
        except Exception as e:
            health_status["components"]["feature_store"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check training orchestrator
    if training_orchestrator:
        try:
            await training_orchestrator.health_check()
            health_status["components"]["training_orchestrator"] = "healthy"
        except Exception as e:
            health_status["components"]["training_orchestrator"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check serving engine
    if serving_engine:
        try:
            await serving_engine.health_check()
            health_status["components"]["serving_engine"] = "healthy"
        except Exception as e:
            health_status["components"]["serving_engine"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    return health_status 