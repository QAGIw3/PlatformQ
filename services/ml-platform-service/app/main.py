"""
ML Platform Service

Unified machine learning platform consolidating model training, serving,
monitoring, and federated learning capabilities.
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
from .core.model_registry import ModelRegistry
from .core.feature_store import FeatureStore
from .core.model_serving import ModelServingEngine
from .core.training_orchestrator import TrainingOrchestrator
from .core.federated_learning import FederatedLearningCoordinator
from .core.neuromorphic_engine import NeuromorphicEngine
from .core.monitoring import ModelMonitor
from .api import models, training, serving, federated, monitoring
from .event_handlers import MLEventHandler

logger = logging.getLogger(__name__)

# Global instances
model_registry: Optional[ModelRegistry] = None
feature_store: Optional[FeatureStore] = None
serving_engine: Optional[ModelServingEngine] = None
training_orchestrator: Optional[TrainingOrchestrator] = None
federated_coordinator: Optional[FederatedLearningCoordinator] = None
neuromorphic_engine: Optional[NeuromorphicEngine] = None
model_monitor: Optional[ModelMonitor] = None
event_handler: Optional[MLEventHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global model_registry, feature_store, serving_engine, training_orchestrator
    global federated_coordinator, neuromorphic_engine, model_monitor, event_handler
    
    # Startup
    logger.info("Starting ML Platform Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize model registry (using MLflow)
    model_registry = ModelRegistry(
        mlflow_url=config.get("mlflow_url", "http://mlflow-server:5000"),
        default_experiment=config.get("default_experiment", "platformq")
    )
    await model_registry.initialize()
    app.state.model_registry = model_registry
    
    # Initialize feature store (using Feast with Ignite backend)
    feature_store = FeatureStore(
        ignite_host=config.get("ignite_host", "ignite"),
        ignite_port=int(config.get("ignite_port", 10800)),
        feast_repo_path=config.get("feast_repo_path", "/feast")
    )
    await feature_store.initialize()
    app.state.feature_store = feature_store
    
    # Initialize model serving engine
    serving_engine = ModelServingEngine(
        model_registry=model_registry,
        feature_store=feature_store,
        triton_url=config.get("triton_url", "triton:8000"),
        torch_serve_url=config.get("torch_serve_url", "torchserve:8080"),
        tf_serving_url=config.get("tf_serving_url", "tensorflow-serving:8501")
    )
    await serving_engine.initialize()
    app.state.serving_engine = serving_engine
    
    # Initialize training orchestrator
    training_orchestrator = TrainingOrchestrator(
        model_registry=model_registry,
        feature_store=feature_store,
        spark_master=config.get("spark_master", "spark://spark-master:7077"),
        max_concurrent_jobs=int(config.get("max_concurrent_training_jobs", 10))
    )
    await training_orchestrator.initialize()
    app.state.training_orchestrator = training_orchestrator
    
    # Initialize federated learning coordinator
    federated_coordinator = FederatedLearningCoordinator(
        model_registry=model_registry,
        flower_server_address=config.get("flower_server", "0.0.0.0:8080"),
        syft_network_url=config.get("syft_network_url")
    )
    await federated_coordinator.initialize()
    app.state.federated_coordinator = federated_coordinator
    
    # Initialize neuromorphic engine
    neuromorphic_engine = NeuromorphicEngine(
        model_registry=model_registry,
        nengo_backend=config.get("nengo_backend", "cpu")
    )
    await neuromorphic_engine.initialize()
    app.state.neuromorphic_engine = neuromorphic_engine
    
    # Initialize model monitor
    model_monitor = ModelMonitor(
        model_registry=model_registry,
        serving_engine=serving_engine,
        monitoring_interval=int(config.get("monitoring_interval", 300))  # 5 minutes
    )
    await model_monitor.initialize()
    app.state.model_monitor = model_monitor
    
    # Initialize event handler
    event_handler = MLEventHandler(
        service_name="ml-platform-service",
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        metrics=EventMetrics("ml-platform-service"),
        model_registry=model_registry,
        training_orchestrator=training_orchestrator,
        serving_engine=serving_engine
    )
    await event_handler.initialize()
    
    # Register event handlers
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/ml-training-requests",
        handler=event_handler.handle_training_request,
        subscription_name="ml-platform-training-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/ml-inference-requests",
        handler=event_handler.handle_inference_request,
        subscription_name="ml-platform-inference-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/federated-learning-events",
        handler=event_handler.handle_federated_event,
        subscription_name="ml-platform-federated-sub"
    )
    
    # Start background tasks
    await event_handler.start()
    asyncio.create_task(model_monitor.start_monitoring())
    asyncio.create_task(training_orchestrator.process_training_queue())
    asyncio.create_task(federated_coordinator.coordinate_rounds())
    
    logger.info("ML Platform Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down ML Platform Service...")
    
    # Stop components
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
        
    if feature_store:
        await feature_store.close()
        
    if model_registry:
        await model_registry.close()
    
    logger.info("ML Platform Service shutdown complete")


# Create app
app = create_base_app(
    service_name="ml-platform-service",
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
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "ml-platform-service",
        "version": "1.0.0",
        "status": "operational",
        "capabilities": {
            "model_registry": "mlflow",
            "feature_store": "feast-with-ignite",
            "serving": ["triton", "torchserve", "tensorflow-serving"],
            "training": ["distributed", "hyperparameter-optimization", "automl"],
            "federated_learning": ["flower", "syft"],
            "neuromorphic": ["spiking-networks", "event-driven"],
            "monitoring": ["drift-detection", "performance-tracking", "explainability"]
        }
    } 