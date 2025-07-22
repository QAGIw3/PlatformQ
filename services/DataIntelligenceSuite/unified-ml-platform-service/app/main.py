"""
Unified ML Platform Service

Comprehensive machine learning platform consolidating:
- Model training, serving, and lifecycle management
- MLOps capabilities
- Federated learning capabilities
- Neuromorphic computing
- Feature store and model registry
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
import asyncio
import os
from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from platformq_shared import create_base_app, ConfigLoader
from platformq_event_framework import BaseEventProcessor, EventMetrics

from .vault_consul_integration import VaultConsulIntegration
from .core.config import settings
from .core.model_registry import UnifiedModelRegistry
from .core.training_orchestrator import TrainingOrchestrator
from .core.serving_engine import ModelServingEngine
from .core.monitoring import ModelMonitor

# Import modules
from .modules.federated_learning import FederatedLearningCoordinator
from .modules.mlops import MLOpsManager
from .modules.automl import AutoMLEngine

# Import API routers
from .api import (
    models,
    training,
    serving,
    federated,
    monitoring,
    experiments
)

# Import ML lineage module
from .ml_lineage import MLModelLineageTracker, ml_lineage_router

# Import event handlers
from .event_handlers import UnifiedMLEventHandler
from .integrations.event_driven_ml import EventDrivenMLIntegration, MLEventType

logger = logging.getLogger(__name__)

# Global instances
vault_consul: Optional[VaultConsulIntegration] = None
model_registry: Optional[UnifiedModelRegistry] = None
training_orchestrator: Optional[TrainingOrchestrator] = None
serving_engine: Optional[ModelServingEngine] = None
model_monitor: Optional[ModelMonitor] = None
federated_coordinator: Optional[FederatedLearningCoordinator] = None
mlops_manager: Optional[MLOpsManager] = None
automl_engine: Optional[AutoMLEngine] = None
event_handler: Optional[UnifiedMLEventHandler] = None
event_driven_ml: Optional[EventDrivenMLIntegration] = None


async def get_vault_consul() -> VaultConsulIntegration:
    """Dependency to get Vault/Consul integration"""
    if not vault_consul:
        raise HTTPException(status_code=500, detail="Vault/Consul integration not initialized")
    return vault_consul


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global vault_consul, model_registry, training_orchestrator, serving_engine
    global model_monitor, federated_coordinator
    global mlops_manager, automl_engine, event_handler, ml_lineage_tracker
    
    # Startup
    logger.info("Starting Unified ML Platform Service...")
    
    # Initialize Vault and Consul integration first
    vault_consul = VaultConsulIntegration()
    await vault_consul.initialize()
    
    # Get configurations from Vault and Consul
    mlflow_config = await vault_consul.get_mlflow_config()
    training_config = await vault_consul.get_training_config()
    serving_config = await vault_consul.get_serving_config()
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize unified model registry with Vault credentials
    model_registry = UnifiedModelRegistry(
        mlflow_url=mlflow_config['tracking_uri'],
        database_uri=mlflow_config['database_uri'],
        api_keys=mlflow_config['api_keys'],
        encryption_key=mlflow_config['encryption_key'],
        ignite_host=config.get("ignite_host", "ignite"),
        ignite_port=int(config.get("ignite_port", 10800))
    )
    await model_registry.initialize()
    app.state.model_registry = model_registry
    
    # Feature store has been extracted to a separate service
    
    # Initialize training orchestrator with credentials
    training_orchestrator = TrainingOrchestrator(
        model_registry=model_registry,
        training_config=training_config,
        vault_integration=vault_consul
    )
    await training_orchestrator.initialize()
    app.state.training_orchestrator = training_orchestrator
    
    # Initialize model serving engine with serving credentials
    serving_engine = ModelServingEngine(
        model_registry=model_registry,
        serving_config=serving_config,
        vault_integration=vault_consul
    )
    await serving_engine.initialize()
    app.state.serving_engine = serving_engine
    
    # Initialize model monitor
    model_monitor = ModelMonitor(
        model_registry=model_registry,
        serving_engine=serving_engine,
        monitoring_interval=int(config.get("monitoring_interval", 300)),
        drift_detection_enabled=True,
        performance_tracking_enabled=True,
        consul_client=vault_consul.consul_client
    )
    await model_monitor.initialize()
    app.state.model_monitor = model_monitor
    
    # Model marketplace has been extracted to a separate service
    
    # Initialize federated learning coordinator with certificates
    federated_coordinator = FederatedLearningCoordinator(
        model_registry=model_registry,
        vault_integration=vault_consul,
        ignite_host=config.get("ignite_host", "ignite"),
        verifiable_credential_service_url=config.get("vc_service_url", "http://verifiable-credential-service:8000")
    )
    await federated_coordinator.initialize()
    app.state.federated_coordinator = federated_coordinator
    
    # Neuromorphic engine has been extracted to a separate service
    
    # Initialize MLOps manager
    mlops_manager = MLOpsManager(
        model_registry=model_registry,
        training_orchestrator=training_orchestrator,
        serving_engine=serving_engine,
        vault_integration=vault_consul,
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
    
    # Initialize ML Model Lineage Tracker
    # Check if JanusGraph is available from configuration
    janusgraph_config = await vault_consul._get_consul_config("janusgraph-config")
    if janusgraph_config and janusgraph_config.get('enabled', False):
        from .db.janusgraph import JanusGraph
        graph_db = JanusGraph()
        graph_db.connect()
        
        ml_lineage_tracker = MLModelLineageTracker(graph_db)
        app.state.ml_lineage_tracker = ml_lineage_tracker
        logger.info("ML Lineage Tracker initialized with JanusGraph")
    else:
        logger.warning("JanusGraph not configured, ML lineage tracking disabled")
        app.state.ml_lineage_tracker = None
    
    # Initialize event handler
    event_handler = UnifiedMLEventHandler(
        service_name="unified-ml-platform-service",
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        metrics=EventMetrics("unified-ml-platform-service"),
        model_registry=model_registry,
        training_orchestrator=training_orchestrator,
        serving_engine=serving_engine,
        federated_coordinator=federated_coordinator,
        mlops_manager=mlops_manager,
        vault_integration=vault_consul
    )
    await event_handler.initialize()

    # Initialize event-driven ML integration
    event_driven_ml = EventDrivenMLIntegration(vault_consul_integration=vault_consul)
    await event_driven_ml.initialize()

    # Hook event-driven integration into training orchestrator
    async def on_training_started(training_job):
        await event_driven_ml.publish_ml_event(
            MLEventType.TRAINING_STARTED,
            {
                "model_metadata": {
                    "model_id": training_job.get("model_id"),
                    "model_name": training_job.get("name"),
                    "algorithm": training_job.get("algorithm"),
                    "framework": training_job.get("framework")
                },
                "training_id": training_job.get("training_id"),
                "dataset_id": training_job.get("dataset_id")
            }
        )

    async def on_training_completed(training_job, metrics):
        await event_driven_ml.publish_ml_event(
            MLEventType.TRAINING_COMPLETED,
            {
                "model_metadata": {
                    "model_id": training_job.get("model_id"),
                    "model_name": training_job.get("name"),
                    "version": training_job.get("version"),
                    "algorithm": training_job.get("algorithm"),
                    "framework": training_job.get("framework"),
                    "metrics": metrics,
                    "parameters": training_job.get("parameters", {}),
                    "dataset_id": training_job.get("dataset_id"),
                    "experiment_id": training_job.get("experiment_id")
                },
                "training_id": training_job.get("training_id"),
                "duration_seconds": training_job.get("duration_seconds"),
                "resource_usage": training_job.get("resource_usage", {})
            }
        )

    # Set event handlers on training orchestrator
    training_orchestrator.on_training_started = on_training_started
    training_orchestrator.on_training_completed = on_training_completed

    # Hook into model registry for model events
    async def on_model_registered(model_info):
        await event_driven_ml.publish_ml_event(
            MLEventType.MODEL_REGISTERED,
            {
                "model_id": model_info.get("model_id"),
                "model_metadata": model_info
            }
        )

    model_registry.on_model_registered = on_model_registered

    # Hook into model monitor for drift detection
    async def on_drift_detected(drift_info):
        await event_driven_ml.publish_ml_event(
            MLEventType.DRIFT_DETECTED,
            drift_info
        )

    model_monitor.on_drift_detected = on_drift_detected
    
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
        
    if vault_consul:
        await vault_consul.close()
    
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

# Include routers with Vault/Consul dependency
app.include_router(models.router, prefix="/api/v1/models", tags=["models"])
app.include_router(training.router, prefix="/api/v1/training", tags=["training"])
app.include_router(serving.router, prefix="/api/v1/serving", tags=["serving"])
app.include_router(federated.router, prefix="/api/v1/federated", tags=["federated"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
app.include_router(experiments.router, prefix="/api/v1/experiments", tags=["experiments"])
app.include_router(ml_lineage_router, tags=["ml-lineage"])

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
                "production": True,
                "encryption": True
            },
            "feature_store": {
                "backend": "feast-with-ignite",
                "online_store": True,
                "offline_store": True,
                "streaming": True,
                "encrypted_features": True
            },
            "training": {
                "distributed": True,
                "frameworks": ["pytorch", "tensorflow", "scikit-learn", "xgboost"],
                "hyperparameter_optimization": True,
                "automl": True,
                "secure_credentials": True
            },
            "serving": {
                "frameworks": ["triton", "torchserve", "tensorflow-serving", "knative"],
                "ab_testing": True,
                "canary_deployment": True,
                "multi_model": True,
                "api_key_protected": True
            },
            "federated_learning": {
                "privacy_preserving": True,
                "secure_aggregation": True,
                "differential_privacy": True,
                "verifiable_credentials": True,
                "certificate_based_auth": True
            },
            "neuromorphic": {
                "spiking_networks": True,
                "event_driven": True,
                "online_learning": True,
                "low_latency": True
            },
            "monitoring": {
                "drift_detection": True,
                "performance_tracking": True,
                "explainability": True,
                "alerts": True,
                "consul_based_config": True
            },
            "security": {
                "vault_integration": True,
                "consul_service_mesh": True,
                "encrypted_artifacts": True,
                "dynamic_credentials": True
            }
        }
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "unified-ml-platform-service",
        "timestamp": datetime.utcnow().isoformat()
    }


# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Get service metrics"""
    return {
        "models_registered": await model_registry.get_model_count() if model_registry else 0,
        "active_training_jobs": training_orchestrator.get_active_jobs_count() if training_orchestrator else 0,
        "models_serving": serving_engine.get_serving_count() if serving_engine else 0,
        "federated_rounds": federated_coordinator.get_total_rounds() if federated_coordinator else 0
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8015) 