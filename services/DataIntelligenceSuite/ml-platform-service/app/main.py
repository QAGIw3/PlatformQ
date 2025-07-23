"""
ML Platform Service

Comprehensive machine learning platform for model training, serving, and lifecycle management
"""
import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService,
    VaultConsulIntegration,
    EventBus,
    StructuredLogger
)

# Import engines
from .engines.training import TrainingOrchestrator, DistributedTrainer
from .engines.serving import ServingEngine
from .engines.mlops import MLOpsManager, ModelMonitor, DriftDetector
from .engines.federated import (
    FederatedCoordinator, ClientManager, FedAvg, DifferentialPrivacy
)
from .engines.automl import (
    AutoMLEngine, ModelSearch, HyperparameterTuner, FeatureEngineer
)

# Import API routers
from .api import training, serving, mlops, federated, automl

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="ml-platform-service",
    version="2.0.0",
    description="Unified ML platform for training, serving, MLOps, and federated learning",
    dependencies=["vault", "consul", "pulsar", "mlflow", "ignite"],
    health_checks=["mlflow", "training", "serving"]
)

logger = StructuredLogger.get_logger(__name__)

# Global instances
vault_consul: Optional[VaultConsulIntegration] = None
event_bus: Optional[EventBus] = None
training_orchestrator: Optional[TrainingOrchestrator] = None
serving_engine: Optional[ServingEngine] = None
mlops_manager: Optional[MLOpsManager] = None
federated_coordinator: Optional[FederatedCoordinator] = None
automl_engine: Optional[AutoMLEngine] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global vault_consul, event_bus, training_orchestrator, serving_engine
    global mlops_manager, federated_coordinator, automl_engine
    
    logger.info("Starting ML Platform Service...")
    
    try:
        # Initialize Vault and Consul
        vault_consul = VaultConsulIntegration()
        await vault_consul.initialize()
        
        # Initialize event bus
        event_bus = EventBus()
        await event_bus.initialize()
        
        # Initialize model registry (placeholder)
        model_registry = {}  # This would be the actual model registry
        
        # Initialize training components
        distributed_trainer = DistributedTrainer()
        training_orchestrator = TrainingOrchestrator(
            vault_consul, event_bus, model_registry, distributed_trainer
        )
        await training_orchestrator.initialize()
        
        # Initialize serving engine
        serving_engine = ServingEngine(vault_consul, event_bus, model_registry)
        await serving_engine.initialize()
        
        # Initialize MLOps components
        model_monitor = ModelMonitor()
        drift_detector = DriftDetector()
        mlops_manager = MLOpsManager(
            vault_consul, event_bus, model_registry, model_monitor, drift_detector
        )
        await mlops_manager.initialize()
        
        # Initialize federated learning
        client_manager = ClientManager()
        aggregation_strategy = FedAvg()
        privacy_mechanism = DifferentialPrivacy()
        federated_coordinator = FederatedCoordinator(
            vault_consul, event_bus, model_registry, client_manager,
            aggregation_strategy, privacy_mechanism
        )
        await federated_coordinator.initialize()
        
        # Initialize AutoML
        model_search = ModelSearch()
        hyperparameter_tuner = HyperparameterTuner()
        feature_engineer = FeatureEngineer()
        automl_engine = AutoMLEngine(
            vault_consul, event_bus, model_search, hyperparameter_tuner,
            feature_engineer, training_orchestrator
        )
        await automl_engine.initialize()
        
        logger.info("ML Platform Service initialized successfully")
        
        yield
        
    finally:
        # Cleanup
        logger.info("Shutting down ML Platform Service...")
        
        if training_orchestrator:
            await training_orchestrator.cleanup()
        if serving_engine:
            await serving_engine.cleanup()
        if mlops_manager:
            await mlops_manager.cleanup()
        if federated_coordinator:
            await federated_coordinator.cleanup()
        if automl_engine:
            await automl_engine.cleanup()
        if event_bus:
            await event_bus.cleanup()
        if vault_consul:
            await vault_consul.cleanup()


# Create app
app = create_data_intelligence_app(SERVICE_METADATA, lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(training.router, prefix="/api/v1/training", tags=["training"])
app.include_router(serving.router, prefix="/api/v1/serving", tags=["serving"])
app.include_router(mlops.router, prefix="/api/v1/mlops", tags=["mlops"])
app.include_router(federated.router, prefix="/api/v1/federated", tags=["federated"])
app.include_router(automl.router, prefix="/api/v1/automl", tags=["automl"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "status": "operational",
        "description": SERVICE_METADATA.description,
        "capabilities": {
            "training": {
                "distributed": True,
                "frameworks": ["pytorch", "tensorflow", "scikit-learn", "xgboost"],
                "hyperparameter_optimization": True,
                "experiment_tracking": True
            },
            "serving": {
                "frameworks": ["triton", "torchserve", "tensorflow-serving", "kserve"],
                "auto_scaling": True,
                "ab_testing": True,
                "multi_model": True
            },
            "mlops": {
                "model_versioning": True,
                "drift_detection": True,
                "automated_retraining": True,
                "model_governance": True
            },
            "federated_learning": {
                "privacy_preserving": True,
                "secure_aggregation": True,
                "differential_privacy": True,
                "multiple_strategies": ["fedavg", "fedprox", "scaffold"]
            },
            "automl": {
                "automated_model_selection": True,
                "hyperparameter_tuning": True,
                "feature_engineering": True,
                "ensemble_methods": True
            }
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 