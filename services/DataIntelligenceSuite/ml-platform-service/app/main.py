"""
ML Platform Service - Main Application

Unified ML platform using the migrated architecture.
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    StructuredLogger
)

from .core.base import MLPlatformConfig, MLPlatformService
from .api.v1 import training_router, serving_router, models_router, experiments_router

# Setup logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="ml-platform-service",
    version="3.0.0",
    description="Unified ML platform for model training, serving, MLOps, and federated learning",
    dependencies=["mlflow", "triton", "ray", "ignite", "pulsar", "minio"],
    health_checks=["model_registry", "serving_engine", "training_engine"],
    capabilities=[
        "model-training", "distributed-training", "federated-learning",
        "model-serving", "online-inference", "batch-inference",
        "experiment-tracking", "model-versioning", "drift-detection",
        "automl", "hyperparameter-tuning", "feature-engineering"
    ],
    data_sources=["feature-store", "data-platform", "streaming"],
    data_outputs=["models", "predictions", "metrics", "experiments"]
)

# Global service instance
ml_service: MLPlatformService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global ml_service
    
    logger.info("Starting ML Platform Service", version=SERVICE_METADATA.version)
    
    # Create service configuration
    config = MLPlatformConfig(
        name=SERVICE_METADATA.name,
        version=SERVICE_METADATA.version,
        
        # ML settings
        model_registry_url=os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
        experiment_tracking_enabled=os.getenv("EXPERIMENT_TRACKING", "true").lower() == "true",
        auto_versioning=os.getenv("AUTO_VERSIONING", "true").lower() == "true",
        
        # Training settings
        default_training_mode=os.getenv("DEFAULT_TRAINING_MODE", "auto"),
        distributed_framework=os.getenv("DISTRIBUTED_FRAMEWORK", "ray"),
        gpu_enabled=os.getenv("GPU_ENABLED", "true").lower() == "true",
        
        # Serving settings
        enable_model_serving=os.getenv("ENABLE_MODEL_SERVING", "true").lower() == "true",
        serving_framework=os.getenv("SERVING_FRAMEWORK", "triton"),
        default_replicas=int(os.getenv("DEFAULT_REPLICAS", "2")),
        autoscaling_enabled=os.getenv("AUTOSCALING_ENABLED", "true").lower() == "true",
        
        # Feature store
        enable_feature_store=os.getenv("ENABLE_FEATURE_STORE", "true").lower() == "true",
        feature_store_backend=os.getenv("FEATURE_STORE_BACKEND", "feast"),
        
        # Monitoring
        enable_model_monitoring=os.getenv("ENABLE_MODEL_MONITORING", "true").lower() == "true",
        drift_detection_enabled=os.getenv("DRIFT_DETECTION", "true").lower() == "true",
        performance_tracking_enabled=os.getenv("PERFORMANCE_TRACKING", "true").lower() == "true",
        
        # Storage
        model_storage_path=os.getenv("MODEL_STORAGE_PATH", "/models"),
        artifact_storage=os.getenv("ARTIFACT_STORAGE", "minio")
    )
    
    # Initialize service
    ml_service = MLPlatformService(config)
    await ml_service.start()
    
    # Set service instance in routers
    training_router.set_service(ml_service)
    serving_router.set_service(ml_service)
    models_router.set_service(ml_service)
    experiments_router.set_service(ml_service)
    
    yield
    
    # Shutdown
    logger.info("Shutting down ML Platform Service")
    await ml_service.stop()


# Create FastAPI app
app = create_data_intelligence_app(
    service_metadata=SERVICE_METADATA,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(training_router, prefix="/api/v1/training", tags=["Training"])
app.include_router(serving_router, prefix="/api/v1/serving", tags=["Serving"])
app.include_router(models_router, prefix="/api/v1/models", tags=["Models"])
app.include_router(experiments_router, prefix="/api/v1/experiments", tags=["Experiments"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "status": "operational",
        "description": SERVICE_METADATA.description,
        "capabilities": SERVICE_METADATA.capabilities,
        "endpoints": {
            "training": "/api/v1/training",
            "serving": "/api/v1/serving",
            "models": "/api/v1/models",
            "experiments": "/api/v1/experiments",
            "health": "/health",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("SERVICE_PORT", "8030"))
    reload = os.getenv("ENVIRONMENT", "development") == "development"
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=reload
    ) 