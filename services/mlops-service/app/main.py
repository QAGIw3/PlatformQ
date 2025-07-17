"""
MLOps Service

Centralized service for ML model lifecycle management, including:
- Model registry integration
- Deployment orchestration  
- Monitoring configuration
- A/B testing management
- Retraining coordination
"""

import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import asyncio

from fastapi import Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import mlflow
from mlflow.tracking import MlflowClient

from platformq.shared.base_service import create_base_app
from platformq.shared.event_publisher import EventPublisher
from platformq_shared.jwt import get_current_tenant_and_user

from .feedback_loop import FeedbackLoopManager, ABTestManager
from .model_registry import ModelRegistryManager
from .deployment_manager import DeploymentManager
from .monitoring_config import MonitoringConfigManager

logger = logging.getLogger(__name__)

# Initialize base app
app = create_base_app(service_name="mlops-service")

# Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
FUNCTIONS_SERVICE_URL = os.getenv("FUNCTIONS_SERVICE_URL", "http://functions-service:80")
WORKFLOW_SERVICE_URL = os.getenv("WORKFLOW_SERVICE_URL", "http://workflow-service:80")

# Initialize MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow_client = MlflowClient()

# Initialize services
event_publisher = EventPublisher(pulsar_url=PULSAR_URL)
feedback_loop_manager = None
ab_test_manager = None
model_registry_manager = None
deployment_manager = None
monitoring_config_manager = None


# Pydantic models
class ModelRegistrationRequest(BaseModel):
    """Request to register a model"""
    model_name: str = Field(..., description="Name of the model")
    run_id: str = Field(..., description="MLflow run ID")
    description: Optional[str] = Field(None, description="Model description")
    tags: Dict[str, str] = Field(default_factory=dict, description="Model tags")
    stage: Optional[str] = Field("None", description="Initial stage: None, Staging, Production")


class ModelDeploymentRequest(BaseModel):
    """Request to deploy a model"""
    model_name: str = Field(..., description="Name of the model")
    version: Optional[str] = Field(None, description="Model version (latest if not specified)")
    environment: str = Field(..., description="Target environment: staging, production")
    deployment_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "replicas": 2,
            "cpu": "1000m",
            "memory": "2Gi",
            "autoscaling": {"min": 1, "max": 10, "target_cpu": 80}
        }
    )
    canary_config: Optional[Dict[str, Any]] = Field(
        None,
        description="Canary deployment configuration"
    )


class MonitoringConfigRequest(BaseModel):
    """Request to configure model monitoring"""
    model_name: str = Field(..., description="Name of the model")
    version: str = Field(..., description="Model version")
    monitoring_config: Dict[str, Any] = Field(..., description="Monitoring configuration")


class ABTestRequest(BaseModel):
    """Request to start A/B test"""
    model_name: str = Field(..., description="Name of the model")
    version_a: str = Field(..., description="Control version")
    version_b: str = Field(..., description="Test version")
    traffic_split: float = Field(0.1, description="Traffic percentage for version B")
    duration_hours: int = Field(24, description="Test duration in hours")
    success_metrics: Dict[str, float] = Field(
        default_factory=lambda: {
            "min_accuracy_improvement": 0.02,
            "max_latency_increase_ms": 50
        }
    )


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global feedback_loop_manager, ab_test_manager, model_registry_manager
    global deployment_manager, monitoring_config_manager
    
    # Connect event publisher
    event_publisher.connect()
    
    # Initialize managers
    model_registry_manager = ModelRegistryManager(mlflow_client, event_publisher)
    deployment_manager = DeploymentManager(FUNCTIONS_SERVICE_URL, mlflow_client)
    monitoring_config_manager = MonitoringConfigManager()
    ab_test_manager = ABTestManager(monitoring_service_url="http://monitoring-service:80")
    
    feedback_loop_manager = FeedbackLoopManager(
        pulsar_url=PULSAR_URL,
        mlflow_url=MLFLOW_TRACKING_URI,
        workflow_service_url=WORKFLOW_SERVICE_URL,
        event_publisher=event_publisher
    )
    
    # Start feedback loop in background
    asyncio.create_task(feedback_loop_manager.start())
    
    logger.info("MLOps service initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    event_publisher.disconnect()


# API Endpoints

@app.post("/api/v1/models/register", response_model=Dict[str, Any])
async def register_model(
    request: ModelRegistrationRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Register a new model version in MLflow registry"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Add tenant prefix to model name
        full_model_name = f"{tenant_id}_{request.model_name}"
        
        # Register model
        model_version = model_registry_manager.register_model(
            model_name=full_model_name,
            run_id=request.run_id,
            description=request.description,
            tags={
                **request.tags,
                "tenant_id": tenant_id,
                "registered_by": user_id,
                "registration_time": datetime.utcnow().isoformat()
            }
        )
        
        # Transition to initial stage if specified
        if request.stage and request.stage != "None":
            model_registry_manager.transition_model_stage(
                model_name=full_model_name,
                version=model_version.version,
                stage=request.stage,
                archive_existing=True
            )
        
        # Trigger validation in background
        background_tasks.add_task(
            model_registry_manager.validate_model,
            full_model_name,
            model_version.version
        )
        
        return {
            "model_name": request.model_name,
            "version": model_version.version,
            "status": "registered",
            "stage": request.stage or "None",
            "run_id": request.run_id
        }
        
    except Exception as e:
        logger.error(f"Failed to register model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/versions", response_model=List[Dict[str, Any]])
async def list_model_versions(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """List all versions of a model"""
    tenant_id = context["tenant_id"]
    full_model_name = f"{tenant_id}_{model_name}"
    
    try:
        versions = model_registry_manager.list_model_versions(full_model_name)
        
        return [
            {
                "version": v.version,
                "stage": v.current_stage,
                "description": v.description,
                "tags": v.tags,
                "creation_time": v.creation_timestamp,
                "last_updated": v.last_updated_timestamp,
                "run_id": v.run_id
            }
            for v in versions
        ]
        
    except Exception as e:
        logger.error(f"Failed to list model versions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/deployments/create", response_model=Dict[str, Any])
async def deploy_model(
    request: ModelDeploymentRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Deploy a model to specified environment"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Deploy model
        deployment_result = await deployment_manager.deploy_model(
            tenant_id=tenant_id,
            model_name=request.model_name,
            version=request.version,
            environment=request.environment,
            deployment_config=request.deployment_config,
            canary_config=request.canary_config
        )
        
        # Configure monitoring in background
        if deployment_result["status"] == "deployed":
            background_tasks.add_task(
                monitoring_config_manager.configure_monitoring,
                tenant_id,
                request.model_name,
                deployment_result["version"],
                request.environment
            )
        
        return deployment_result
        
    except Exception as e:
        logger.error(f"Failed to deploy model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/deployments", response_model=List[Dict[str, Any]])
async def list_deployments(
    context: dict = Depends(get_current_tenant_and_user)
):
    """List all model deployments for tenant"""
    tenant_id = context["tenant_id"]
    
    try:
        deployments = await deployment_manager.list_deployments(tenant_id)
        return deployments
        
    except Exception as e:
        logger.error(f"Failed to list deployments: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ab-tests/start", response_model=Dict[str, Any])
async def start_ab_test(
    request: ABTestRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Start A/B test between two model versions"""
    tenant_id = context["tenant_id"]
    
    try:
        test_id = await ab_test_manager.start_ab_test(
            model_name=f"{tenant_id}_{request.model_name}",
            version_a=request.version_a,
            version_b=request.version_b,
            traffic_split=request.traffic_split,
            duration_hours=request.duration_hours
        )
        
        return {
            "test_id": test_id,
            "status": "started",
            "model_name": request.model_name,
            "version_a": request.version_a,
            "version_b": request.version_b,
            "traffic_split": request.traffic_split,
            "end_time": datetime.utcnow() + timedelta(hours=request.duration_hours)
        }
        
    except Exception as e:
        logger.error(f"Failed to start A/B test: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/ab-tests/{test_id}/results", response_model=Dict[str, Any])
async def get_ab_test_results(
    test_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get A/B test results"""
    try:
        results = await ab_test_manager.evaluate_ab_test(test_id)
        return results
        
    except Exception as e:
        logger.error(f"Failed to get A/B test results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/models/{model_name}/promote", response_model=Dict[str, Any])
async def promote_model(
    model_name: str,
    version: str,
    target_stage: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Promote model to a new stage"""
    tenant_id = context["tenant_id"]
    full_model_name = f"{tenant_id}_{model_name}"
    
    if target_stage not in ["Staging", "Production"]:
        raise HTTPException(status_code=400, detail="Invalid target stage")
    
    try:
        model_registry_manager.transition_model_stage(
            model_name=full_model_name,
            version=version,
            stage=target_stage,
            archive_existing=True
        )
        
        return {
            "model_name": model_name,
            "version": version,
            "stage": target_stage,
            "status": "promoted"
        }
        
    except Exception as e:
        logger.error(f"Failed to promote model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/metrics", response_model=Dict[str, Any])
async def get_model_metrics(
    model_name: str,
    version: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get model performance metrics"""
    tenant_id = context["tenant_id"]
    
    try:
        metrics = await monitoring_config_manager.get_model_metrics(
            tenant_id=tenant_id,
            model_name=model_name,
            version=version
        )
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get model metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/lineage", response_model=List[Dict[str, Any]])
async def get_model_lineage(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get model training lineage"""
    tenant_id = context["tenant_id"]
    full_model_name = f"{tenant_id}_{model_name}"
    
    try:
        lineage = model_registry_manager.get_model_lineage(full_model_name)
        return lineage
        
    except Exception as e:
        logger.error(f"Failed to get model lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "mlops-service",
        "timestamp": datetime.utcnow().isoformat(),
        "mlflow_connected": mlflow_client is not None
    } 