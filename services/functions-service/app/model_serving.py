"""
Model Serving Module

This module extends the functions-service to deploy ML models from MLflow
as serverless functions using Knative.
"""

import logging
import os
import json
import tempfile
import shutil
from typing import Dict, Any, Optional, List
from datetime import datetime
import yaml
import base64

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from kubernetes import client, config
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models import Model
import docker

from platformq_shared.security import get_current_user_from_trusted_header

logger = logging.getLogger(__name__)

# Configure MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Load Kubernetes config
try:
    config.load_incluster_config()
except:
    config.load_kube_config()

k8s_api = client.CustomObjectsApi()
core_v1 = client.CoreV1Api()

router = APIRouter()


class ModelDeploymentRequest(BaseModel):
    """Request to deploy a model from MLflow"""
    model_name: str = Field(..., description="Name of the model in MLflow registry")
    model_version: Optional[str] = Field(None, description="Version to deploy (latest if not specified)")
    deployment_name: Optional[str] = Field(None, description="Custom deployment name")
    resources: Dict[str, str] = Field(
        default={"cpu": "500m", "memory": "1Gi"},
        description="Resource requirements"
    )
    autoscaling: Dict[str, Any] = Field(
        default={"min": 1, "max": 5, "target_cpu": 80},
        description="Autoscaling configuration"
    )
    environment_variables: Dict[str, str] = Field(
        default={},
        description="Additional environment variables"
    )


class ModelDeploymentStatus(BaseModel):
    """Model deployment status"""
    deployment_id: str
    model_name: str
    model_version: str
    status: str
    endpoint_url: Optional[str]
    created_at: str
    last_updated: str


class ModelPredictionRequest(BaseModel):
    """Request for model prediction"""
    inputs: Dict[str, Any] = Field(..., description="Input data for prediction")
    params: Optional[Dict[str, Any]] = Field(None, description="Additional prediction parameters")


class ModelServingManager:
    """Manages model deployments from MLflow to Knative"""
    
    def __init__(self):
        self.mlflow_client = MlflowClient()
        self.docker_client = docker.from_env()
        
    def get_latest_model_version(self, model_name: str, tenant_id: str) -> str:
        """Get the latest version of a model"""
        full_model_name = f"{tenant_id}_{model_name}"
        try:
            latest_version = self.mlflow_client.get_latest_versions(
                full_model_name,
                stages=["Production", "Staging", "None"]
            )[0]
            return latest_version.version
        except Exception as e:
            logger.error(f"Failed to get latest version for {full_model_name}: {e}")
            raise HTTPException(status_code=404, detail=f"Model {model_name} not found")
            
    def build_model_serving_image(self, 
                                model_name: str, 
                                model_version: str, 
                                tenant_id: str) -> str:
        """Build a container image for serving the model"""
        full_model_name = f"{tenant_id}_{model_name}"
        
        # Download model from MLflow
        model_uri = f"models:/{full_model_name}/{model_version}"
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download model artifacts
            model_path = mlflow.artifacts.download_artifacts(
                artifact_uri=model_uri,
                dst_path=tmpdir
            )
            
            # Load model metadata
            model = Model.load(os.path.join(model_path, "MLmodel"))
            
            # Create Dockerfile
            dockerfile_content = self._generate_dockerfile(model)
            with open(os.path.join(tmpdir, "Dockerfile"), "w") as f:
                f.write(dockerfile_content)
                
            # Create serving script
            serving_script = self._generate_serving_script(model)
            with open(os.path.join(tmpdir, "serve.py"), "w") as f:
                f.write(serving_script)
                
            # Copy model artifacts
            shutil.copytree(model_path, os.path.join(tmpdir, "model"))
            
            # Build image
            image_tag = f"platformq/ml-model-{model_name}:{model_version}"
            image, logs = self.docker_client.images.build(
                path=tmpdir,
                tag=image_tag,
                rm=True
            )
            
            # Push to registry (in production, push to actual registry)
            logger.info(f"Built image {image_tag}")
            
            return image_tag
            
    def _generate_dockerfile(self, model: Model) -> str:
        """Generate Dockerfile for model serving"""
        # Determine base image based on model flavor
        if "python_function" in model.flavors:
            base_image = "python:3.9-slim"
            requirements = model.flavors["python_function"].get("requirements", [])
        elif "spark" in model.flavors:
            base_image = "apache/spark-py:v3.4.0"
            requirements = ["pyspark", "mlflow"]
        else:
            base_image = "python:3.9-slim"
            requirements = ["mlflow"]
            
        dockerfile = f"""
FROM {base_image}

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

# Copy model artifacts
COPY model /app/model

# Copy serving script
COPY serve.py /app/

# Install Python dependencies
RUN pip install --no-cache-dir \\
    mlflow==2.9.2 \\
    fastapi==0.104.1 \\
    uvicorn==0.24.0 \\
    {' '.join(requirements)}

# Create non-root user
RUN useradd -m -u 1000 model && \\
    chown -R model:model /app

USER model

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:8080/health || exit 1

# Run server
CMD ["uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8080"]
"""
        return dockerfile
        
    def _generate_serving_script(self, model: Model) -> str:
        """Generate FastAPI serving script for the model"""
        script = """
import os
import logging
from typing import Dict, Any
import mlflow
import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load model
model_path = "/app/model"
model = mlflow.pyfunc.load_model(model_path)

# Create FastAPI app
app = FastAPI(title="Model Serving API")

class PredictionRequest(BaseModel):
    inputs: Dict[str, Any]
    params: Dict[str, Any] = {}

class PredictionResponse(BaseModel):
    predictions: Any
    model_version: str
    
@app.get("/health")
async def health():
    return {"status": "healthy"}
    
@app.get("/metadata")
async def metadata():
    return {
        "model_path": model_path,
        "model_type": type(model).__name__
    }
    
@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    try:
        # Convert inputs to DataFrame if needed
        if isinstance(request.inputs, dict):
            if "data" in request.inputs:
                input_data = pd.DataFrame(request.inputs["data"])
            else:
                input_data = pd.DataFrame([request.inputs])
        else:
            input_data = request.inputs
            
        # Make prediction
        predictions = model.predict(input_data, params=request.params)
        
        # Convert numpy arrays to lists for JSON serialization
        if isinstance(predictions, np.ndarray):
            predictions = predictions.tolist()
            
        return PredictionResponse(
            predictions=predictions,
            model_version=os.getenv("MODEL_VERSION", "unknown")
        )
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
"""
        return script
        
    def deploy_model_to_knative(self,
                              deployment_name: str,
                              image_tag: str,
                              tenant_id: str,
                              resources: Dict[str, str],
                              autoscaling: Dict[str, Any],
                              env_vars: Dict[str, str]) -> str:
        """Deploy model container to Knative"""
        
        # Create Knative Service manifest
        knative_service = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": deployment_name,
                "namespace": tenant_id,
                "labels": {
                    "model-serving": "true",
                    "tenant": tenant_id
                }
            },
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "autoscaling.knative.dev/minScale": str(autoscaling.get("min", 1)),
                            "autoscaling.knative.dev/maxScale": str(autoscaling.get("max", 5)),
                            "autoscaling.knative.dev/target": str(autoscaling.get("target_cpu", 80))
                        }
                    },
                    "spec": {
                        "containers": [{
                            "image": image_tag,
                            "resources": {
                                "requests": resources,
                                "limits": resources
                            },
                            "env": [
                                {"name": k, "value": v} 
                                for k, v in env_vars.items()
                            ] + [
                                {"name": "MLFLOW_TRACKING_URI", "value": MLFLOW_TRACKING_URI}
                            ],
                            "ports": [{
                                "containerPort": 8080,
                                "protocol": "TCP"
                            }]
                        }]
                    }
                }
            }
        }
        
        try:
            # Create or update the service
            k8s_api.create_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1",
                namespace=tenant_id,
                plural="services",
                body=knative_service
            )
            
            # Get service URL
            service_url = f"http://{deployment_name}.{tenant_id}.svc.cluster.local"
            
            return service_url
            
        except client.ApiException as e:
            if e.status == 409:  # Already exists, update it
                k8s_api.patch_namespaced_custom_object(
                    group="serving.knative.dev",
                    version="v1",
                    namespace=tenant_id,
                    plural="services",
                    name=deployment_name,
                    body=knative_service
                )
                return f"http://{deployment_name}.{tenant_id}.svc.cluster.local"
            else:
                raise


# Initialize manager
model_manager = ModelServingManager()


@router.post("/deployments", response_model=ModelDeploymentStatus)
async def deploy_model(
    request: ModelDeploymentRequest,
    context: dict = Depends(get_current_user_from_trusted_header),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Deploy a model from MLflow registry as a Knative service"""
    tenant_id = context["tenant_id"]
    
    # Get model version
    if request.model_version is None:
        model_version = model_manager.get_latest_model_version(
            request.model_name, 
            tenant_id
        )
    else:
        model_version = request.model_version
        
    # Generate deployment name
    deployment_name = request.deployment_name or f"model-{request.model_name}-v{model_version}"
    deployment_id = f"{tenant_id}-{deployment_name}"
    
    # Build container image in background
    def build_and_deploy():
        try:
            # Build image
            image_tag = model_manager.build_model_serving_image(
                request.model_name,
                model_version,
                tenant_id
            )
            
            # Deploy to Knative
            service_url = model_manager.deploy_model_to_knative(
                deployment_name,
                image_tag,
                tenant_id,
                request.resources,
                request.autoscaling,
                request.environment_variables
            )
            
            # Update deployment status
            logger.info(f"Successfully deployed model {request.model_name} version {model_version}")
            
        except Exception as e:
            logger.error(f"Failed to deploy model: {e}")
            raise
            
    background_tasks.add_task(build_and_deploy)
    
    return ModelDeploymentStatus(
        deployment_id=deployment_id,
        model_name=request.model_name,
        model_version=model_version,
        status="deploying",
        endpoint_url=None,
        created_at=datetime.utcnow().isoformat(),
        last_updated=datetime.utcnow().isoformat()
    )


@router.get("/deployments", response_model=List[ModelDeploymentStatus])
async def list_deployments(
    context: dict = Depends(get_current_user_from_trusted_header)
):
    """List all model deployments for the tenant"""
    tenant_id = context["tenant_id"]
    
    try:
        # List Knative services with model-serving label
        services = k8s_api.list_namespaced_custom_object(
            group="serving.knative.dev",
            version="v1",
            namespace=tenant_id,
            plural="services",
            label_selector="model-serving=true"
        )
        
        deployments = []
        for service in services.get("items", []):
            metadata = service["metadata"]
            status = service.get("status", {})
            
            deployments.append(ModelDeploymentStatus(
                deployment_id=f"{tenant_id}-{metadata['name']}",
                model_name=metadata["labels"].get("model-name", "unknown"),
                model_version=metadata["labels"].get("model-version", "unknown"),
                status="ready" if status.get("conditions", [{}])[0].get("status") == "True" else "deploying",
                endpoint_url=status.get("url"),
                created_at=metadata["creationTimestamp"],
                last_updated=metadata.get("lastModifiedTime", metadata["creationTimestamp"])
            ))
            
        return deployments
        
    except Exception as e:
        logger.error(f"Failed to list deployments: {e}")
        return []


@router.delete("/deployments/{deployment_name}")
async def delete_deployment(
    deployment_name: str,
    context: dict = Depends(get_current_user_from_trusted_header)
):
    """Delete a model deployment"""
    tenant_id = context["tenant_id"]
    
    try:
        k8s_api.delete_namespaced_custom_object(
            group="serving.knative.dev",
            version="v1",
            namespace=tenant_id,
            plural="services",
            name=deployment_name
        )
        
        return {"message": f"Deployment {deployment_name} deleted successfully"}
        
    except client.ApiException as e:
        if e.status == 404:
            raise HTTPException(status_code=404, detail="Deployment not found")
        else:
            raise HTTPException(status_code=500, detail=str(e)) 