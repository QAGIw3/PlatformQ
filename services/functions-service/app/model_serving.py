"""
Model Serving Module

This module extends the functions-service to deploy ML models from MLflow
as serverless functions using Knative. Enhanced to support marketplace models
and license validation.
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
import httpx

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
MLOPS_SERVICE_URL = os.getenv("MLOPS_SERVICE_URL", "http://mlops-service:80")
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
    model_cid: Optional[str] = Field(None, description="Digital asset CID for marketplace models")
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


class ModelPredictionRequest(BaseModel):
    """Request for model prediction"""
    inputs: Dict[str, Any] = Field(..., description="Input data for prediction")
    model_cid: Optional[str] = Field(None, description="Model CID for usage tracking")


class ModelDeploymentStatus(BaseModel):
    """Status of a model deployment"""
    deployment_id: str
    model_name: str
    model_version: str
    model_cid: Optional[str] = None
    status: str  # deploying, ready, failed
    endpoint_url: Optional[str]
    created_at: str
    last_updated: str
    usage_count: int = 0
    license_type: Optional[str] = None
    license_valid_until: Optional[str] = None


class ModelServingManager:
    """Manages model deployments from MLflow to Knative"""
    
    def __init__(self):
        self.mlflow_client = MlflowClient()
        self.docker_client = docker.from_env()
        self.deployment_cache = {}  # In production, use persistent storage
        
    async def check_model_access(self, tenant_id: str, user_id: str, 
                                 model_cid: str) -> Dict[str, Any]:
        """Check if tenant has access to deploy a model"""
        if not model_cid:
            # Own model, always has access
            return {"has_access": True, "license_type": "owner"}
        
        try:
            # Check with MLOps service
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{MLOPS_SERVICE_URL}/api/v1/marketplace/models/{model_cid}/access",
                    headers={
                        "X-Tenant-ID": tenant_id,
                        "X-User-ID": user_id
                    }
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return {"has_access": False, "reason": "access_check_failed"}
                    
        except Exception as e:
            logger.error(f"Failed to check model access: {e}")
            return {"has_access": False, "reason": "error", "error": str(e)}
        
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
    
    def build_model_serving_image(self, model_name: str, model_version: str, 
                                  tenant_id: str, model_cid: Optional[str] = None) -> str:
        """Build container image for model serving"""
        full_model_name = f"{tenant_id}_{model_name}"
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download model artifacts
            model_uri = f"models:/{full_model_name}/{model_version}"
            model_path = mlflow.artifacts.download_artifacts(model_uri, dst_path=tmpdir)
            
            # Load model metadata
            model_meta = Model.load(os.path.join(model_path, "MLmodel"))
            
            # Create Dockerfile based on model flavor
            if "sklearn" in model_meta.flavors:
                base_image = "python:3.9-slim"
                requirements = "scikit-learn\nmlflow\nfastapi\nuvicorn\nhttpx"
            elif "tensorflow" in model_meta.flavors:
                base_image = "tensorflow/tensorflow:2.13.0"
                requirements = "mlflow\nfastapi\nuvicorn\nhttpx"
            elif "pytorch" in model_meta.flavors:
                base_image = "pytorch/pytorch:2.0.1-cuda11.7-cudnn8-runtime"
                requirements = "mlflow\nfastapi\nuvicorn\nhttpx"
            else:
                base_image = "python:3.9-slim"
                requirements = "mlflow\nfastapi\nuvicorn\nhttpx"
            
            # Create serving script
            serving_script = f'''
import os
import json
import mlflow
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any

app = FastAPI()

# Load model
model_uri = "{model_uri}"
model = mlflow.pyfunc.load_model(model_uri)

# Model metadata
MODEL_NAME = "{model_name}"
MODEL_VERSION = "{model_version}"
MODEL_CID = "{model_cid or 'none'}"
TENANT_ID = "{tenant_id}"
MLOPS_SERVICE_URL = os.getenv("MLOPS_SERVICE_URL", "{MLOPS_SERVICE_URL}")

class PredictionRequest(BaseModel):
    inputs: Dict[str, Any]

class PredictionResponse(BaseModel):
    predictions: Any
    model_name: str
    model_version: str

@app.get("/health")
async def health():
    return {{"status": "healthy", "model": MODEL_NAME, "version": MODEL_VERSION}}

@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    try:
        # Track usage if marketplace model
        if MODEL_CID != "none":
            try:
                async with httpx.AsyncClient() as client:
                    await client.post(
                        f"{{MLOPS_SERVICE_URL}}/api/v1/marketplace/usage",
                        json={{
                            "model_cid": MODEL_CID,
                            "tenant_id": TENANT_ID,
                            "usage_count": 1
                        }}
                    )
            except Exception as e:
                # Don't fail prediction if usage tracking fails
                print(f"Failed to track usage: {{e}}")
        
        # Make prediction
        predictions = model.predict(request.inputs)
        
        return PredictionResponse(
            predictions=predictions.tolist() if hasattr(predictions, "tolist") else predictions,
            model_name=MODEL_NAME,
            model_version=MODEL_VERSION
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
'''
            
            # Write files
            with open(os.path.join(tmpdir, "serve.py"), "w") as f:
                f.write(serving_script)
            
            with open(os.path.join(tmpdir, "requirements.txt"), "w") as f:
                f.write(requirements)
            
            # Create Dockerfile
            dockerfile = f"""
FROM {base_image}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["python", "serve.py"]
"""
            
            with open(os.path.join(tmpdir, "Dockerfile"), "w") as f:
                f.write(dockerfile)
            
            # Build image
            image_tag = f"platformq/{tenant_id}-{model_name}:v{model_version}"
            
            logger.info(f"Building Docker image {image_tag}")
            image, build_logs = self.docker_client.images.build(
                path=tmpdir,
                tag=image_tag,
                rm=True,
                forcerm=True
            )
            
            # Push to registry (in production)
            # self.docker_client.images.push(image_tag)
            
            return image_tag
    
    def deploy_model_to_knative(self, deployment_name: str, image_tag: str,
                                tenant_id: str, resources: Dict[str, str],
                                autoscaling: Dict[str, Any],
                                environment_variables: Dict[str, str],
                                model_cid: Optional[str] = None) -> str:
        """Deploy model container to Knative"""
        # Create Knative service manifest
        knative_service = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": deployment_name,
                "namespace": tenant_id,
                "labels": {
                    "model-serving": "true",
                    "model-name": deployment_name.split("-")[1] if "-" in deployment_name else deployment_name,
                    "model-version": deployment_name.split("-")[-1] if "-" in deployment_name else "latest",
                    "tenant-id": tenant_id
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
                                "limits": {
                                    "cpu": resources.get("cpu", "1000m"),
                                    "memory": resources.get("memory", "2Gi")
                                },
                                "requests": {
                                    "cpu": resources.get("cpu", "500m"),
                                    "memory": resources.get("memory", "1Gi")
                                }
                            },
                            "env": [
                                {"name": k, "value": v} 
                                for k, v in environment_variables.items()
                            ] + [
                                {"name": "MLOPS_SERVICE_URL", "value": MLOPS_SERVICE_URL}
                            ]
                        }]
                    }
                }
            }
        }
        
        # Add model CID to annotations if marketplace model
        if model_cid:
            knative_service["metadata"]["annotations"] = {"model-cid": model_cid}
        
        try:
            # Create or update the service
            try:
                k8s_api.patch_namespaced_custom_object(
                    group="serving.knative.dev",
                    version="v1",
                    namespace=tenant_id,
                    plural="services",
                    name=deployment_name,
                    body=knative_service
                )
                logger.info(f"Updated Knative service {deployment_name}")
            except client.ApiException as e:
                if e.status == 404:
                    k8s_api.create_namespaced_custom_object(
                        group="serving.knative.dev",
                        version="v1",
                        namespace=tenant_id,
                        plural="services",
                        body=knative_service
                    )
                    logger.info(f"Created Knative service {deployment_name}")
                else:
                    raise
            
            # Get service URL (in production, wait for ready state)
            service_url = f"https://{deployment_name}.{tenant_id}.example.com"
            
            return service_url
            
        except Exception as e:
            logger.error(f"Failed to deploy to Knative: {e}")
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
    user_id = context.get("user_id", "unknown")
    
    # Check access if marketplace model
    if request.model_cid:
        access_info = await model_manager.check_model_access(
            tenant_id, user_id, request.model_cid
        )
        
        if not access_info.get("has_access"):
            raise HTTPException(
                status_code=403,
                detail=f"No access to model: {access_info.get('reason', 'unknown')}"
            )
    
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
                tenant_id,
                request.model_cid
            )
            
            # Deploy to Knative
            service_url = model_manager.deploy_model_to_knative(
                deployment_name,
                image_tag,
                tenant_id,
                request.resources,
                request.autoscaling,
                request.environment_variables,
                request.model_cid
            )
            
            # Store deployment info
            deployment_info = {
                "deployment_id": deployment_id,
                "model_name": request.model_name,
                "model_version": model_version,
                "model_cid": request.model_cid,
                "status": "ready",
                "endpoint_url": service_url,
                "created_at": datetime.utcnow().isoformat(),
                "last_updated": datetime.utcnow().isoformat(),
                "usage_count": 0,
                "license_type": access_info.get("license_type") if request.model_cid else "owner",
                "license_valid_until": access_info.get("valid_until") if request.model_cid else None
            }
            
            model_manager.deployment_cache[deployment_id] = deployment_info
            
            # Update deployment status
            logger.info(f"Successfully deployed model {request.model_name} version {model_version}")
            
        except Exception as e:
            logger.error(f"Failed to deploy model: {e}")
            # Update deployment status to failed
            if deployment_id in model_manager.deployment_cache:
                model_manager.deployment_cache[deployment_id]["status"] = "failed"
            raise
            
    background_tasks.add_task(build_and_deploy)
    
    # Store initial deployment info
    deployment_info = ModelDeploymentStatus(
        deployment_id=deployment_id,
        model_name=request.model_name,
        model_version=model_version,
        model_cid=request.model_cid,
        status="deploying",
        endpoint_url=None,
        created_at=datetime.utcnow().isoformat(),
        last_updated=datetime.utcnow().isoformat()
    )
    
    model_manager.deployment_cache[deployment_id] = deployment_info.dict()
    
    return deployment_info


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
            
            deployment_id = f"{tenant_id}-{metadata['name']}"
            
            # Get deployment info from cache or construct from service
            if deployment_id in model_manager.deployment_cache:
                deployment_info = model_manager.deployment_cache[deployment_id]
                deployments.append(ModelDeploymentStatus(**deployment_info))
            else:
                deployments.append(ModelDeploymentStatus(
                    deployment_id=deployment_id,
                    model_name=metadata["labels"].get("model-name", "unknown"),
                    model_version=metadata["labels"].get("model-version", "unknown"),
                    model_cid=metadata.get("annotations", {}).get("model-cid"),
                    status="ready" if status.get("conditions", [{}])[0].get("status") == "True" else "deploying",
                    endpoint_url=status.get("url"),
                    created_at=metadata["creationTimestamp"],
                    last_updated=metadata.get("lastModifiedTime", metadata["creationTimestamp"]),
                    usage_count=0
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
        
        # Remove from cache
        deployment_id = f"{tenant_id}-{deployment_name}"
        if deployment_id in model_manager.deployment_cache:
            del model_manager.deployment_cache[deployment_id]
        
        return {"message": f"Deployment {deployment_name} deleted successfully"}
        
    except client.ApiException as e:
        if e.status == 404:
            raise HTTPException(status_code=404, detail="Deployment not found")
        else:
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict/{deployment_name}")
async def predict(
    deployment_name: str,
    request: ModelPredictionRequest,
    context: dict = Depends(get_current_user_from_trusted_header)
):
    """Make prediction using deployed model"""
    tenant_id = context["tenant_id"]
    user_id = context.get("user_id", "unknown")
    
    # Get deployment info
    deployment_id = f"{tenant_id}-{deployment_name}"
    deployment_info = model_manager.deployment_cache.get(deployment_id)
    
    if not deployment_info:
        # Try to get from Kubernetes
        try:
            service = k8s_api.get_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1",
                namespace=tenant_id,
                plural="services",
                name=deployment_name
            )
            
            endpoint_url = service.get("status", {}).get("url")
            if not endpoint_url:
                raise HTTPException(status_code=503, detail="Model endpoint not ready")
                
        except client.ApiException as e:
            if e.status == 404:
                raise HTTPException(status_code=404, detail="Deployment not found")
            else:
                raise HTTPException(status_code=500, detail=str(e))
    else:
        endpoint_url = deployment_info.get("endpoint_url")
        
        # Check license validity for marketplace models
        if deployment_info.get("model_cid"):
            if deployment_info.get("license_type") == "time_based":
                valid_until = deployment_info.get("license_valid_until")
                if valid_until and datetime.fromisoformat(valid_until) < datetime.utcnow():
                    raise HTTPException(status_code=403, detail="Model license expired")
            
            elif deployment_info.get("license_type") == "usage_based":
                # Check with MLOps service for remaining usage
                access_info = await model_manager.check_model_access(
                    tenant_id, user_id, deployment_info["model_cid"]
                )
                if not access_info.get("has_access"):
                    raise HTTPException(status_code=403, detail="Usage limit reached")
    
    # Make prediction request
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{endpoint_url}/predict",
            json={"inputs": request.inputs},
            timeout=30.0
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Prediction failed: {response.text}"
            )
        
        prediction_result = response.json()
        
        # Update usage count
        if deployment_id in model_manager.deployment_cache:
            model_manager.deployment_cache[deployment_id]["usage_count"] += 1
        
        return prediction_result


@router.get("/deployments/{deployment_name}/metrics")
async def get_deployment_metrics(
    deployment_name: str,
    context: dict = Depends(get_current_user_from_trusted_header)
):
    """Get metrics for a model deployment"""
    tenant_id = context["tenant_id"]
    deployment_id = f"{tenant_id}-{deployment_name}"
    
    deployment_info = model_manager.deployment_cache.get(deployment_id, {})
    
    # In production, get real metrics from monitoring service
    return {
        "deployment_name": deployment_name,
        "usage_count": deployment_info.get("usage_count", 0),
        "average_latency_ms": 45.2,
        "error_rate": 0.002,
        "requests_per_minute": 120,
        "cpu_utilization": 0.65,
        "memory_utilization": 0.78,
        "active_instances": 3,
        "last_prediction": datetime.utcnow().isoformat()
    } 