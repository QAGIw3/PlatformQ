"""
Models API endpoints
"""
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from dependency_injector.wiring import inject, Provide

from ...core.container import Container
from ...core.model_registry import ModelRegistryManager
from ...domain.models.model import (
    Model, ModelStage, ModelFormat, ModelVersion,
    ModelDeployment, ServingFramework, DeploymentStrategy
)
from ..dependencies import get_current_user

router = APIRouter(prefix="/models", tags=["models"])


@router.post("/", response_model=Model)
@inject
async def register_model(
    name: str,
    framework: str,
    model_format: ModelFormat,
    model_file: UploadFile = File(...),
    experiment_id: Optional[str] = None,
    run_id: Optional[str] = None,
    metrics: Optional[Dict[str, float]] = None,
    parameters: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    current_user: dict = Depends(get_current_user),
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> Model:
    """Register a new model"""
    try:
        # Save uploaded file temporarily
        temp_path = f"/tmp/{model_file.filename}"
        with open(temp_path, "wb") as f:
            content = await model_file.read()
            f.write(content)
        
        # Register model
        model = await model_registry.register_model(
            name=name,
            model_path=temp_path,
            framework=framework,
            model_format=model_format,
            experiment_id=experiment_id,
            run_id=run_id,
            metrics=metrics,
            parameters=parameters,
            tags=tags,
            created_by=current_user["user_id"]
        )
        
        return model
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[Model])
@inject
async def list_models(
    stage: Optional[ModelStage] = Query(None),
    limit: int = Query(100, le=1000),
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> List[Model]:
    """List models"""
    try:
        models = await model_registry.list_models(
            stage=stage,
            limit=limit
        )
        return models
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/{version}", response_model=Model)
@inject
async def get_model(
    name: str,
    version: str,
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> Model:
    """Get model details"""
    model = await model_registry.get_model(name, version)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


@router.put("/{name}/{version}/stage", response_model=ModelVersion)
@inject
async def promote_model(
    name: str,
    version: str,
    target_stage: ModelStage,
    archive_existing: bool = True,
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> ModelVersion:
    """Promote model to a new stage"""
    try:
        model_version = await model_registry.promote_model(
            name=name,
            version=version,
            target_stage=target_stage,
            archive_existing=archive_existing
        )
        return model_version
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{name}/{version}")
@inject
async def delete_model_version(
    name: str,
    version: str,
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> dict:
    """Delete a model version"""
    try:
        await model_registry.delete_model_version(name, version)
        return {"message": "Model version deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{name}/{version}/lineage")
@inject
async def get_model_lineage(
    name: str,
    version: str,
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> Dict[str, Any]:
    """Get model lineage information"""
    try:
        lineage = await model_registry.get_model_lineage(name, version)
        return lineage
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{name}/{version}/deployments", response_model=ModelDeployment)
@inject
async def create_deployment(
    name: str,
    version: str,
    deployment_name: str,
    serving_framework: ServingFramework,
    deployment_strategy: DeploymentStrategy,
    replicas: int = 1,
    cpu_request: str = "1",
    memory_request: str = "2Gi",
    gpu_request: int = 0,
    environment_variables: Optional[Dict[str, str]] = None,
    current_user: dict = Depends(get_current_user),
    model_registry: ModelRegistryManager = Depends(Provide[Container.model_registry_manager])
) -> ModelDeployment:
    """Create a model deployment"""
    try:
        deployment = await model_registry.create_deployment(
            model_name=name,
            model_version=version,
            deployment_name=deployment_name,
            serving_framework=serving_framework,
            deployment_strategy=deployment_strategy,
            deployed_by=current_user["user_id"],
            replicas=replicas,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            environment_variables=environment_variables
        )
        return deployment
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 