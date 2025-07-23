"""
Serving API endpoints
"""
from typing import List, Dict, Any
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from dependency_injector.wiring import inject, Provide

from ...core.container import Container
from ...core.serving_manager import ServingManager
from ..dependencies import get_current_user

router = APIRouter(prefix="/serving", tags=["serving"])


@router.post("/deployments/{deployment_id}/deploy")
@inject
async def deploy_model(
    deployment_id: UUID,
    serving_manager: ServingManager = Depends(Provide[Container.serving_manager])
) -> dict:
    """Deploy a model"""
    try:
        success = await serving_manager.deploy_model(deployment_id)
        if success:
            return {"message": "Model deployed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to deploy model")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deployments/{deployment_id}/undeploy")
@inject
async def undeploy_model(
    deployment_id: UUID,
    serving_manager: ServingManager = Depends(Provide[Container.serving_manager])
) -> dict:
    """Undeploy a model"""
    try:
        success = await serving_manager.undeploy_model(deployment_id)
        if success:
            return {"message": "Model undeployed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to undeploy model")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deployments/{deployment_id}/predict")
@inject
async def predict(
    deployment_id: UUID,
    input_data: Dict[str, Any],
    serving_manager: ServingManager = Depends(Provide[Container.serving_manager])
) -> Dict[str, Any]:
    """Run inference"""
    try:
        result = await serving_manager.predict(deployment_id, input_data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deployments/{deployment_id}/predict/batch")
@inject
async def batch_predict(
    deployment_id: UUID,
    input_data: List[Dict[str, Any]],
    serving_manager: ServingManager = Depends(Provide[Container.serving_manager])
) -> List[Dict[str, Any]]:
    """Run batch inference"""
    try:
        results = await serving_manager.batch_predict(deployment_id, input_data)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deployments/{deployment_id}/status")
@inject
async def get_deployment_status(
    deployment_id: UUID,
    serving_manager: ServingManager = Depends(Provide[Container.serving_manager])
) -> Dict[str, Any]:
    """Get deployment status"""
    try:
        status = await serving_manager.get_deployment_status(deployment_id)
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 