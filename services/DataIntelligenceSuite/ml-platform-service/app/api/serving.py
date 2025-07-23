"""
Model Serving API endpoints
"""

from typing import Dict, Any, List, Union
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class DeploymentRequest(BaseModel):
    """Model deployment request"""
    model_id: str = Field(..., description="Model ID from registry")
    model_version: str = Field(None, description="Model version")
    name: str = Field(..., description="Deployment name")
    framework: str = Field(..., description="Serving framework")
    resources: Dict[str, Any] = Field(default={}, description="Resource requirements")
    scaling: Dict[str, Any] = Field(default={}, description="Auto-scaling configuration")
    endpoints: Dict[str, Any] = Field(default={}, description="API endpoints configuration")


class PredictionRequest(BaseModel):
    """Prediction request"""
    input_data: Union[Dict, List] = Field(..., description="Input data for prediction")
    options: Dict[str, Any] = Field(default={}, description="Prediction options")


class DeploymentResponse(BaseModel):
    """Deployment response"""
    deployment_id: str
    status: str
    message: str


@router.post("/deployments", response_model=DeploymentResponse)
async def deploy_model(request: DeploymentRequest) -> DeploymentResponse:
    """Deploy a model for serving"""
    try:
        from ..main import serving_engine
        
        if not serving_engine:
            raise HTTPException(status_code=503, detail="Serving engine not available")
        
        deployment_id = await serving_engine.deploy_model(request.dict())
        
        return DeploymentResponse(
            deployment_id=deployment_id,
            status="deploying",
            message="Model deployment started"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error deploying model: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/deployments/{deployment_id}")
async def get_deployment_status(deployment_id: str) -> Dict[str, Any]:
    """Get deployment status"""
    try:
        from ..main import serving_engine
        
        if not serving_engine:
            raise HTTPException(status_code=503, detail="Serving engine not available")
        
        status = await serving_engine.get_deployment_status(deployment_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting deployment status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/deployments/{deployment_id}/predict")
async def predict(deployment_id: str, request: PredictionRequest) -> Dict[str, Any]:
    """Make prediction using deployed model"""
    try:
        from ..main import serving_engine
        
        if not serving_engine:
            raise HTTPException(status_code=503, detail="Serving engine not available")
        
        result = await serving_engine.predict(deployment_id, request.input_data)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error making prediction: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/deployments/{deployment_id}")
async def undeploy_model(deployment_id: str) -> Dict[str, Any]:
    """Undeploy a model"""
    try:
        from ..main import serving_engine
        
        if not serving_engine:
            raise HTTPException(status_code=503, detail="Serving engine not available")
        
        success = await serving_engine.undeploy_model(deployment_id)
        
        return {
            "deployment_id": deployment_id,
            "undeployed": success,
            "message": "Model undeployed successfully" if success else "Model could not be undeployed"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error undeploying model: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics")
async def get_serving_metrics() -> Dict[str, Any]:
    """Get serving engine metrics"""
    try:
        from ..main import serving_engine
        
        if not serving_engine:
            raise HTTPException(status_code=503, detail="Serving engine not available")
        
        metrics = await serving_engine.get_serving_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting serving metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 