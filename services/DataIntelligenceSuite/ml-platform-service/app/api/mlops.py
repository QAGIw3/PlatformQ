"""
MLOps API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class ModelRegistrationRequest(BaseModel):
    """Model registration request"""
    name: str = Field(..., description="Model name")
    framework: str = Field(..., description="ML framework")
    model_type: str = Field(..., description="Model type")
    metrics: Dict[str, float] = Field(..., description="Model metrics")
    artifacts: Dict[str, str] = Field(..., description="Model artifacts")
    metadata: Dict[str, Any] = Field(default={}, description="Additional metadata")


class ModelPromotionRequest(BaseModel):
    """Model promotion request"""
    target_stage: str = Field(..., description="Target stage (staging, production)")
    reason: str = Field(None, description="Reason for promotion")


class RetrainingRequest(BaseModel):
    """Model retraining request"""
    reason: str = Field(..., description="Reason for retraining")
    config_overrides: Dict[str, Any] = Field(default={}, description="Configuration overrides")


@router.post("/models", response_model=Dict[str, str])
async def register_model(request: ModelRegistrationRequest) -> Dict[str, str]:
    """Register a new model in MLOps"""
    try:
        from ..main import mlops_manager
        
        if not mlops_manager:
            raise HTTPException(status_code=503, detail="MLOps manager not available")
        
        model_id = await mlops_manager.register_model(request.dict())
        
        return {
            "model_id": model_id,
            "status": "registered",
            "message": "Model registered successfully"
        }
        
    except Exception as e:
        logger.error(f"Error registering model: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/models/{model_id}/status")
async def get_model_status(model_id: str) -> Dict[str, Any]:
    """Get comprehensive model status"""
    try:
        from ..main import mlops_manager
        
        if not mlops_manager:
            raise HTTPException(status_code=503, detail="MLOps manager not available")
        
        status = await mlops_manager.get_model_status(model_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting model status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/models/{model_id}/promote")
async def promote_model(model_id: str, request: ModelPromotionRequest) -> Dict[str, Any]:
    """Promote model to a new stage"""
    try:
        from ..main import mlops_manager
        
        if not mlops_manager:
            raise HTTPException(status_code=503, detail="MLOps manager not available")
        
        success = await mlops_manager.promote_model(
            model_id,
            request.target_stage,
            request.reason
        )
        
        return {
            "model_id": model_id,
            "promoted": success,
            "target_stage": request.target_stage,
            "message": "Model promoted successfully" if success else "Promotion requires approval"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error promoting model: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/models/{model_id}/retrain")
async def trigger_retraining(model_id: str, request: RetrainingRequest) -> Dict[str, str]:
    """Trigger model retraining"""
    try:
        from ..main import mlops_manager
        
        if not mlops_manager:
            raise HTTPException(status_code=503, detail="MLOps manager not available")
        
        job_id = await mlops_manager.trigger_retraining(model_id, request.reason)
        
        return {
            "model_id": model_id,
            "job_id": job_id,
            "status": "triggered",
            "message": "Retraining triggered successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error triggering retraining: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics")
async def get_mlops_metrics() -> Dict[str, Any]:
    """Get MLOps metrics"""
    try:
        from ..main import mlops_manager
        
        if not mlops_manager:
            raise HTTPException(status_code=503, detail="MLOps manager not available")
        
        metrics = await mlops_manager.get_mlops_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting MLOps metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/alerts")
async def get_model_alerts(
    model_id: str = None,
    severity: str = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """Get model alerts"""
    try:
        from ..main import mlops_manager
        
        if not mlops_manager:
            raise HTTPException(status_code=503, detail="MLOps manager not available")
        
        # This would get alerts from MLOps manager
        alerts = []
        
        # Filter by model_id if provided
        if model_id:
            alerts = [a for a in alerts if a.get("model_id") == model_id]
        
        # Filter by severity if provided
        if severity:
            alerts = [a for a in alerts if a.get("severity") == severity]
        
        return alerts[:limit]
        
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 