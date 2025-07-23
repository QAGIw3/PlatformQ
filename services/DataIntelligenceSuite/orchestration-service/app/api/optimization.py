"""
Optimization API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class WorkflowOptimizationRequest(BaseModel):
    """Workflow optimization request"""
    workflow_config: Dict[str, Any] = Field(..., description="Workflow configuration")
    target: str = Field("balanced", description="Optimization target")


class ResourcePredictionRequest(BaseModel):
    """Resource prediction request"""
    pipeline_config: Dict[str, Any] = Field(..., description="Pipeline configuration")


class AnomalyDetectionRequest(BaseModel):
    """Anomaly detection request"""
    execution_metrics: Dict[str, Any] = Field(..., description="Execution metrics")


class LearningDataRequest(BaseModel):
    """Learning data request"""
    execution_data: Dict[str, Any] = Field(..., description="Execution data for learning")


@router.post("/optimize/workflow")
async def optimize_workflow(request: WorkflowOptimizationRequest) -> Dict[str, Any]:
    """Optimize workflow configuration using ML"""
    try:
        from ..main import ml_optimizer
        
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        from ..engines.optimization import OptimizationTarget
        target_enum = OptimizationTarget(request.target)
        
        recommendations = await ml_optimizer.optimize_workflow(
            request.workflow_config,
            target_enum
        )
        
        return recommendations
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error optimizing workflow: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/optimize/predict-resources")
async def predict_resources(request: ResourcePredictionRequest) -> Dict[str, Any]:
    """Predict resource requirements for pipeline"""
    try:
        from ..main import ml_optimizer
        
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        predictions = await ml_optimizer.predict_resource_needs(request.pipeline_config)
        
        return predictions
        
    except Exception as e:
        logger.error(f"Error predicting resources: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/optimize/detect-anomalies")
async def detect_anomalies(request: AnomalyDetectionRequest) -> Dict[str, Any]:
    """Detect anomalies in execution metrics"""
    try:
        from ..main import ml_optimizer
        
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        result = await ml_optimizer.detect_anomalies(request.execution_metrics)
        
        return result
        
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/optimize/learn")
async def learn_from_execution(request: LearningDataRequest) -> Dict[str, str]:
    """Submit execution data for ML learning"""
    try:
        from ..main import ml_optimizer
        
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        await ml_optimizer.learn_from_execution(request.execution_data)
        
        return {
            "status": "success",
            "message": "Execution data submitted for learning"
        }
        
    except Exception as e:
        logger.error(f"Error submitting learning data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/optimize/recommendations/{workflow_id}")
async def get_workflow_recommendations(workflow_id: str) -> Dict[str, Any]:
    """Get optimization recommendations for specific workflow"""
    try:
        from ..main import workflow_manager, ml_optimizer
        
        if not workflow_manager or not ml_optimizer:
            raise HTTPException(status_code=503, detail="Services not available")
        
        # Get workflow configuration
        workflow = workflow_manager.workflows.get(workflow_id)
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        # Get optimization recommendations
        from ..engines.optimization import OptimizationTarget
        recommendations = await ml_optimizer.optimize_workflow(
            workflow["config"],
            OptimizationTarget.BALANCED
        )
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/optimize/metrics")
async def get_optimization_metrics() -> Dict[str, Any]:
    """Get ML optimizer metrics"""
    try:
        from ..main import ml_optimizer
        
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        metrics = await ml_optimizer.get_optimization_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting optimization metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/optimize/models/status")
async def get_model_status() -> Dict[str, Any]:
    """Get ML model training status"""
    try:
        from ..main import ml_optimizer
        
        if not ml_optimizer:
            raise HTTPException(status_code=503, detail="ML optimizer not available")
        
        status = {
            "resource_model": ml_optimizer.resource_model is not None,
            "performance_model": ml_optimizer.performance_model is not None,
            "anomaly_detector": ml_optimizer.anomaly_detector is not None,
            "cost_model": ml_optimizer.cost_model is not None,
            "training_samples": len(ml_optimizer.execution_history),
            "min_samples_required": ml_optimizer.config["min_samples_for_training"]
        }
        
        return status
        
    except Exception as e:
        logger.error(f"Error getting model status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 