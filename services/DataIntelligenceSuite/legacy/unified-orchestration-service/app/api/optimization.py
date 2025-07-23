"""
ML optimization API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from ..core import MLPipelineOptimizer, OptimizationTarget

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/optimize", tags=["optimization"])

# Dependency injection
ml_optimizer: Optional[MLPipelineOptimizer] = None

def set_dependencies(optimizer: MLPipelineOptimizer):
    """Set API dependencies"""
    global ml_optimizer
    ml_optimizer = optimizer


# Request/Response models
class OptimizeWorkflowRequest(BaseModel):
    workflow: Dict[str, Any]
    target: OptimizationTarget = OptimizationTarget.BALANCED
    constraints: Optional[Dict[str, Any]] = None
    historical_data: Optional[List[Dict[str, Any]]] = None


class PredictResourcesRequest(BaseModel):
    workflow: Dict[str, Any]
    context: Optional[Dict[str, Any]] = None


class TrainModelsRequest(BaseModel):
    training_data: List[Dict[str, Any]]


class OptimizationResponse(BaseModel):
    workflow_id: Optional[str]
    workflow_name: Optional[str]
    target: OptimizationTarget
    timestamp: str
    original_config: Dict[str, Any]
    optimized_config: Dict[str, Any]
    predicted_improvements: Dict[str, float]
    confidence: float
    resource_allocation: Optional[Dict[str, Any]] = None
    performance_prediction: Optional[Dict[str, Any]] = None


class ResourcePredictionResponse(BaseModel):
    cpu: float
    memory: float
    storage: float
    predicted: bool


class PerformancePredictionResponse(BaseModel):
    estimated_duration: float
    success_probability: float
    throughput: float
    predicted: bool


class AnomalyDetectionResponse(BaseModel):
    workflow_id: str
    workflow_name: str
    anomaly_score: float
    detected_at: str


# API Endpoints
@router.post("/workflow", response_model=OptimizationResponse)
async def optimize_workflow(request: OptimizeWorkflowRequest = Body(...)):
    """Optimize workflow configuration using ML"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        recommendations = await ml_optimizer.optimize_workflow(
            workflow=request.workflow,
            target=request.target,
            constraints=request.constraints,
            historical_data=request.historical_data
        )
        
        return OptimizationResponse(**recommendations)
        
    except Exception as e:
        logger.error(f"Failed to optimize workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/{workflow_id}", response_model=List[OptimizationResponse])
async def get_recommendations(
    workflow_id: str,
    limit: int = Query(10, ge=1, le=50)
):
    """Get optimization recommendations for a workflow"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        recommendations = await ml_optimizer.get_recommendations(workflow_id)
        
        # Limit results
        limited = recommendations[:limit]
        
        return [OptimizationResponse(**rec) for rec in limited]
        
    except Exception as e:
        logger.error(f"Failed to get recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict-resources", response_model=ResourcePredictionResponse)
async def predict_resources(request: PredictResourcesRequest = Body(...)):
    """Predict resource requirements for a workflow"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        prediction = await ml_optimizer.predict_resources(
            workflow=request.workflow,
            context=request.context
        )
        
        return ResourcePredictionResponse(**prediction)
        
    except Exception as e:
        logger.error(f"Failed to predict resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/detect-anomalies", response_model=List[AnomalyDetectionResponse])
async def detect_anomalies(
    workflows: List[Dict[str, Any]] = Body(...)
):
    """Detect anomalous workflows"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        anomalies = await ml_optimizer.detect_anomalies(workflows)
        
        return [AnomalyDetectionResponse(**anomaly) for anomaly in anomalies]
        
    except Exception as e:
        logger.error(f"Failed to detect anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/train-models")
async def train_models(request: TrainModelsRequest = Body(...)):
    """Train optimization models on historical data"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        await ml_optimizer.train_models(request.training_data)
        
        return {
            "message": "Model training completed successfully",
            "samples_used": len(request.training_data),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to train models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/feature-importance")
async def get_feature_importance():
    """Get feature importance from trained models"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        # Get feature importance from models
        importance = {}
        
        if 'resource_predictor' in ml_optimizer.models:
            model = ml_optimizer.models['resource_predictor']
            if hasattr(model, 'feature_importances_'):
                importance['resource_prediction'] = {
                    'features': [
                        'num_steps', 'num_dependencies', 'retry_count', 'timeout',
                        'parallel_execution', 'transform_steps', 'ml_steps', 
                        'quality_steps', 'max_cpu', 'max_memory', 'avg_duration',
                        'duration_variance', 'failure_rate', 'avg_cpu_usage', 
                        'avg_memory_usage'
                    ],
                    'importances': model.feature_importances_.tolist()
                }
                
        if 'performance_predictor' in ml_optimizer.models:
            model = ml_optimizer.models['performance_predictor']
            if hasattr(model, 'feature_importances_'):
                importance['performance_prediction'] = {
                    'features': [
                        'num_steps', 'num_dependencies', 'retry_count', 'timeout',
                        'parallel_execution', 'transform_steps', 'ml_steps', 
                        'quality_steps', 'max_cpu', 'max_memory', 'avg_duration',
                        'duration_variance', 'failure_rate', 'avg_cpu_usage', 
                        'avg_memory_usage'
                    ],
                    'importances': model.feature_importances_.tolist()
                }
                
        return importance
        
    except Exception as e:
        logger.error(f"Failed to get feature importance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/optimization-history")
async def get_optimization_history(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    workflow_id: Optional[str] = Query(None),
    target: Optional[OptimizationTarget] = Query(None)
):
    """Get optimization history"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        history = ml_optimizer.optimization_history
        
        # Apply filters
        if workflow_id:
            history = [h for h in history if h.get('workflow_id') == workflow_id]
            
        if target:
            history = [h for h in history if h.get('target') == target]
            
        # Sort by timestamp (newest first)
        history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        # Apply pagination
        start = offset
        end = offset + limit
        paginated = history[start:end]
        
        return {
            "total": len(history),
            "offset": offset,
            "limit": limit,
            "items": paginated
        }
        
    except Exception as e:
        logger.error(f"Failed to get optimization history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-status")
async def get_model_status():
    """Get status of ML models"""
    if not ml_optimizer:
        raise HTTPException(status_code=503, detail="ML optimizer not initialized")
        
    try:
        status = {
            "models": {},
            "anomaly_detector": False,
            "last_training": None,
            "optimization_count": len(ml_optimizer.optimization_history)
        }
        
        # Check resource predictor
        if 'resource_predictor' in ml_optimizer.models:
            status['models']['resource_predictor'] = {
                "available": True,
                "type": type(ml_optimizer.models['resource_predictor']).__name__
            }
            
        # Check performance predictor
        if 'performance_predictor' in ml_optimizer.models:
            status['models']['performance_predictor'] = {
                "available": True,
                "type": type(ml_optimizer.models['performance_predictor']).__name__
            }
            
        # Check anomaly detector
        if ml_optimizer.anomaly_detector:
            status['anomaly_detector'] = True
            
        return status
        
    except Exception as e:
        logger.error(f"Failed to get model status: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 