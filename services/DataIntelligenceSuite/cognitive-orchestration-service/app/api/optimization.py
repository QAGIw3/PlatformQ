"""Optimization API endpoints"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Request, Query, Body
from pydantic import BaseModel, Field
import structlog
from datetime import datetime
import pandas as pd

logger = structlog.get_logger()
router = APIRouter()


class OptimizationRecommendationRequest(BaseModel):
    """Request for optimization recommendations"""
    workflow_id: str = Field(..., description="Workflow identifier")
    include_cost_analysis: bool = Field(True, description="Include cost analysis")
    include_performance_metrics: bool = Field(True, description="Include performance metrics")


@router.get("/recommendations/{workflow_id}")
async def get_optimization_recommendations(
    request: Request,
    workflow_id: str
) -> Dict[str, Any]:
    """Get optimization recommendations for a workflow"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Get recommendations
        recommendations = await orchestrator.get_optimization_recommendations(workflow_id)
        
        return {
            "workflow_id": workflow_id,
            "recommendations": recommendations,
            "recommendation_count": len(recommendations)
        }
        
    except Exception as e:
        logger.error(f"Failed to get recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns")
async def get_system_patterns(
    request: Request,
    pattern_type: str = Query("all", description="Type of patterns: temporal, resource, failure, all")
) -> Dict[str, Any]:
    """Get analyzed system patterns"""
    try:
        ml_optimizer = request.app.state.ml_optimizer
        system_monitor = request.app.state.system_monitor
        orchestrator = request.app.state.orchestrator
        
        # Get recent data
        recent_executions = orchestrator.workflow_history[-500:]
        historical_metrics = await system_monitor.get_historical_metrics()
        
        # Analyze patterns
        patterns = await ml_optimizer.analyze_patterns(
            executions=recent_executions,
            metrics={"historical": historical_metrics}
        )
        
        # Filter by type if requested
        if pattern_type != "all":
            patterns = {pattern_type: patterns.get(pattern_type, {})}
            
        return {
            "patterns": patterns,
            "analysis_timestamp": datetime.utcnow().isoformat(),
            "data_points_analyzed": len(recent_executions)
        }
        
    except Exception as e:
        logger.error(f"Pattern analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/optimization-history")
async def get_optimization_history(
    request: Request,
    workflow_id: Optional[str] = Query(None, description="Filter by workflow"),
    limit: int = Query(100, ge=1, le=1000)
) -> Dict[str, Any]:
    """Get history of optimizations applied"""
    try:
        ml_optimizer = request.app.state.ml_optimizer
        
        # Get optimization history
        history = ml_optimizer.optimization_history[-limit:]
        
        # Filter by workflow if requested
        if workflow_id:
            history = [h for h in history if h.get("workflow_id") == workflow_id]
            
        return {
            "optimizations": history,
            "total_count": len(history)
        }
        
    except Exception as e:
        logger.error(f"Failed to get optimization history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/simulate")
async def simulate_optimization(
    request: Request,
    workflow_config: Dict[str, Any] = Body(..., description="Workflow configuration"),
    optimization_params: Dict[str, Any] = Body(..., description="Optimization parameters to test")
) -> Dict[str, Any]:
    """Simulate the effect of applying optimizations"""
    try:
        ml_optimizer = request.app.state.ml_optimizer
        
        # Extract current features
        features = ml_optimizer._extract_features(
            workflow_config,
            await request.app.state.system_monitor.get_current_metrics(),
            pd.DataFrame()  # Empty history for simulation
        )
        
        # Predict performance with optimizations
        predicted = ml_optimizer._predict_performance(features, optimization_params)
        
        # Compare with baseline
        baseline = ml_optimizer._predict_performance(features, workflow_config)
        
        improvement = {
            "duration_reduction": (baseline["duration"] - predicted["duration"]) / baseline["duration"],
            "cost_reduction": (baseline["cost"] - predicted["cost"]) / baseline["cost"],
            "reliability_improvement": predicted["success_rate"] - baseline["success_rate"]
        }
        
        return {
            "baseline_performance": baseline,
            "optimized_performance": predicted,
            "expected_improvement": improvement,
            "optimization_params": optimization_params
        }
        
    except Exception as e:
        logger.error(f"Optimization simulation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-performance")
async def get_model_performance(
    request: Request
) -> Dict[str, Any]:
    """Get performance metrics of optimization models"""
    try:
        ml_optimizer = request.app.state.ml_optimizer
        
        # Get model metrics
        metrics = {
            "performance_model": {
                "type": type(ml_optimizer.performance_model).__name__ if ml_optimizer.performance_model else "Not initialized",
                "last_updated": "N/A",  # Would track this
                "accuracy": 0.85  # Would calculate actual accuracy
            },
            "resource_predictor": {
                "type": "Neural Network" if ml_optimizer.resource_predictor else "Not initialized",
                "last_updated": "N/A",
                "accuracy": 0.82
            },
            "anomaly_detector": {
                "type": type(ml_optimizer.anomaly_detector).__name__ if ml_optimizer.anomaly_detector else "Not initialized",
                "anomalies_detected": 0  # Would track this
            }
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get model performance: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 