"""
Real-time Analytics Service

Provides advanced monitoring, analytics, and predictive capabilities
for multi-physics simulations.
"""

import asyncio
from typing import Dict, List, Any
from datetime import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging

from .monitoring.dashboard_service import SimulationDashboardService
from .monitoring.predictive_maintenance import PredictiveMaintenanceModel
from .monitoring.timeseries_analysis import TimeSeriesAnalyzer

logger = logging.getLogger(__name__)

# Configuration
IGNITE_CONFIG = {
    'host': 'ignite',
    'port': 10800
}

ES_CONFIG = {
    'host': 'http://elasticsearch:9200'
}

# Initialize FastAPI app
app = FastAPI(
    title="Real-time Analytics Service",
    description="Advanced monitoring and analytics for multi-physics simulations",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
dashboard_service = None
maintenance_model = None
timeseries_analyzer = None


# Pydantic models
class DashboardCreateRequest(BaseModel):
    simulation_id: str
    dashboard_config: Dict[str, Any] = {}


class MetricsUpdateRequest(BaseModel):
    simulation_id: str
    metrics: Dict[str, Any]


class MaintenancePredictionRequest(BaseModel):
    component_id: str
    component_type: str


class TimeSeriesAnalysisRequest(BaseModel):
    simulation_id: str
    window_hours: int = 24


class ComponentTrainingRequest(BaseModel):
    component_type: str
    lookback_days: int = 30


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global dashboard_service, maintenance_model, timeseries_analyzer
    
    dashboard_service = SimulationDashboardService(IGNITE_CONFIG, ES_CONFIG)
    maintenance_model = PredictiveMaintenanceModel(IGNITE_CONFIG, ES_CONFIG)
    timeseries_analyzer = TimeSeriesAnalyzer(IGNITE_CONFIG, ES_CONFIG)
    
    logger.info("Real-time Analytics Service started")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    if dashboard_service:
        dashboard_service.close()
    if maintenance_model:
        maintenance_model.close()
    if timeseries_analyzer:
        timeseries_analyzer.close()
        
    logger.info("Real-time Analytics Service stopped")


# Dashboard endpoints
@app.post("/api/v1/dashboards")
async def create_dashboard(request: DashboardCreateRequest):
    """Create a new simulation dashboard"""
    try:
        dashboard = await dashboard_service.create_simulation_dashboard(
            request.simulation_id
        )
        return {
            "status": "success",
            "dashboard": dashboard
        }
    except Exception as e:
        logger.error(f"Error creating dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/metrics/update")
async def update_metrics(request: MetricsUpdateRequest):
    """Update simulation metrics"""
    try:
        await dashboard_service.update_metrics(
            request.simulation_id,
            request.metrics
        )
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analytics/convergence/{simulation_id}")
async def get_convergence_analytics(simulation_id: str):
    """Get convergence analytics for a simulation"""
    try:
        analytics = await dashboard_service.get_convergence_analytics(simulation_id)
        return analytics
    except Exception as e:
        logger.error(f"Error getting convergence analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analytics/resources/{simulation_id}")
async def get_resource_analytics(simulation_id: str):
    """Get resource usage analytics"""
    try:
        analytics = await dashboard_service.get_resource_analytics(simulation_id)
        return analytics
    except Exception as e:
        logger.error(f"Error getting resource analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Predictive maintenance endpoints
@app.post("/api/v1/maintenance/train")
async def train_maintenance_models(
    request: ComponentTrainingRequest,
    background_tasks: BackgroundTasks
):
    """Train predictive maintenance models"""
    background_tasks.add_task(
        maintenance_model.train_component_models,
        request.component_type,
        request.lookback_days
    )
    return {
        "status": "training_started",
        "component_type": request.component_type
    }


@app.post("/api/v1/maintenance/predict")
async def predict_maintenance(request: MaintenancePredictionRequest):
    """Get maintenance predictions for a component"""
    try:
        predictions = await maintenance_model.predict_maintenance(
            request.component_id,
            request.component_type
        )
        return predictions
    except Exception as e:
        logger.error(f"Error predicting maintenance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/maintenance/fleet-health")
async def get_fleet_health(component_type: str = None):
    """Get fleet health status"""
    try:
        health = await maintenance_model.get_fleet_health(component_type)
        return health
    except Exception as e:
        logger.error(f"Error getting fleet health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Time-series analysis endpoints
@app.post("/api/v1/timeseries/analyze")
async def analyze_timeseries(request: TimeSeriesAnalysisRequest):
    """Analyze time-series convergence pattern"""
    try:
        analysis = await timeseries_analyzer.analyze_convergence_pattern(
            request.simulation_id,
            request.window_hours
        )
        return analysis
    except Exception as e:
        logger.error(f"Error analyzing time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/timeseries/detect-anomalies")
async def detect_anomalies(simulation_ids: List[str]):
    """Detect anomalous patterns across simulations"""
    try:
        anomalies = await timeseries_analyzer.detect_anomalous_patterns(
            simulation_ids
        )
        return {
            "anomalous_simulations": anomalies,
            "total_checked": len(simulation_ids),
            "anomalies_found": len(anomalies)
        }
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/timeseries/compare")
async def compare_simulations(simulation_ids: List[str]):
    """Compare convergence patterns across simulations"""
    try:
        comparison = await timeseries_analyzer.compare_simulation_patterns(
            simulation_ids
        )
        return comparison
    except Exception as e:
        logger.error(f"Error comparing simulations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket endpoint for real-time updates
@app.websocket("/ws/dashboard/{simulation_id}")
async def dashboard_websocket(websocket: WebSocket, simulation_id: str):
    """WebSocket for real-time dashboard updates"""
    await websocket.accept()
    
    try:
        # Register websocket
        await dashboard_service.register_websocket(simulation_id, websocket)
        
        # Keep connection alive
        while True:
            # Wait for client messages (ping/pong)
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for simulation {simulation_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await dashboard_service.unregister_websocket(simulation_id, websocket)


# Health check
@app.get("/health")
async def health_check():
    """Service health check"""
    return {
        "status": "healthy",
        "service": "realtime-analytics-service",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "dashboard_service": dashboard_service is not None,
            "maintenance_model": maintenance_model is not None,
            "timeseries_analyzer": timeseries_analyzer is not None
        }
    }


# Advanced analytics endpoints
@app.get("/api/v1/analytics/simulation-insights/{simulation_id}")
async def get_simulation_insights(simulation_id: str):
    """Get comprehensive insights for a simulation"""
    try:
        # Gather data from all components
        convergence = await dashboard_service.get_convergence_analytics(simulation_id)
        resources = await dashboard_service.get_resource_analytics(simulation_id)
        timeseries = await timeseries_analyzer.analyze_convergence_pattern(simulation_id)
        
        # Combine insights
        insights = {
            "simulation_id": simulation_id,
            "convergence_status": {
                "quality": convergence.get("convergence_quality", "unknown"),
                "current_residual": convergence.get("current_residual"),
                "estimated_completion": convergence.get("estimated_iterations_remaining")
            },
            "resource_efficiency": {
                "cpu_utilization": resources.get("cpu", {}).get("average", 0),
                "memory_utilization": resources.get("memory", {}).get("average", 0),
                "gpu_utilization": resources.get("gpu", {}).get("average", 0)
            },
            "pattern_analysis": {
                "pattern_type": timeseries.get("pattern_type", "unknown"),
                "trend": timeseries.get("trend", {}),
                "change_points": len(timeseries.get("change_points", [])),
                "forecast": timeseries.get("forecast", {})
            },
            "recommendations": _generate_recommendations(convergence, resources, timeseries)
        }
        
        return insights
        
    except Exception as e:
        logger.error(f"Error getting simulation insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _generate_recommendations(convergence: Dict, resources: Dict, timeseries: Dict) -> List[Dict]:
    """Generate recommendations based on analytics"""
    recommendations = []
    
    # Convergence-based recommendations
    if convergence.get("convergence_quality") == "stagnant":
        recommendations.append({
            "type": "convergence",
            "priority": "high",
            "action": "adjust_parameters",
            "description": "Convergence has stagnated. Consider adjusting relaxation factors or using quantum optimization."
        })
        
    # Resource-based recommendations
    cpu_avg = resources.get("cpu", {}).get("average", 0)
    if cpu_avg < 50:
        recommendations.append({
            "type": "resource",
            "priority": "medium",
            "action": "increase_parallelism",
            "description": "CPU utilization is low. Consider increasing parallelism or domain decomposition."
        })
        
    # Pattern-based recommendations
    pattern = timeseries.get("pattern_type")
    if pattern == "oscillatory":
        recommendations.append({
            "type": "stability",
            "priority": "high",
            "action": "damping_adjustment",
            "description": "Oscillatory convergence detected. Increase damping or relaxation factors."
        })
    elif pattern == "diverging":
        recommendations.append({
            "type": "stability",
            "priority": "critical",
            "action": "emergency_stabilization",
            "description": "Simulation is diverging! Immediate parameter adjustment required."
        })
        
    return recommendations


@app.post("/api/v1/analytics/batch-analysis")
async def batch_analysis(simulation_ids: List[str], background_tasks: BackgroundTasks):
    """Perform batch analysis on multiple simulations"""
    
    async def _perform_batch_analysis():
        results = {}
        for sim_id in simulation_ids:
            try:
                analysis = await timeseries_analyzer.analyze_convergence_pattern(sim_id)
                results[sim_id] = {
                    "status": "completed",
                    "pattern": analysis.get("pattern_type"),
                    "quality": analysis.get("convergence_rate", {}).get("average_rate", 0)
                }
            except Exception as e:
                results[sim_id] = {
                    "status": "failed",
                    "error": str(e)
                }
                
        # Store results in Ignite
        timeseries_analyzer.analysis_cache.put(
            f"batch_analysis_{datetime.utcnow().timestamp()}",
            results
        )
        
    background_tasks.add_task(_perform_batch_analysis)
    
    return {
        "status": "batch_analysis_started",
        "simulations": len(simulation_ids),
        "message": "Results will be available shortly"
    }


@app.get('/api/v1/trust-analytics')
async def trust_analytics():
    # Ignite query with trust
    query = "SELECT * FROM metrics WHERE trust_score > 0.5"
    results = ignite_client.sql(query)
    return {'trusted_metrics': results}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 