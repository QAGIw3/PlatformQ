"""
Real-time Analytics Service

Provides advanced monitoring, analytics, and predictive capabilities
using Apache Druid for time-series OLAP, Apache Ignite for in-memory
computing, and real-time ML for streaming analytics.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import logging
import json
import numpy as np
from contextlib import asynccontextmanager

# Import enhanced modules
from .analytics.druid_analytics import DruidAnalyticsEngine
from .analytics.stream_processor import StreamProcessor
from .analytics.realtime_ml import RealtimeMLEngine
from .monitoring.dashboard_service import SimulationDashboardService
from .monitoring.predictive_maintenance import PredictiveMaintenanceModel
from .monitoring.timeseries_analysis import TimeSeriesAnalyzer
from .monitoring.metrics_aggregator import MetricsAggregator

logger = logging.getLogger(__name__)

# Configuration
DRUID_CONFIG = {
    'coordinator_url': 'http://druid-coordinator:8081',
    'broker_url': 'http://druid-broker:8082',
    'overlord_url': 'http://druid-overlord:8090'
}

IGNITE_CONFIG = {
    'host': 'ignite',
    'port': 10800
}

ES_CONFIG = {
    'host': 'http://elasticsearch:9200'
}

# Global service instances
druid_engine = None
stream_processor = None
realtime_ml = None
dashboard_service = None
maintenance_model = None
timeseries_analyzer = None
metrics_aggregator = None

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global druid_engine, stream_processor, realtime_ml
    global dashboard_service, maintenance_model, timeseries_analyzer, metrics_aggregator
    
    # Startup
    logger.info("Initializing Real-time Analytics Service...")
    
    # Initialize services
    druid_engine = DruidAnalyticsEngine(DRUID_CONFIG)
    stream_processor = StreamProcessor(DRUID_CONFIG, IGNITE_CONFIG)
    realtime_ml = RealtimeMLEngine()
    
    dashboard_service = SimulationDashboardService(IGNITE_CONFIG, ES_CONFIG)
    maintenance_model = PredictiveMaintenanceModel(IGNITE_CONFIG, ES_CONFIG)
    timeseries_analyzer = TimeSeriesAnalyzer(IGNITE_CONFIG, ES_CONFIG)
    metrics_aggregator = MetricsAggregator(IGNITE_CONFIG, DRUID_CONFIG)
    
    # Start background services
    await stream_processor.start()
    await realtime_ml.start()
    
    logger.info("Real-time Analytics Service initialized")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Real-time Analytics Service...")
    
    await stream_processor.stop()
    await realtime_ml.stop()
    
    if dashboard_service:
        dashboard_service.close()
    if maintenance_model:
        maintenance_model.close()
    if timeseries_analyzer:
        timeseries_analyzer.close()
        
    logger.info("Real-time Analytics Service stopped")

# Initialize FastAPI app
app = FastAPI(
    title="Real-time Analytics Service",
    description="Advanced monitoring and analytics with Apache Druid and real-time ML",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class MetricIngestRequest(BaseModel):
    """Request to ingest metrics"""
    metrics: List[Dict[str, Any]] = Field(..., description="List of metrics to ingest")
    
class TimeSeriesQueryRequest(BaseModel):
    """Time series query request"""
    datasource: str = Field(..., description="Druid datasource name")
    metric: str = Field(..., description="Metric to query")
    aggregation: str = Field("sum", description="Aggregation type: sum, avg, min, max, count")
    granularity: str = Field("minute", description="Time granularity: second, minute, hour, day")
    filter: Optional[Dict[str, Any]] = Field(None, description="Dimension filters")
    group_by: Optional[List[str]] = Field(None, description="Dimensions to group by")
    start_time: datetime = Field(..., description="Query start time")
    end_time: datetime = Field(..., description="Query end time")

class OLAPQueryRequest(BaseModel):
    """OLAP cube query request"""
    datasource: str
    metrics: List[str]
    dimensions: List[str]
    filters: Optional[Dict[str, Any]] = None
    time_range: Optional[Dict[str, datetime]] = None
    rollup: Optional[str] = None
    
class AnomalyDetectionRequest(BaseModel):
    """Anomaly detection request"""
    metric_name: str
    sensitivity: float = Field(0.95, ge=0.5, le=0.99)
    lookback_hours: int = Field(24, ge=1, le=168)
    
class DashboardCreateRequest(BaseModel):
    """Dashboard creation request"""
    name: str
    description: Optional[str] = None
    datasources: List[str]
    refresh_interval: int = Field(5, description="Refresh interval in seconds")
    widgets: List[Dict[str, Any]] = Field(default_factory=list)

class MetricsUpdateRequest(BaseModel):
    simulation_id: str
    metrics: Dict[str, Any]
    timestamp: Optional[datetime] = None

class MaintenancePredictionRequest(BaseModel):
    component_id: str
    component_type: str

class TimeSeriesAnalysisRequest(BaseModel):
    simulation_id: str
    window_hours: int = 24

# Metrics ingestion endpoints
@app.post("/api/v1/metrics/ingest")
async def ingest_metrics(request: MetricIngestRequest):
    """Ingest metrics into Druid for real-time analytics"""
    try:
        # Process metrics through stream processor
        for metric in request.metrics:
            # Add timestamp if not present
            if 'timestamp' not in metric:
                metric['timestamp'] = datetime.utcnow().isoformat()
            
            # Send to stream processor
            await stream_processor.process_metric(metric)
            
            # Update real-time ML models
            await realtime_ml.update_with_metric(metric)
        
        return {
            "status": "success",
            "metrics_ingested": len(request.metrics)
        }
    except Exception as e:
        logger.error(f"Error ingesting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/query/timeseries")
async def query_timeseries(request: TimeSeriesQueryRequest):
    """Execute time series query on Druid"""
    try:
        results = await druid_engine.query_timeseries(
            datasource=request.datasource,
            metric=request.metric,
            aggregation=request.aggregation,
            granularity=request.granularity,
            filter=request.filter,
            group_by=request.group_by,
            start_time=request.start_time,
            end_time=request.end_time
        )
        
        return {
            "query": {
                "metric": request.metric,
                "aggregation": request.aggregation,
                "time_range": f"{request.start_time} to {request.end_time}"
            },
            "results": results
        }
    except Exception as e:
        logger.error(f"Error executing time series query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/query/olap")
async def query_olap(request: OLAPQueryRequest):
    """Execute OLAP query for multi-dimensional analysis"""
    try:
        results = await druid_engine.query_olap(
            datasource=request.datasource,
            metrics=request.metrics,
            dimensions=request.dimensions,
            filters=request.filters,
            time_range=request.time_range,
            rollup=request.rollup
        )
        
        return {
            "query_type": "olap",
            "dimensions": request.dimensions,
            "metrics": request.metrics,
            "results": results
        }
    except Exception as e:
        logger.error(f"Error executing OLAP query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/query/topn/{datasource}/{metric}")
async def query_topn(
    datasource: str,
    metric: str,
    dimension: str = Query(..., description="Dimension to rank by"),
    threshold: int = Query(10, description="Number of top results"),
    start_time: datetime = Query(...),
    end_time: datetime = Query(...)
):
    """Get top N values for a dimension"""
    try:
        results = await druid_engine.query_topn(
            datasource=datasource,
            metric=metric,
            dimension=dimension,
            threshold=threshold,
            start_time=start_time,
            end_time=end_time
        )
        
        return {
            "dimension": dimension,
            "metric": metric,
            "top_n": threshold,
            "results": results
        }
    except Exception as e:
        logger.error(f"Error executing TopN query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Real-time analytics endpoints
@app.post("/api/v1/analytics/anomaly-detection")
async def detect_anomalies(request: AnomalyDetectionRequest):
    """Detect anomalies in real-time metrics"""
    try:
        # Get recent metric data
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=request.lookback_hours)
        
        metric_data = await druid_engine.query_timeseries(
            datasource="metrics",
            metric=request.metric_name,
            aggregation="avg",
            granularity="minute",
            start_time=start_time,
            end_time=end_time
        )
        
        # Run anomaly detection
        anomalies = await realtime_ml.detect_anomalies(
            metric_data,
            sensitivity=request.sensitivity
        )
        
        return {
            "metric": request.metric_name,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies
        }
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/forecasting/{metric_name}")
async def forecast_metric(
    metric_name: str,
    forecast_hours: int = Query(24, description="Hours to forecast"),
    confidence_level: float = Query(0.95, description="Confidence interval")
):
    """Forecast future metric values"""
    try:
        # Get historical data
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=7)  # Use 7 days of history
        
        historical_data = await druid_engine.query_timeseries(
            datasource="metrics",
            metric=metric_name,
            aggregation="avg",
            granularity="hour",
            start_time=start_time,
            end_time=end_time
        )
        
        # Generate forecast
        forecast = await realtime_ml.forecast_metric(
            historical_data,
            forecast_hours=forecast_hours,
            confidence_level=confidence_level
        )
        
        return {
            "metric": metric_name,
            "forecast_period": f"{forecast_hours} hours",
            "confidence_level": confidence_level,
            "forecast": forecast
        }
    except Exception as e:
        logger.error(f"Error generating forecast: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Dashboard endpoints (keeping existing functionality)
@app.post("/api/v1/dashboards")
async def create_dashboard(request: DashboardCreateRequest):
    """Create a real-time analytics dashboard"""
    try:
        dashboard_id = await dashboard_service.create_dashboard(
            name=request.name,
            description=request.description,
            datasources=request.datasources,
            refresh_interval=request.refresh_interval,
            widgets=request.widgets
        )
        
        # Configure Druid datasources for dashboard
        for datasource in request.datasources:
            await druid_engine.ensure_datasource(datasource)
        
        return {
            "dashboard_id": dashboard_id,
            "status": "created"
        }
    except Exception as e:
        logger.error(f"Error creating dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/metrics/update")
async def update_metrics(request: MetricsUpdateRequest):
    """Update simulation metrics (legacy endpoint)"""
    try:
        # Store in both Ignite and Druid
        await dashboard_service.update_metrics(
            request.simulation_id,
            request.metrics
        )
        
        # Also ingest into Druid
        druid_metric = {
            "timestamp": request.timestamp or datetime.utcnow(),
            "simulation_id": request.simulation_id,
            **request.metrics
        }
        await druid_engine.ingest_metric("simulation_metrics", druid_metric)
        
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Predictive maintenance endpoints
@app.post("/api/v1/maintenance/predict")
async def predict_maintenance(request: MaintenancePredictionRequest):
    """Predict maintenance requirements"""
    try:
        prediction = await maintenance_model.predict_maintenance(
            request.component_id,
            request.component_type
        )
        
        # Enrich with real-time metrics
        current_metrics = await druid_engine.get_latest_metrics(
            datasource="component_metrics",
            filters={"component_id": request.component_id}
        )
        
        prediction["current_metrics"] = current_metrics
        
        return prediction
    except Exception as e:
        logger.error(f"Error predicting maintenance: {e}")
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
        
        # Add Druid-based trend analysis
        druid_trends = await druid_engine.analyze_trends(
            datasource="simulation_metrics",
            filters={"simulation_id": request.simulation_id},
            window_hours=request.window_hours
        )
        
        analysis["druid_trends"] = druid_trends
        
        return analysis
    except Exception as e:
        logger.error(f"Error analyzing time series: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Advanced analytics endpoints
@app.get("/api/v1/analytics/data-quality/{datasource}")
async def assess_data_quality(datasource: str):
    """Assess data quality for a datasource"""
    try:
        quality_metrics = await druid_engine.assess_data_quality(datasource)
        
        return {
            "datasource": datasource,
            "quality_score": quality_metrics.get("overall_score", 0),
            "completeness": quality_metrics.get("completeness", 0),
            "consistency": quality_metrics.get("consistency", 0),
            "timeliness": quality_metrics.get("timeliness", 0),
            "issues": quality_metrics.get("issues", [])
        }
    except Exception as e:
        logger.error(f"Error assessing data quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/analytics/correlation-analysis")
async def analyze_correlations(
    metrics: List[str] = Query(..., description="Metrics to analyze"),
    datasource: str = Query("metrics", description="Datasource"),
    window_hours: int = Query(24, description="Analysis window")
):
    """Analyze correlations between metrics"""
    try:
        correlations = await realtime_ml.analyze_correlations(
            datasource=datasource,
            metrics=metrics,
            window_hours=window_hours
        )
        
        return {
            "metrics": metrics,
            "window": f"{window_hours} hours",
            "correlations": correlations
        }
    except Exception as e:
        logger.error(f"Error analyzing correlations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Streaming analytics endpoints
@app.get("/api/v1/streaming/window-aggregates/{metric}")
async def get_window_aggregates(
    metric: str,
    windows: List[str] = Query(["1m", "5m", "15m"], description="Time windows")
):
    """Get real-time window aggregates for a metric"""
    try:
        aggregates = await stream_processor.get_window_aggregates(metric, windows)
        
        return {
            "metric": metric,
            "windows": windows,
            "aggregates": aggregates
        }
    except Exception as e:
        logger.error(f"Error getting window aggregates: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket endpoint for real-time updates
@app.websocket("/ws/analytics/{client_id}")
async def analytics_websocket(websocket: WebSocket, client_id: str):
    """WebSocket for real-time analytics updates"""
    await websocket.accept()
    
    try:
        # Register websocket with stream processor
        await stream_processor.register_websocket(client_id, websocket)
        
        # Keep connection alive
        while True:
            data = await websocket.receive_text()
            
            if data == "ping":
                await websocket.send_text("pong")
            else:
                # Handle subscription requests
                message = json.loads(data)
                if message.get("type") == "subscribe":
                    metrics = message.get("metrics", [])
                    await stream_processor.subscribe_client(client_id, metrics)
                elif message.get("type") == "unsubscribe":
                    metrics = message.get("metrics", [])
                    await stream_processor.unsubscribe_client(client_id, metrics)
                    
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for client {client_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await stream_processor.unregister_websocket(client_id)

# Export endpoints
@app.get("/api/v1/export/prometheus")
async def export_prometheus_metrics():
    """Export metrics in Prometheus format"""
    try:
        # Get current metrics from Druid
        metrics = await druid_engine.get_current_metrics()
        
        # Format as Prometheus metrics
        prometheus_data = metrics_aggregator.format_prometheus(metrics)
        
        return prometheus_data
    except Exception as e:
        logger.error(f"Error exporting Prometheus metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/export/batch-report")
async def generate_batch_report(
    datasources: List[str],
    start_time: datetime,
    end_time: datetime,
    format: str = Query("json", description="Output format: json, csv, parquet")
):
    """Generate batch analytics report"""
    try:
        report_data = await druid_engine.generate_batch_report(
            datasources=datasources,
            start_time=start_time,
            end_time=end_time
        )
        
        if format == "csv":
            # Convert to CSV
            return {"message": "CSV export not implemented yet"}
        elif format == "parquet":
            # Convert to Parquet
            return {"message": "Parquet export not implemented yet"}
        else:
            return report_data
            
    except Exception as e:
        logger.error(f"Error generating batch report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Health check
@app.get("/health")
async def health_check():
    """Service health check"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "druid": await druid_engine.check_health() if druid_engine else False,
            "ignite": dashboard_service.ignite_client is not None if dashboard_service else False,
            "elasticsearch": True,  # Add proper ES health check
            "stream_processor": stream_processor.is_running if stream_processor else False,
            "realtime_ml": realtime_ml.is_running if realtime_ml else False
        }
    }
    
    # Overall health
    all_healthy = all(health_status["components"].values())
    health_status["status"] = "healthy" if all_healthy else "degraded"
    
    return health_status

# Legacy endpoints (keeping for backward compatibility)
@app.websocket("/ws/dashboard/{simulation_id}")
async def dashboard_websocket(websocket: WebSocket, simulation_id: str):
    """WebSocket for real-time dashboard updates (legacy)"""
    await websocket.accept()
    
    try:
        await dashboard_service.register_websocket(simulation_id, websocket)
        
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for simulation {simulation_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await dashboard_service.unregister_websocket(simulation_id, websocket)

@app.get("/api/v1/analytics/simulation-insights/{simulation_id}")
async def get_simulation_insights(simulation_id: str):
    """Get comprehensive insights for a simulation"""
    try:
        # Gather data from all components
        convergence = await dashboard_service.get_convergence_analytics(simulation_id)
        resources = await dashboard_service.get_resource_analytics(simulation_id)
        timeseries = await timeseries_analyzer.analyze_convergence_pattern(simulation_id)
        
        # Add Druid-based insights
        druid_insights = await druid_engine.get_simulation_insights(simulation_id)
        
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
            "druid_insights": druid_insights,
            "recommendations": _generate_recommendations(convergence, resources, timeseries)
        }
        
        return insights
        
    except Exception as e:
        logger.error(f"Error getting simulation insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _generate_recommendations(convergence: Dict, resources: Dict, timeseries: Dict) -> List[str]:
    """Generate recommendations based on analytics"""
    recommendations = []
    
    # Convergence recommendations
    if convergence.get("convergence_quality") == "poor":
        recommendations.append("Consider adjusting solver parameters for better convergence")
    
    if convergence.get("stagnation_periods"):
        recommendations.append("Detected convergence stagnation - try adaptive relaxation")
    
    # Resource recommendations
    cpu_avg = resources.get("cpu", {}).get("average", 0)
    if cpu_avg > 90:
        recommendations.append("High CPU utilization - consider scaling up compute resources")
    elif cpu_avg < 30:
        recommendations.append("Low CPU utilization - resources may be over-provisioned")
    
    # Pattern-based recommendations
    if timeseries.get("pattern_type") == "oscillating":
        recommendations.append("Oscillating convergence detected - check for numerical instabilities")
    
    return recommendations

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 