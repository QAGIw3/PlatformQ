"""
Unified Analytics Service

Provides comprehensive analytics capabilities including:
- Batch analytics via Trino
- Real-time analytics via Druid and Ignite
- ML-based predictions and anomaly detection
- Cross-service monitoring and dashboards
- Streaming analytics and event processing
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from enum import Enum
import json

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query as QueryParam, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from elasticsearch import AsyncElasticsearch
from pyignite import Client as IgniteClient
import httpx
import pulsar

from platformq_shared import (
    create_base_app, 
    EventProcessor, 
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients,
    NotFoundError,
    ValidationError
)
from platformq_shared.event_publisher import EventPublisher
from platformq_events import SimulationMetricEvent

# Import all analytics modules
from .analytics.druid_analytics import DruidAnalyticsEngine
from .analytics.stream_processor import StreamProcessor
from .analytics.realtime_ml import RealtimeMLEngine
from .analytics.simulation_analytics import SimulationAnalyticsConsumer
from .monitoring.dashboard_service import SimulationDashboardService
from .monitoring.predictive_maintenance import PredictiveMaintenanceModel
from .monitoring.timeseries_analysis import TimeSeriesAnalyzer, SimulationMetricsConsumer
from .monitoring.metrics_aggregator import MetricsAggregator
from .monitoring.anomaly_detection import SimulationAnomalyDetector, AnomalyDetectionConfig
from .dashboards.cross_service_dashboard import CrossServiceDashboard, DashboardOrchestrator, DashboardConfig

logger = logging.getLogger(__name__)


# ============= Models =============

class AnalyticsMode(str, Enum):
    """Analytics execution mode"""
    BATCH = "batch"         # Use Trino for complex queries
    REALTIME = "realtime"   # Use Druid/Ignite for low latency
    AUTO = "auto"           # Automatically choose based on query


class UnifiedQuery(BaseModel):
    """Unified query model supporting all query types"""
    # Common fields
    query: Optional[str] = Field(None, description="SQL query for batch mode")
    query_type: Optional[str] = Field(None, description="Predefined query type")
    mode: AnalyticsMode = Field(AnalyticsMode.AUTO, description="Execution mode")
    filters: Optional[Dict[str, Any]] = Field({}, description="Query filters")
    time_range: Optional[str] = Field("7d", description="Time range: 1h, 1d, 7d, 30d, 90d")
    
    # Grouping and aggregation
    group_by: Optional[List[str]] = Field([], description="Fields to group by")
    metrics: Optional[List[str]] = Field([], description="Metrics to calculate")
    aggregations: Optional[List[str]] = Field([], description="Aggregation functions")
    
    # Time series specific
    granularity: Optional[str] = Field(None, description="Time granularity")
    intervals: Optional[List[str]] = Field(None, description="Time intervals")
    
    # Advanced options
    limit: Optional[int] = Field(1000, description="Result limit")
    realtime_options: Optional[Dict[str, Any]] = Field({}, description="Real-time specific options")
    cache_ttl: Optional[int] = Field(300, description="Cache TTL in seconds")


class UnifiedResult(BaseModel):
    """Unified analytics result"""
    mode: str = Field(..., description="Mode used for execution")
    query_type: Optional[str] = None
    data: List[Dict[str, Any]]
    summary: Dict[str, Any]
    metadata: Dict[str, Any]
    execution_time_ms: float
    cached: bool = False


class TimeSeriesQuery(BaseModel):
    """Time series specific query"""
    datasource: str
    metrics: List[str]
    intervals: List[str]
    granularity: str = "hour"
    filter: Optional[Dict[str, Any]] = None
    aggregations: Optional[List[Dict[str, Any]]] = None
    context: Optional[Dict[str, Any]] = None


class AnomalyDetectionRequest(BaseModel):
    """Anomaly detection request"""
    datasource: str
    metrics: List[str]
    method: str = "isolation_forest"
    sensitivity: float = 0.95
    time_window: str = "1h"
    historical_days: int = 7


class ForecastRequest(BaseModel):
    """Forecasting request"""
    metric_name: str
    horizon_days: int = 7
    confidence_interval: float = 0.95
    include_components: bool = True


class DashboardCreate(BaseModel):
    """Dashboard creation request"""
    name: str
    type: str
    config: Dict[str, Any]
    refresh_interval: int = 30
    layout: Optional[Dict[str, Any]] = None


class MetricUpdate(BaseModel):
    """Metric update request"""
    simulation_id: str
    metrics: Dict[str, float]
    timestamp: Optional[datetime] = None
    tags: Optional[Dict[str, str]] = None


class MaintenancePrediction(BaseModel):
    """Maintenance prediction request"""
    component_id: str
    component_type: str
    metrics: Dict[str, float]
    history_days: int = 30


# ============= Configuration =============

TRINO_CONFIG = {
    'host': 'trino',
    'port': 8080,
    'catalog': 'iceberg',
    'schema': 'analytics'
}

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

PULSAR_CONFIG = {
    'url': 'pulsar://pulsar:6650'
}


# ============= Global Services =============

# Analytics engines
druid_engine = None
stream_processor = None
realtime_ml = None
metrics_aggregator = None

# Monitoring services
dashboard_service = None
maintenance_model = None
timeseries_analyzer = None
anomaly_detector = None

# Dashboard services
cross_service_dashboard = None
dashboard_orchestrator = None

# Data access
ignite_client = None
es_client = None
trino_client = None

# Event handling
event_publisher = None
simulation_analytics_consumer = None


# ============= Lifespan Management =============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global druid_engine, stream_processor, realtime_ml, metrics_aggregator
    global dashboard_service, maintenance_model, timeseries_analyzer, anomaly_detector
    global cross_service_dashboard, dashboard_orchestrator
    global ignite_client, es_client, trino_client, event_publisher
    global simulation_analytics_consumer
    
    # Startup
    logger.info("Initializing Unified Analytics Service...")
    
    try:
        # Initialize data clients
        ignite_client = IgniteClient()
        ignite_client.connect(IGNITE_CONFIG['host'], IGNITE_CONFIG['port'])
        
        es_client = AsyncElasticsearch([ES_CONFIG['host']])
        
        # Initialize Trino client
        trino_client = httpx.AsyncClient(base_url=f"http://{TRINO_CONFIG['host']}:{TRINO_CONFIG['port']}")
        
        # Initialize event publisher
        event_publisher = EventPublisher(PULSAR_CONFIG['url'])
        event_publisher.connect()
    
    # Initialize analytics engines
    druid_engine = DruidAnalyticsEngine(DRUID_CONFIG)
    stream_processor = StreamProcessor(DRUID_CONFIG, IGNITE_CONFIG)
        await stream_processor.initialize()
        
    realtime_ml = RealtimeMLEngine()
        await realtime_ml.initialize()
        
        metrics_aggregator = MetricsAggregator(IGNITE_CONFIG, DRUID_CONFIG)
        await metrics_aggregator.initialize()
    
    # Initialize monitoring services
    dashboard_service = SimulationDashboardService(IGNITE_CONFIG, ES_CONFIG)
    maintenance_model = PredictiveMaintenanceModel(IGNITE_CONFIG, ES_CONFIG)
        timeseries_analyzer = TimeSeriesAnalyzer(IGNITE_CONFIG, ES_CONFIG, event_publisher)
    
    # Initialize anomaly detector
    anomaly_detector = SimulationAnomalyDetector(
        ignite_client,
            event_publisher,
        mlops_service_url="http://mlops-service:8000",
        seatunnel_service_url="http://seatunnel-service:8000"
    )
    
    # Initialize cross-service dashboard
        cross_service_dashboard = CrossServiceDashboard(
            ignite_client=ignite_client,
            es_client=es_client,
            event_publisher=event_publisher
        )
        await cross_service_dashboard.initialize()
        
        dashboard_orchestrator = DashboardOrchestrator(
            cross_service_dashboard=cross_service_dashboard,
            dashboard_service=dashboard_service,
            anomaly_detector=anomaly_detector
        )
        
        # Initialize consumers
        simulation_analytics_consumer = SimulationAnalyticsConsumer(
            pulsar_url=PULSAR_CONFIG['url'],
            subscription_name="analytics-service",
            ignite_client=ignite_client,
            es_client=es_client
        )
        await simulation_analytics_consumer.start()
        
        logger.info("Unified Analytics Service initialized successfully")
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Unified Analytics Service...")
    
    try:
        # Close all connections
        if stream_processor:
            await stream_processor.close()
        if metrics_aggregator:
            await metrics_aggregator.close()
        if ignite_client:
    ignite_client.close()
        if es_client:
    await es_client.close()
        if trino_client:
            await trino_client.aclose()
        if event_publisher:
            event_publisher.close()
        if simulation_analytics_consumer:
            await simulation_analytics_consumer.stop()
            
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


# ============= Create App =============

app = FastAPI(
    title="Unified Analytics Service",
    description="Complete analytics platform with real-time, batch, ML, and monitoring capabilities",
    version="3.0.0",
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


# ============= Query Router =============

class UnifiedQueryRouter:
    """Routes queries to appropriate engine based on characteristics"""
    
    @staticmethod
    async def route_query(query: UnifiedQuery) -> UnifiedResult:
        """Route query to appropriate engine"""
        start_time = datetime.utcnow()
        
        # Determine execution mode
        if query.mode == AnalyticsMode.AUTO:
            mode = UnifiedQueryRouter._determine_mode(query)
        else:
            mode = query.mode
            
        # Execute based on mode
        if mode == AnalyticsMode.BATCH:
            result = await UnifiedQueryRouter._execute_batch(query)
        else:
            result = await UnifiedQueryRouter._execute_realtime(query)
            
        # Calculate execution time
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        return UnifiedResult(
            mode=mode.value,
            query_type=query.query_type,
            data=result.get('data', []),
            summary=result.get('summary', {}),
            metadata=result.get('metadata', {}),
            execution_time_ms=execution_time,
            cached=result.get('cached', False)
        )
            
    @staticmethod
    def _determine_mode(query: UnifiedQuery) -> AnalyticsMode:
        """Determine optimal execution mode"""
        # Use realtime for recent data
        if query.time_range and query.time_range in ['1h', '6h', '1d']:
            return AnalyticsMode.REALTIME
            
        # Use batch for complex queries
        if query.query and ('JOIN' in query.query.upper() or 'UNION' in query.query.upper()):
            return AnalyticsMode.BATCH
            
        # Use realtime for simple aggregations
        if query.metrics and len(query.metrics) < 5:
            return AnalyticsMode.REALTIME
            
        return AnalyticsMode.BATCH
        
    @staticmethod
    async def _execute_batch(query: UnifiedQuery) -> Dict[str, Any]:
        """Execute query using Trino"""
        # Build SQL query if not provided
        if not query.query:
            sql = UnifiedQueryRouter._build_sql_query(query)
        else:
            sql = query.query
            
        # Execute via Trino
        response = await trino_client.post(
            "/v1/statement",
            headers={"X-Trino-User": "analytics"},
            data=sql
        )
            
        # Process results
        data = []
        while response.status_code == 200:
            result = response.json()
            if 'data' in result:
                data.extend(result['data'])
            if 'nextUri' not in result:
                break
            response = await trino_client.get(result['nextUri'])
            
        return {
            'data': data,
            'summary': {'row_count': len(data)},
            'metadata': {'engine': 'trino'}
        }
        
    @staticmethod
    async def _execute_realtime(query: UnifiedQuery) -> Dict[str, Any]:
        """Execute query using Druid/Ignite"""
        # Check Ignite cache first
        cache_key = f"query:{hash(str(query.dict()))}"
        cache = ignite_client.get_cache('query_cache')
        cached = cache.get(cache_key)
        
        if cached:
            return {**cached, 'cached': True}
            
        # Execute via appropriate engine
        if query.query_type == 'timeseries':
            result = await druid_engine.query_timeseries(
                datasource='platform_metrics',
                intervals=query.intervals or [f"-{query.time_range}/now"],
                granularity=query.granularity or 'hour',
                aggregations=[
                    {"type": "doubleSum", "name": m, "fieldName": m}
                    for m in query.metrics
                ],
                filter=query.filters
            )
        else:
            # Use metrics aggregator for mixed queries
            result = await metrics_aggregator.aggregate_metrics(
                source='mixed',
                metrics=query.metrics,
                time_range=query.time_range,
                group_by=query.group_by,
                filters=query.filters
            )
            
        # Cache result
        cache.put(cache_key, result, query.cache_ttl)
            
        return result
        
    @staticmethod
    def _build_sql_query(query: UnifiedQuery) -> str:
        """Build SQL query from structured query"""
        # Simple query builder (can be enhanced)
        select_clause = ", ".join(query.metrics) if query.metrics else "*"
        from_clause = "platform_metrics"
        where_clause = ""
        group_clause = ""
        
        if query.filters:
            conditions = [f"{k} = '{v}'" for k, v in query.filters.items()]
            where_clause = f"WHERE {' AND '.join(conditions)}"
        
        if query.group_by:
            group_clause = f"GROUP BY {', '.join(query.group_by)}"
            
        return f"SELECT {select_clause} FROM {from_clause} {where_clause} {group_clause}"


# ============= API Endpoints =============

# --- Unified Query Endpoint ---

@app.post("/api/v1/query", response_model=UnifiedResult)
async def unified_query(query: UnifiedQuery) -> UnifiedResult:
    """
    Unified query endpoint that intelligently routes to appropriate engine.
    
    Supports:
    - Batch queries via Trino for complex analytics
    - Real-time queries via Druid for time-series
    - Cached queries via Ignite for sub-ms response
    - ML predictions and anomaly detection
    """
    try:
        return await UnifiedQueryRouter.route_query(query)
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Time Series Analytics ---

@app.post("/api/v1/query/timeseries")
async def query_timeseries(query: TimeSeriesQuery):
    """Execute time-series query using Druid"""
    try:
        result = await druid_engine.query_timeseries(
            datasource=query.datasource,
            intervals=query.intervals,
            granularity=query.granularity,
            aggregations=query.aggregations,
            filter=query.filter,
            context=query.context
        )
        return result
    except Exception as e:
        logger.error(f"Time series query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Monitoring Endpoints ---

@app.get("/api/v1/monitor/{scope}")
async def unified_monitoring(
    scope: str,
    time_range: str = QueryParam("1h"),
    service_id: Optional[str] = None
):
    """
    Unified monitoring endpoint.
    
    Scopes:
    - platform: Cross-service overview
    - service: Specific service metrics
    - simulation: Simulation monitoring
    - resource: Resource utilization
    """
    try:
        if scope == "platform":
            return await cross_service_dashboard.get_platform_overview(time_range)
        elif scope == "service" and service_id:
            return await cross_service_dashboard.get_service_metrics(service_id, time_range)
        elif scope == "simulation" and service_id:
            return await dashboard_service.get_simulation_dashboard(service_id)
        elif scope == "resource":
            return await cross_service_dashboard.get_resource_utilization(time_range)
        else:
            raise HTTPException(status_code=400, detail="Invalid scope or missing parameters")
    except Exception as e:
        logger.error(f"Monitoring query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Dashboard Management ---

@app.get("/api/v1/dashboards/{dashboard_type}")
async def get_dashboard(
    dashboard_type: str,
    dashboard_id: Optional[str] = None,
    time_range: str = QueryParam("1h")
):
    """Get dashboard by type"""
    try:
        if dashboard_type == "platform-overview":
            return await dashboard_orchestrator.get_platform_overview()
        elif dashboard_type == "service-comparison":
            return await cross_service_dashboard.get_service_comparison(time_range)
        elif dashboard_type == "simulation" and dashboard_id:
            return await dashboard_service.get_simulation_dashboard(dashboard_id)
        elif dashboard_type == "user-activity":
            return await cross_service_dashboard.get_user_activity_dashboard(time_range)
        elif dashboard_type == "ml-performance":
            return await cross_service_dashboard.get_ml_performance_dashboard(time_range)
        else:
            raise HTTPException(status_code=404, detail="Dashboard type not found")
    except Exception as e:
        logger.error(f"Dashboard retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/dashboards")
async def create_dashboard(dashboard: DashboardCreate):
    """Create a new dashboard"""
    try:
        result = await dashboard_service.create_dashboard(
            name=dashboard.name,
            type=dashboard.type,
            config=dashboard.config,
            refresh_interval=dashboard.refresh_interval,
            layout=dashboard.layout
        )
        return {"dashboard_id": result, "status": "created"}
    except Exception as e:
        logger.error(f"Dashboard creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- ML Operations ---

@app.post("/api/v1/ml/{operation}")
async def ml_operation(
    operation: str,
    data: Dict[str, Any],
    background_tasks: BackgroundTasks
):
    """
    Unified ML operations endpoint.
    
    Operations:
    - detect-anomalies: Real-time anomaly detection
    - forecast: Time series forecasting
    - predict-maintenance: Predictive maintenance
    - train-online: Online model training
    """
    try:
        if operation == "detect-anomalies":
            result = await anomaly_detector.detect_anomalies(
                simulation_id=data.get('simulation_id'),
                metrics=data.get('metrics', {}),
                config=AnomalyDetectionConfig(**data.get('config', {}))
        )
        
        elif operation == "forecast":
            result = await realtime_ml.forecast(
                time_series=data.get('time_series', []),
                target_column=data.get('target_column'),
                horizon_days=data.get('horizon_days', 7)
            )
            
        elif operation == "predict-maintenance":
            result = await maintenance_model.predict_failure(
                component_id=data.get('component_id'),
                metrics=data.get('metrics', {}),
                history_days=data.get('history_days', 30)
            )
            
        elif operation == "train-online":
            # Run training in background
            background_tasks.add_task(
                realtime_ml.train_online_model,
                model_name=data.get('model_name'),
                data=data.get('training_data', []),
                features=data.get('features', []),
                target=data.get('target')
            )
            result = {"status": "training started"}
            
        else:
            raise HTTPException(status_code=400, detail=f"Unknown operation: {operation}")
            
        return result
        
    except Exception as e:
        logger.error(f"ML operation {operation} failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- WebSocket Endpoints ---

@app.websocket("/api/v1/ws/{stream_type}/{stream_id}")
async def unified_websocket(
    websocket: WebSocket,
    stream_type: str,
    stream_id: str
):
    """
    Unified WebSocket endpoint for real-time streams.
    
    Stream types:
    - dashboard: Dashboard updates
    - metrics: Real-time metrics
    - anomalies: Anomaly alerts
    - analytics: Analytics results
    """
    await websocket.accept()
    
    try:
        if stream_type == "dashboard":
            await dashboard_orchestrator.handle_dashboard_websocket(websocket, stream_id)
            
        elif stream_type == "metrics":
            # Stream real-time metrics
            while True:
                metrics = await stream_processor.get_stream_metrics(stream_id, last_n_seconds=10)
                await websocket.send_json({
                    "type": "metrics_update",
                    "data": metrics,
                    "timestamp": datetime.utcnow().isoformat()
                })
                await asyncio.sleep(1)
                
        elif stream_type == "anomalies":
            # Stream anomaly alerts
            consumer = event_publisher.create_consumer(
                topic=f"anomalies.{stream_id}",
                subscription_name=f"ws-{stream_id}"
            )
            
            while True:
                msg = consumer.receive(timeout_millis=1000)
                if msg:
                    await websocket.send_json({
                        "type": "anomaly_alert",
                        "data": json.loads(msg.data().decode('utf-8')),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    consumer.acknowledge(msg)
                    
        elif stream_type == "analytics":
            # Stream analytics results
            await stream_analytics_websocket(websocket, stream_id)
            
        else:
            await websocket.close(code=1008, reason="Unknown stream type")
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {stream_type}/{stream_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close(code=1011, reason=str(e))


async def stream_analytics_websocket(websocket: WebSocket, client_id: str):
    """Handle analytics streaming via WebSocket"""
    try:
        # Register client
        await dashboard_service.register_websocket_client(client_id, websocket)
        
        # Keep connection alive and handle messages
        while True:
            data = await websocket.receive_json()
            
            if data.get('type') == 'subscribe':
                metric = data.get('metric')
                # Subscribe to metric updates
                await stream_processor.create_stream(
                    stream_name=f"ws_{client_id}_{metric}",
                    topic=f"metrics.{metric}",
                    processing_func=lambda x: x,  # Pass through
                    aggregation_window=60
                )
                
            elif data.get('type') == 'query':
                # Execute query and stream results
                query = UnifiedQuery(**data.get('query', {}))
                result = await UnifiedQueryRouter.route_query(query)
                await websocket.send_json({
                    "type": "query_result",
                    "data": result.dict()
                })
                
    except WebSocketDisconnect:
        await dashboard_service.unregister_websocket_client(client_id)


# --- Data Ingestion ---

@app.post("/api/v1/metrics/ingest")
async def ingest_metrics(metrics: List[Dict[str, Any]]):
    """Ingest metrics into the analytics pipeline"""
    try:
        # Send to stream processor
        for metric in metrics:
            await stream_processor.process_metric(metric)
            
        # Also send to Druid for historical analysis
        await druid_engine.ingest_batch(metrics)
        
        return {"status": "success", "count": len(metrics)}
        
    except Exception as e:
        logger.error(f"Metrics ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Export Endpoints ---

@app.get("/api/v1/export/prometheus")
async def export_prometheus_metrics():
    """Export metrics in Prometheus format"""
    try:
        metrics = await metrics_aggregator.get_metric_trends(
            metrics=['cpu_usage', 'memory_usage', 'request_rate', 'error_rate'],
            time_range='5m'
        )
        
        # Format as Prometheus metrics
        output = []
        for metric_name, trend in metrics.items():
            output.append(f"# HELP {metric_name} Current value of {metric_name}")
            output.append(f"# TYPE {metric_name} gauge")
            output.append(f"{metric_name} {trend['current_value']}")
                
        return "\n".join(output)
        
    except Exception as e:
        logger.error(f"Prometheus export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Health Check ---

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "druid": "unknown",
            "ignite": "unknown",
            "elasticsearch": "unknown",
            "trino": "unknown"
        }
    }
    
    # Check Druid
    try:
        await druid_engine.health_check()
        health_status["services"]["druid"] = "healthy"
    except:
        health_status["services"]["druid"] = "unhealthy"
        health_status["status"] = "degraded"
        
    # Check Ignite
    try:
        if ignite_client and ignite_client.get_cache('health_check'):
            health_status["services"]["ignite"] = "healthy"
    except:
        health_status["services"]["ignite"] = "unhealthy"
        health_status["status"] = "degraded"
        
    # Check Elasticsearch
    try:
        if await es_client.ping():
            health_status["services"]["elasticsearch"] = "healthy"
    except:
        health_status["services"]["elasticsearch"] = "unhealthy"
        health_status["status"] = "degraded"
        
    # Check Trino
    try:
        response = await trino_client.get("/v1/info")
            if response.status_code == 200:
            health_status["services"]["trino"] = "healthy"
    except:
        health_status["services"]["trino"] = "unhealthy"
        health_status["status"] = "degraded"
        
    return health_status


# ============= Event Handlers =============

@event_handler(SimulationMetricEvent)
async def handle_simulation_metrics(event: SimulationMetricEvent, service_clients: ServiceClients) -> ProcessingResult:
    """Handle incoming simulation metrics"""
    try:
        # Process through stream processor
        await stream_processor.process_metric({
            'simulation_id': event.simulation_id,
            'metrics': event.metrics,
            'timestamp': event.timestamp
        })
        
        # Check for anomalies
        anomalies = await anomaly_detector.detect_anomalies(
            simulation_id=event.simulation_id,
            metrics=event.metrics
        )
        
        if anomalies:
            # Publish anomaly events
            for anomaly in anomalies:
                await event_publisher.publish(
                    topic=f"anomalies.{event.simulation_id}",
                    data=anomaly
                )
                
        return ProcessingResult(
            status=ProcessingStatus.SUCCESS,
            data={"anomalies_detected": len(anomalies)}
        )
        
    except Exception as e:
        logger.error(f"Failed to process simulation metrics: {e}")
        return ProcessingResult(
            status=ProcessingStatus.FAILED,
            error=str(e)
        )


# ============= Initialize Event Processor =============

event_processor = EventProcessor(
    service_name="unified-analytics-service",
    pulsar_url=PULSAR_CONFIG['url']
)

# Register handlers
event_processor.register_handler(SimulationMetricEvent, handle_simulation_metrics)


# ============= Additional Utility Endpoints =============

@app.get("/api/v1/analytics/capabilities")
async def get_capabilities():
    """Get service capabilities"""
    return {
        "engines": {
            "batch": {
                "name": "Trino",
                "capabilities": ["complex joins", "window functions", "CTEs", "full SQL"],
                "best_for": "historical analysis, complex queries"
            },
            "realtime": {
                "name": "Druid",
                "capabilities": ["time-series", "OLAP", "rollups", "approximate queries"],
                "best_for": "real-time analytics, time-based aggregations"
            },
            "cache": {
                "name": "Ignite",
                "capabilities": ["in-memory", "SQL", "compute grid", "transactions"],
                "best_for": "sub-millisecond queries, hot data"
            }
        },
        "ml_operations": [
            "anomaly_detection",
            "forecasting",
            "predictive_maintenance",
            "pattern_recognition",
            "online_learning"
        ],
        "streaming": {
            "supported": True,
            "protocols": ["websocket", "sse"],
            "features": ["real-time updates", "subscriptions", "windowed aggregations"]
        },
        "monitoring": {
            "scopes": ["platform", "service", "simulation", "resource"],
            "dashboards": ["overview", "comparison", "activity", "ml-performance"]
        }
    }


@app.get("/api/v1/analytics/metadata/{datasource}")
async def get_datasource_metadata(datasource: str):
    """Get metadata about a datasource"""
    try:
        # Get from Druid
        metadata = await druid_engine.get_datasource_metadata(datasource)
        
        # Enhance with Ignite cache info
        cache = ignite_client.get_cache(datasource)
        if cache:
            metadata['cache_info'] = {
                'size': cache.size(),
                'mode': 'available'
            }
            
        return metadata
        
    except Exception as e:
        logger.error(f"Failed to get datasource metadata: {e}")
        raise HTTPException(status_code=404, detail=f"Datasource {datasource} not found")


# Run the event processor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 