"""
Unified Analytics Service

Provides both batch analytics (via Trino/Hive) and real-time analytics (via Druid/Ignite)
with a unified API supporting multiple execution modes.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query as QueryParam, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from elasticsearch import AsyncElasticsearch
from pyignite import Client as IgniteClient
import httpx

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

# Import analytics modules
from .analytics.druid_analytics import DruidAnalyticsEngine
from .analytics.stream_processor import StreamProcessor
from .analytics.realtime_ml import RealtimeMLEngine
from .monitoring.dashboard_service import SimulationDashboardService
from .monitoring.predictive_maintenance import PredictiveMaintenanceModel
from .monitoring.timeseries_analysis import TimeSeriesAnalyzer
from .monitoring.metrics_aggregator import MetricsAggregator
from .monitoring.anomaly_detection import SimulationAnomalyDetector
from .dashboards.cross_service_dashboard import CrossServiceDashboard, DashboardOrchestrator

logger = logging.getLogger(__name__)


class AnalyticsMode(str, Enum):
    """Analytics execution mode"""
    BATCH = "batch"         # Use Trino for complex queries
    REALTIME = "realtime"   # Use Druid/Ignite for low latency
    AUTO = "auto"           # Automatically choose based on query


class AnalyticsQuery(BaseModel):
    """Unified analytics query model"""
    query: Optional[str] = Field(None, description="SQL query for batch mode")
    query_type: Optional[str] = Field(None, description="Predefined query type")
    mode: AnalyticsMode = Field(AnalyticsMode.AUTO, description="Execution mode")
    filters: Optional[Dict[str, Any]] = Field({}, description="Query filters")
    time_range: Optional[str] = Field("7d", description="Time range: 1h, 1d, 7d, 30d, 90d")
    group_by: Optional[List[str]] = Field([], description="Fields to group by")
    metrics: Optional[List[str]] = Field([], description="Metrics to calculate")
    limit: Optional[int] = Field(1000, description="Result limit")
    realtime_options: Optional[Dict[str, Any]] = Field({}, description="Real-time specific options")


class AnalyticsResult(BaseModel):
    """Unified analytics result"""
    mode: str = Field(..., description="Mode used for execution")
    query_type: Optional[str] = None
    data: List[Dict[str, Any]]
    summary: Dict[str, Any]
    metadata: Dict[str, Any]
    execution_time_ms: float


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
anomaly_detector = None
cross_service_dashboard = None
dashboard_orchestrator = None
unified_data_client = None


class MetricsEventProcessor(EventProcessor):
    """Process real-time metrics events"""
    
    async def on_start(self):
        """Initialize processor resources"""
        self.ignite_client = IgniteClient()
        self.ignite_client.connect(IGNITE_CONFIG['host'], IGNITE_CONFIG['port'])
        
    async def on_stop(self):
        """Cleanup processor resources"""
        if hasattr(self, 'ignite_client'):
            self.ignite_client.close()
    
    @event_handler("persistent://platformq/*/simulation-metrics", SimulationMetricEvent)
    async def handle_simulation_metrics(self, event: SimulationMetricEvent, msg):
        """Process simulation metrics for real-time analytics"""
        try:
            # Store in Ignite for real-time access
            cache = self.ignite_client.get_or_create_cache('simulation_metrics')
            key = f"{event.simulation_id}:{event.timestamp}"
            cache.put(key, event.dict())
            
            # Forward to stream processor if needed
            if stream_processor:
                await stream_processor.process_metric(event)
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
        except Exception as e:
            logger.error(f"Error processing metrics: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global druid_engine, stream_processor, realtime_ml
    global dashboard_service, maintenance_model, timeseries_analyzer
    global metrics_aggregator, anomaly_detector, cross_service_dashboard
    global dashboard_orchestrator, unified_data_client
    
    # Startup
    logger.info("Initializing Unified Analytics Service...")
    
    # Initialize analytics engines
    druid_engine = DruidAnalyticsEngine(DRUID_CONFIG)
    stream_processor = StreamProcessor(DRUID_CONFIG, IGNITE_CONFIG)
    realtime_ml = RealtimeMLEngine()
    
    # Initialize monitoring services
    dashboard_service = SimulationDashboardService(IGNITE_CONFIG, ES_CONFIG)
    maintenance_model = PredictiveMaintenanceModel(IGNITE_CONFIG, ES_CONFIG)
    timeseries_analyzer = TimeSeriesAnalyzer(IGNITE_CONFIG, ES_CONFIG, app.state.event_publisher)
    metrics_aggregator = MetricsAggregator(IGNITE_CONFIG, DRUID_CONFIG)
    
    # Initialize anomaly detector
    ignite_client = IgniteClient()
    ignite_client.connect(IGNITE_CONFIG['host'], IGNITE_CONFIG['port'])
    
    anomaly_detector = SimulationAnomalyDetector(
        ignite_client,
        app.state.event_publisher,
        mlops_service_url="http://mlops-service:8000",
        seatunnel_service_url="http://seatunnel-service:8000"
    )
    
    # Initialize cross-service dashboard
    es_client = AsyncElasticsearch([ES_CONFIG["host"]])
    cross_service_dashboard = CrossServiceDashboard(ignite_client, es_client, DRUID_CONFIG)
    dashboard_orchestrator = DashboardOrchestrator(cross_service_dashboard, cache_ttl=60)
    
    # Initialize unified data client
    unified_data_client = ServiceClients.digital_asset()  # Reuse for unified data queries
    
    # Start background tasks
    asyncio.create_task(stream_processor.start())
    asyncio.create_task(anomaly_detector.start_monitoring())
    asyncio.create_task(maintenance_model.start_continuous_monitoring())
    
    logger.info("Analytics Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Analytics Service...")
    
    # Stop background tasks
    await stream_processor.stop()
    await anomaly_detector.stop()
    
    # Cleanup connections
    ignite_client.close()
    await es_client.close()
    
    logger.info("Analytics Service shutdown complete")


# Create base app with event processors
event_processor = MetricsEventProcessor(
    service_name="analytics-service",
    pulsar_url="pulsar://pulsar:6650"
)

app = create_base_app(
    service_name="analytics-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None,
    event_processors=[event_processor]
)

# Set lifespan
app.router.lifespan_context = lifespan


class AnalyticsQueryRouter:
    """Routes queries to appropriate engine based on mode and characteristics"""
    
    @staticmethod
    async def route_query(query: AnalyticsQuery) -> AnalyticsResult:
        """Route query to appropriate engine"""
        
        # Determine execution mode
        if query.mode == AnalyticsMode.AUTO:
            mode = AnalyticsQueryRouter._determine_mode(query)
        else:
            mode = query.mode
            
        # Execute based on mode
        if mode == AnalyticsMode.REALTIME:
            return await AnalyticsQueryRouter._execute_realtime(query)
        else:
            return await AnalyticsQueryRouter._execute_batch(query)
            
    @staticmethod
    def _determine_mode(query: AnalyticsQuery) -> AnalyticsMode:
        """Automatically determine best execution mode"""
        
        # Use real-time for:
        # - Short time ranges (< 1 day)
        # - Simple aggregations
        # - Specific real-time query types
        realtime_query_types = ["current_metrics", "live_dashboard", "stream_stats"]
        
        if query.query_type in realtime_query_types:
            return AnalyticsMode.REALTIME
            
        if query.time_range in ["1h", "1d"]:
            return AnalyticsMode.REALTIME
            
        # Default to batch for complex queries
        return AnalyticsMode.BATCH
        
    @staticmethod
    async def _execute_realtime(query: AnalyticsQuery) -> AnalyticsResult:
        """Execute using real-time engines (Druid/Ignite)"""
        start_time = datetime.utcnow()
        
        if query.query_type == "current_metrics":
            # Get from Ignite cache
            ignite_client = IgniteClient()
            ignite_client.connect(IGNITE_CONFIG['host'], IGNITE_CONFIG['port'])
            
            try:
                cache = ignite_client.get_cache('simulation_metrics')
                data = []
                
                # Scan cache with filters
                for key, value in cache.scan():
                    if AnalyticsQueryRouter._matches_filters(value, query.filters):
                        data.append(value)
                        
                # Apply limit
                data = data[:query.limit]
                
            finally:
                ignite_client.close()
                
        elif query.query_type == "stream_stats":
            # Get from Druid
            data = await druid_engine.query_timeseries(
                datasource="platform_metrics",
                intervals=AnalyticsQueryRouter._get_druid_interval(query.time_range),
                granularity="minute",
                aggregations=query.metrics or ["count"],
                filter=query.filters
            )
        else:
            # Generic Druid query
            data = await druid_engine.query(query.dict())
            
        # Calculate execution time
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Process results
        df = pd.DataFrame(data) if data else pd.DataFrame()
        
        return AnalyticsResult(
            mode=AnalyticsMode.REALTIME,
            query_type=query.query_type,
            data=data,
            summary=AnalyticsQueryRouter._calculate_summary(df),
            metadata={
                "row_count": len(data),
                "execution_time_ms": execution_time,
                "engine": "druid/ignite"
            },
            execution_time_ms=execution_time
        )
        
    @staticmethod
    async def _execute_batch(query: AnalyticsQuery) -> AnalyticsResult:
        """Execute using batch engine (Trino)"""
        start_time = datetime.utcnow()
        
        # Build SQL query
        if query.query:
            sql = query.query
        else:
            sql = AnalyticsQueryRouter._build_sql_from_query_type(query)
            
        # Execute via unified data service or Trino directly
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://unified-data-service:8000/api/v1/query",
                json={"query": sql, "limit": query.limit}
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail="Query execution failed"
                )
                
            result = response.json()
            
        # Process results
        data = []
        for row in result["data"]:
            data.append(dict(zip([col["name"] for col in result["columns"]], row)))
            
        # Calculate execution time
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Process results
        df = pd.DataFrame(data) if data else pd.DataFrame()
        
        return AnalyticsResult(
            mode=AnalyticsMode.BATCH,
            query_type=query.query_type,
            data=data,
            summary=AnalyticsQueryRouter._calculate_summary(df),
            metadata={
                "row_count": len(data),
                "execution_time_ms": execution_time,
                "engine": "trino"
            },
            execution_time_ms=execution_time
        )
        
    @staticmethod
    def _matches_filters(data: Dict, filters: Dict) -> bool:
        """Check if data matches filters"""
        for key, value in filters.items():
            if key not in data or data[key] != value:
                return False
        return True
        
    @staticmethod
    def _get_druid_interval(time_range: str) -> str:
        """Convert time range to Druid interval"""
        now = datetime.utcnow()
        
        if time_range == "1h":
            start = now - timedelta(hours=1)
        elif time_range == "1d":
            start = now - timedelta(days=1)
        elif time_range == "7d":
            start = now - timedelta(days=7)
        elif time_range == "30d":
            start = now - timedelta(days=30)
        else:
            start = now - timedelta(days=7)
            
        return f"{start.isoformat()}/{now.isoformat()}"
        
    @staticmethod
    def _build_sql_from_query_type(query: AnalyticsQuery) -> str:
        """Build SQL query from query type"""
        
        # Query type to SQL mapping
        query_builders = {
            "asset_summary": AnalyticsQueryRouter._build_asset_summary_sql,
            "user_behavior": AnalyticsQueryRouter._build_user_behavior_sql,
            "quality_trends": AnalyticsQueryRouter._build_quality_trends_sql,
            "simulation_stats": AnalyticsQueryRouter._build_simulation_stats_sql
        }
        
        builder = query_builders.get(query.query_type)
        if not builder:
            raise ValidationError(f"Unknown query type: {query.query_type}")
            
        return builder(query)
        
    @staticmethod
    def _build_asset_summary_sql(query: AnalyticsQuery) -> str:
        """Build asset summary SQL"""
        time_filter = AnalyticsQueryRouter._get_time_filter(query.time_range)
        
        sql = f"""
        WITH asset_metrics AS (
            SELECT 
                da.asset_id,
                da.asset_name,
                da.asset_type,
                da.owner_id,
                da.created_at,
                COUNT(DISTINCT l.target_asset_id) as downstream_count,
                COUNT(DISTINCT l2.source_asset_id) as upstream_count,
                AVG(q.metric_value) as avg_quality_score
            FROM cassandra.platformq.digital_assets da
            LEFT JOIN hive.silver.data_lineage l ON da.asset_id = l.source_asset_id
            LEFT JOIN hive.silver.data_lineage l2 ON da.asset_id = l2.target_asset_id
            LEFT JOIN hive.silver.data_quality_metrics q ON da.asset_id = q.asset_id
            WHERE da.created_at >= {time_filter}
        """
        
        if query.filters:
            conditions = []
            for key, value in query.filters.items():
                conditions.append(f"da.{key} = '{value}'")
            if conditions:
                sql += " AND " + " AND ".join(conditions)
                
        sql += """
            GROUP BY da.asset_id, da.asset_name, da.asset_type, da.owner_id, da.created_at
        )
        SELECT 
            asset_type,
            COUNT(*) as asset_count,
            AVG(downstream_count) as avg_downstream,
            AVG(upstream_count) as avg_upstream,
            AVG(avg_quality_score) as avg_quality
        FROM asset_metrics
        """
        
        if query.group_by:
            sql += f" GROUP BY {', '.join(query.group_by)}"
        else:
            sql += " GROUP BY asset_type"
            
        sql += " ORDER BY asset_count DESC"
        
        return sql
        
    @staticmethod
    def _build_user_behavior_sql(query: AnalyticsQuery) -> str:
        """Build user behavior SQL"""
        time_filter = AnalyticsQueryRouter._get_time_filter(query.time_range)
        
        return f"""
        WITH user_activity AS (
            SELECT 
                user_id,
                DATE(event_timestamp) as activity_date,
                event_type,
                COUNT(*) as event_count
            FROM cassandra.auth_keyspace.activity_stream
            WHERE event_timestamp >= {time_filter}
            GROUP BY user_id, DATE(event_timestamp), event_type
        )
        SELECT 
            user_id,
            activity_date,
            SUM(event_count) as total_events,
            COUNT(DISTINCT event_type) as event_diversity
        FROM user_activity
        GROUP BY user_id, activity_date
        ORDER BY total_events DESC
        """
        
    @staticmethod
    def _build_quality_trends_sql(query: AnalyticsQuery) -> str:
        """Build quality trends SQL"""
        time_filter = AnalyticsQueryRouter._get_time_filter(query.time_range)
        
        return f"""
        SELECT 
            DATE(measured_at) as measurement_date,
            metric_type,
            AVG(metric_value) as avg_score,
            COUNT(*) as check_count,
            COUNT(CASE WHEN passed THEN 1 END) as passed_count
        FROM hive.silver.data_quality_metrics
        WHERE measured_at >= {time_filter}
        GROUP BY DATE(measured_at), metric_type
        ORDER BY measurement_date DESC, metric_type
        """
        
    @staticmethod
    def _build_simulation_stats_sql(query: AnalyticsQuery) -> str:
        """Build simulation statistics SQL"""
        time_filter = AnalyticsQueryRouter._get_time_filter(query.time_range)
        
        return f"""
        SELECT 
            s.simulation_id,
            s.name,
            s.status,
            COUNT(DISTINCT a.agent_id) as agent_count,
            AVG(m.value) as avg_metric_value,
            MAX(m.timestamp) as last_update
        FROM simulations s
        LEFT JOIN agents a ON s.simulation_id = a.simulation_id
        LEFT JOIN metrics m ON s.simulation_id = m.simulation_id
        WHERE s.created_at >= {time_filter}
        GROUP BY s.simulation_id, s.name, s.status
        ORDER BY last_update DESC
        """
        
    @staticmethod
    def _get_time_filter(time_range: str) -> str:
        """Convert time range to SQL filter"""
        mappings = {
            "1h": "CURRENT_TIMESTAMP - INTERVAL '1' HOUR",
            "1d": "CURRENT_DATE - INTERVAL '1' DAY",
            "7d": "CURRENT_DATE - INTERVAL '7' DAY",
            "30d": "CURRENT_DATE - INTERVAL '30' DAY",
            "90d": "CURRENT_DATE - INTERVAL '90' DAY"
        }
        return mappings.get(time_range, "CURRENT_DATE - INTERVAL '7' DAY")
        
    @staticmethod
    def _calculate_summary(df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate summary statistics"""
        summary = {}
        
        if not df.empty:
            numeric_cols = df.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                summary[col] = {
                    "mean": float(df[col].mean()) if not df[col].isna().all() else 0,
                    "min": float(df[col].min()) if not df[col].isna().all() else 0,
                    "max": float(df[col].max()) if not df[col].isna().all() else 0,
                    "std": float(df[col].std()) if not df[col].isna().all() else 0,
                    "count": int(df[col].count())
                }
                
        return summary


# API Endpoints

@app.post("/api/v1/query", response_model=AnalyticsResult)
async def execute_query(query: AnalyticsQuery):
    """
    Execute analytics query with automatic mode selection.
    
    Supports both batch (Trino) and real-time (Druid/Ignite) execution.
    """
    try:
        return await AnalyticsQueryRouter.route_query(query)
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/dashboards/overview")
async def get_overview_dashboard(
    time_range: str = QueryParam("7d", description="Time range for dashboard")
):
    """Get platform overview dashboard with key metrics"""
    
    # Execute multiple queries in parallel
    queries = [
        AnalyticsQuery(query_type="asset_summary", time_range=time_range, mode=AnalyticsMode.BATCH),
        AnalyticsQuery(query_type="user_behavior", time_range=time_range, mode=AnalyticsMode.BATCH),
        AnalyticsQuery(query_type="quality_trends", time_range=time_range, mode=AnalyticsMode.BATCH),
        AnalyticsQuery(query_type="current_metrics", time_range="1h", mode=AnalyticsMode.REALTIME)
    ]
    
    results = await asyncio.gather(*[
        AnalyticsQueryRouter.route_query(q) for q in queries
    ])
    
    return {
        "timestamp": datetime.utcnow(),
        "time_range": time_range,
        "sections": {
            "assets": {
                "summary": results[0].summary,
                "data": results[0].data[:10]  # Top 10
            },
            "users": {
                "summary": results[1].summary,
                "data": results[1].data[:10]
            },
            "quality": {
                "summary": results[2].summary,
                "trends": results[2].data
            },
            "realtime": {
                "current_metrics": results[3].data,
                "summary": results[3].summary
            }
        }
    }


@app.get("/api/v1/dashboards/simulation/{simulation_id}")
async def get_simulation_dashboard(simulation_id: str):
    """Get real-time simulation dashboard"""
    
    if not dashboard_service:
        raise HTTPException(status_code=503, detail="Dashboard service not initialized")
        
    try:
        dashboard = await dashboard_service.get_simulation_dashboard(simulation_id)
        return dashboard
    except Exception as e:
        logger.error(f"Failed to get simulation dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/api/v1/ws/metrics/{simulation_id}")
async def websocket_metrics(websocket: WebSocket, simulation_id: str):
    """WebSocket endpoint for real-time metrics streaming"""
    
    await websocket.accept()
    
    try:
        # Subscribe to simulation metrics
        subscription_id = await dashboard_service.subscribe_to_simulation(
            simulation_id,
            lambda metrics: asyncio.create_task(websocket.send_json(metrics))
        )
        
        # Keep connection alive
        while True:
            # Ping every 30 seconds
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for simulation {simulation_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Unsubscribe
        if 'subscription_id' in locals():
            await dashboard_service.unsubscribe(subscription_id)


@app.post("/api/v1/analytics/ml/predict")
async def predict_metrics(
    simulation_id: str,
    metric_name: str,
    horizon: int = 10
):
    """Predict future metrics using ML models"""
    
    if not realtime_ml:
        raise HTTPException(status_code=503, detail="ML engine not initialized")
        
    try:
        predictions = await realtime_ml.predict_time_series(
            simulation_id=simulation_id,
            metric_name=metric_name,
            horizon=horizon
        )
        
        return {
            "simulation_id": simulation_id,
            "metric_name": metric_name,
            "predictions": predictions,
            "confidence_intervals": realtime_ml.calculate_confidence_intervals(predictions)
        }
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analytics/anomalies")
async def get_anomalies(
    time_range: str = QueryParam("1h", description="Time range for anomaly detection")
):
    """Get detected anomalies across the platform"""
    
    if not anomaly_detector:
        raise HTTPException(status_code=503, detail="Anomaly detector not initialized")
        
    try:
        anomalies = await anomaly_detector.get_recent_anomalies(time_range)
        
        return {
            "timestamp": datetime.utcnow(),
            "time_range": time_range,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies,
            "summary": anomaly_detector.summarize_anomalies(anomalies)
        }
    except Exception as e:
        logger.error(f"Failed to get anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/analytics/reports/generate")
async def generate_report(
    report_type: str,
    parameters: Dict[str, Any] = {}
):
    """Generate analytical reports"""
    
    supported_reports = [
        "executive_summary",
        "performance_analysis",
        "quality_assessment",
        "user_engagement",
        "simulation_performance"
    ]
    
    if report_type not in supported_reports:
        raise ValidationError(f"Unsupported report type. Choose from: {supported_reports}")
        
    # Build queries for report
    queries = []
    
    if report_type == "executive_summary":
        queries = [
            AnalyticsQuery(query_type="asset_summary", time_range="30d"),
            AnalyticsQuery(query_type="user_behavior", time_range="30d"),
            AnalyticsQuery(query_type="quality_trends", time_range="30d")
        ]
    elif report_type == "simulation_performance":
        queries = [
            AnalyticsQuery(query_type="simulation_stats", time_range="7d"),
            AnalyticsQuery(query_type="current_metrics", mode=AnalyticsMode.REALTIME)
        ]
        
    # Execute queries
    results = await asyncio.gather(*[
        AnalyticsQueryRouter.route_query(q) for q in queries
    ])
    
    # Generate report
    report = {
        "report_type": report_type,
        "generated_at": datetime.utcnow(),
        "parameters": parameters,
        "sections": []
    }
    
    for i, (query, result) in enumerate(zip(queries, results)):
        report["sections"].append({
            "title": f"Section {i+1}: {query.query_type}",
            "data": result.data,
            "summary": result.summary,
            "visualizations": _generate_visualizations(result)
        })
        
    return report


def _generate_visualizations(result: AnalyticsResult) -> List[Dict[str, Any]]:
    """Generate visualization recommendations for results"""
    visualizations = []
    
    if result.data:
        df = pd.DataFrame(result.data)
        
        # Recommend visualizations based on data characteristics
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        if len(numeric_cols) >= 2:
            visualizations.append({
                "type": "scatter",
                "x": numeric_cols[0],
                "y": numeric_cols[1],
                "title": f"{numeric_cols[0]} vs {numeric_cols[1]}"
            })
            
        if categorical_cols and numeric_cols:
            visualizations.append({
                "type": "bar",
                "x": categorical_cols[0],
                "y": numeric_cols[0],
                "title": f"{numeric_cols[0]} by {categorical_cols[0]}"
            })
            
        if 'timestamp' in df.columns or 'date' in df.columns:
            time_col = 'timestamp' if 'timestamp' in df.columns else 'date'
            for num_col in numeric_cols:
                visualizations.append({
                    "type": "line",
                    "x": time_col,
                    "y": num_col,
                    "title": f"{num_col} over time"
                })
                
    return visualizations


@app.get("/api/v1/analytics/health")
async def health_check():
    """Enhanced health check with dependency status"""
    
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "dependencies": {}
    }
    
    # Check Druid
    try:
        await druid_engine.health_check()
        health["dependencies"]["druid"] = "healthy"
    except:
        health["dependencies"]["druid"] = "unhealthy"
        health["status"] = "degraded"
        
    # Check Ignite
    try:
        ignite_client = IgniteClient()
        ignite_client.connect(IGNITE_CONFIG['host'], IGNITE_CONFIG['port'])
        ignite_client.close()
        health["dependencies"]["ignite"] = "healthy"
    except:
        health["dependencies"]["ignite"] = "unhealthy"
        health["status"] = "degraded"
        
    # Check Elasticsearch
    try:
        es = AsyncElasticsearch([ES_CONFIG["host"]])
        await es.ping()
        await es.close()
        health["dependencies"]["elasticsearch"] = "healthy"
    except:
        health["dependencies"]["elasticsearch"] = "unhealthy"
        health["status"] = "degraded"
        
    # Check unified data service
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("http://unified-data-service:8000/health")
            if response.status_code == 200:
                health["dependencies"]["unified_data"] = "healthy"
            else:
                health["dependencies"]["unified_data"] = "unhealthy"
                health["status"] = "degraded"
    except:
        health["dependencies"]["unified_data"] = "unhealthy"
        health["status"] = "degraded"
        
    return health 