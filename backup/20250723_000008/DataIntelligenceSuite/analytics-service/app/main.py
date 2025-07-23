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
import os

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

# Import Vault/Consul integration
from .vault_consul_integration import VaultConsulIntegration

# Import new real-time OLAP clients
from data_intelligence_common.integrations.realtime import (
    PinotClient, PinotConfig, TableSchema as PinotTableSchema, TableConfig as PinotTableConfig, TableType as PinotTableType,
    ClickHouseClient, ClickHouseConfig, TableDefinition as CHTableDefinition, TableColumn as CHColumn, Engine as CHEngine, DataType as CHDataType,
    DorisClient, DorisConfig, Column as DorisColumn, DorisTableDefinition, TableModel as DorisTableModel, StreamLoadResult
)

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

# Security and Configuration Endpoints
vault_consul: Optional[VaultConsulIntegration] = None
trino_client: Optional[Any] = None
elasticsearch_client: Optional[AsyncElasticsearch] = None
ignite_client: Optional[IgniteClient] = None

# Real-time OLAP clients
pinot_client: Optional[PinotClient] = None
clickhouse_client: Optional[ClickHouseClient] = None
doris_client: Optional[DorisClient] = None


# ============= Lifespan Management =============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global vault_consul, trino_client, elasticsearch_client, ignite_client, pinot_client, clickhouse_client, doris_client
    
    # Initialize Vault/Consul integration
    vault_consul = VaultConsulIntegration({
        "vault_addr": os.getenv("VAULT_ADDR", "http://vault:8200"),
        "vault_token": os.getenv("VAULT_TOKEN"),
        "consul_addr": os.getenv("CONSUL_ADDR", "http://consul:8500")
    })
    
    await vault_consul.initialize()
    
    # Register service with Consul
    await vault_consul.register_service(
        tags=["analytics", "trino", "data-processing", "real-time-olap"],
        meta={
            "version": "1.0.0",
            "capabilities": "batch,streaming,ml,pinot,clickhouse,doris"
        }
    )
    
    # Initialize Trino with secure credentials
    trino_creds = await vault_consul.get_trino_credentials()
    trino_client = await initialize_trino_client(trino_creds)
    
    # Initialize Elasticsearch with secure credentials
    es_creds = await vault_consul.get_elasticsearch_credentials()
    elasticsearch_client = AsyncElasticsearch(
        hosts=es_creds.get("hosts", ["elasticsearch:9200"]),
        basic_auth=(es_creds["username"], es_creds["password"]),
        ssl_context=await vault_consul.get_ssl_context() if es_creds.get("secure") else None
    )
    
    # Initialize Ignite with secure credentials
    ignite_creds = await vault_consul.get_ignite_credentials()
    ignite_client = IgniteClient(
        username=ignite_creds["username"],
        password=ignite_creds["password"]
    )
    ignite_client.connect('ignite', 10800)
    
    # Initialize Pinot client
    try:
        pinot_creds = await vault_consul.get_credentials("pinot")
        pinot_config = PinotConfig(
            controller_url=pinot_creds.get("controller_url", "http://pinot-controller:9000"),
            broker_url=pinot_creds.get("broker_url", "http://pinot-broker:8099"),
            vault_client=vault_consul.vault_client,
            consul_client=vault_consul.consul_client
        )
        pinot_client = PinotClient(pinot_config)
        await pinot_client.connect()
        logger.info("Initialized Apache Pinot client")
    except Exception as e:
        logger.warning(f"Failed to initialize Pinot client: {e}")
    
    # Initialize ClickHouse client
    try:
        ch_creds = await vault_consul.get_credentials("clickhouse")
        clickhouse_config = ClickHouseConfig(
            host=ch_creds.get("host", "clickhouse"),
            port=ch_creds.get("port", 9000),
            user=ch_creds.get("user", "default"),
            password=ch_creds.get("password", ""),
            database=ch_creds.get("database", "analytics"),
            vault_client=vault_consul.vault_client,
            consul_client=vault_consul.consul_client
        )
        clickhouse_client = ClickHouseClient(clickhouse_config)
        await clickhouse_client.connect()
        logger.info("Initialized ClickHouse client")
    except Exception as e:
        logger.warning(f"Failed to initialize ClickHouse client: {e}")
    
    # Initialize Doris client
    try:
        doris_creds = await vault_consul.get_credentials("doris")
        doris_config = DorisConfig(
            fe_host=doris_creds.get("fe_host", "doris-fe"),
            fe_port=doris_creds.get("fe_port", 9030),
            user=doris_creds.get("user", "root"),
            password=doris_creds.get("password", ""),
            database=doris_creds.get("database", "analytics"),
            vault_client=vault_consul.vault_client,
            consul_client=vault_consul.consul_client
        )
        doris_client = DorisClient(doris_config)
        await doris_client.connect()
        logger.info("Initialized Apache Doris client")
    except Exception as e:
        logger.warning(f"Failed to initialize Doris client: {e}")
    
    # Initialize data lake connections
    minio_creds = await vault_consul.get_data_lake_credentials("minio")
    await initialize_data_lake(minio_creds)
    
    # Initialize analytics components with secure config
    query_cache_config = await vault_consul.get_query_cache_config()
    await initialize_analytics_components(query_cache_config)
    
    # Start configuration watchers
    asyncio.create_task(watch_configuration_changes())
    
    yield
    
    # Cleanup
    if elasticsearch_client:
        await elasticsearch_client.close()
    if ignite_client:
        ignite_client.close()
    if pinot_client:
        await pinot_client.close()
    if clickhouse_client:
        await clickhouse_client.close()
    if doris_client:
        await doris_client.close()
    await vault_consul.deregister_service()
    await vault_consul.shutdown()


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
        """Execute query using real-time OLAP engines"""
        # Check Ignite cache first
        cache_key = f"query:{hash(str(query.dict()))}"
        cache = ignite_client.get_cache('query_cache')
        cached = cache.get(cache_key)
        
        if cached:
            return {**cached, 'cached': True}
        
        # Determine best engine based on query characteristics
        engine = UnifiedQueryRouter._select_realtime_engine(query)
        result = None
        
        try:
            if engine == "pinot" and pinot_client:
                # Use Pinot for real-time aggregations
                pql = UnifiedQueryRouter._build_pinot_query(query)
                result = await pinot_client.query(pql)
                result = {
                    'data': result,
                    'summary': {'row_count': len(result), 'engine': 'pinot'},
                    'metadata': {'query': pql}
                }
            
            elif engine == "clickhouse" and clickhouse_client:
                # Use ClickHouse for complex analytics
                sql = UnifiedQueryRouter._build_clickhouse_query(query)
                result = await clickhouse_client.query(sql)
                result = {
                    'data': result,
                    'summary': {'row_count': len(result), 'engine': 'clickhouse'},
                    'metadata': {'query': sql}
                }
            
            elif engine == "doris" and doris_client:
                # Use Doris for OLAP queries
                sql = UnifiedQueryRouter._build_doris_query(query)
                result = await doris_client.query(sql)
                result = {
                    'data': result,
                    'summary': {'row_count': len(result), 'engine': 'doris'},
                    'metadata': {'query': sql}
                }
            
            elif druid_engine:
                # Fallback to Druid
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
            
            else:
                raise HTTPException(status_code=503, detail="No real-time engine available")
            
            # Cache result
            if result:
                cache.put(cache_key, result, query.cache_ttl)
            
            return result
            
        except Exception as e:
            logger.error(f"Real-time query failed with {engine}: {e}")
            # Try fallback engine
            if engine != "druid" and druid_engine:
                logger.info("Falling back to Druid")
                return await UnifiedQueryRouter._execute_realtime_fallback(query)
            raise
    
    @staticmethod
    def _select_realtime_engine(query: UnifiedQuery) -> str:
        """Select the best real-time engine for the query"""
        # Use Pinot for real-time aggregations with low latency
        if query.query_type == "realtime_aggregation" or (
            query.metrics and len(query.metrics) <= 3 and query.time_range in ['1m', '5m', '15m']
        ):
            return "pinot"
        
        # Use ClickHouse for complex analytical queries
        if query.query and any(keyword in query.query.upper() for keyword in ['WINDOW', 'ARRAY', 'MATCH']):
            return "clickhouse"
        
        # Use Doris for OLAP-style queries with multiple dimensions
        if query.group_by and len(query.group_by) > 2:
            return "doris"
        
        # Default to Druid for time-series
        return "druid"
    
    @staticmethod
    def _build_pinot_query(query: UnifiedQuery) -> str:
        """Build Pinot query from unified query"""
        select_clause = ", ".join(query.metrics) if query.metrics else "*"
        from_clause = "platform_metrics"
        where_clause = ""
        group_clause = ""
        
        # Build WHERE clause
        conditions = []
        if query.filters:
            conditions.extend([f"{k} = '{v}'" for k, v in query.filters.items()])
        if query.time_range:
            # Convert time range to timestamp filter
            conditions.append(f"timestamp >= ago('{query.time_range}')")
        
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"
        
        # Build GROUP BY clause
        if query.group_by:
            group_clause = f"GROUP BY {', '.join(query.group_by)}"
        
        return f"SELECT {select_clause} FROM {from_clause} {where_clause} {group_clause} LIMIT {query.limit}"
    
    @staticmethod
    def _build_clickhouse_query(query: UnifiedQuery) -> str:
        """Build ClickHouse query from unified query"""
        if query.query:
            return query.query
        
        select_clause = ", ".join(query.metrics) if query.metrics else "*"
        from_clause = "analytics.platform_metrics"
        where_clause = ""
        group_clause = ""
        
        # Build WHERE clause
        conditions = []
        if query.filters:
            conditions.extend([f"{k} = '{v}'" for k, v in query.filters.items()])
        if query.time_range:
            # Convert time range to ClickHouse interval
            conditions.append(f"timestamp >= now() - INTERVAL {query.time_range}")
        
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"
        
        # Build GROUP BY clause
        if query.group_by:
            group_clause = f"GROUP BY {', '.join(query.group_by)}"
        
        return f"SELECT {select_clause} FROM {from_clause} {where_clause} {group_clause} LIMIT {query.limit}"
    
    @staticmethod
    def _build_doris_query(query: UnifiedQuery) -> str:
        """Build Doris query from unified query"""
        if query.query:
            return query.query
        
        select_clause = ", ".join(query.metrics) if query.metrics else "*"
        from_clause = "analytics.platform_metrics"
        where_clause = ""
        group_clause = ""
        
        # Build WHERE clause
        conditions = []
        if query.filters:
            conditions.extend([f"{k} = '{v}'" for k, v in query.filters.items()])
        if query.time_range:
            # Convert time range to Doris date function
            conditions.append(f"timestamp >= date_sub(now(), INTERVAL {query.time_range})")
        
        if conditions:
            where_clause = f"WHERE {' AND '.join(conditions)}"
        
        # Build GROUP BY clause
        if query.group_by:
            group_clause = f"GROUP BY {', '.join(query.group_by)}"
        
        return f"SELECT {select_clause} FROM {from_clause} {where_clause} {group_clause} LIMIT {query.limit}"
    
    @staticmethod
    async def _execute_realtime_fallback(query: UnifiedQuery) -> Dict[str, Any]:
        """Fallback to Druid for real-time queries"""
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
            result = await metrics_aggregator.aggregate_metrics(
                source='mixed',
                metrics=query.metrics,
                time_range=query.time_range,
                group_by=query.group_by,
                filters=query.filters
            )
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
    """Enhanced health check with Vault/Consul status"""
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    # Check Vault connection
    if vault_consul:
        health["checks"]["vault"] = await vault_consul.check_vault_health()
        health["checks"]["consul"] = await vault_consul.check_consul_health()
    else:
        health["checks"]["vault"] = {"status": "not_initialized"}
        health["checks"]["consul"] = {"status": "not_initialized"}
    
    # Check Trino
    if trino_client:
        try:
            await trino_client.execute("SELECT 1")
            health["checks"]["trino"] = {"status": "healthy"}
        except Exception as e:
            health["checks"]["trino"] = {"status": "unhealthy", "error": str(e)}
            health["status"] = "degraded"
    
    # Check Elasticsearch
    if elasticsearch_client:
        try:
            await elasticsearch_client.ping()
            health["checks"]["elasticsearch"] = {"status": "healthy"}
        except Exception:
            health["checks"]["elasticsearch"] = {"status": "unhealthy"}
            health["status"] = "degraded"
    
    # Check Ignite
    if ignite_client:
        try:
            ignite_client.get_cache("test")
            health["checks"]["ignite"] = {"status": "healthy"}
        except Exception:
            health["checks"]["ignite"] = {"status": "unhealthy"}
            health["status"] = "degraded"
    
    return health


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
    capabilities = {
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
    
    # Add real-time OLAP engines if available
    if pinot_client:
        capabilities["engines"]["pinot"] = {
            "name": "Apache Pinot",
            "capabilities": ["real-time aggregations", "star-tree index", "segment-based", "lambda architecture"],
            "best_for": "ultra-low latency analytics, real-time dashboards"
        }
    
    if clickhouse_client:
        capabilities["engines"]["clickhouse"] = {
            "name": "ClickHouse",
            "capabilities": ["columnar storage", "vectorized execution", "materialized views", "SQL arrays"],
            "best_for": "complex analytical queries, log analytics"
        }
    
    if doris_client:
        capabilities["engines"]["doris"] = {
            "name": "Apache Doris",
            "capabilities": ["MPP architecture", "vectorized execution", "rollup tables", "bitmap indexes"],
            "best_for": "multi-dimensional OLAP, ad-hoc queries"
        }
    
    return capabilities


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


# --- Real-time OLAP Endpoints ---

@app.post("/api/v1/olap/pinot/query")
async def query_pinot(query_request: Dict[str, Any]):
    """Execute query on Apache Pinot"""
    if not pinot_client:
        raise HTTPException(status_code=503, detail="Pinot client not initialized")
    
    try:
        pql = query_request.get("pql") or query_request.get("query")
        options = query_request.get("options", {})
        
        result = await pinot_client.query(pql, **options)
        
        return {
            "engine": "pinot",
            "data": result,
            "execution_time_ms": query_request.get("execution_time", 0),
            "metadata": {
                "query": pql,
                "num_docs_scanned": result.get("numDocsScanned", 0),
                "num_servers_queried": result.get("numServersQueried", 0)
            }
        }
    except Exception as e:
        logger.error(f"Pinot query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/olap/clickhouse/query")
async def query_clickhouse(query_request: Dict[str, Any]):
    """Execute query on ClickHouse"""
    if not clickhouse_client:
        raise HTTPException(status_code=503, detail="ClickHouse client not initialized")
    
    try:
        sql = query_request.get("sql") or query_request.get("query")
        params = query_request.get("params", {})
        settings = query_request.get("settings", {})
        
        result = await clickhouse_client.query(sql, params=params, settings=settings)
        
        return {
            "engine": "clickhouse",
            "data": result,
            "execution_time_ms": query_request.get("execution_time", 0),
            "metadata": {
                "query": sql,
                "rows_read": result.get("rows_read", 0),
                "bytes_read": result.get("bytes_read", 0)
            }
        }
    except Exception as e:
        logger.error(f"ClickHouse query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/olap/doris/query")
async def query_doris(query_request: Dict[str, Any]):
    """Execute query on Apache Doris"""
    if not doris_client:
        raise HTTPException(status_code=503, detail="Doris client not initialized")
    
    try:
        sql = query_request.get("sql") or query_request.get("query")
        
        result = await doris_client.query(sql)
        
        return {
            "engine": "doris",
            "data": result,
            "execution_time_ms": query_request.get("execution_time", 0),
            "metadata": {
                "query": sql,
                "row_count": len(result)
            }
        }
    except Exception as e:
        logger.error(f"Doris query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/olap/pinot/tables")
async def create_pinot_table(table_config: Dict[str, Any]):
    """Create a table in Pinot"""
    if not pinot_client:
        raise HTTPException(status_code=503, detail="Pinot client not initialized")
    
    try:
        table_name = table_config["tableName"]
        schema = PinotTableSchema(**table_config.get("schema", {}))
        config = PinotTableConfig(**table_config.get("tableConfig", {}))
        
        await pinot_client.create_table(
            table_name=table_name,
            schema=schema,
            table_config=config,
            table_type=PinotTableType(table_config.get("tableType", "REALTIME"))
        )
        
        return {"status": "created", "table": table_name}
    except Exception as e:
        logger.error(f"Pinot table creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/olap/clickhouse/tables")
async def create_clickhouse_table(table_def: Dict[str, Any]):
    """Create a table in ClickHouse"""
    if not clickhouse_client:
        raise HTTPException(status_code=503, detail="ClickHouse client not initialized")
    
    try:
        columns = [CHColumn(**col) for col in table_def["columns"]]
        engine = CHEngine[table_def.get("engine", "MergeTree")]
        
        table_definition = CHTableDefinition(
            name=table_def["name"],
            columns=columns,
            engine=engine,
            partition_by=table_def.get("partition_by"),
            order_by=table_def.get("order_by", []),
            settings=table_def.get("settings", {})
        )
        
        await clickhouse_client.create_table(table_definition)
        
        return {"status": "created", "table": table_def["name"]}
    except Exception as e:
        logger.error(f"ClickHouse table creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/olap/doris/tables")
async def create_doris_table(table_def: Dict[str, Any]):
    """Create a table in Doris"""
    if not doris_client:
        raise HTTPException(status_code=503, detail="Doris client not initialized")
    
    try:
        columns = [DorisColumn(**col) for col in table_def["columns"]]
        
        table_definition = DorisTableDefinition(
            table_name=table_def["table_name"],
            columns=columns,
            partition_info=table_def.get("partition_info"),
            distribution_info=table_def.get("distribution_info"),
            properties=table_def.get("properties", {})
        )
        
        await doris_client.create_table(table_definition)
        
        return {"status": "created", "table": table_def["table_name"]}
    except Exception as e:
        logger.error(f"Doris table creation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/olap/status")
async def get_olap_status():
    """Get status of all OLAP engines"""
    status = {}
    
    if pinot_client:
        try:
            pinot_health = await pinot_client.get_health()
            status["pinot"] = {
                "status": "healthy" if pinot_health else "unhealthy",
                "controller": pinot_client.config.controller_url,
                "broker": pinot_client.config.broker_url
            }
        except Exception as e:
            status["pinot"] = {"status": "error", "error": str(e)}
    else:
        status["pinot"] = {"status": "not_configured"}
    
    if clickhouse_client:
        try:
            ch_health = await clickhouse_client.ping()
            status["clickhouse"] = {
                "status": "healthy" if ch_health else "unhealthy",
                "host": clickhouse_client.config.host,
                "database": clickhouse_client.config.database
            }
        except Exception as e:
            status["clickhouse"] = {"status": "error", "error": str(e)}
    else:
        status["clickhouse"] = {"status": "not_configured"}
    
    if doris_client:
        try:
            doris_health = await doris_client.get_health()
            status["doris"] = {
                "status": "healthy" if doris_health else "unhealthy",
                "fe_host": doris_client.config.fe_host,
                "database": doris_client.config.database
            }
        except Exception as e:
            status["doris"] = {"status": "error", "error": str(e)}
    else:
        status["doris"] = {"status": "not_configured"}
    
    return status


# Run the event processor
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 

# Security and Configuration Endpoints

@app.get("/api/analytics/config")
async def get_analytics_configuration():
    """Get current analytics configuration"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    return {
        "query_cache": await vault_consul.get_query_cache_config(),
        "data_catalog": await vault_consul.get_data_catalog_config(),
        "resource_limits": vault_consul.analytics_config.get("resource-limits", {}),
        "security_policies": vault_consul.analytics_config.get("security-policies", {})
    }

@app.post("/api/analytics/data-sources/{source_name}")
async def register_data_source(
    source_name: str,
    source_config: Dict[str, Any]
):
    """Register a new data source"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    try:
        await vault_consul.register_data_source(source_name, source_config)
        return {"status": "registered", "source": source_name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/analytics/pipelines/{pipeline_name}/secrets")
async def store_pipeline_secrets(
    pipeline_name: str,
    secrets: Dict[str, Any]
):
    """Store secrets for a data pipeline"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    try:
        await vault_consul.store_pipeline_secrets(pipeline_name, secrets)
        return {"status": "stored", "pipeline": pipeline_name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Enhanced Query Execution with Encryption

class SecureQueryRequest(BaseModel):
    query: str
    catalog: str = "analytics"
    schema: str = "default"
    encrypt_results: bool = False
    encryption_key: Optional[str] = None

@app.post("/api/analytics/secure-query")
async def execute_secure_query(request: SecureQueryRequest):
    """Execute query with optional result encryption"""
    if not vault_consul or not trino_client:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    try:
        # Execute query
        results = await trino_client.execute(
            request.query,
            catalog=request.catalog,
            schema=request.schema
        )
        
        # Convert to JSON
        results_json = results.to_json(orient="records")
        
        # Encrypt if requested
        if request.encrypt_results:
            key_name = request.encryption_key or "analytics-results"
            encrypted = await vault_consul.encrypt_data(
                results_json.encode(),
                key_name
            )
            
            return {
                "encrypted": True,
                "data": encrypted,
                "key_name": key_name
            }
        else:
            return {
                "encrypted": False,
                "data": json.loads(results_json)
            }
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/analytics/decrypt-results")
async def decrypt_query_results(
    ciphertext: str,
    key_name: str = "analytics-results"
):
    """Decrypt encrypted query results"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    try:
        decrypted = await vault_consul.decrypt_data(ciphertext, key_name)
        return json.loads(decrypted)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Distributed Job Coordination

class DistributedJobRequest(BaseModel):
    job_name: str
    job_type: str  # spark, flink, custom
    config: Dict[str, Any]
    workers: int = 4

@app.post("/api/analytics/jobs/submit")
async def submit_distributed_job(request: DistributedJobRequest):
    """Submit a distributed analytics job"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    try:
        # Get pipeline secrets if needed
        secrets = await vault_consul.get_pipeline_secrets(request.job_name)
        request.config.update(secrets)
        
        # Coordinate job execution
        result = await vault_consul.coordinate_distributed_job(
            request.job_name,
            request.config
        )
        
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Helper Functions

async def initialize_trino_client(credentials: Dict[str, str]):
    """Initialize Trino client with credentials"""
    # This would create actual Trino connection
    # For now, return mock client
    class MockTrinoClient:
        async def execute(self, query: str, **kwargs):
            return pd.DataFrame({"result": ["mock_data"]})
    
    return MockTrinoClient()

async def initialize_data_lake(credentials: Dict[str, str]):
    """Initialize data lake connections"""
    # Configure MinIO/S3 clients with credentials
    logger.info(f"Initialized data lake with endpoint: {credentials.get('endpoint')}")

async def initialize_analytics_components(cache_config: Dict[str, Any]):
    """Initialize analytics components with configuration"""
    logger.info(f"Initialized analytics with cache config: {cache_config}")

async def watch_configuration_changes():
    """Watch for configuration changes and reload"""
    while True:
        try:
            # Check for config updates every 30 seconds
            await asyncio.sleep(30)
            
            if vault_consul:
                # Reload query cache config
                new_cache_config = await vault_consul.get_query_cache_config()
                # Apply new configuration
                
        except Exception as e:
            logger.error(f"Configuration watch error: {e}")
            await asyncio.sleep(60) 