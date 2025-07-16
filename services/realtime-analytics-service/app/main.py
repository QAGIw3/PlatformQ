from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Union
from enum import Enum
import logging
import os
import json
import asyncio
from datetime import datetime, timedelta
import time
from collections import defaultdict
import threading
from queue import Queue
import numpy as np
from pyignite import Client as IgniteClient
from pyignite.datatypes import String, IntObject, FloatObject, TimestampObject
from pyignite.cache import Cache
from platformq_shared.security import get_current_tenant_and_user
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
import pulsar
from pulsar.schema import AvroSchema
import websocket
import uuid

logger = logging.getLogger(__name__)

# Configuration
IGNITE_HOST = os.getenv("IGNITE_HOST", "ignite")
IGNITE_PORT = int(os.getenv("IGNITE_PORT", "10800"))
PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")

# Analytics types
class MetricType(str, Enum):
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    PERCENTILE = "percentile"
    RATE = "rate"
    UNIQUE = "unique"

class AggregationWindow(str, Enum):
    SECOND = "1s"
    MINUTE = "1m"
    FIVE_MINUTES = "5m"
    HOUR = "1h"
    DAY = "1d"

class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    value: float
    metadata: Optional[Dict[str, Any]] = None

class Metric(BaseModel):
    name: str
    type: MetricType
    value: float
    window: Optional[AggregationWindow] = None
    tags: Optional[Dict[str, str]] = None

class Dashboard(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    widgets: List[Dict[str, Any]]
    refresh_interval: int = 30  # seconds
    created_at: datetime
    updated_at: datetime

class Widget(BaseModel):
    id: str
    type: str  # line_chart, bar_chart, gauge, counter, heatmap
    title: str
    metric_query: Dict[str, Any]
    visualization_config: Optional[Dict[str, Any]] = None
    position: Dict[str, int]  # x, y, width, height

class MetricQuery(BaseModel):
    metrics: List[str]
    filters: Optional[Dict[str, Any]] = None
    group_by: Optional[List[str]] = None
    window: AggregationWindow
    aggregation: MetricType
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

# Ignite cache manager
class IgniteCacheManager:
    def __init__(self):
        self.client = None
        self.caches = {}
        self.continuous_queries = {}
        
    def connect(self):
        """Connect to Ignite cluster"""
        self.client = IgniteClient()
        self.client.connect(IGNITE_HOST, IGNITE_PORT)
        
        # Create or get caches
        self._initialize_caches()
        
    def disconnect(self):
        """Disconnect from Ignite"""
        if self.client:
            self.client.close()
            
    def _initialize_caches(self):
        """Initialize Ignite caches for analytics"""
        # Real-time metrics cache
        self.caches['metrics'] = self.client.get_or_create_cache({
            'NAME': 'realtime_metrics',
            'VALUE_TYPE_NAME': 'Metric',
            'EXPIRY_POLICY': {
                'DURATION': 3600000  # 1 hour TTL
            }
        })
        
        # Time series data cache
        self.caches['timeseries'] = self.client.get_or_create_cache({
            'NAME': 'timeseries_data',
            'VALUE_TYPE_NAME': 'TimeSeriesData',
            'EXPIRY_POLICY': {
                'DURATION': 86400000  # 24 hour TTL
            }
        })
        
        # Aggregated data cache
        self.caches['aggregations'] = self.client.get_or_create_cache({
            'NAME': 'aggregated_metrics',
            'VALUE_TYPE_NAME': 'AggregatedData'
        })
        
        # Dashboard configuration cache
        self.caches['dashboards'] = self.client.get_or_create_cache({
            'NAME': 'dashboards',
            'VALUE_TYPE_NAME': 'Dashboard'
        })
        
        # User session cache for WebSocket connections
        self.caches['sessions'] = self.client.get_or_create_cache({
            'NAME': 'analytics_sessions',
            'VALUE_TYPE_NAME': 'Session',
            'EXPIRY_POLICY': {
                'DURATION': 3600000  # 1 hour TTL
            }
        })
        
    def put_metric(self, tenant_id: str, metric: Metric):
        """Store a metric in cache"""
        key = f"{tenant_id}:{metric.name}:{int(time.time())}"
        value = {
            'name': metric.name,
            'type': metric.type.value,
            'value': metric.value,
            'window': metric.window.value if metric.window else None,
            'tags': json.dumps(metric.tags) if metric.tags else None,
            'timestamp': int(time.time())
        }
        self.caches['metrics'].put(key, value)
        
    def get_metrics(self, tenant_id: str, metric_name: str, 
                   start_time: int, end_time: int) -> List[Dict[str, Any]]:
        """Get metrics from cache within time range"""
        metrics = []
        
        # Scan cache for matching metrics
        # In production, use SQL queries on Ignite
        for key, value in self.caches['metrics'].scan():
            if key.startswith(f"{tenant_id}:{metric_name}:"):
                timestamp = int(key.split(':')[-1])
                if start_time <= timestamp <= end_time:
                    metrics.append(value)
                    
        return sorted(metrics, key=lambda x: x['timestamp'])
        
    def put_timeseries(self, tenant_id: str, series_name: str, 
                      points: List[TimeSeriesPoint]):
        """Store time series data"""
        for point in points:
            key = f"{tenant_id}:{series_name}:{int(point.timestamp.timestamp())}"
            value = {
                'timestamp': int(point.timestamp.timestamp()),
                'value': point.value,
                'metadata': json.dumps(point.metadata) if point.metadata else None
            }
            self.caches['timeseries'].put(key, value)
            
    def compute_aggregation(self, tenant_id: str, query: MetricQuery) -> Dict[str, Any]:
        """Compute aggregated metrics based on query"""
        # This is a simplified implementation
        # In production, use Ignite's compute grid for distributed processing
        
        window_seconds = {
            AggregationWindow.SECOND: 1,
            AggregationWindow.MINUTE: 60,
            AggregationWindow.FIVE_MINUTES: 300,
            AggregationWindow.HOUR: 3600,
            AggregationWindow.DAY: 86400
        }
        
        window_size = window_seconds[query.window]
        
        # Get time range
        end_time = query.end_time or datetime.utcnow()
        start_time = query.start_time or (end_time - timedelta(hours=1))
        
        results = defaultdict(list)
        
        # Aggregate metrics
        for metric_name in query.metrics:
            metrics = self.get_metrics(
                tenant_id,
                metric_name,
                int(start_time.timestamp()),
                int(end_time.timestamp())
            )
            
            # Group by window
            windows = defaultdict(list)
            for metric in metrics:
                window_key = (metric['timestamp'] // window_size) * window_size
                windows[window_key].append(metric['value'])
                
            # Compute aggregations
            for window_key, values in windows.items():
                if query.aggregation == MetricType.COUNT:
                    result = len(values)
                elif query.aggregation == MetricType.SUM:
                    result = sum(values)
                elif query.aggregation == MetricType.AVERAGE:
                    result = sum(values) / len(values) if values else 0
                elif query.aggregation == MetricType.MIN:
                    result = min(values) if values else None
                elif query.aggregation == MetricType.MAX:
                    result = max(values) if values else None
                elif query.aggregation == MetricType.PERCENTILE:
                    # Default to 95th percentile
                    result = np.percentile(values, 95) if values else None
                elif query.aggregation == MetricType.UNIQUE:
                    result = len(set(values))
                else:
                    result = None
                    
                if result is not None:
                    results[metric_name].append({
                        'timestamp': datetime.fromtimestamp(window_key),
                        'value': result
                    })
                    
        return dict(results)
        
    def create_continuous_query(self, tenant_id: str, query_id: str, 
                              query: MetricQuery, callback):
        """Create a continuous query that calls callback with results"""
        # Simplified implementation - in production use Ignite's continuous queries
        def query_loop():
            while query_id in self.continuous_queries:
                results = self.compute_aggregation(tenant_id, query)
                callback(results)
                time.sleep(5)  # Run every 5 seconds
                
        thread = threading.Thread(target=query_loop, daemon=True)
        thread.start()
        self.continuous_queries[query_id] = thread
        
    def stop_continuous_query(self, query_id: str):
        """Stop a continuous query"""
        if query_id in self.continuous_queries:
            del self.continuous_queries[query_id]

# Stream processor for real-time events
class StreamProcessor(threading.Thread):
    def __init__(self, cache_manager: IgniteCacheManager):
        super().__init__(daemon=True)
        self.cache_manager = cache_manager
        self.running = True
        
    def run(self):
        """Process events from Pulsar and compute metrics"""
        client = pulsar.Client(PULSAR_URL)
        
        # Subscribe to activity events
        consumer = client.subscribe(
            "persistent://platformq/.*/activity-events",
            "realtime-analytics",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        while self.running:
            try:
                msg = consumer.receive(timeout_millis=1000)
                if msg:
                    self._process_event(msg)
                    consumer.acknowledge(msg)
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing event: {e}")
                    
        consumer.close()
        client.close()
        
    def _process_event(self, msg):
        """Process an event and update metrics"""
        try:
            # Extract tenant_id from topic
            topic = msg.topic_name()
            tenant_id = topic.split("/")[3]
            
            # Parse event
            event_data = json.loads(msg.data())
            event_type = event_data.get("event_type", "unknown")
            
            # Update various metrics
            metrics = []
            
            # Event count metric
            metrics.append(Metric(
                name=f"events.{event_type}.count",
                type=MetricType.COUNT,
                value=1,
                tags={"event_type": event_type}
            ))
            
            # User activity metric
            if "user_id" in event_data:
                metrics.append(Metric(
                    name="users.active",
                    type=MetricType.UNIQUE,
                    value=event_data["user_id"],
                    window=AggregationWindow.MINUTE
                ))
                
            # Response time metric (if available)
            if "duration_ms" in event_data:
                metrics.append(Metric(
                    name="performance.response_time",
                    type=MetricType.AVERAGE,
                    value=event_data["duration_ms"],
                    window=AggregationWindow.MINUTE,
                    tags={"endpoint": event_data.get("endpoint", "unknown")}
                ))
                
            # Store metrics
            for metric in metrics:
                self.cache_manager.put_metric(tenant_id, metric)
                
        except Exception as e:
            logger.error(f"Failed to process event: {e}")

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.subscriptions: Dict[str, List[str]] = defaultdict(list)
        
    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket
        
    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]
        if client_id in self.subscriptions:
            del self.subscriptions[client_id]
            
    async def send_data(self, client_id: str, data: Dict[str, Any]):
        if client_id in self.active_connections:
            websocket = self.active_connections[client_id]
            try:
                await websocket.send_json(data)
            except:
                self.disconnect(client_id)
                
    async def broadcast(self, tenant_id: str, data: Dict[str, Any]):
        """Broadcast data to all clients of a tenant"""
        disconnected = []
        for client_id, websocket in self.active_connections.items():
            if client_id.startswith(f"{tenant_id}:"):
                try:
                    await websocket.send_json(data)
                except:
                    disconnected.append(client_id)
                    
        for client_id in disconnected:
            self.disconnect(client_id)
            
    def subscribe(self, client_id: str, metric_names: List[str]):
        """Subscribe a client to specific metrics"""
        self.subscriptions[client_id] = metric_names
        
    def get_subscriptions(self, client_id: str) -> List[str]:
        return self.subscriptions.get(client_id, [])

# Create managers
cache_manager = IgniteCacheManager()
connection_manager = ConnectionManager()

# Create the FastAPI app
app = create_base_app(
    service_name="realtime-analytics-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# API Endpoints
@app.post("/api/v1/metrics")
async def ingest_metrics(
    metrics: List[Metric],
    context: dict = Depends(get_current_tenant_and_user)
):
    """Ingest metrics into the real-time analytics system"""
    tenant_id = context["tenant_id"]
    
    for metric in metrics:
        cache_manager.put_metric(tenant_id, metric)
        
        # Broadcast to connected clients
        await connection_manager.broadcast(tenant_id, {
            "type": "metric_update",
            "metric": metric.dict()
        })
        
    return {"message": f"Ingested {len(metrics)} metrics"}

@app.post("/api/v1/query")
async def query_metrics(
    query: MetricQuery,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Query aggregated metrics"""
    tenant_id = context["tenant_id"]
    
    results = cache_manager.compute_aggregation(tenant_id, query)
    
    return {
        "query": query.dict(),
        "results": results,
        "timestamp": datetime.utcnow()
    }

@app.get("/api/v1/metrics/{metric_name}/current")
async def get_current_metric(
    metric_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get the current value of a metric"""
    tenant_id = context["tenant_id"]
    
    # Get metrics from the last minute
    end_time = int(time.time())
    start_time = end_time - 60
    
    metrics = cache_manager.get_metrics(tenant_id, metric_name, start_time, end_time)
    
    if not metrics:
        raise HTTPException(status_code=404, detail="Metric not found")
        
    # Return the most recent value
    latest = max(metrics, key=lambda x: x['timestamp'])
    
    return {
        "metric_name": metric_name,
        "value": latest['value'],
        "timestamp": datetime.fromtimestamp(latest['timestamp']),
        "type": latest['type']
    }

@app.post("/api/v1/dashboards")
async def create_dashboard(
    dashboard: Dashboard,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a new dashboard"""
    tenant_id = context["tenant_id"]
    user_id = context["user"]["id"]
    
    dashboard.id = str(uuid.uuid4())
    dashboard.created_at = datetime.utcnow()
    dashboard.updated_at = datetime.utcnow()
    
    # Store in cache
    key = f"{tenant_id}:dashboard:{dashboard.id}"
    cache_manager.caches['dashboards'].put(key, dashboard.dict())
    
    return dashboard

@app.get("/api/v1/dashboards")
async def list_dashboards(
    context: dict = Depends(get_current_tenant_and_user)
):
    """List all dashboards for the tenant"""
    tenant_id = context["tenant_id"]
    
    dashboards = []
    prefix = f"{tenant_id}:dashboard:"
    
    for key, value in cache_manager.caches['dashboards'].scan():
        if key.startswith(prefix):
            dashboards.append(Dashboard(**value))
            
    return {"dashboards": dashboards}

@app.get("/api/v1/dashboards/{dashboard_id}")
async def get_dashboard(
    dashboard_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get a specific dashboard"""
    tenant_id = context["tenant_id"]
    
    key = f"{tenant_id}:dashboard:{dashboard_id}"
    dashboard_data = cache_manager.caches['dashboards'].get(key)
    
    if not dashboard_data:
        raise HTTPException(status_code=404, detail="Dashboard not found")
        
    return Dashboard(**dashboard_data)

@app.websocket("/ws/metrics/{client_id}")
async def websocket_metrics(
    websocket: WebSocket,
    client_id: str,
    tenant_id: str = Query(...)
):
    """WebSocket endpoint for real-time metric updates"""
    full_client_id = f"{tenant_id}:{client_id}"
    
    await connection_manager.connect(websocket, full_client_id)
    
    try:
        while True:
            # Receive messages from client
            data = await websocket.receive_json()
            
            if data["type"] == "subscribe":
                # Subscribe to specific metrics
                metrics = data.get("metrics", [])
                connection_manager.subscribe(full_client_id, metrics)
                
                # Start continuous query
                query = MetricQuery(
                    metrics=metrics,
                    window=AggregationWindow.MINUTE,
                    aggregation=MetricType.AVERAGE
                )
                
                def send_results(results):
                    asyncio.create_task(
                        connection_manager.send_data(full_client_id, {
                            "type": "metric_data",
                            "data": results
                        })
                    )
                    
                cache_manager.create_continuous_query(
                    tenant_id,
                    full_client_id,
                    query,
                    send_results
                )
                
            elif data["type"] == "unsubscribe":
                # Stop continuous query
                cache_manager.stop_continuous_query(full_client_id)
                
            elif data["type"] == "ping":
                # Keep-alive
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        connection_manager.disconnect(full_client_id)
        cache_manager.stop_continuous_query(full_client_id)

@app.get("/api/v1/export/prometheus")
async def export_prometheus_metrics(
    context: dict = Depends(get_current_tenant_and_user)
):
    """Export metrics in Prometheus format"""
    tenant_id = context["tenant_id"]
    
    # Get all current metrics
    end_time = int(time.time())
    start_time = end_time - 300  # Last 5 minutes
    
    lines = []
    
    # Scan all metrics
    for key, value in cache_manager.caches['metrics'].scan():
        if key.startswith(f"{tenant_id}:"):
            metric_name = value['name'].replace('.', '_')
            metric_value = value['value']
            timestamp = value['timestamp']
            
            # Format as Prometheus metric
            if value.get('tags'):
                tags = json.loads(value['tags'])
                tag_str = ','.join([f'{k}="{v}"' for k, v in tags.items()])
                line = f'{metric_name}{{{tag_str}}} {metric_value} {timestamp}000'
            else:
                line = f'{metric_name} {metric_value} {timestamp}000'
                
            lines.append(line)
            
    content = '\n'.join(lines)
    
    return StreamingResponse(
        iter([content]),
        media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=metrics.txt"}
    )

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize the service"""
    # Connect to Ignite
    cache_manager.connect()
    
    # Start stream processor
    processor = StreamProcessor(cache_manager)
    processor.start()
    app.state.stream_processor = processor
    
    logger.info("Real-time analytics service started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    # Stop stream processor
    if hasattr(app.state, "stream_processor"):
        app.state.stream_processor.running = False
        app.state.stream_processor.join(timeout=5)
        
    # Disconnect from Ignite
    cache_manager.disconnect()
    
    logger.info("Real-time analytics service stopped")

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    ignite_status = "unhealthy"
    try:
        # Check Ignite connection
        if cache_manager.client and cache_manager.client.connected:
            ignite_status = "healthy"
    except:
        pass
        
    return {
        "status": "healthy" if ignite_status == "healthy" else "degraded",
        "service": "realtime-analytics-service",
        "ignite": ignite_status,
        "active_connections": len(connection_manager.active_connections)
    } 