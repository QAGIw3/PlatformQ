"""
Analytics Engine Service - Migrated Version

Fully utilizes the unified data-intelligence-common library.
"""

import asyncio
from typing import Dict, Any, List, Optional
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from data_intelligence_common.core.events import EventType, create_processing_event
from data_intelligence_common.monitoring import StructuredLogger

from .core.base import AnalyticsBaseService, AnalyticsServiceConfig, AnalyticsMode

logger = StructuredLogger.get_logger(__name__)


# ============= API Models =============

class UnifiedQuery(BaseModel):
    """Unified query model supporting all query types"""
    query: Optional[str] = Field(None, description="SQL query for batch mode")
    query_type: Optional[str] = Field(None, description="Predefined query type")
    mode: AnalyticsMode = Field(AnalyticsMode.AUTO, description="Execution mode")
    filters: Dict[str, Any] = Field(default_factory=dict, description="Query filters")
    time_range: str = Field("7d", description="Time range: 1h, 1d, 7d, 30d, 90d")
    
    # Grouping and aggregation
    group_by: List[str] = Field(default_factory=list, description="Fields to group by")
    metrics: List[str] = Field(default_factory=list, description="Metrics to calculate")
    
    # Advanced options
    limit: int = Field(1000, description="Result limit")
    cache_ttl: int = Field(300, description="Cache TTL in seconds")


class QueryResult(BaseModel):
    """Query execution result"""
    data: List[Dict[str, Any]]
    mode: str
    execution_time: float
    engine: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


# ============= Service Setup =============

# Global service instance
analytics_service: Optional[AnalyticsBaseService] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage service lifecycle"""
    global analytics_service
    
    # Load configuration
    config = AnalyticsServiceConfig(
        name="analytics-engine-service",
        version="2.0.0",
        description="Unified Analytics Engine Service",
        
        # Enable features
        enable_metrics=True,
        enable_tracing=True,
        enable_health_check=True,
        
        # Analytics specific
        enable_streaming=True,
        enable_ml_predictions=True,
        
        # Cache configuration
        enable_caching=True,
        cache_ttl=timedelta(minutes=5)
    )
    
    # Create and initialize service
    analytics_service = AnalyticsBaseService(config)
    await analytics_service.initialize_service()
    
    logger.info("Analytics service started successfully")
    
    yield
    
    # Cleanup
    await analytics_service.cleanup_service()
    logger.info("Analytics service stopped")


# Create FastAPI app
app = FastAPI(
    title="Analytics Engine Service",
    description="Unified analytics service with batch, real-time, and streaming capabilities",
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


# ============= API Endpoints =============

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    health_status = await analytics_service.get_health_status()
    
    if not health_status["healthy"]:
        raise HTTPException(status_code=503, detail=health_status)
        
    return health_status


@app.get("/metrics")
async def get_metrics():
    """Get service metrics"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    return analytics_service.get_service_metrics()


@app.post("/query", response_model=QueryResult)
async def execute_query(query: UnifiedQuery, background_tasks: BackgroundTasks):
    """
    Execute analytics query.
    
    Supports batch, real-time, and streaming modes with automatic optimization.
    """
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    try:
        # Execute query
        result = await analytics_service.execute_query(
            query=query.query or "",
            mode=query.mode,
            filters=query.filters,
            time_range=query.time_range,
            group_by=query.group_by,
            metrics=query.metrics,
            limit=query.limit
        )
        
        # Schedule background analytics if needed
        if query.mode == AnalyticsMode.STREAM:
            background_tasks.add_task(
                _monitor_stream_query,
                result.get("subscription_id")
            )
        
        return QueryResult(
            data=result.get("data", []),
            mode=result.get("mode", "unknown"),
            execution_time=result.get("execution_time", 0),
            engine=result.get("engine", "unknown"),
            metadata={
                "cached": result.get("cached", False),
                "query_hash": hash(query.query) if query.query else None
            }
        )
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query/batch")
async def execute_batch_query(queries: List[UnifiedQuery]):
    """Execute multiple queries in batch"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    results = []
    
    for query in queries:
        try:
            result = await analytics_service.execute_query(
                query=query.query or "",
                mode=query.mode,
                **query.dict(exclude={"query", "mode"})
            )
            results.append({
                "success": True,
                "result": result
            })
        except Exception as e:
            results.append({
                "success": False,
                "error": str(e)
            })
            
    return {"results": results}


@app.get("/query/templates")
async def get_query_templates():
    """Get predefined query templates"""
    return {
        "templates": [
            {
                "name": "top_metrics",
                "description": "Get top N metrics by value",
                "query": "SELECT metric_name, AVG(value) as avg_value FROM metrics WHERE timestamp > NOW() - INTERVAL '{{time_range}}' GROUP BY metric_name ORDER BY avg_value DESC LIMIT {{limit}}",
                "parameters": ["time_range", "limit"]
            },
            {
                "name": "anomaly_detection",
                "description": "Detect anomalies in metrics",
                "query": "SELECT * FROM metrics WHERE _is_anomaly = true AND timestamp > NOW() - INTERVAL '{{time_range}}'",
                "parameters": ["time_range"]
            },
            {
                "name": "trend_analysis",
                "description": "Analyze metric trends",
                "query": "SELECT DATE_TRUNC('hour', timestamp) as hour, metric_name, AVG(value) as avg_value FROM metrics WHERE timestamp > NOW() - INTERVAL '{{time_range}}' GROUP BY hour, metric_name",
                "parameters": ["time_range"]
            }
        ]
    }


@app.post("/stream/subscribe")
async def subscribe_to_stream(
    topic: str,
    filters: Optional[Dict[str, Any]] = None
):
    """Subscribe to real-time analytics stream"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    # Create subscription
    subscription_id = await analytics_service.event_bus.subscribe(
        topic_pattern=f"analytics.{topic}",
        handler=lambda event: logger.info(f"Stream event: {event}"),
        event_types=["analytics.data"]
    )
    
    return {
        "subscription_id": subscription_id,
        "topic": topic,
        "status": "active"
    }


@app.delete("/stream/unsubscribe/{subscription_id}")
async def unsubscribe_from_stream(subscription_id: str):
    """Unsubscribe from analytics stream"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    await analytics_service.event_bus.unsubscribe(subscription_id)
    
    return {"status": "unsubscribed"}


@app.get("/engines/status")
async def get_engines_status():
    """Get status of all analytics engines"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    return {
        "batch_engine": await analytics_service._check_batch_engine_health(),
        "realtime_engine": await analytics_service._check_realtime_engine_health(),
        "stream_processor": {
            "healthy": analytics_service._stream_processor is not None,
            "active": analytics_service._stream_processor is not None
        },
        "ml_engine": {
            "healthy": analytics_service._ml_engine is not None,
            "models_loaded": 0  # Would query actual model count
        }
    }


@app.post("/cache/clear")
async def clear_cache(pattern: Optional[str] = None):
    """Clear analytics cache"""
    if not analytics_service:
        raise HTTPException(status_code=503, detail="Service not initialized")
        
    if pattern:
        await analytics_service.invalidate_cache(pattern)
    else:
        await analytics_service.clear_cache()
        
    return {"status": "cache_cleared"}


# ============= Background Tasks =============

async def _monitor_stream_query(subscription_id: str):
    """Monitor streaming query in background"""
    # This would monitor the streaming query and handle results
    logger.info(f"Monitoring stream query: {subscription_id}")
    
    # Simulate monitoring for 60 seconds
    for i in range(60):
        await asyncio.sleep(1)
        
        # Emit progress event
        if analytics_service:
            await analytics_service.publish_event(
                event_type="analytics.stream_progress",
                data={
                    "subscription_id": subscription_id,
                    "progress": i + 1,
                    "total": 60
                }
            )


# ============= WebSocket Support =============

from fastapi import WebSocket, WebSocketDisconnect

@app.websocket("/ws/analytics")
async def analytics_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time analytics"""
    await websocket.accept()
    
    if not analytics_service:
        await websocket.close(code=1003, reason="Service not initialized")
        return
        
    subscription_id = None
    
    try:
        # Subscribe to analytics events
        subscription_id = await analytics_service.event_bus.subscribe(
            topic_pattern="analytics.*",
            handler=lambda event: asyncio.create_task(
                websocket.send_json({
                    "type": event.event_type,
                    "data": event.payload,
                    "timestamp": event.timestamp.isoformat()
                })
            )
        )
        
        # Keep connection alive
        while True:
            # Wait for client messages
            data = await websocket.receive_json()
            
            # Handle different message types
            if data.get("type") == "query":
                # Execute query and send result
                result = await analytics_service.execute_query(
                    query=data.get("query", ""),
                    mode=AnalyticsMode(data.get("mode", "auto"))
                )
                
                await websocket.send_json({
                    "type": "query_result",
                    "data": result
                })
                
            elif data.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Cleanup subscription
        if subscription_id and analytics_service:
            await analytics_service.event_bus.unsubscribe(subscription_id)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 