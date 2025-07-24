"""
Query API Router

Handles unified query execution across multiple engines.
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Query as QueryParam
from pydantic import BaseModel, Field

from ...core.base import AnalyticsBaseService, QueryMode

# Router instance
router = APIRouter()

# Global service reference
_service: Optional[AnalyticsBaseService] = None


def set_service(service: AnalyticsBaseService):
    """Set the service instance for the router"""
    global _service
    _service = service


# Request/Response Models
class UnifiedQuery(BaseModel):
    """Unified query request"""
    query: Optional[str] = Field(None, description="SQL query")
    query_type: Optional[str] = Field(None, description="Query type (batch, realtime, timeseries)")
    mode: Optional[QueryMode] = Field(None, description="Execution mode")
    
    # Filters and parameters
    filters: Dict[str, Any] = Field(default_factory=dict)
    time_range: Optional[str] = Field(None, description="Time range (1h, 1d, 7d, etc)")
    
    # Grouping and aggregation
    group_by: List[str] = Field(default_factory=list)
    metrics: List[str] = Field(default_factory=list)
    aggregations: List[str] = Field(default_factory=list)
    
    # Options
    limit: int = Field(1000, ge=1, le=10000)
    cache_ttl: int = Field(300, ge=0)
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "SELECT * FROM events WHERE timestamp > now() - interval '1 hour'",
                "mode": "auto",
                "limit": 100
            }
        }


class QueryResult(BaseModel):
    """Query execution result"""
    mode: str
    engine: str
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    execution_time_ms: float
    cached: bool = False
    
    class Config:
        json_schema_extra = {
            "example": {
                "mode": "realtime",
                "engine": "druid",
                "data": [{"metric": "cpu_usage", "value": 45.2}],
                "metadata": {"rows_scanned": 1000},
                "execution_time_ms": 23.5,
                "cached": False
            }
        }


# Endpoints
@router.post("/execute", response_model=QueryResult)
async def execute_query(query: UnifiedQuery) -> QueryResult:
    """
    Execute a unified query across analytics engines.
    
    The system will automatically route to the appropriate engine based on:
    - Query characteristics
    - Data recency requirements
    - Performance requirements
    """
    if not _service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        result = await _service.execute_query(
            query=query.query,
            mode=query.mode,
            filters=query.filters,
            time_range=query.time_range,
            group_by=query.group_by,
            metrics=query.metrics,
            aggregations=query.aggregations,
            limit=query.limit,
            cache_ttl=query.cache_ttl
        )
        
        return QueryResult(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")


@router.get("/engines")
async def get_available_engines():
    """Get list of available query engines and their capabilities"""
    if not _service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    engines = await _service.get_engine_status()
    
    return {
        "engines": engines,
        "routing_rules": {
            "batch": "Complex queries with joins, large historical data",
            "realtime": "Recent data, simple aggregations, low latency",
            "auto": "System selects optimal engine"
        }
    }


@router.post("/explain")
async def explain_query(query: UnifiedQuery):
    """
    Explain query execution plan without running it.
    
    Shows which engine would be used and why.
    """
    if not _service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Determine execution plan
        mode = query.mode or QueryMode.AUTO
        
        if mode == QueryMode.AUTO:
            # Analyze query to determine best engine
            selected_mode = _service._determine_query_mode(
                query=query.query,
                time_range=query.time_range,
                metrics=query.metrics
            )
        else:
            selected_mode = mode
        
        # Get engine for mode
        engine = "trino" if selected_mode == QueryMode.BATCH else "druid"
        
        return {
            "query": query.query or "Generated from parameters",
            "selected_mode": selected_mode.value,
            "selected_engine": engine,
            "reasoning": _service._explain_engine_selection(
                query=query.query,
                mode=selected_mode,
                time_range=query.time_range
            ),
            "estimated_performance": {
                "latency": "< 100ms" if selected_mode == QueryMode.REALTIME else "1-5s",
                "throughput": "high" if selected_mode == QueryMode.BATCH else "medium"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to explain query: {str(e)}")


@router.get("/cache/stats")
async def get_cache_statistics():
    """Get query cache statistics"""
    if not _service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    stats = await _service.get_cache_stats()
    
    return {
        "cache_enabled": True,
        "statistics": stats,
        "performance": {
            "hit_rate": stats.get("hit_rate", 0),
            "avg_latency_ms": stats.get("avg_latency", 0),
            "memory_used_mb": stats.get("memory_used", 0) / 1024 / 1024
        }
    }


@router.delete("/cache")
async def clear_query_cache(
    pattern: Optional[str] = QueryParam(None, description="Cache key pattern to clear")
):
    """Clear query cache"""
    if not _service:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        if pattern:
            cleared = await _service.clear_cache_pattern(pattern)
        else:
            cleared = await _service.clear_cache()
        
        return {
            "status": "success",
            "cleared_entries": cleared
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear cache: {str(e)}") 