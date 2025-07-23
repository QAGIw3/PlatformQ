"""Health check API endpoints"""

from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, Depends

from app.core.config import Settings, get_settings
from app.graph.janusgraph_client import JanusGraphClient
from app.analytics.graphx_engine import GraphXEngine
from app.core.cache_manager import CacheManager


router = APIRouter(tags=["health"])

# Global instances (will be injected)
graph_client: Optional[JanusGraphClient] = None
graphx_engine: Optional[GraphXEngine] = None
cache_manager: Optional[CacheManager] = None


@router.get("/health")
async def health_check(settings: Settings = Depends(get_settings)):
    """Basic health check"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.service_version,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/health/live")
async def liveness_check(settings: Settings = Depends(get_settings)):
    """Kubernetes liveness probe"""
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness_check(settings: Settings = Depends(get_settings)):
    """Kubernetes readiness probe"""
    checks = {
        "janusgraph": False,
        "graphx": False,
        "cache": False
    }
    
    # Check JanusGraph
    if graph_client and graph_client.connected:
        try:
            # Simple query to test connection
            await graph_client.count_nodes()
            checks["janusgraph"] = True
        except:
            pass
            
    # Check GraphX
    if graphx_engine and graphx_engine.initialized:
        checks["graphx"] = True
        
    # Check Cache
    if cache_manager and cache_manager.connected:
        checks["cache"] = True
        
    # Overall readiness
    ready = all(checks.values())
    
    return {
        "ready": ready,
        "checks": checks
    }


@router.get("/health/detailed")
async def detailed_health(settings: Settings = Depends(get_settings)):
    """Detailed health status"""
    health_status = {
        "service": {
            "name": settings.service_name,
            "version": settings.service_version,
            "environment": settings.environment,
            "uptime": datetime.utcnow().isoformat()
        },
        "components": {}
    }
    
    # JanusGraph health
    janusgraph_health = {
        "connected": False,
        "error": None,
        "metrics": {}
    }
    
    if graph_client:
        janusgraph_health["connected"] = graph_client.connected
        if graph_client.connected:
            try:
                node_count = await graph_client.count_nodes()
                edge_count = await graph_client.count_edges()
                janusgraph_health["metrics"] = {
                    "nodes": node_count,
                    "edges": edge_count
                }
            except Exception as e:
                janusgraph_health["error"] = str(e)
                
    health_status["components"]["janusgraph"] = janusgraph_health
    
    # GraphX health
    graphx_health = {
        "initialized": False,
        "running_jobs": 0,
        "spark_context": "inactive"
    }
    
    if graphx_engine:
        graphx_health["initialized"] = graphx_engine.initialized
        graphx_health["running_jobs"] = len(graphx_engine.running_jobs)
        if graphx_engine.sc:
            graphx_health["spark_context"] = "active"
            
    health_status["components"]["graphx"] = graphx_health
    
    # Cache health
    cache_health = {
        "connected": False,
        "error": None,
        "stats": {}
    }
    
    if cache_manager:
        cache_health["connected"] = cache_manager.connected
        if cache_manager.connected:
            try:
                stats = await cache_manager.get_stats()
                cache_health["stats"] = stats
            except Exception as e:
                cache_health["error"] = str(e)
                
    health_status["components"]["cache"] = cache_health
    
    # Overall health
    all_healthy = (
        janusgraph_health["connected"] and 
        graphx_health["initialized"] and 
        cache_health["connected"]
    )
    
    health_status["overall_status"] = "healthy" if all_healthy else "degraded"
    
    return health_status


def set_dependencies(gc: JanusGraphClient, ge: GraphXEngine, cm: CacheManager):
    """Set global dependencies"""
    global graph_client, graphx_engine, cache_manager
    graph_client = gc
    graphx_engine = ge
    cache_manager = cm 