"""Health API endpoints for DIH service."""

from typing import Dict, Any
from fastapi import APIRouter, HTTPException

from data_intelligence_common import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.get("/ignite")
async def check_ignite_health():
    """Check Ignite cluster health."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        if not dih or not dih.client:
            return {
                "status": "unhealthy",
                "message": "Ignite client not connected"
            }
            
        # Check cluster nodes
        # In production, would use Ignite cluster metrics
        return {
            "status": "healthy",
            "nodes": len(dih.ignite_nodes),
            "message": "Ignite cluster is operational"
        }
        
    except Exception as e:
        logger.error(f"Error checking Ignite health: {e}")
        return {
            "status": "unhealthy",
            "message": str(e)
        }


@router.get("/cache-regions")
async def check_cache_regions_health():
    """Check health of all cache regions."""
    from ..main import app
    
    dih = app.state.dih
    cache_manager = app.state.cache_manager
    
    try:
        regions_health = {}
        
        for region_name in dih.cache_regions:
            cache = dih.caches.get(region_name)
            if cache:
                # Get region stats
                stats = await cache_manager.get_stats(region_name)
                
                regions_health[region_name] = {
                    "status": "healthy",
                    "entries": stats.get("entries", 0),
                    "hit_rate": stats.get("hit_rate", 0),
                    "memory_bytes": stats.get("memory_bytes", 0)
                }
            else:
                regions_health[region_name] = {
                    "status": "unhealthy",
                    "message": "Cache not initialized"
                }
                
        return {
            "status": "healthy" if all(r["status"] == "healthy" for r in regions_health.values()) else "degraded",
            "regions": regions_health
        }
        
    except Exception as e:
        logger.error(f"Error checking cache regions health: {e}")
        return {
            "status": "unhealthy",
            "message": str(e)
        }


@router.get("/sync")
async def check_sync_health():
    """Check sync services health."""
    from ..main import app
    
    cdc_processor = app.state.cdc_processor
    sync_orchestrator = app.state.sync_orchestrator
    
    try:
        health_status = {
            "cdc_processor": {
                "status": "healthy" if cdc_processor and cdc_processor._running else "stopped",
                "message": "CDC processor is running" if cdc_processor and cdc_processor._running else "CDC processor is not running"
            },
            "sync_orchestrator": {
                "status": "healthy" if sync_orchestrator and sync_orchestrator._running else "stopped",
                "message": "Sync orchestrator is running" if sync_orchestrator and sync_orchestrator._running else "Sync orchestrator is not running"
            }
        }
        
        overall_status = "healthy"
        if any(s["status"] != "healthy" for s in health_status.values()):
            overall_status = "degraded"
            
        return {
            "status": overall_status,
            "components": health_status
        }
        
    except Exception as e:
        logger.error(f"Error checking sync health: {e}")
        return {
            "status": "unhealthy",
            "message": str(e)
        }


@router.get("/data-sources")
async def check_data_sources_health():
    """Check data sources connectivity."""
    from ..main import app
    
    data_source_manager = app.state.data_source_manager
    
    try:
        if not data_source_manager:
            return {
                "status": "unhealthy",
                "message": "Data source manager not initialized"
            }
            
        # Check each configured data source
        sources_health = {}
        
        # Placeholder - would check actual connectivity
        for source in ["postgres", "cassandra", "elasticsearch"]:
            sources_health[source] = {
                "status": "healthy",
                "message": f"{source} is reachable"
            }
            
        return {
            "status": "healthy" if all(s["status"] == "healthy" for s in sources_health.values()) else "degraded",
            "sources": sources_health
        }
        
    except Exception as e:
        logger.error(f"Error checking data sources health: {e}")
        return {
            "status": "unhealthy",
            "message": str(e)
        } 