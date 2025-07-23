"""Cache API endpoints for DIH service."""

from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime

from data_intelligence_common import get_logger

logger = get_logger(__name__)

router = APIRouter()


class CacheEntry(BaseModel):
    """Cache entry model."""
    key: str
    value: Any
    ttl_seconds: Optional[int] = Field(None, description="Time to live in seconds")
    
    
class CacheBulkEntry(BaseModel):
    """Bulk cache entry model."""
    entries: List[CacheEntry]
    
    
class CacheQueryRequest(BaseModel):
    """Cache query request."""
    keys: List[str]
    

class CacheStatsResponse(BaseModel):
    """Cache statistics response."""
    region: str
    hits: int
    misses: int
    hit_rate: float
    evictions: int
    entries: int
    memory_bytes: int


@router.get("/{region_name}/{key}")
async def get_cache_entry(region_name: str, key: str):
    """Get a cache entry."""
    from ..main import app
    
    dih = app.state.dih
    cache_manager = app.state.cache_manager
    
    try:
        value = await dih.get(region_name, key)
        
        if value is not None:
            cache_manager.track_hit(region_name)
            return {"key": key, "value": value, "found": True}
        else:
            cache_manager.track_miss(region_name)
            return {"key": key, "value": None, "found": False}
            
    except Exception as e:
        logger.error(f"Error getting cache entry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{region_name}")
async def put_cache_entry(region_name: str, entry: CacheEntry):
    """Put a cache entry."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        await dih.put(
            region_name, 
            entry.key, 
            entry.value,
            ttl_seconds=entry.ttl_seconds
        )
        
        return {"status": "success", "key": entry.key}
        
    except Exception as e:
        logger.error(f"Error putting cache entry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{region_name}/bulk")
async def put_cache_bulk(region_name: str, request: CacheBulkEntry):
    """Put multiple cache entries."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        success_count = 0
        errors = []
        
        for entry in request.entries:
            try:
                await dih.put(
                    region_name,
                    entry.key,
                    entry.value,
                    ttl_seconds=entry.ttl_seconds
                )
                success_count += 1
            except Exception as e:
                errors.append({"key": entry.key, "error": str(e)})
                
        return {
            "status": "completed",
            "success_count": success_count,
            "error_count": len(errors),
            "errors": errors
        }
        
    except Exception as e:
        logger.error(f"Error in bulk put: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{region_name}/query")
async def query_cache(region_name: str, request: CacheQueryRequest):
    """Query multiple cache entries."""
    from ..main import app
    
    dih = app.state.dih
    cache_manager = app.state.cache_manager
    
    try:
        results = []
        hits = 0
        misses = 0
        
        for key in request.keys:
            value = await dih.get(region_name, key)
            
            if value is not None:
                hits += 1
                results.append({"key": key, "value": value, "found": True})
            else:
                misses += 1
                results.append({"key": key, "value": None, "found": False})
                
        # Track stats
        for _ in range(hits):
            cache_manager.track_hit(region_name)
        for _ in range(misses):
            cache_manager.track_miss(region_name)
            
        return {
            "results": results,
            "hits": hits,
            "misses": misses,
            "hit_rate": hits / len(request.keys) if request.keys else 0
        }
        
    except Exception as e:
        logger.error(f"Error querying cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{region_name}/{key}")
async def delete_cache_entry(region_name: str, key: str):
    """Delete a cache entry."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        await dih.remove(region_name, key)
        return {"status": "success", "key": key}
        
    except Exception as e:
        logger.error(f"Error deleting cache entry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{region_name}")
async def clear_cache_region(region_name: str, pattern: Optional[str] = Query(None)):
    """Clear cache region or entries matching pattern."""
    from ..main import app
    
    cache_manager = app.state.cache_manager
    
    try:
        await cache_manager.evict_region(region_name, pattern)
        
        return {
            "status": "success",
            "region": region_name,
            "pattern": pattern
        }
        
    except Exception as e:
        logger.error(f"Error clearing cache region: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{region_name}/stats")
async def get_cache_stats(region_name: str):
    """Get cache statistics for a region."""
    from ..main import app
    
    cache_manager = app.state.cache_manager
    
    try:
        stats = await cache_manager.get_stats(region_name)
        return stats
        
    except Exception as e:
        logger.error(f"Error getting cache stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{region_name}/warm-up")
async def warm_up_cache(
    region_name: str,
    data_source: str,
    query: str,
    refresh_interval: Optional[int] = None
):
    """Start cache warm-up process."""
    from ..main import app
    
    cache_manager = app.state.cache_manager
    
    try:
        await cache_manager.warm_up_cache(
            region_name,
            data_source,
            query,
            refresh_interval
        )
        
        return {
            "status": "started",
            "region": region_name,
            "data_source": data_source,
            "refresh_interval": refresh_interval
        }
        
    except Exception as e:
        logger.error(f"Error starting cache warm-up: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{region_name}/optimize")
async def optimize_cache(region_name: str):
    """Optimize cache configuration based on usage patterns."""
    from ..main import app
    
    cache_manager = app.state.cache_manager
    
    try:
        await cache_manager.optimize_cache(region_name)
        
        return {
            "status": "completed",
            "region": region_name,
            "message": "Cache optimization analysis completed. Check logs for suggestions."
        }
        
    except Exception as e:
        logger.error(f"Error optimizing cache: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 