"""Region management API endpoints for DIH service."""

from typing import Dict, Any, Optional, List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from data_intelligence_common import get_logger
from ..core.dih import CacheStrategy

logger = get_logger(__name__)

router = APIRouter()


class CreateRegionRequest(BaseModel):
    """Create region request."""
    name: str
    cache_mode: str = Field("PARTITIONED", description="PARTITIONED, REPLICATED, or LOCAL")
    backups: int = Field(1, ge=0, le=3)
    eviction_policy: Optional[str] = Field("LRU", description="LRU, LFU, FIFO, or RANDOM")
    eviction_max_size: Optional[int] = Field(10000, gt=0)
    atomicity_mode: Optional[str] = Field("ATOMIC", description="ATOMIC or TRANSACTIONAL")
    write_synchronization_mode: Optional[str] = Field("PRIMARY_SYNC", description="FULL_SYNC, FULL_ASYNC, or PRIMARY_SYNC")
    ttl_seconds: Optional[int] = Field(None, gt=0)
    read_through: bool = False
    write_through: bool = False
    write_behind: bool = False
    data_source: Optional[str] = None


class UpdateRegionRequest(BaseModel):
    """Update region request."""
    eviction_policy: Optional[str] = None
    eviction_max_size: Optional[int] = None
    ttl_seconds: Optional[int] = None
    read_through: Optional[bool] = None
    write_through: Optional[bool] = None
    write_behind: Optional[bool] = None


class RegionInfo(BaseModel):
    """Region information."""
    name: str
    cache_mode: str
    backups: int
    eviction_policy: Optional[str]
    eviction_max_size: Optional[int]
    ttl_seconds: Optional[int]
    atomicity_mode: str
    write_synchronization_mode: str
    read_through: bool
    write_through: bool
    write_behind: bool
    data_source: Optional[str]
    entry_count: int
    memory_bytes: int


@router.get("/")
async def list_regions():
    """List all cache regions."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        regions = []
        
        for name, region in dih.cache_regions.items():
            # Get cache metrics
            cache = dih.caches.get(name)
            entry_count = 0
            memory_bytes = 0
            
            if cache:
                # In production, would use Ignite metrics
                # entry_count = cache.size()
                # memory_bytes = cache.metrics().getCacheSize()
                pass
                
            regions.append({
                "name": name,
                "cache_mode": region.cache_mode,
                "backups": region.backups,
                "eviction_policy": region.eviction_policy,
                "eviction_max_size": region.eviction_max_size,
                "ttl_seconds": region.ttl_seconds,
                "entry_count": entry_count,
                "memory_bytes": memory_bytes
            })
            
        return {"regions": regions, "count": len(regions)}
        
    except Exception as e:
        logger.error(f"Error listing regions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{region_name}")
async def get_region_info(region_name: str):
    """Get information about a specific region."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        region = dih.cache_regions.get(region_name)
        if not region:
            raise HTTPException(status_code=404, detail=f"Region {region_name} not found")
            
        cache = dih.caches.get(region_name)
        entry_count = 0
        memory_bytes = 0
        
        if cache:
            # In production, would use Ignite metrics
            pass
            
        return RegionInfo(
            name=region_name,
            cache_mode=region.cache_mode,
            backups=region.backups,
            eviction_policy=region.eviction_policy,
            eviction_max_size=region.eviction_max_size,
            ttl_seconds=region.ttl_seconds,
            atomicity_mode=region.atomicity_mode,
            write_synchronization_mode=region.write_synchronization_mode,
            read_through=region.cache_strategy == CacheStrategy.READ_THROUGH,
            write_through=region.cache_strategy == CacheStrategy.WRITE_THROUGH,
            write_behind=region.cache_strategy == CacheStrategy.WRITE_BEHIND,
            data_source=None,  # TODO: Track data source
            entry_count=entry_count,
            memory_bytes=memory_bytes
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting region info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_region(request: CreateRegionRequest):
    """Create a new cache region."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        # Check if region already exists
        if request.name in dih.cache_regions:
            raise HTTPException(
                status_code=409,
                detail=f"Region {request.name} already exists"
            )
            
        # Create region
        await dih.create_cache_region(
            name=request.name,
            cache_mode=request.cache_mode,
            backups=request.backups,
            eviction_policy=request.eviction_policy,
            eviction_max_size=request.eviction_max_size,
            atomicity_mode=request.atomicity_mode,
            write_synchronization_mode=request.write_synchronization_mode,
            expiry_policy_factory="CreatedExpiryPolicy" if request.ttl_seconds else None,
            expiry_duration=request.ttl_seconds * 1000 if request.ttl_seconds else None
        )
        
        # Configure cache strategy
        if request.read_through:
            strategy = CacheStrategy.READ_THROUGH
        elif request.write_through:
            strategy = CacheStrategy.WRITE_THROUGH
        elif request.write_behind:
            strategy = CacheStrategy.WRITE_BEHIND
        else:
            strategy = CacheStrategy.CACHE_ASIDE
            
        dih.cache_regions[request.name].cache_strategy = strategy
        
        return {"status": "created", "region": request.name}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating region: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{region_name}")
async def update_region(region_name: str, request: UpdateRegionRequest):
    """Update region configuration."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        region = dih.cache_regions.get(region_name)
        if not region:
            raise HTTPException(status_code=404, detail=f"Region {region_name} not found")
            
        # Update configuration
        updates = []
        
        if request.eviction_policy is not None:
            region.eviction_policy = request.eviction_policy
            updates.append(f"eviction_policy={request.eviction_policy}")
            
        if request.eviction_max_size is not None:
            region.eviction_max_size = request.eviction_max_size
            updates.append(f"eviction_max_size={request.eviction_max_size}")
            
        if request.ttl_seconds is not None:
            region.ttl_seconds = request.ttl_seconds
            updates.append(f"ttl_seconds={request.ttl_seconds}")
            
        # Update cache strategy
        if any([request.read_through, request.write_through, request.write_behind]):
            if request.read_through:
                region.cache_strategy = CacheStrategy.READ_THROUGH
            elif request.write_through:
                region.cache_strategy = CacheStrategy.WRITE_THROUGH
            elif request.write_behind:
                region.cache_strategy = CacheStrategy.WRITE_BEHIND
                
        # Note: Some changes may require cache restart in production
        logger.info(f"Updated region {region_name}: {', '.join(updates)}")
        
        return {
            "status": "updated",
            "region": region_name,
            "updates": updates
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating region: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{region_name}")
async def delete_region(region_name: str):
    """Delete a cache region."""
    from ..main import app
    
    dih = app.state.dih
    
    try:
        if region_name not in dih.cache_regions:
            raise HTTPException(status_code=404, detail=f"Region {region_name} not found")
            
        # Remove cache
        if region_name in dih.caches:
            cache = dih.caches[region_name]
            cache.destroy()
            del dih.caches[region_name]
            
        # Remove region config
        del dih.cache_regions[region_name]
        
        logger.info(f"Deleted region {region_name}")
        
        return {"status": "deleted", "region": region_name}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting region: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 