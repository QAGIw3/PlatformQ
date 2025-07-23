"""Cache management for DIH service."""

from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio
import logging

from data_intelligence_common import get_logger

from .dih import DigitalIntegrationHub, CacheRegion, CacheStrategy

logger = get_logger(__name__)


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    entries: int = 0
    memory_bytes: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class CacheManager:
    """
    Manages cache operations and policies for DIH.
    
    Features:
    - Cache warming
    - Eviction management
    - Statistics tracking
    - Cache optimization
    - Region management
    """
    
    def __init__(self, dih: DigitalIntegrationHub):
        self.dih = dih
        self._stats: Dict[str, CacheStats] = {}
        self._warm_up_tasks: Dict[str, asyncio.Task] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize cache manager."""
        # Start monitoring
        self._monitoring_task = asyncio.create_task(self._monitor_caches())
        
        logger.info("Cache manager initialized")
        
    async def cleanup(self):
        """Cleanup cache manager."""
        # Stop warm-up tasks
        for task in self._warm_up_tasks.values():
            task.cancel()
            
        # Stop monitoring
        if self._monitoring_task:
            self._monitoring_task.cancel()
            
        # Wait for tasks
        tasks = list(self._warm_up_tasks.values())
        if self._monitoring_task:
            tasks.append(self._monitoring_task)
            
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info("Cache manager cleaned up")
        
    async def warm_up_cache(
        self,
        region_name: str,
        data_source: str,
        query: str,
        refresh_interval: Optional[int] = None
    ):
        """
        Warm up cache with data from source.
        
        Args:
            region_name: Cache region to warm up
            data_source: Data source to load from
            query: Query to execute
            refresh_interval: Optional refresh interval in seconds
        """
        # Cancel existing warm-up if any
        if region_name in self._warm_up_tasks:
            self._warm_up_tasks[region_name].cancel()
            
        # Create warm-up task
        task = asyncio.create_task(
            self._warm_up_loop(region_name, data_source, query, refresh_interval)
        )
        self._warm_up_tasks[region_name] = task
        
        logger.info(f"Started cache warm-up for region {region_name}")
        
    async def _warm_up_loop(
        self,
        region_name: str,
        data_source: str,
        query: str,
        refresh_interval: Optional[int]
    ):
        """Cache warm-up loop."""
        while True:
            try:
                # Load data from source
                data = await self._load_from_source(data_source, query)
                
                # Get cache region
                region = self.dih.cache_regions.get(region_name)
                if not region:
                    logger.error(f"Cache region {region_name} not found")
                    break
                    
                # Load data into cache
                loaded_count = 0
                for key, value in data.items():
                    await self.dih.put(region_name, key, value)
                    loaded_count += 1
                    
                logger.info(f"Loaded {loaded_count} entries into {region_name}")
                
                # Break if one-time warm-up
                if refresh_interval is None:
                    break
                    
                # Wait for refresh interval
                await asyncio.sleep(refresh_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache warm-up: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute
                
    async def _load_from_source(self, data_source: str, query: str) -> Dict[str, Any]:
        """Load data from source (placeholder)."""
        # In production, this would connect to the actual data source
        # For now, return empty dict
        return {}
        
    async def evict_region(self, region_name: str, pattern: Optional[str] = None):
        """
        Evict entries from cache region.
        
        Args:
            region_name: Cache region name
            pattern: Optional key pattern to evict
        """
        cache = self.dih.caches.get(region_name)
        if not cache:
            raise ValueError(f"Cache region {region_name} not found")
            
        if pattern:
            # Evict by pattern
            # In production, this would use Ignite's query capabilities
            logger.info(f"Evicting entries matching {pattern} from {region_name}")
        else:
            # Clear entire cache
            cache.clear()
            logger.info(f"Cleared cache region {region_name}")
            
        # Update stats
        if region_name in self._stats:
            self._stats[region_name].evictions += 1
            
    async def get_stats(self, region_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Args:
            region_name: Optional specific region, otherwise all regions
        """
        if region_name:
            stats = self._stats.get(region_name, CacheStats())
            return {
                "region": region_name,
                "hits": stats.hits,
                "misses": stats.misses,
                "hit_rate": stats.hit_rate,
                "evictions": stats.evictions,
                "entries": stats.entries,
                "memory_bytes": stats.memory_bytes
            }
        else:
            # All regions
            all_stats = {}
            for name, stats in self._stats.items():
                all_stats[name] = {
                    "hits": stats.hits,
                    "misses": stats.misses,
                    "hit_rate": stats.hit_rate,
                    "evictions": stats.evictions,
                    "entries": stats.entries,
                    "memory_bytes": stats.memory_bytes
                }
            return all_stats
            
    async def optimize_cache(self, region_name: str):
        """
        Optimize cache configuration based on usage patterns.
        
        Args:
            region_name: Cache region to optimize
        """
        stats = self._stats.get(region_name)
        if not stats:
            logger.warning(f"No stats available for region {region_name}")
            return
            
        region = self.dih.cache_regions.get(region_name)
        if not region:
            logger.error(f"Cache region {region_name} not found")
            return
            
        # Optimization logic
        if stats.hit_rate < 0.5:
            # Low hit rate - might need different eviction policy
            logger.info(f"Low hit rate ({stats.hit_rate:.2%}) for {region_name}")
            
            # Suggest optimization
            suggestions = []
            
            if region.eviction_policy == "LRU":
                suggestions.append("Consider LFU policy for better hit rate")
            
            if region.ttl_seconds and region.ttl_seconds < 300:
                suggestions.append("Consider increasing TTL")
                
            if suggestions:
                logger.info(f"Optimization suggestions for {region_name}: {suggestions}")
                
    async def _monitor_caches(self):
        """Monitor cache statistics."""
        while True:
            try:
                # Update stats for each region
                for region_name, cache in self.dih.caches.items():
                    if region_name not in self._stats:
                        self._stats[region_name] = CacheStats()
                        
                    stats = self._stats[region_name]
                    
                    # Get cache metrics (simplified)
                    # In production, would use Ignite metrics API
                    try:
                        # Placeholder metrics
                        stats.entries = 0  # cache.size()
                        stats.memory_bytes = 0  # cache.metrics().getCacheSize()
                    except:
                        pass
                        
                # Log summary
                total_hits = sum(s.hits for s in self._stats.values())
                total_misses = sum(s.misses for s in self._stats.values())
                overall_hit_rate = total_hits / (total_hits + total_misses) if (total_hits + total_misses) > 0 else 0
                
                logger.debug(
                    f"Cache stats: {len(self._stats)} regions, "
                    f"hit rate: {overall_hit_rate:.2%}"
                )
                
                await asyncio.sleep(60)  # Update every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring caches: {e}")
                await asyncio.sleep(60)
                
    def track_hit(self, region_name: str):
        """Track cache hit."""
        if region_name not in self._stats:
            self._stats[region_name] = CacheStats()
        self._stats[region_name].hits += 1
        
    def track_miss(self, region_name: str):
        """Track cache miss."""
        if region_name not in self._stats:
            self._stats[region_name] = CacheStats()
        self._stats[region_name].misses += 1 