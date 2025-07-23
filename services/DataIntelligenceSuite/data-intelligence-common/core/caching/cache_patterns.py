"""
Cache patterns for DataIntelligenceSuite

Implements advanced caching patterns and multi-level cache support.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Callable, List, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import hashlib
import json

from .strategies import CacheStrategy, CacheEntry, BaseCacheStrategy, create_cache_strategy
from .cache_manager import CacheManager, CacheConfig

logger = logging.getLogger(__name__)


class CachePattern(ABC):
    """Base class for cache patterns"""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value using pattern"""
        pass
        
    @abstractmethod
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put value using pattern"""
        pass
        
    @abstractmethod
    async def remove(self, key: str) -> bool:
        """Remove value using pattern"""
        pass


class MultiLevelCache(CachePattern):
    """
    Multi-level cache implementation (L1, L2, L3, etc.)
    
    Provides a hierarchy of caches with different characteristics:
    - L1: Fast, small (e.g., in-memory)
    - L2: Medium speed, medium size (e.g., distributed cache)
    - L3: Slow, large (e.g., database)
    """
    
    def __init__(self, levels: List[Tuple[str, BaseCacheStrategy, timedelta]]):
        """
        Initialize multi-level cache
        
        Args:
            levels: List of (name, cache_strategy, ttl) tuples
        """
        self.levels = levels
        self._metrics = {
            "l1_hits": 0,
            "l2_hits": 0,
            "l3_hits": 0,
            "misses": 0
        }
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache hierarchy"""
        for i, (name, cache, ttl) in enumerate(self.levels):
            try:
                value = await cache.get(key)
                
                if value is not None:
                    # Record hit
                    self._metrics[f"l{i+1}_hits"] += 1
                    
                    # Populate higher levels
                    for j in range(i):
                        higher_name, higher_cache, higher_ttl = self.levels[j]
                        await higher_cache.put(key, value, higher_ttl)
                        
                    return value
                    
            except Exception as e:
                logger.error(f"Error getting from {name}: {e}")
                
        self._metrics["misses"] += 1
        return None
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put value in all cache levels"""
        tasks = []
        
        for name, cache, level_ttl in self.levels:
            try:
                # Use level-specific TTL if not overridden
                effective_ttl = ttl or level_ttl
                tasks.append(cache.put(key, value, effective_ttl))
            except Exception as e:
                logger.error(f"Error putting to {name}: {e}")
                
        # Execute all puts in parallel
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
    async def remove(self, key: str) -> bool:
        """Remove value from all cache levels"""
        results = []
        
        for name, cache, _ in self.levels:
            try:
                result = await cache.remove(key)
                results.append(result)
            except Exception as e:
                logger.error(f"Error removing from {name}: {e}")
                results.append(False)
                
        return any(results)
        
    def get_metrics(self) -> Dict[str, int]:
        """Get cache metrics"""
        return self._metrics.copy()


class CacheWarmer:
    """
    Cache warming utility
    
    Pre-populates caches with frequently accessed data
    """
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self._warming_tasks: Dict[str, asyncio.Task] = {}
        
    async def warm_cache(
        self,
        cache_name: str,
        data_loader: Callable,
        keys: Optional[List[str]] = None,
        batch_size: int = 100,
        parallel_loads: int = 10
    ) -> int:
        """
        Warm cache with data
        
        Args:
            cache_name: Name of cache to warm
            data_loader: Function to load data
            keys: Specific keys to load (None for all)
            batch_size: Batch size for loading
            parallel_loads: Number of parallel load operations
            
        Returns:
            Number of entries loaded
        """
        logger.info(f"Starting cache warming for {cache_name}")
        
        loaded_count = 0
        
        try:
            if keys:
                # Load specific keys in batches
                for i in range(0, len(keys), batch_size):
                    batch = keys[i:i + batch_size]
                    
                    # Load batch in parallel
                    tasks = []
                    for j in range(0, len(batch), parallel_loads):
                        sub_batch = batch[j:j + parallel_loads]
                        tasks.append(self._load_batch(cache_name, data_loader, sub_batch))
                        
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, int):
                            loaded_count += result
                            
            else:
                # Load all data
                data = await data_loader()
                
                if isinstance(data, dict):
                    # Load in batches
                    items = list(data.items())
                    
                    for i in range(0, len(items), batch_size):
                        batch = dict(items[i:i + batch_size])
                        await self.cache_manager.put_all(cache_name, batch)
                        loaded_count += len(batch)
                        
            logger.info(f"Cache warming completed for {cache_name}: {loaded_count} entries")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Cache warming failed: {e}")
            raise
            
    async def _load_batch(
        self,
        cache_name: str,
        loader: Callable,
        keys: List[str]
    ) -> int:
        """Load a batch of keys"""
        loaded = 0
        
        for key in keys:
            try:
                value = await loader(key) if asyncio.iscoroutinefunction(loader) else loader(key)
                
                if value is not None:
                    await self.cache_manager.put(cache_name, key, value)
                    loaded += 1
                    
            except Exception as e:
                logger.error(f"Failed to load key {key}: {e}")
                
        return loaded
        
    async def start_periodic_warming(
        self,
        cache_name: str,
        data_loader: Callable,
        interval: timedelta,
        keys: Optional[List[str]] = None
    ):
        """Start periodic cache warming"""
        async def warm_loop():
            while True:
                try:
                    await self.warm_cache(cache_name, data_loader, keys)
                    await asyncio.sleep(interval.total_seconds())
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Periodic warming failed: {e}")
                    await asyncio.sleep(60)  # Retry after 1 minute
                    
        task = asyncio.create_task(warm_loop())
        self._warming_tasks[cache_name] = task
        
        logger.info(f"Started periodic warming for {cache_name} every {interval}")
        
    async def stop_periodic_warming(self, cache_name: str):
        """Stop periodic cache warming"""
        if cache_name in self._warming_tasks:
            task = self._warming_tasks.pop(cache_name)
            task.cancel()
            
            try:
                await task
            except asyncio.CancelledError:
                pass
                
            logger.info(f"Stopped periodic warming for {cache_name}")
            
    async def stop_all(self):
        """Stop all warming tasks"""
        for cache_name in list(self._warming_tasks.keys()):
            await self.stop_periodic_warming(cache_name)


class CacheInvalidator:
    """
    Cache invalidation utility
    
    Handles cache invalidation patterns and strategies
    """
    
    def __init__(self, cache_manager: CacheManager):
        self.cache_manager = cache_manager
        self._invalidation_rules: Dict[str, List[Callable]] = {}
        
    def add_invalidation_rule(
        self,
        cache_name: str,
        rule: Callable[[str, Any], bool]
    ):
        """
        Add invalidation rule for cache
        
        Args:
            cache_name: Cache to apply rule to
            rule: Function that returns True if key should be invalidated
        """
        if cache_name not in self._invalidation_rules:
            self._invalidation_rules[cache_name] = []
            
        self._invalidation_rules[cache_name].append(rule)
        
    async def invalidate_by_pattern(
        self,
        cache_name: str,
        pattern: str
    ) -> int:
        """
        Invalidate cache entries matching pattern
        
        Args:
            cache_name: Cache to invalidate
            pattern: Pattern to match (supports wildcards)
            
        Returns:
            Number of entries invalidated
        """
        invalidated = 0
        
        try:
            # Get all keys (this would need to be implemented in cache manager)
            # For now, we'll use a simplified approach
            logger.info(f"Invalidating entries matching {pattern} in {cache_name}")
            
            # In a real implementation, this would query the cache
            # and invalidate matching entries
            
            return invalidated
            
        except Exception as e:
            logger.error(f"Pattern invalidation failed: {e}")
            raise
            
    async def invalidate_by_tags(
        self,
        tags: List[str]
    ) -> Dict[str, int]:
        """
        Invalidate cache entries by tags
        
        Args:
            tags: Tags to match
            
        Returns:
            Dict of cache_name -> count invalidated
        """
        results = {}
        
        # This would need tag support in the cache implementation
        logger.info(f"Invalidating entries with tags: {tags}")
        
        return results
        
    async def cascade_invalidation(
        self,
        cache_name: str,
        key: str,
        related_caches: Dict[str, Callable[[str], List[str]]]
    ) -> Dict[str, int]:
        """
        Cascade invalidation to related caches
        
        Args:
            cache_name: Primary cache
            key: Key to invalidate
            related_caches: Dict of cache_name -> key_mapper function
            
        Returns:
            Dict of cache_name -> count invalidated
        """
        results = {cache_name: 0}
        
        try:
            # Invalidate primary key
            if await self.cache_manager.remove(cache_name, key):
                results[cache_name] = 1
                
            # Cascade to related caches
            for related_cache, key_mapper in related_caches.items():
                related_keys = key_mapper(key)
                
                if related_keys:
                    count = 0
                    for related_key in related_keys:
                        if await self.cache_manager.remove(related_cache, related_key):
                            count += 1
                            
                    results[related_cache] = count
                    
            logger.info(f"Cascade invalidation completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Cascade invalidation failed: {e}")
            raise
            
    async def smart_invalidation(
        self,
        event_type: str,
        event_data: Dict[str, Any]
    ) -> Dict[str, int]:
        """
        Smart invalidation based on events
        
        Args:
            event_type: Type of event
            event_data: Event data
            
        Returns:
            Dict of cache_name -> count invalidated
        """
        results = {}
        
        # Apply invalidation rules based on event
        for cache_name, rules in self._invalidation_rules.items():
            count = 0
            
            for rule in rules:
                try:
                    if rule(event_type, event_data):
                        # Rule triggered, invalidate cache
                        # This is simplified - real implementation would
                        # determine which keys to invalidate
                        count += 1
                        
                except Exception as e:
                    logger.error(f"Invalidation rule failed: {e}")
                    
            if count > 0:
                results[cache_name] = count
                
        return results


# Helper functions
def generate_cache_key(*args, **kwargs) -> str:
    """Generate cache key from arguments"""
    key_parts = [str(arg) for arg in args]
    key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
    
    key_string = ":".join(key_parts)
    
    # Hash if too long
    if len(key_string) > 250:
        key_hash = hashlib.sha256(key_string.encode()).hexdigest()
        return f"hash:{key_hash}"
        
    return key_string


def parse_ttl(ttl_str: str) -> timedelta:
    """Parse TTL string to timedelta"""
    units = {
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days"
    }
    
    # Extract number and unit
    import re
    match = re.match(r"(\d+)([smhd])", ttl_str.lower())
    
    if not match:
        raise ValueError(f"Invalid TTL format: {ttl_str}")
        
    value = int(match.group(1))
    unit = units.get(match.group(2))
    
    if not unit:
        raise ValueError(f"Unknown TTL unit: {match.group(2)}")
        
    return timedelta(**{unit: value}) 