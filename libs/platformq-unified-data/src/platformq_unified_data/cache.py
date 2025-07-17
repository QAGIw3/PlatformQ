"""Multi-level cache management for unified data access"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Callable
from functools import lru_cache
import hashlib

from cachetools import TTLCache
from pyignite import Client as IgniteClient
from prometheus_client import Counter, Histogram, Gauge
import pickle

from .exceptions import CacheError

logger = logging.getLogger(__name__)


# Prometheus metrics
cache_hits = Counter('platformq_cache_hits_total', 'Total cache hits', ['cache_level', 'model'])
cache_misses = Counter('platformq_cache_misses_total', 'Total cache misses', ['cache_level', 'model'])
cache_operations = Histogram('platformq_cache_operation_duration_seconds', 'Cache operation duration', ['operation', 'cache_level'])
cache_size = Gauge('platformq_cache_size_bytes', 'Current cache size in bytes', ['cache_level'])


class CacheKey:
    """Cache key builder for consistent key generation"""
    
    @staticmethod
    def build(model_name: str, operation: str, **params) -> str:
        """Build a cache key from model name, operation, and parameters"""
        # Sort parameters for consistent key generation
        sorted_params = sorted(params.items())
        param_str = json.dumps(sorted_params, sort_keys=True)
        
        # Create hash for long keys
        key_data = f"{model_name}:{operation}:{param_str}"
        if len(key_data) > 250:  # Ignite has key length limits
            key_hash = hashlib.sha256(key_data.encode()).hexdigest()
            return f"{model_name}:{operation}:{key_hash}"
        
        return key_data
    
    @staticmethod
    def build_pattern(model_name: str, operation: str = "*") -> str:
        """Build a key pattern for cache invalidation"""
        return f"{model_name}:{operation}:*"


class CacheLevel:
    """Base class for cache levels"""
    
    def __init__(self, name: str, ttl: int = 3600):
        self.name = name
        self.ttl = ttl
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        raise NotImplementedError
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache"""
        raise NotImplementedError
    
    async def delete(self, key: str):
        """Delete value from cache"""
        raise NotImplementedError
    
    async def clear(self, pattern: Optional[str] = None):
        """Clear cache entries matching pattern"""
        raise NotImplementedError
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in cache"""
        raise NotImplementedError


class L1Cache(CacheLevel):
    """Level 1: In-memory LRU cache (per service instance)"""
    
    def __init__(self, max_size: int = 1000, ttl: int = 300):
        super().__init__("L1", ttl)
        self.cache = TTLCache(maxsize=max_size, ttl=ttl)
        self.lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from L1 cache"""
        async with self.lock:
            value = self.cache.get(key)
            if value is not None:
                cache_hits.labels(cache_level="L1", model=key.split(":")[0]).inc()
                return pickle.loads(value)
            
            cache_misses.labels(cache_level="L1", model=key.split(":")[0]).inc()
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in L1 cache"""
        async with self.lock:
            try:
                serialized = pickle.dumps(value)
                self.cache[key] = serialized
                cache_size.labels(cache_level="L1").set(len(serialized))
            except Exception as e:
                logger.error(f"Failed to set L1 cache: {e}")
    
    async def delete(self, key: str):
        """Delete value from L1 cache"""
        async with self.lock:
            self.cache.pop(key, None)
    
    async def clear(self, pattern: Optional[str] = None):
        """Clear L1 cache entries"""
        async with self.lock:
            if pattern:
                # Remove keys matching pattern
                keys_to_remove = [k for k in self.cache.keys() if self._matches_pattern(k, pattern)]
                for key in keys_to_remove:
                    self.cache.pop(key, None)
            else:
                self.cache.clear()
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in L1 cache"""
        async with self.lock:
            return key in self.cache
    
    def _matches_pattern(self, key: str, pattern: str) -> bool:
        """Check if key matches pattern (simple wildcard support)"""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)


class L2Cache(CacheLevel):
    """Level 2: Distributed Ignite cache (shared across services)"""
    
    def __init__(self, ignite_client: IgniteClient, cache_name: str = "unified_data_cache", ttl: int = 3600):
        super().__init__("L2", ttl)
        self.ignite_client = ignite_client
        self.cache_name = cache_name
        self._init_cache()
    
    def _init_cache(self):
        """Initialize Ignite cache with configuration"""
        try:
            self.cache = self.ignite_client.get_or_create_cache({
                'name': self.cache_name,
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'ATOMIC',
                'backups': 1,
                'expiry_policy': {
                    'access': self.ttl * 1000,  # milliseconds
                    'create': self.ttl * 1000,
                    'update': self.ttl * 1000
                }
            })
        except Exception as e:
            logger.error(f"Failed to initialize Ignite cache: {e}")
            raise CacheError(f"Failed to initialize L2 cache: {e}")
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from L2 cache"""
        with cache_operations.labels(operation="get", cache_level="L2").time():
            try:
                value = self.cache.get(key)
                if value is not None:
                    cache_hits.labels(cache_level="L2", model=key.split(":")[0]).inc()
                    return pickle.loads(value)
                
                cache_misses.labels(cache_level="L2", model=key.split(":")[0]).inc()
                return None
            except Exception as e:
                logger.error(f"Failed to get from L2 cache: {e}")
                return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in L2 cache"""
        with cache_operations.labels(operation="set", cache_level="L2").time():
            try:
                serialized = pickle.dumps(value)
                if ttl:
                    # Ignite doesn't support per-key TTL easily, so we'll add expiry timestamp
                    wrapper = {
                        'data': serialized,
                        'expires_at': datetime.utcnow() + timedelta(seconds=ttl)
                    }
                    self.cache.put(key, pickle.dumps(wrapper))
                else:
                    self.cache.put(key, serialized)
                    
                cache_size.labels(cache_level="L2").set(len(serialized))
            except Exception as e:
                logger.error(f"Failed to set L2 cache: {e}")
    
    async def delete(self, key: str):
        """Delete value from L2 cache"""
        with cache_operations.labels(operation="delete", cache_level="L2").time():
            try:
                self.cache.remove(key)
            except Exception as e:
                logger.error(f"Failed to delete from L2 cache: {e}")
    
    async def clear(self, pattern: Optional[str] = None):
        """Clear L2 cache entries"""
        with cache_operations.labels(operation="clear", cache_level="L2").time():
            try:
                if pattern:
                    # Scan cache for matching keys
                    for key in self.cache.scan():
                        if self._matches_pattern(key[0], pattern):
                            self.cache.remove(key[0])
                else:
                    self.cache.clear()
            except Exception as e:
                logger.error(f"Failed to clear L2 cache: {e}")
    
    async def exists(self, key: str) -> bool:
        """Check if key exists in L2 cache"""
        try:
            return self.cache.contains_key(key)
        except Exception as e:
            logger.error(f"Failed to check L2 cache existence: {e}")
            return False
    
    def _matches_pattern(self, key: str, pattern: str) -> bool:
        """Check if key matches pattern"""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)


class CacheManager:
    """Multi-level cache manager with automatic fallback"""
    
    def __init__(self, 
                 ignite_client: Optional[IgniteClient] = None,
                 l1_max_size: int = 1000,
                 l1_ttl: int = 300,
                 l2_ttl: int = 3600,
                 enable_l1: bool = True,
                 enable_l2: bool = True):
        
        self.levels: List[CacheLevel] = []
        
        # Initialize L1 cache (in-memory)
        if enable_l1:
            self.l1_cache = L1Cache(max_size=l1_max_size, ttl=l1_ttl)
            self.levels.append(self.l1_cache)
        else:
            self.l1_cache = None
        
        # Initialize L2 cache (Ignite)
        if enable_l2 and ignite_client:
            self.l2_cache = L2Cache(ignite_client, ttl=l2_ttl)
            self.levels.append(self.l2_cache)
        else:
            self.l2_cache = None
        
        self._invalidation_callbacks: Dict[str, List[Callable]] = {}
    
    async def get(self, key: str, fetch_func: Optional[Callable] = None) -> Optional[Any]:
        """Get value from cache with automatic fallback and fetch"""
        # Try each cache level
        for level in self.levels:
            value = await level.get(key)
            if value is not None:
                # Populate higher levels
                await self._populate_higher_levels(key, value, level)
                return value
        
        # If not found in any cache and fetch_func provided, fetch and cache
        if fetch_func:
            value = await fetch_func()
            if value is not None:
                await self.set(key, value)
            return value
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in all cache levels"""
        tasks = []
        for level in self.levels:
            tasks.append(level.set(key, value, ttl))
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def delete(self, key: str):
        """Delete value from all cache levels"""
        tasks = []
        for level in self.levels:
            tasks.append(level.delete(key))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Trigger invalidation callbacks
        await self._trigger_invalidation_callbacks(key)
    
    async def invalidate(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        tasks = []
        for level in self.levels:
            tasks.append(level.clear(pattern))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Trigger invalidation callbacks for pattern
        await self._trigger_invalidation_callbacks(pattern)
    
    async def clear_all(self):
        """Clear all cache levels"""
        tasks = []
        for level in self.levels:
            tasks.append(level.clear())
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _populate_higher_levels(self, key: str, value: Any, found_level: CacheLevel):
        """Populate cache levels above where value was found"""
        for level in self.levels:
            if level == found_level:
                break
            await level.set(key, value)
    
    def register_invalidation_callback(self, pattern: str, callback: Callable):
        """Register a callback for cache invalidation events"""
        if pattern not in self._invalidation_callbacks:
            self._invalidation_callbacks[pattern] = []
        self._invalidation_callbacks[pattern].append(callback)
    
    async def _trigger_invalidation_callbacks(self, key_or_pattern: str):
        """Trigger registered invalidation callbacks"""
        for pattern, callbacks in self._invalidation_callbacks.items():
            if self._matches_pattern(key_or_pattern, pattern):
                for callback in callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(key_or_pattern)
                        else:
                            callback(key_or_pattern)
                    except Exception as e:
                        logger.error(f"Error in invalidation callback: {e}")
    
    def _matches_pattern(self, key: str, pattern: str) -> bool:
        """Check if key matches pattern"""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        stats = {
            'levels': []
        }
        
        for level in self.levels:
            level_stats = {
                'name': level.name,
                'ttl': level.ttl
            }
            
            if isinstance(level, L1Cache):
                level_stats['size'] = len(level.cache)
                level_stats['max_size'] = level.cache.maxsize
            
            stats['levels'].append(level_stats)
        
        return stats


# Caching decorator
def cached(ttl: int = 3600, key_prefix: Optional[str] = None):
    """Decorator for caching method results"""
    def decorator(func):
        async def wrapper(self, *args, **kwargs):
            # Build cache key
            if hasattr(self, '__class__'):
                model_name = self.__class__.__name__
            else:
                model_name = "unknown"
            
            cache_key = CacheKey.build(
                model_name,
                key_prefix or func.__name__,
                args=str(args),
                kwargs=str(kwargs)
            )
            
            # Try to get from cache
            if hasattr(self, '_cache_manager'):
                value = await self._cache_manager.get(cache_key)
                if value is not None:
                    return value
            
            # Execute function
            result = await func(self, *args, **kwargs)
            
            # Cache result
            if hasattr(self, '_cache_manager') and result is not None:
                await self._cache_manager.set(cache_key, result, ttl)
            
            return result
        
        return wrapper
    return decorator 