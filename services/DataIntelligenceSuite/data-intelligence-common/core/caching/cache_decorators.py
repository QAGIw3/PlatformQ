"""
Cache decorators for DataIntelligenceSuite

Provides easy-to-use decorators for caching function results.
"""

import asyncio
import functools
import hashlib
import json
from typing import Any, Optional, Callable, Union
from datetime import timedelta

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


def _generate_cache_key(func_name: str, args: tuple, kwargs: dict, prefix: Optional[str] = None) -> str:
    """Generate a cache key from function name and arguments"""
    key_data = {
        'func': func_name,
        'args': args,
        'kwargs': kwargs
    }
    key_str = json.dumps(key_data, sort_keys=True, default=str)
    key_hash = hashlib.md5(key_str.encode()).hexdigest()
    
    if prefix:
        return f"{prefix}:{func_name}:{key_hash}"
    return f"{func_name}:{key_hash}"


def cached(
    cache_name: str,
    ttl: Optional[Union[int, timedelta]] = None,
    key_prefix: Optional[str] = None,
    key_func: Optional[Callable] = None,
    condition: Optional[Callable] = None
):
    """
    Decorator to cache function results.
    
    Args:
        cache_name: Name of the cache to use
        ttl: Time to live (seconds or timedelta)
        key_prefix: Prefix for cache keys
        key_func: Custom function to generate cache key
        condition: Function to determine if result should be cached
        
    Example:
        @cached("query_results", ttl=3600)
        async def get_user_data(user_id: str):
            return await db.fetch_user(user_id)
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            # Check if caching is enabled
            if not hasattr(self, 'cache_manager') or not self.cache_manager:
                return await func(self, *args, **kwargs)
                
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = _generate_cache_key(func.__name__, args, kwargs, key_prefix)
                
            # Try to get from cache
            try:
                cached_value = await self.cache_manager.get(cache_name, cache_key)
                if cached_value is not None:
                    logger.debug(f"Cache hit for {cache_key}")
                    return cached_value
            except Exception as e:
                logger.warning(f"Cache get failed for {cache_key}: {e}")
                
            # Call function
            result = await func(self, *args, **kwargs)
            
            # Cache result if condition is met
            if condition is None or condition(result):
                try:
                    # Convert ttl to timedelta if needed
                    cache_ttl = None
                    if ttl is not None:
                        cache_ttl = ttl if isinstance(ttl, timedelta) else timedelta(seconds=ttl)
                        
                    await self.cache_manager.put(cache_name, cache_key, result, cache_ttl)
                    logger.debug(f"Cached result for {cache_key}")
                except Exception as e:
                    logger.warning(f"Cache put failed for {cache_key}: {e}")
                    
            return result
            
        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            # For sync functions, we need to handle differently
            # This is a simplified version - actual implementation would use sync cache client
            logger.warning(f"Sync caching not fully implemented for {func.__name__}")
            return func(self, *args, **kwargs)
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


def cache_aside(
    cache_name: str,
    loader: Callable,
    ttl: Optional[Union[int, timedelta]] = None,
    key_func: Optional[Callable] = None
):
    """
    Decorator for cache-aside pattern.
    
    The decorated function becomes a cache accessor that:
    1. Checks cache first
    2. Calls loader on cache miss
    3. Stores result in cache
    
    Args:
        cache_name: Name of the cache to use
        loader: Function to load data on cache miss
        ttl: Time to live
        key_func: Custom function to generate cache key
        
    Example:
        @cache_aside("user_cache", loader=fetch_user_from_db, ttl=3600)
        async def get_user(self, user_id: str):
            pass  # The decorator handles everything
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            if not hasattr(self, 'cache_manager') or not self.cache_manager:
                # Fallback to loader if no cache
                if asyncio.iscoroutinefunction(loader):
                    return await loader(self, *args, **kwargs)
                else:
                    return loader(self, *args, **kwargs)
                    
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Use first argument as key by default
                cache_key = str(args[0]) if args else str(kwargs)
                
            # Try cache first
            try:
                cached_value = await self.cache_manager.get(cache_name, cache_key)
                if cached_value is not None:
                    return cached_value
            except Exception as e:
                logger.warning(f"Cache get failed: {e}")
                
            # Load from source
            if asyncio.iscoroutinefunction(loader):
                result = await loader(self, *args, **kwargs)
            else:
                result = loader(self, *args, **kwargs)
                
            # Store in cache
            if result is not None:
                try:
                    cache_ttl = None
                    if ttl is not None:
                        cache_ttl = ttl if isinstance(ttl, timedelta) else timedelta(seconds=ttl)
                        
                    await self.cache_manager.put(cache_name, cache_key, result, cache_ttl)
                except Exception as e:
                    logger.warning(f"Cache put failed: {e}")
                    
            return result
            
        return async_wrapper
        
    return decorator


def cache_invalidate(
    cache_name: str,
    key_func: Optional[Callable] = None,
    pattern: Optional[str] = None
):
    """
    Decorator to invalidate cache entries.
    
    Use this on functions that modify data to ensure cache consistency.
    
    Args:
        cache_name: Name of the cache to invalidate
        key_func: Function to generate keys to invalidate
        pattern: Pattern to match keys for invalidation
        
    Example:
        @cache_invalidate("user_cache", key_func=lambda user_id: f"user:{user_id}")
        async def update_user(self, user_id: str, data: dict):
            return await db.update_user(user_id, data)
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            # Execute the function first
            result = await func(self, *args, **kwargs)
            
            # Invalidate cache
            if hasattr(self, 'cache_manager') and self.cache_manager:
                try:
                    if key_func:
                        # Invalidate specific keys
                        keys_to_invalidate = key_func(*args, **kwargs)
                        if isinstance(keys_to_invalidate, str):
                            keys_to_invalidate = [keys_to_invalidate]
                            
                        for key in keys_to_invalidate:
                            await self.cache_manager.remove(cache_name, key)
                            logger.debug(f"Invalidated cache key: {key}")
                            
                    elif pattern:
                        # Pattern-based invalidation would require cache scanning
                        # This is a simplified version
                        logger.warning("Pattern-based invalidation not fully implemented")
                        
                    else:
                        # Clear entire cache
                        await self.cache_manager.clear(cache_name)
                        logger.debug(f"Cleared entire cache: {cache_name}")
                        
                except Exception as e:
                    logger.error(f"Cache invalidation failed: {e}")
                    
            return result
            
        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            result = func(self, *args, **kwargs)
            logger.warning(f"Sync cache invalidation not implemented for {func.__name__}")
            return result
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


class CacheContext:
    """
    Context manager for batch cache operations.
    
    Example:
        async with CacheContext(cache_manager, "my_cache") as cache:
            await cache.put("key1", "value1")
            await cache.put("key2", "value2")
            # All operations are batched
    """
    
    def __init__(self, cache_manager, cache_name: str):
        self.cache_manager = cache_manager
        self.cache_name = cache_name
        self.batch_puts = {}
        self.batch_removes = []
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Execute batched operations
        if self.batch_puts:
            await self.cache_manager.put_all(self.cache_name, self.batch_puts)
            
        if self.batch_removes:
            # Remove keys one by one (batch remove not implemented)
            for key in self.batch_removes:
                await self.cache_manager.remove(self.cache_name, key)
                
    async def put(self, key: str, value: Any):
        """Add to batch put"""
        self.batch_puts[key] = value
        
    async def remove(self, key: str):
        """Add to batch remove"""
        self.batch_removes.append(key)
        
    async def get(self, key: str) -> Optional[Any]:
        """Get from cache (immediate, not batched)"""
        # Check batch puts first
        if key in self.batch_puts:
            return self.batch_puts[key]
            
        return await self.cache_manager.get(self.cache_name, key) 