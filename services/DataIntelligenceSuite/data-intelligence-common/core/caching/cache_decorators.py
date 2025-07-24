"""
Cache decorators for DataIntelligenceSuite

Provides easy-to-use decorators for caching function results.
This module consolidates all caching decorators from across the codebase.
"""

import asyncio
import functools
import hashlib
import json
import pickle
from typing import Any, Optional, Callable, Union, TypeVar, Dict
from datetime import timedelta, datetime
from collections import OrderedDict
import threading

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


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
    cache_name: Optional[str] = None,
    ttl: Optional[Union[int, timedelta]] = None,
    key_prefix: Optional[str] = None,
    key_func: Optional[Callable] = None,
    condition: Optional[Callable] = None,
    cache_none: bool = False,
    cache_exceptions: bool = False
):
    """
    Decorator to cache function results.
    
    This is the primary caching decorator that consolidates functionality from:
    - core/caching/cache_decorators.py (original)
    - clients/base.py (client caching)
    - utils/helpers.py (memoize)
    
    Args:
        cache_name: Name of the cache to use (defaults to function name)
        ttl: Time to live (seconds or timedelta)
        key_prefix: Prefix for cache keys
        key_func: Custom function to generate cache key
        condition: Function to determine if result should be cached
        cache_none: Whether to cache None results
        cache_exceptions: Whether to cache exceptions
        
    Example:
        @cached("query_results", ttl=3600)
        async def get_user_data(user_id: str):
            return await db.fetch_user(user_id)
            
        @cached(ttl=timedelta(minutes=10), key_func=lambda x, y: f"{x}:{y}")
        def compute_result(x: int, y: int):
            return expensive_computation(x, y)
    """
    def decorator(func):
        # Determine cache name
        actual_cache_name = cache_name or func.__name__
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Handle both instance methods and regular functions
            if args and hasattr(args[0], 'cache_manager'):
                self = args[0]
                cache_manager = self.cache_manager
                func_args = args[1:]
            elif args and hasattr(args[0], 'cache'):
                self = args[0]
                cache_manager = self.cache
                func_args = args[1:]
            else:
                # No cache manager available, just call function
                return await func(*args, **kwargs)
                
            if not cache_manager:
                return await func(*args, **kwargs)
                
            # Generate cache key
            if key_func:
                if args and hasattr(args[0], 'cache_manager'):
                    # Instance method - pass self to key_func
                    cache_key = key_func(self, *func_args, **kwargs)
                else:
                    cache_key = key_func(*args, **kwargs)
            else:
                cache_key = _generate_cache_key(func.__name__, func_args, kwargs, key_prefix)
                
            # Try to get from cache
            try:
                cached_value = await cache_manager.get(actual_cache_name, cache_key)
                if cached_value is not None or (cache_none and cached_value is None):
                    logger.debug(f"Cache hit for {cache_key}")
                    # Handle cached exceptions
                    if cache_exceptions and isinstance(cached_value, dict) and cached_value.get('__exception__'):
                        raise pickle.loads(cached_value['__exception__'])
                    return cached_value
            except Exception as e:
                logger.warning(f"Cache get failed for {cache_key}: {e}")
                
            # Call function
            try:
                result = await func(*args, **kwargs)
                
                # Cache result if condition is met
                if (result is not None or cache_none) and (condition is None or condition(result)):
                    try:
                        # Convert ttl to timedelta if needed
                        cache_ttl = None
                        if ttl is not None:
                            cache_ttl = ttl if isinstance(ttl, timedelta) else timedelta(seconds=ttl)
                            
                        await cache_manager.put(actual_cache_name, cache_key, result, cache_ttl)
                        logger.debug(f"Cached result for {cache_key}")
                    except Exception as e:
                        logger.warning(f"Cache put failed for {cache_key}: {e}")
                        
                return result
                
            except Exception as e:
                if cache_exceptions:
                    # Cache the exception
                    try:
                        exception_data = {'__exception__': pickle.dumps(e)}
                        cache_ttl = ttl if isinstance(ttl, timedelta) else timedelta(seconds=ttl or 60)
                        await cache_manager.put(actual_cache_name, cache_key, exception_data, cache_ttl)
                    except Exception as cache_error:
                        logger.warning(f"Failed to cache exception: {cache_error}")
                raise
                
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Handle both instance methods and regular functions
            if args and hasattr(args[0], 'cache_manager'):
                self = args[0]
                cache_manager = self.cache_manager
                func_args = args[1:]
            else:
                # No cache manager, fall back to simple memoization
                return _memoized_wrapper(func, actual_cache_name, ttl, key_prefix, key_func, 
                                       condition, cache_none)(*args, **kwargs)
                
            if not cache_manager:
                return func(*args, **kwargs)
                
            # For sync functions with async cache manager, we need to handle differently
            logger.warning(f"Sync caching with async cache manager not fully supported for {func.__name__}")
            return func(*args, **kwargs)
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


# Backward compatibility aliases
cache_result = cached


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
    pattern: Optional[str] = None,
    invalidate_before: bool = False
):
    """
    Decorator to invalidate cache entries.
    
    Can be used to invalidate cache before or after function execution.
    
    Args:
        cache_name: Name of the cache to invalidate
        key_func: Function to generate keys to invalidate
        pattern: Pattern for keys to invalidate (not all backends support this)
        invalidate_before: If True, invalidate before function execution
        
    Example:
        @cache_invalidate("user_cache", key_func=lambda user_id: user_id)
        async def update_user(self, user_id: str, data: dict):
            return await db.update_user(user_id, data)
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            async def do_invalidate():
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
                        
            if invalidate_before:
                await do_invalidate()
                
            result = await func(self, *args, **kwargs)
            
            if not invalidate_before:
                await do_invalidate()
                
            return result
            
        @functools.wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            if invalidate_before:
                logger.warning(f"Sync cache invalidation before execution not supported for {func.__name__}")
                
            result = func(self, *args, **kwargs)
            
            if not invalidate_before:
                logger.warning(f"Sync cache invalidation after execution not supported for {func.__name__}")
                
            return result
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


# Backward compatibility alias
invalidate_cache = cache_invalidate


def cache_key_generator(key_func: Callable):
    """
    Decorator to specify a custom cache key generator for a method.
    
    The key function will be stored as an attribute on the method and can be
    used by caching decorators.
    
    Args:
        key_func: Function to generate cache key
        
    Example:
        @cache_key_generator(lambda self, user_id, include_deleted=False: f"{user_id}:{include_deleted}")
        @cached("user_cache")
        async def get_user(self, user_id: str, include_deleted: bool = False):
            ...
    """
    def decorator(func):
        func._cache_key_func = key_func
        return func
    return decorator


def memoize(maxsize: Optional[int] = 128, ttl: Optional[Union[int, timedelta]] = None):
    """
    Simple in-memory memoization decorator.
    
    This provides a lightweight alternative to full caching for functions that
    don't have access to a cache manager.
    
    Args:
        maxsize: Maximum number of cached results (None for unlimited)
        ttl: Time to live for cached results
        
    Example:
        @memoize(maxsize=100, ttl=300)
        def expensive_computation(x, y):
            return x ** y
    """
    def decorator(func):
        # Use OrderedDict for LRU behavior
        cache = OrderedDict()
        cache_lock = threading.Lock()
        
        # Convert ttl to seconds
        ttl_seconds = None
        if ttl is not None:
            ttl_seconds = ttl.total_seconds() if isinstance(ttl, timedelta) else ttl
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key
            key = str(args) + str(sorted(kwargs.items()))
            
            with cache_lock:
                # Check if key exists and is not expired
                if key in cache:
                    value, timestamp = cache[key]
                    if ttl_seconds is None or (asyncio.get_event_loop().time() - timestamp) < ttl_seconds:
                        # Move to end (most recently used)
                        cache.move_to_end(key)
                        return value
                    else:
                        # Expired
                        del cache[key]
                
                # Cache miss - compute value
                result = func(*args, **kwargs)
                
                # Add to cache
                cache[key] = (result, asyncio.get_event_loop().time())
                
                # Enforce maxsize
                if maxsize is not None and len(cache) > maxsize:
                    # Remove oldest item
                    cache.popitem(last=False)
                    
            return result
            
        # Add cache control methods
        wrapper.cache_info = lambda: {"size": len(cache), "maxsize": maxsize}
        wrapper.cache_clear = lambda: cache.clear()
        
        return wrapper
    return decorator


def _memoized_wrapper(func, cache_name: str, ttl, key_prefix, key_func, condition, cache_none):
    """Internal wrapper for sync functions without cache manager"""
    cache = OrderedDict()
    cache_lock = threading.Lock()
    maxsize = 128
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Generate cache key
        if key_func:
            cache_key = key_func(*args, **kwargs)
        else:
            cache_key = _generate_cache_key(func.__name__, args, kwargs, key_prefix)
            
        with cache_lock:
            # Check cache
            if cache_key in cache:
                value, timestamp = cache[cache_key]
                # Simple TTL check (if ttl provided)
                if ttl is None:
                    return value
                ttl_seconds = ttl.total_seconds() if isinstance(ttl, timedelta) else ttl
                if (datetime.now().timestamp() - timestamp) < ttl_seconds:
                    cache.move_to_end(cache_key)
                    return value
                else:
                    del cache[cache_key]
                    
            # Compute result
            result = func(*args, **kwargs)
            
            # Cache if appropriate
            if (result is not None or cache_none) and (condition is None or condition(result)):
                cache[cache_key] = (result, datetime.now().timestamp())
                
                # Enforce maxsize
                if len(cache) > maxsize:
                    cache.popitem(last=False)
                    
        return result
        
    return wrapper


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


# Export all decorators and utilities
__all__ = [
    'cached',
    'cache_result',
    'cache_aside',
    'cache_invalidate',
    'invalidate_cache',
    'cache_key_generator',
    'memoize',
    'CacheContext'
] 