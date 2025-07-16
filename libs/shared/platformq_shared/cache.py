"""
Cache module for PlatformQ shared library

Provides caching functionality using Apache Ignite.
"""

import asyncio
import json
import logging
from typing import Optional, Any, Dict
from pyignite import Client, AioClient
from pyignite.exceptions import CacheError

logger = logging.getLogger(__name__)

# Default Ignite configuration
IGNITE_HOST = "localhost"
IGNITE_PORT = 10800
CACHE_NAME = "platformq_cache"


class IgniteCacheClient:
    """Synchronous Ignite cache client"""
    
    def __init__(self, host: str = IGNITE_HOST, port: int = IGNITE_PORT):
        self.host = host
        self.port = port
        self.client = None
        self.cache = None
        
    def connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client = Client()
            self.client.connect(self.host, self.port)
            self.cache = self.client.get_or_create_cache(CACHE_NAME)
            logger.info(f"Connected to Ignite at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from Ignite"""
        if self.client:
            self.client.close()
            
    def get(self, key: str) -> Optional[str]:
        """Get value from cache"""
        try:
            return self.cache.get(key)
        except CacheError as e:
            logger.error(f"Cache get error: {e}")
            return None
            
    def set(self, key: str, value: str):
        """Set value in cache"""
        try:
            self.cache.put(key, value)
        except CacheError as e:
            logger.error(f"Cache set error: {e}")
            
    def setex(self, key: str, ttl: int, value: str):
        """Set value with TTL (in seconds)"""
        # Note: Ignite doesn't have native TTL per key in Python client
        # You would need to configure cache expiry policy
        self.set(key, value)
        
    def delete(self, key: str):
        """Delete key from cache"""
        try:
            self.cache.remove_key(key)
        except CacheError as e:
            logger.error(f"Cache delete error: {e}")


class AsyncIgniteCacheClient:
    """Asynchronous Ignite cache client"""
    
    def __init__(self, host: str = IGNITE_HOST, port: int = IGNITE_PORT):
        self.host = host
        self.port = port
        self.client = None
        self.cache = None
        
    async def connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client = AioClient()
            await self.client.connect(self.host, self.port)
            self.cache = await self.client.get_or_create_cache(CACHE_NAME)
            logger.info(f"Connected to Ignite async at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from Ignite"""
        if self.client:
            await self.client.close()
            
    async def get(self, key: str) -> Optional[str]:
        """Get value from cache"""
        try:
            return await self.cache.get(key)
        except CacheError as e:
            logger.error(f"Cache get error: {e}")
            return None
            
    async def set(self, key: str, value: str):
        """Set value in cache"""
        try:
            await self.cache.put(key, value)
        except CacheError as e:
            logger.error(f"Cache set error: {e}")
            
    async def setex(self, key: str, ttl: int, value: str):
        """Set value with TTL (in seconds)"""
        # Note: Ignite doesn't have native TTL per key in Python client
        # You would need to configure cache expiry policy
        await self.set(key, value)
        
    async def delete(self, key: str):
        """Delete key from cache"""
        try:
            await self.cache.remove_key(key)
        except CacheError as e:
            logger.error(f"Cache delete error: {e}")


# Fallback Redis-compatible client for development
class RedisCacheClient:
    """Redis-compatible cache client (fallback for development)"""
    
    def __init__(self):
        self._cache = {}
        
    async def get(self, key: str) -> Optional[str]:
        return self._cache.get(key)
        
    async def set(self, key: str, value: str):
        self._cache[key] = value
        
    async def setex(self, key: str, ttl: int, value: str):
        # Simplified - doesn't actually expire
        self._cache[key] = value
        
    async def delete(self, key: str):
        self._cache.pop(key, None)


# Global cache client instance
_cache_client = None


async def get_cache_client() -> Any:
    """Get or create cache client instance"""
    global _cache_client
    
    if _cache_client is None:
        try:
            # Try Ignite first
            _cache_client = AsyncIgniteCacheClient()
            await _cache_client.connect()
        except Exception as e:
            logger.warning(f"Failed to connect to Ignite, using in-memory cache: {e}")
            # Fallback to in-memory cache
            _cache_client = RedisCacheClient()
            
    return _cache_client


def get_sync_cache_client() -> IgniteCacheClient:
    """Get synchronous cache client"""
    client = IgniteCacheClient()
    client.connect()
    return client 