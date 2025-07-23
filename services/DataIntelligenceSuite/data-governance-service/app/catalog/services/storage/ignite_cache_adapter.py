"""
Apache Ignite Cache Adapter

Provides caching functionality using Apache Ignite instead of Redis.
"""

import logging
from typing import Any, Optional, Dict
import json
import pickle
from datetime import datetime, timedelta

from pyignite import AsyncClient
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PROP_EXPIRY_POLICY

from ..interfaces import CacheAdapter
from ...core.config import settings

logger = logging.getLogger(__name__)


class IgniteCacheAdapter(CacheAdapter):
    """
    Apache Ignite implementation of CacheAdapter
    
    Provides distributed caching with support for TTL and various data types.
    """
    
    def __init__(
        self,
        nodes: Optional[list] = None,
        cache_prefix: str = "catalog",
        default_ttl: int = 3600
    ):
        self.nodes = nodes or [f"{settings.IGNITE_HOST}:{settings.IGNITE_PORT}"]
        self.cache_prefix = cache_prefix
        self.default_ttl = default_ttl
        self.client = None
        self._caches = {}
        
    async def initialize(self) -> None:
        """Initialize Ignite connection"""
        try:
            self.client = AsyncClient()
            await self.client.connect(self.nodes)
            logger.info(f"Connected to Ignite cluster: {self.nodes}")
            
            # Create default caches
            await self._create_default_caches()
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def _create_default_caches(self):
        """Create default caches with appropriate configurations"""
        cache_configs = {
            f"{self.cache_prefix}_embeddings": {
                "cache_mode": CacheMode.PARTITIONED,
                "backups": 1,
                "atomicity_mode": "ATOMIC"
            },
            f"{self.cache_prefix}_search": {
                "cache_mode": CacheMode.REPLICATED,
                "atomicity_mode": "ATOMIC"
            },
            f"{self.cache_prefix}_analytics": {
                "cache_mode": CacheMode.PARTITIONED,
                "backups": 0,
                "atomicity_mode": "ATOMIC"
            }
        }
        
        for cache_name, config in cache_configs.items():
            cache = await self.client.get_or_create_cache({
                "name": cache_name,
                **config
            })
            self._caches[cache_name] = cache
            
    def _get_cache_name(self, key: str) -> str:
        """Determine which cache to use based on key pattern"""
        if "embedding" in key:
            return f"{self.cache_prefix}_embeddings"
        elif "search" in key or "query" in key:
            return f"{self.cache_prefix}_search"
        else:
            return f"{self.cache_prefix}_analytics"
            
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if not self.client:
            return None
            
        try:
            cache_name = self._get_cache_name(key)
            cache = self._caches.get(cache_name)
            if not cache:
                cache = await self.client.get_or_create_cache(cache_name)
                self._caches[cache_name] = cache
                
            # Get value
            value = await cache.get(key)
            
            if value is None:
                return None
                
            # Check if it's a structured value with metadata
            if isinstance(value, dict) and "_ttl_expires" in value:
                # Check TTL
                if datetime.utcnow() > datetime.fromisoformat(value["_ttl_expires"]):
                    await cache.remove(key)
                    return None
                return value.get("data")
                
            return value
            
        except Exception as e:
            logger.warning(f"Cache get failed for key {key}: {e}")
            return None
            
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """Set value in cache with optional TTL"""
        if not self.client:
            return False
            
        try:
            cache_name = self._get_cache_name(key)
            cache = self._caches.get(cache_name)
            if not cache:
                cache = await self.client.get_or_create_cache(cache_name)
                self._caches[cache_name] = cache
                
            # Handle TTL by wrapping value
            if ttl:
                expires = datetime.utcnow() + timedelta(seconds=ttl)
                wrapped_value = {
                    "data": value,
                    "_ttl_expires": expires.isoformat()
                }
                await cache.put(key, wrapped_value)
            else:
                await cache.put(key, value)
                
            return True
            
        except Exception as e:
            logger.warning(f"Cache set failed for key {key}: {e}")
            return False
            
    async def delete(self, key: str) -> bool:
        """Delete value from cache"""
        if not self.client:
            return False
            
        try:
            cache_name = self._get_cache_name(key)
            cache = self._caches.get(cache_name)
            if not cache:
                return True  # Key doesn't exist
                
            return await cache.remove(key)
            
        except Exception as e:
            logger.warning(f"Cache delete failed for key {key}: {e}")
            return False
            
    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        if not self.client:
            return False
            
        try:
            cache_name = self._get_cache_name(key)
            cache = self._caches.get(cache_name)
            if not cache:
                cache = await self.client.get_or_create_cache(cache_name)
                self._caches[cache_name] = cache
                
            return await cache.contains_key(key)
            
        except Exception as e:
            logger.warning(f"Cache exists check failed for key {key}: {e}")
            return False
            
    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern"""
        if not self.client:
            return 0
            
        cleared = 0
        try:
            # Ignite doesn't have native pattern matching like Redis
            # Would need to implement using scan query
            for cache_name, cache in self._caches.items():
                # This is a simplified implementation
                # In production, use Ignite SQL or Scan queries
                async with cache.scan() as cursor:
                    async for key, _ in cursor:
                        if pattern in str(key):
                            await cache.remove(key)
                            cleared += 1
                            
            return cleared
            
        except Exception as e:
            logger.error(f"Failed to clear pattern {pattern}: {e}")
            return 0
            
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        stats = {}
        
        if not self.client:
            return stats
            
        try:
            for cache_name, cache in self._caches.items():
                metrics = await cache.get_metrics()
                stats[cache_name] = {
                    "size": metrics.size,
                    "hits": metrics.cache_hits,
                    "misses": metrics.cache_misses,
                    "hit_rate": metrics.cache_hit_percentage
                }
                
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {}
            
    async def close(self) -> None:
        """Close Ignite connection"""
        if self.client:
            try:
                await self.client.close()
                logger.info("Closed Ignite connection")
            except Exception as e:
                logger.error(f"Error closing Ignite connection: {e}")
                
    async def health_check(self) -> bool:
        """Check if Ignite is healthy"""
        if not self.client:
            return False
            
        try:
            # Try to access a cache
            cache = await self.client.get_or_create_cache(f"{self.cache_prefix}_health")
            await cache.put("health_check", "ok")
            result = await cache.get("health_check")
            await cache.remove("health_check")
            return result == "ok"
        except:
            return False 