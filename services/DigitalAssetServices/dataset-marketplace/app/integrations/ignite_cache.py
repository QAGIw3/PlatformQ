"""
Apache Ignite Cache Client
"""

import asyncio
from typing import Any, Dict, Optional, List
import logging
import json
from pyignite import AsyncClient
from pyignite.cache import Cache

logger = logging.getLogger(__name__)


class IgniteCache:
    """Apache Ignite distributed cache client"""
    
    def __init__(self, nodes: Optional[List[str]] = None):
        self.nodes = nodes or ['ignite-node1:10800', 'ignite-node2:10800']
        self.client: Optional[AsyncClient] = None
        self._caches: Dict[str, Cache] = {}
    
    async def connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client = AsyncClient()
            await self.client.connect(self.nodes)
            logger.info("Connected to Ignite cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    async def get_cache(self, cache_name: str) -> Cache:
        """Get or create a cache"""
        if cache_name not in self._caches:
            cache = await self.client.get_or_create_cache(cache_name)
            self._caches[cache_name] = cache
        return self._caches[cache_name]
    
    async def put(self, cache_name: str, key: str, value: Any, ttl: Optional[int] = None):
        """Put value in cache with optional TTL (seconds)"""
        try:
            cache = await self.get_cache(cache_name)
            if ttl:
                await cache.put(key, json.dumps(value), ttl=ttl)
            else:
                await cache.put(key, json.dumps(value))
        except Exception as e:
            logger.error(f"Failed to put value in cache: {e}")
            raise
    
    async def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            cache = await self.get_cache(cache_name)
            value = await cache.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Failed to get value from cache: {e}")
            return None
    
    async def put_all(self, cache_name: str, entries: Dict[str, Any]):
        """Put multiple entries in cache"""
        try:
            cache = await self.get_cache(cache_name)
            serialized_entries = {k: json.dumps(v) for k, v in entries.items()}
            await cache.put_all(serialized_entries)
        except Exception as e:
            logger.error(f"Failed to put multiple values in cache: {e}")
            raise
    
    async def get_all(self, cache_name: str, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache"""
        try:
            cache = await self.get_cache(cache_name)
            values = await cache.get_all(keys)
            return {k: json.loads(v) if v else None for k, v in values.items()}
        except Exception as e:
            logger.error(f"Failed to get multiple values from cache: {e}")
            return {}
    
    async def remove(self, cache_name: str, key: str):
        """Remove value from cache"""
        try:
            cache = await self.get_cache(cache_name)
            await cache.remove(key)
        except Exception as e:
            logger.error(f"Failed to remove value from cache: {e}")
    
    async def clear(self, cache_name: str):
        """Clear all entries from cache"""
        try:
            cache = await self.get_cache(cache_name)
            await cache.clear()
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
    
    async def query(self, cache_name: str, sql_query: str) -> List[Dict[str, Any]]:
        """Execute SQL query on cache"""
        try:
            cache = await self.get_cache(cache_name)
            result = cache.query(sql_query)
            rows = []
            async for row in result:
                rows.append(dict(row))
            return rows
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            return []
    
    async def increment(self, cache_name: str, key: str, delta: int = 1) -> int:
        """Atomic increment operation"""
        try:
            cache = await self.get_cache(cache_name)
            current = await cache.get(key)
            if current:
                new_value = json.loads(current) + delta
            else:
                new_value = delta
            await cache.put(key, json.dumps(new_value))
            return new_value
        except Exception as e:
            logger.error(f"Failed to increment value: {e}")
            raise
    
    async def close(self):
        """Close connection to Ignite"""
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Ignite cluster") 