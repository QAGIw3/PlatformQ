"""
Apache Ignite Cache Implementation for Compliance Service
Provides distributed caching and compute grid capabilities
"""

import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
import asyncio
from dataclasses import asdict, is_dataclass
import json

from pyignite import Client as IgniteClient
from pyignite.datatypes import String, IntObject, BoolObject
from pyignite.cache import Cache

logger = logging.getLogger(__name__)


class IgniteCache:
    """
    Apache Ignite cache wrapper with async support and distributed compute
    """
    
    def __init__(self, ignite_client: IgniteClient, cache_name: str):
        self.client = ignite_client
        self.cache_name = cache_name
        self._cache: Optional[Cache] = None
        
    async def initialize(self):
        """Initialize the cache"""
        self._cache = await asyncio.get_event_loop().run_in_executor(
            None, self.client.get_or_create_cache, self.cache_name
        )
        logger.info(f"Initialized Ignite cache: {self.cache_name}")
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if not self._cache:
            await self.initialize()
            
        try:
            value = await asyncio.get_event_loop().run_in_executor(
                None, self._cache.get, key
            )
            
            # Deserialize if needed
            if value and isinstance(value, str):
                try:
                    return json.loads(value)
                except:
                    return value
                    
            return value
            
        except Exception as e:
            logger.error(f"Error getting key {key} from cache: {e}")
            return None
            
    async def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None):
        """Put value in cache with optional TTL"""
        if not self._cache:
            await self.initialize()
            
        try:
            # Serialize dataclasses and complex objects
            if is_dataclass(value):
                value = asdict(value)
            elif hasattr(value, '__dict__'):
                value = value.__dict__
                
            # Convert to JSON for complex objects
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
                
            # Put with TTL if specified
            if ttl_seconds:
                # Ignite doesn't have direct TTL support in Python client
                # We'll store expiry time with the value
                wrapped_value = {
                    "value": value,
                    "expires_at": (datetime.utcnow() + timedelta(seconds=ttl_seconds)).isoformat()
                }
                await asyncio.get_event_loop().run_in_executor(
                    None, self._cache.put, key, json.dumps(wrapped_value)
                )
            else:
                await asyncio.get_event_loop().run_in_executor(
                    None, self._cache.put, key, value
                )
                
        except Exception as e:
            logger.error(f"Error putting key {key} in cache: {e}")
            
    async def delete(self, key: str):
        """Delete key from cache"""
        if not self._cache:
            await self.initialize()
            
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self._cache.remove, key
            )
        except Exception as e:
            logger.error(f"Error deleting key {key} from cache: {e}")
            
    async def get_all(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache"""
        if not self._cache:
            await self.initialize()
            
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._cache.get_all, keys
            )
            
            # Deserialize values
            deserialized = {}
            for k, v in result.items():
                if v and isinstance(v, str):
                    try:
                        deserialized[k] = json.loads(v)
                    except:
                        deserialized[k] = v
                else:
                    deserialized[k] = v
                    
            return deserialized
            
        except Exception as e:
            logger.error(f"Error getting multiple keys from cache: {e}")
            return {}
            
    async def put_all(self, entries: Dict[str, Any]):
        """Put multiple values in cache"""
        if not self._cache:
            await self.initialize()
            
        try:
            # Serialize values
            serialized = {}
            for k, v in entries.items():
                if is_dataclass(v):
                    v = asdict(v)
                elif hasattr(v, '__dict__'):
                    v = v.__dict__
                    
                if isinstance(v, (dict, list)):
                    serialized[k] = json.dumps(v)
                else:
                    serialized[k] = v
                    
            await asyncio.get_event_loop().run_in_executor(
                None, self._cache.put_all, serialized
            )
            
        except Exception as e:
            logger.error(f"Error putting multiple entries in cache: {e}")
            
    async def clear(self):
        """Clear all entries from cache"""
        if not self._cache:
            await self.initialize()
            
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self._cache.clear
            )
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            
    async def size(self) -> int:
        """Get cache size"""
        if not self._cache:
            await self.initialize()
            
        try:
            return await asyncio.get_event_loop().run_in_executor(
                None, self._cache.get_size
            )
        except Exception as e:
            logger.error(f"Error getting cache size: {e}")
            return 0
            
    async def scan(self, filter_func=None) -> List[tuple]:
        """Scan cache entries with optional filter"""
        if not self._cache:
            await self.initialize()
            
        try:
            entries = []
            cursor = await asyncio.get_event_loop().run_in_executor(
                None, self._cache.scan
            )
            
            for key, value in cursor:
                # Deserialize value
                if value and isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except:
                        pass
                        
                # Apply filter if provided
                if filter_func is None or filter_func(key, value):
                    entries.append((key, value))
                    
            return entries
            
        except Exception as e:
            logger.error(f"Error scanning cache: {e}")
            return []


class IgniteComputeGrid:
    """
    Apache Ignite compute grid for distributed processing
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.client = ignite_client
        
    async def execute_task(self, task_name: str, args: Dict[str, Any]) -> Any:
        """Execute a distributed compute task"""
        try:
            # Ignite compute tasks would be registered separately
            # This is a placeholder for the compute API
            compute = self.client.get_compute()
            result = await asyncio.get_event_loop().run_in_executor(
                None, compute.call, task_name, args
            )
            return result
            
        except Exception as e:
            logger.error(f"Error executing compute task {task_name}: {e}")
            raise
            
    async def map_reduce(
        self,
        map_func: str,
        reduce_func: str,
        data: List[Any]
    ) -> Any:
        """Execute map-reduce operation"""
        try:
            # Split data for distributed processing
            chunk_size = max(1, len(data) // 10)  # 10 chunks
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
            
            # Map phase
            map_tasks = []
            for chunk in chunks:
                task = self.execute_task(map_func, {"data": chunk})
                map_tasks.append(task)
                
            map_results = await asyncio.gather(*map_tasks)
            
            # Reduce phase
            reduce_result = await self.execute_task(
                reduce_func,
                {"results": map_results}
            )
            
            return reduce_result
            
        except Exception as e:
            logger.error(f"Error in map-reduce operation: {e}")
            raise
            
    async def broadcast(self, task_name: str, args: Dict[str, Any]) -> List[Any]:
        """Broadcast task to all nodes"""
        try:
            # Get cluster nodes
            nodes = self.client.get_cluster().get_nodes()
            
            # Execute on all nodes
            tasks = []
            for node in nodes:
                task = self.execute_task(task_name, args)
                tasks.append(task)
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out exceptions
            return [r for r in results if not isinstance(r, Exception)]
            
        except Exception as e:
            logger.error(f"Error broadcasting task: {e}")
            raise


class IgniteCacheManager:
    """
    Manager for multiple Ignite caches
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.client = ignite_client
        self.caches: Dict[str, IgniteCache] = {}
        self.compute_grid = IgniteComputeGrid(ignite_client)
        
    async def get_cache(self, cache_name: str) -> IgniteCache:
        """Get or create a cache"""
        if cache_name not in self.caches:
            cache = IgniteCache(self.client, cache_name)
            await cache.initialize()
            self.caches[cache_name] = cache
            
        return self.caches[cache_name]
        
    async def get_or_create_cache(self, cache_name: str) -> IgniteCache:
        """Alias for get_cache"""
        return await self.get_cache(cache_name)
        
    def get_compute_grid(self) -> IgniteComputeGrid:
        """Get compute grid instance"""
        return self.compute_grid
        
    async def clear_all_caches(self):
        """Clear all managed caches"""
        for cache in self.caches.values():
            await cache.clear()
            
    async def get_cache_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all caches"""
        stats = {}
        
        for name, cache in self.caches.items():
            size = await cache.size()
            stats[name] = {
                "size": size,
                "name": name
            }
            
        return stats 