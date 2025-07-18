"""
Ignite Cache Manager

Provides intelligent caching using Apache Ignite with TTL, eviction policies,
and cache warming strategies.
"""

import asyncio
import logging
import hashlib
import json
from typing import Any, Dict, Optional, List, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
import xxhash

from pyignite import AsyncClient as IgniteClient
from pyignite.datatypes import String, IntObject, BoolObject, TimestampObject
from pyignite.datatypes.cache_config import CacheMode, CacheAtomicityMode

logger = logging.getLogger(__name__)


@dataclass
class CacheConfig:
    """Configuration for a cache"""
    name: str
    ttl_seconds: int = 300  # 5 minutes default
    max_size: int = 10000
    eviction_policy: str = "LRU"  # LRU, FIFO, RANDOM
    cache_mode: str = "PARTITIONED"  # PARTITIONED, REPLICATED, LOCAL
    backups: int = 1
    write_through: bool = False
    read_through: bool = False


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    created_at: datetime
    accessed_at: datetime
    access_count: int = 0
    ttl_seconds: int = 300
    tenant_id: Optional[str] = None
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired"""
        return datetime.utcnow() > self.created_at + timedelta(seconds=self.ttl_seconds)


class IgniteCacheManager:
    """Manages caching with Apache Ignite"""
    
    def __init__(self, ignite_host: str, ignite_port: int, cache_ttl: int = 300):
        self.ignite_host = ignite_host
        self.ignite_port = ignite_port
        self.default_ttl = cache_ttl
        self.client: Optional[IgniteClient] = None
        self.caches: Dict[str, Any] = {}
        self.cache_configs: Dict[str, CacheConfig] = {}
        self._stats: Dict[str, Dict[str, int]] = {}
        self._warmup_tasks: List[asyncio.Task] = []
        
    async def connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client = IgniteClient()
            await self.client.connect(self.ignite_host, self.ignite_port)
            
            # Initialize default caches
            await self._initialize_default_caches()
            
            logger.info(f"Connected to Ignite cache at {self.ignite_host}:{self.ignite_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def _initialize_default_caches(self):
        """Initialize default cache configurations"""
        default_caches = [
            CacheConfig(
                name="query_results",
                ttl_seconds=300,
                max_size=50000,
                eviction_policy="LRU",
                cache_mode="PARTITIONED",
                backups=1
            ),
            CacheConfig(
                name="metadata",
                ttl_seconds=3600,
                max_size=10000,
                eviction_policy="LRU",
                cache_mode="REPLICATED"
            ),
            CacheConfig(
                name="tenant_data",
                ttl_seconds=600,
                max_size=100000,
                eviction_policy="LRU",
                cache_mode="PARTITIONED",
                backups=2
            ),
            CacheConfig(
                name="aggregations",
                ttl_seconds=60,
                max_size=5000,
                eviction_policy="FIFO",
                cache_mode="PARTITIONED"
            )
        ]
        
        for config in default_caches:
            await self.create_cache(config)
            
    async def create_cache(self, config: CacheConfig):
        """Create a new cache with configuration"""
        try:
            cache_config = {
                'cache_mode': getattr(CacheMode, config.cache_mode, CacheMode.PARTITIONED),
                'atomicity_mode': CacheAtomicityMode.ATOMIC,
                'backups': config.backups,
                'name': config.name
            }
            
            cache = await self.client.get_or_create_cache(cache_config)
            self.caches[config.name] = cache
            self.cache_configs[config.name] = config
            self._stats[config.name] = {
                'hits': 0,
                'misses': 0,
                'evictions': 0,
                'writes': 0
            }
            
            logger.info(f"Created cache '{config.name}' with config: {config}")
            
        except Exception as e:
            logger.error(f"Failed to create cache '{config.name}': {e}")
            raise
            
    def _generate_cache_key(self, key_parts: Union[str, List[Any]], tenant_id: Optional[str] = None) -> str:
        """Generate a cache key from parts"""
        if isinstance(key_parts, str):
            base_key = key_parts
        else:
            # Convert all parts to strings and join
            base_key = ":".join(str(part) for part in key_parts)
            
        # Add tenant prefix if provided
        if tenant_id:
            base_key = f"{tenant_id}:{base_key}"
            
        # Use xxhash for fast hashing
        return xxhash.xxh64(base_key.encode()).hexdigest()
        
    async def get(self, cache_name: str, key: Union[str, List[Any]], 
                  tenant_id: Optional[str] = None) -> Optional[Any]:
        """Get value from cache"""
        if cache_name not in self.caches:
            logger.warning(f"Cache '{cache_name}' not found")
            return None
            
        cache_key = self._generate_cache_key(key, tenant_id)
        
        try:
            # Get from Ignite
            value = await self.caches[cache_name].get(cache_key)
            
            if value is not None:
                # Check if it's our CacheEntry format
                if isinstance(value, dict) and 'value' in value and 'ttl_seconds' in value:
                    entry = CacheEntry(**value)
                    
                    # Check expiration
                    if entry.is_expired():
                        await self.delete(cache_name, key, tenant_id)
                        self._stats[cache_name]['misses'] += 1
                        return None
                        
                    # Update access metadata
                    entry.accessed_at = datetime.utcnow()
                    entry.access_count += 1
                    await self.caches[cache_name].put(cache_key, entry.__dict__)
                    
                    self._stats[cache_name]['hits'] += 1
                    return entry.value
                else:
                    # Legacy format, return as-is
                    self._stats[cache_name]['hits'] += 1
                    return value
            else:
                self._stats[cache_name]['misses'] += 1
                return None
                
        except Exception as e:
            logger.error(f"Error getting key '{key}' from cache '{cache_name}': {e}")
            return None
            
    async def put(self, cache_name: str, key: Union[str, List[Any]], value: Any,
                  ttl_seconds: Optional[int] = None, tenant_id: Optional[str] = None):
        """Put value into cache"""
        if cache_name not in self.caches:
            logger.warning(f"Cache '{cache_name}' not found")
            return
            
        cache_key = self._generate_cache_key(key, tenant_id)
        ttl = ttl_seconds or self.cache_configs[cache_name].ttl_seconds
        
        try:
            # Create cache entry
            entry = CacheEntry(
                key=cache_key,
                value=value,
                created_at=datetime.utcnow(),
                accessed_at=datetime.utcnow(),
                ttl_seconds=ttl,
                tenant_id=tenant_id
            )
            
            # Store in Ignite
            await self.caches[cache_name].put(cache_key, entry.__dict__)
            self._stats[cache_name]['writes'] += 1
            
        except Exception as e:
            logger.error(f"Error putting key '{key}' into cache '{cache_name}': {e}")
            
    async def delete(self, cache_name: str, key: Union[str, List[Any]], 
                     tenant_id: Optional[str] = None):
        """Delete value from cache"""
        if cache_name not in self.caches:
            return
            
        cache_key = self._generate_cache_key(key, tenant_id)
        
        try:
            await self.caches[cache_name].remove(cache_key)
        except Exception as e:
            logger.error(f"Error deleting key '{key}' from cache '{cache_name}': {e}")
            
    async def invalidate_pattern(self, cache_name: str, pattern: str, 
                                tenant_id: Optional[str] = None):
        """Invalidate all keys matching a pattern"""
        if cache_name not in self.caches:
            return
            
        try:
            # Use Ignite SQL query to find matching keys
            query = f"SELECT _key FROM {cache_name}.CACHE WHERE _key LIKE ?"
            params = [f"{tenant_id}:{pattern}%" if tenant_id else f"{pattern}%"]
            
            cursor = self.caches[cache_name].query(query, params)
            
            # Delete all matching keys
            async for row in cursor:
                await self.caches[cache_name].remove(row[0])
                
        except Exception as e:
            logger.error(f"Error invalidating pattern '{pattern}' in cache '{cache_name}': {e}")
            
    async def clear_cache(self, cache_name: str, tenant_id: Optional[str] = None):
        """Clear all entries from a cache"""
        if cache_name not in self.caches:
            return
            
        try:
            if tenant_id:
                # Clear only tenant-specific entries
                await self.invalidate_pattern(cache_name, "", tenant_id)
            else:
                # Clear entire cache
                await self.caches[cache_name].clear()
                
            logger.info(f"Cleared cache '{cache_name}'" + 
                       (f" for tenant '{tenant_id}'" if tenant_id else ""))
                       
        except Exception as e:
            logger.error(f"Error clearing cache '{cache_name}': {e}")
            
    async def get_or_compute(self, cache_name: str, key: Union[str, List[Any]], 
                            compute_func: Callable, ttl_seconds: Optional[int] = None,
                            tenant_id: Optional[str] = None) -> Any:
        """Get from cache or compute if missing"""
        # Try to get from cache
        value = await self.get(cache_name, key, tenant_id)
        
        if value is not None:
            return value
            
        # Compute value
        if asyncio.iscoroutinefunction(compute_func):
            value = await compute_func()
        else:
            value = compute_func()
            
        # Store in cache
        await self.put(cache_name, key, value, ttl_seconds, tenant_id)
        
        return value
        
    async def warm_cache(self, cache_name: str, data_loader: Callable,
                        key_extractor: Callable[[Any], Union[str, List[Any]]]):
        """Warm cache with preloaded data"""
        try:
            logger.info(f"Warming cache '{cache_name}'...")
            
            # Load data
            if asyncio.iscoroutinefunction(data_loader):
                data = await data_loader()
            else:
                data = data_loader()
                
            # Store in cache
            count = 0
            for item in data:
                key = key_extractor(item)
                await self.put(cache_name, key, item)
                count += 1
                
            logger.info(f"Warmed cache '{cache_name}' with {count} entries")
            
        except Exception as e:
            logger.error(f"Error warming cache '{cache_name}': {e}")
            
    def get_stats(self, cache_name: Optional[str] = None) -> Dict[str, Any]:
        """Get cache statistics"""
        if cache_name:
            return self._stats.get(cache_name, {})
        return self._stats
        
    async def cleanup_expired_entries(self):
        """Background task to cleanup expired entries"""
        while True:
            try:
                for cache_name, cache in self.caches.items():
                    # Run cleanup for each cache
                    await self._cleanup_cache(cache_name)
                    
                # Sleep for 60 seconds between cleanups
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(60)
                
    async def _cleanup_cache(self, cache_name: str):
        """Cleanup expired entries from a specific cache"""
        try:
            evicted_count = 0
            
            # Query all entries
            query = f"SELECT _key, _val FROM {cache_name}.CACHE"
            cursor = self.caches[cache_name].query(query)
            
            async for row in cursor:
                key, value = row
                
                if isinstance(value, dict) and 'ttl_seconds' in value:
                    entry = CacheEntry(**value)
                    
                    if entry.is_expired():
                        await self.caches[cache_name].remove(key)
                        evicted_count += 1
                        
            if evicted_count > 0:
                self._stats[cache_name]['evictions'] += evicted_count
                logger.info(f"Evicted {evicted_count} expired entries from cache '{cache_name}'")
                
        except Exception as e:
            logger.error(f"Error cleaning up cache '{cache_name}': {e}")
            
    async def disconnect(self):
        """Disconnect from Ignite"""
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Ignite cache") 