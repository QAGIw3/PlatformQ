"""
Apache Ignite Client for Distributed Caching
Provides distributed caching capabilities for DataIntelligence services
"""

import asyncio
import logging
import json
from typing import Any, Dict, Optional, List, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import pyignite
from pyignite import Client, AioClient
from pyignite.datatypes import String, IntObject, BoolObject, DoubleObject
from pyignite.datatypes.cache_config import CacheMode
from pyignite.cache import Cache
import hashlib
from functools import wraps
import pickle

logger = logging.getLogger(__name__)


class CacheMode(Enum):
    """Cache modes for different use cases"""
    REPLICATED = "REPLICATED"
    PARTITIONED = "PARTITIONED"
    LOCAL = "LOCAL"


class ExpiryPolicy(Enum):
    """Expiry policies for cache entries"""
    ACCESS = "ACCESS"  # Expire after last access
    WRITE = "WRITE"   # Expire after write
    CREATE = "CREATE"  # Expire after creation


@dataclass
class CacheConfig:
    """Configuration for a cache"""
    name: str
    mode: CacheMode = CacheMode.PARTITIONED
    backups: int = 1
    atomicity_mode: str = "ATOMIC"
    expiry_policy: Optional[ExpiryPolicy] = None
    expiry_duration: Optional[timedelta] = None
    sql_schema: Optional[str] = None
    query_entities: Optional[List[Dict]] = None
    cache_group: Optional[str] = None
    data_region: Optional[str] = None
    write_through_enabled: bool = False
    read_through_enabled: bool = False
    statistics_enabled: bool = True


class IgniteClient:
    """Apache Ignite client for distributed caching"""
    
    def __init__(self, nodes: List[Tuple[str, int]], 
                 use_ssl: bool = False,
                 ssl_keyfile: Optional[str] = None,
                 ssl_certfile: Optional[str] = None,
                 ssl_ca_certfile: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 timeout: int = 10,
                 partition_aware: bool = True):
        """
        Initialize Ignite client
        
        Args:
            nodes: List of (host, port) tuples for Ignite nodes
            use_ssl: Whether to use SSL
            ssl_keyfile: Path to SSL key file
            ssl_certfile: Path to SSL certificate file
            ssl_ca_certfile: Path to CA certificate file
            username: Username for authentication
            password: Password for authentication
            timeout: Connection timeout in seconds
            partition_aware: Enable partition awareness for better performance
        """
        self.nodes = nodes
        self.use_ssl = use_ssl
        self.ssl_keyfile = ssl_keyfile
        self.ssl_certfile = ssl_certfile
        self.ssl_ca_certfile = ssl_ca_certfile
        self.username = username
        self.password = password
        self.timeout = timeout
        self.partition_aware = partition_aware
        
        self._client: Optional[Client] = None
        self._async_client: Optional[AioClient] = None
        self._caches: Dict[str, Cache] = {}
        self._cache_configs: Dict[str, CacheConfig] = {}
        
    def connect(self) -> None:
        """Connect to Ignite cluster"""
        try:
            self._client = Client(
                use_ssl=self.use_ssl,
                ssl_keyfile=self.ssl_keyfile,
                ssl_certfile=self.ssl_certfile,
                ssl_ca_certfile=self.ssl_ca_certfile,
                username=self.username,
                password=self.password,
                timeout=self.timeout,
                partition_aware=self.partition_aware
            )
            self._client.connect(self.nodes)
            logger.info(f"Connected to Ignite cluster at {self.nodes}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def connect_async(self) -> None:
        """Connect to Ignite cluster asynchronously"""
        try:
            self._async_client = AioClient(
                use_ssl=self.use_ssl,
                ssl_keyfile=self.ssl_keyfile,
                ssl_certfile=self.ssl_certfile,
                ssl_ca_certfile=self.ssl_ca_certfile,
                username=self.username,
                password=self.password,
                timeout=self.timeout,
                partition_aware=self.partition_aware
            )
            await self._async_client.connect(self.nodes)
            logger.info(f"Async connected to Ignite cluster at {self.nodes}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    def disconnect(self) -> None:
        """Disconnect from Ignite cluster"""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Disconnected from Ignite cluster")
            
    async def disconnect_async(self) -> None:
        """Disconnect from Ignite cluster asynchronously"""
        if self._async_client:
            await self._async_client.close()
            self._async_client = None
            logger.info("Async disconnected from Ignite cluster")
            
    def create_cache(self, config: CacheConfig) -> Cache:
        """Create a new cache with given configuration"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        cache_cfg = {
            'cache_mode': config.mode.value,
            'backups': config.backups,
            'atomicity_mode': config.atomicity_mode,
            'statistics_enabled': config.statistics_enabled
        }
        
        if config.sql_schema:
            cache_cfg['sql_schema'] = config.sql_schema
            
        if config.query_entities:
            cache_cfg['query_entities'] = config.query_entities
            
        if config.cache_group:
            cache_cfg['group_name'] = config.cache_group
            
        if config.data_region:
            cache_cfg['data_region_name'] = config.data_region
            
        if config.write_through_enabled:
            cache_cfg['write_through_enabled'] = True
            
        if config.read_through_enabled:
            cache_cfg['read_through_enabled'] = True
            
        cache = self._client.create_cache(config.name)
        cache.with_config(**cache_cfg)
        
        self._caches[config.name] = cache
        self._cache_configs[config.name] = config
        
        logger.info(f"Created cache '{config.name}' with mode {config.mode.value}")
        return cache
        
    def get_or_create_cache(self, config: CacheConfig) -> Cache:
        """Get existing cache or create new one"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        if config.name in self._caches:
            return self._caches[config.name]
            
        cache = self._client.get_or_create_cache(config.name)
        self._caches[config.name] = cache
        self._cache_configs[config.name] = config
        
        return cache
        
    def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache"""
        cache = self._get_cache(cache_name)
        return cache.get(key)
        
    async def get_async(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.get(key)
        
    def put(self, cache_name: str, key: str, value: Any, 
            expiry_time: Optional[timedelta] = None) -> None:
        """Put value into cache"""
        cache = self._get_cache(cache_name)
        
        if expiry_time:
            # Ignite expects expiry in milliseconds
            expiry_ms = int(expiry_time.total_seconds() * 1000)
            cache.put(key, value, expiry_policy=expiry_ms)
        else:
            cache.put(key, value)
            
    async def put_async(self, cache_name: str, key: str, value: Any,
                       expiry_time: Optional[timedelta] = None) -> None:
        """Put value into cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        
        if expiry_time:
            expiry_ms = int(expiry_time.total_seconds() * 1000)
            await cache.put(key, value, expiry_policy=expiry_ms)
        else:
            await cache.put(key, value)
            
    def get_all(self, cache_name: str, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache"""
        cache = self._get_cache(cache_name)
        return cache.get_all(keys)
        
    async def get_all_async(self, cache_name: str, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.get_all(keys)
        
    def put_all(self, cache_name: str, entries: Dict[str, Any]) -> None:
        """Put multiple values into cache"""
        cache = self._get_cache(cache_name)
        cache.put_all(entries)
        
    async def put_all_async(self, cache_name: str, entries: Dict[str, Any]) -> None:
        """Put multiple values into cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        await cache.put_all(entries)
        
    def remove(self, cache_name: str, key: str) -> bool:
        """Remove value from cache"""
        cache = self._get_cache(cache_name)
        return cache.remove_key(key)
        
    async def remove_async(self, cache_name: str, key: str) -> bool:
        """Remove value from cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.remove_key(key)
        
    def remove_all(self, cache_name: str, keys: List[str]) -> None:
        """Remove multiple values from cache"""
        cache = self._get_cache(cache_name)
        cache.remove_keys(keys)
        
    async def remove_all_async(self, cache_name: str, keys: List[str]) -> None:
        """Remove multiple values from cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        await cache.remove_keys(keys)
        
    def clear(self, cache_name: str) -> None:
        """Clear all entries from cache"""
        cache = self._get_cache(cache_name)
        cache.clear()
        
    async def clear_async(self, cache_name: str) -> None:
        """Clear all entries from cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        await cache.clear()
        
    def get_size(self, cache_name: str) -> int:
        """Get number of entries in cache"""
        cache = self._get_cache(cache_name)
        return cache.get_size()
        
    async def get_size_async(self, cache_name: str) -> int:
        """Get number of entries in cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.get_size()
        
    def contains_key(self, cache_name: str, key: str) -> bool:
        """Check if cache contains key"""
        cache = self._get_cache(cache_name)
        return cache.contains_key(key)
        
    async def contains_key_async(self, cache_name: str, key: str) -> bool:
        """Check if cache contains key asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.contains_key(key)
        
    def sql_query(self, cache_name: str, query: str, 
                  args: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query on cache"""
        cache = self._get_cache(cache_name)
        
        if args:
            result = cache.query_sql(query, query_args=args)
        else:
            result = cache.query_sql(query)
            
        return [dict(row) for row in result]
        
    async def sql_query_async(self, cache_name: str, query: str,
                             args: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query on cache asynchronously"""
        cache = await self._get_cache_async(cache_name)
        
        if args:
            result = await cache.query_sql(query, query_args=args)
        else:
            result = await cache.query_sql(query)
            
        return [dict(row) async for row in result]
        
    def scan(self, cache_name: str, partition: Optional[int] = None) -> List[Tuple[Any, Any]]:
        """Scan cache entries"""
        cache = self._get_cache(cache_name)
        
        if partition is not None:
            result = cache.scan(partition)
        else:
            result = cache.scan()
            
        return list(result)
        
    async def scan_async(self, cache_name: str, 
                        partition: Optional[int] = None) -> List[Tuple[Any, Any]]:
        """Scan cache entries asynchronously"""
        cache = await self._get_cache_async(cache_name)
        
        if partition is not None:
            result = await cache.scan(partition)
        else:
            result = await cache.scan()
            
        return [item async for item in result]
        
    def get_and_put(self, cache_name: str, key: str, value: Any) -> Optional[Any]:
        """Get and put atomically"""
        cache = self._get_cache(cache_name)
        return cache.get_and_put(key, value)
        
    async def get_and_put_async(self, cache_name: str, key: str, value: Any) -> Optional[Any]:
        """Get and put atomically asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.get_and_put(key, value)
        
    def put_if_absent(self, cache_name: str, key: str, value: Any) -> bool:
        """Put value if key doesn't exist"""
        cache = self._get_cache(cache_name)
        return cache.put_if_absent(key, value)
        
    async def put_if_absent_async(self, cache_name: str, key: str, value: Any) -> bool:
        """Put value if key doesn't exist asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.put_if_absent(key, value)
        
    def replace(self, cache_name: str, key: str, value: Any) -> bool:
        """Replace existing value"""
        cache = self._get_cache(cache_name)
        return cache.replace(key, value)
        
    async def replace_async(self, cache_name: str, key: str, value: Any) -> bool:
        """Replace existing value asynchronously"""
        cache = await self._get_cache_async(cache_name)
        return await cache.replace(key, value)
        
    def _get_cache(self, cache_name: str) -> Cache:
        """Get cache by name"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        if cache_name not in self._caches:
            cache = self._client.get_cache(cache_name)
            if cache is None:
                raise ValueError(f"Cache '{cache_name}' not found")
            self._caches[cache_name] = cache
            
        return self._caches[cache_name]
        
    async def _get_cache_async(self, cache_name: str) -> Cache:
        """Get cache by name asynchronously"""
        if not self._async_client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        return await self._async_client.get_cache(cache_name)
        
    def destroy_cache(self, cache_name: str) -> None:
        """Destroy cache"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        cache = self._get_cache(cache_name)
        cache.destroy()
        
        if cache_name in self._caches:
            del self._caches[cache_name]
        if cache_name in self._cache_configs:
            del self._cache_configs[cache_name]
            
        logger.info(f"Destroyed cache '{cache_name}'")
        
    async def destroy_cache_async(self, cache_name: str) -> None:
        """Destroy cache asynchronously"""
        if not self._async_client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        cache = await self._get_cache_async(cache_name)
        await cache.destroy()
        
        logger.info(f"Destroyed cache '{cache_name}'")
        
    def get_cache_metrics(self, cache_name: str) -> Dict[str, Any]:
        """Get cache metrics"""
        cache = self._get_cache(cache_name)
        
        metrics = {
            "size": cache.get_size(),
            "name": cache_name,
            "mode": self._cache_configs.get(cache_name, CacheConfig(cache_name)).mode.value
        }
        
        return metrics
        
    def list_caches(self) -> List[str]:
        """List all cache names"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite cluster")
            
        return self._client.get_cache_names()


def cache_key(*args, **kwargs) -> str:
    """Generate cache key from arguments"""
    key_data = {
        'args': args,
        'kwargs': kwargs
    }
    key_str = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.md5(key_str.encode()).hexdigest()


def cached(cache_name: str, ttl: Optional[timedelta] = None, 
          key_prefix: Optional[str] = None):
    """
    Decorator for caching function results
    
    Args:
        cache_name: Name of the cache to use
        ttl: Time to live for cached values
        key_prefix: Prefix for cache keys
    """
    def decorator(func):
        @wraps(func)
        async def async_wrapper(self, *args, **kwargs):
            # Generate cache key
            func_key = f"{key_prefix or ''}{func.__name__}:{cache_key(*args, **kwargs)}"
            
            # Try to get from cache
            if hasattr(self, '_ignite_client') and self._ignite_client:
                try:
                    cached_value = await self._ignite_client.get_async(cache_name, func_key)
                    if cached_value is not None:
                        logger.debug(f"Cache hit for {func_key}")
                        return pickle.loads(cached_value)
                except Exception as e:
                    logger.warning(f"Cache get failed: {e}")
                    
            # Call function
            result = await func(self, *args, **kwargs)
            
            # Store in cache
            if hasattr(self, '_ignite_client') and self._ignite_client:
                try:
                    await self._ignite_client.put_async(
                        cache_name, func_key, pickle.dumps(result), ttl
                    )
                    logger.debug(f"Cached result for {func_key}")
                except Exception as e:
                    logger.warning(f"Cache put failed: {e}")
                    
            return result
            
        @wraps(func)
        def sync_wrapper(self, *args, **kwargs):
            # Generate cache key
            func_key = f"{key_prefix or ''}{func.__name__}:{cache_key(*args, **kwargs)}"
            
            # Try to get from cache
            if hasattr(self, '_ignite_client') and self._ignite_client:
                try:
                    cached_value = self._ignite_client.get(cache_name, func_key)
                    if cached_value is not None:
                        logger.debug(f"Cache hit for {func_key}")
                        return pickle.loads(cached_value)
                except Exception as e:
                    logger.warning(f"Cache get failed: {e}")
                    
            # Call function
            result = func(self, *args, **kwargs)
            
            # Store in cache
            if hasattr(self, '_ignite_client') and self._ignite_client:
                try:
                    self._ignite_client.put(
                        cache_name, func_key, pickle.dumps(result), ttl
                    )
                    logger.debug(f"Cached result for {func_key}")
                except Exception as e:
                    logger.warning(f"Cache put failed: {e}")
                    
            return result
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


class IgniteCacheManager:
    """Manager for multiple Ignite caches with different purposes"""
    
    def __init__(self, ignite_client: IgniteClient):
        self.client = ignite_client
        self._initialized_caches: Set[str] = set()
        
    def initialize_standard_caches(self):
        """Initialize standard caches for DataIntelligence services"""
        
        # Session cache for user sessions
        self.client.get_or_create_cache(CacheConfig(
            name="user_sessions",
            mode=CacheMode.REPLICATED,
            expiry_policy=ExpiryPolicy.ACCESS,
            expiry_duration=timedelta(hours=24)
        ))
        
        # ML model metadata cache
        self.client.get_or_create_cache(CacheConfig(
            name="ml_model_metadata",
            mode=CacheMode.REPLICATED,
            backups=2
        ))
        
        # Feature store cache
        self.client.get_or_create_cache(CacheConfig(
            name="feature_store",
            mode=CacheMode.PARTITIONED,
            backups=1,
            sql_schema="PUBLIC"
        ))
        
        # Query results cache
        self.client.get_or_create_cache(CacheConfig(
            name="query_results",
            mode=CacheMode.PARTITIONED,
            expiry_policy=ExpiryPolicy.CREATE,
            expiry_duration=timedelta(hours=1)
        ))
        
        # Pipeline state cache
        self.client.get_or_create_cache(CacheConfig(
            name="pipeline_state",
            mode=CacheMode.REPLICATED,
            backups=2
        ))
        
        # Data quality metrics cache
        self.client.get_or_create_cache(CacheConfig(
            name="data_quality_metrics",
            mode=CacheMode.PARTITIONED,
            expiry_policy=ExpiryPolicy.WRITE,
            expiry_duration=timedelta(hours=6)
        ))
        
        # Service discovery cache
        self.client.get_or_create_cache(CacheConfig(
            name="service_discovery",
            mode=CacheMode.REPLICATED,
            expiry_policy=ExpiryPolicy.WRITE,
            expiry_duration=timedelta(minutes=5)
        ))
        
        # Configuration cache
        self.client.get_or_create_cache(CacheConfig(
            name="configuration",
            mode=CacheMode.REPLICATED,
            backups=2
        ))
        
        logger.info("Initialized standard DataIntelligence caches") 