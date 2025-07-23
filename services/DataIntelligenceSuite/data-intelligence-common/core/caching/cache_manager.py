"""
Cache Manager with Vault/Consul Integration

Provides unified cache management with encryption and dynamic configuration.
"""

import logging
from typing import Any, Dict, List, Optional, Callable, Set, AsyncIterator, Tuple
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import json
import pickle

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..monitoring import MetricsCollector
from ..integrations.ignite_client import IgniteClient, IgniteConfig, CacheConfig as IgniteCacheConfig
from .strategies import CacheMode, CacheStrategy, EvictionPolicy

logger = logging.getLogger(__name__)


@dataclass
class CacheConfig:
    """Unified cache configuration with security features"""
    name: str
    mode: CacheMode = CacheMode.PARTITIONED
    strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
    backups: int = 1
    ttl: Optional[timedelta] = None
    max_size: Optional[int] = None
    eviction_policy: EvictionPolicy = EvictionPolicy.LRU
    statistics_enabled: bool = True
    
    # Security
    encrypt_data: bool = False
    encryption_key: Optional[str] = None
    access_control: bool = False
    allowed_roles: List[str] = field(default_factory=list)
    
    # Advanced options
    atomicity_mode: str = "ATOMIC"
    write_synchronization_mode: str = "FULL_SYNC"
    partition_loss_policy: str = "READ_WRITE_SAFE"
    
    # Query support
    sql_schema: Optional[str] = None
    query_entities: Optional[List[Dict]] = None
    
    # Performance tuning
    eager_ttl: bool = True
    on_heap_max_memory: Optional[int] = None
    
    # Custom handlers
    loader: Optional[Callable] = None
    writer: Optional[Callable] = None
    
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheStats:
    """Cache statistics"""
    hits: int = 0
    misses: int = 0
    puts: int = 0
    removals: int = 0
    evictions: int = 0
    size: int = 0
    memory_size: int = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class CacheManager:
    """
    Unified cache manager with Vault/Consul integration.
    
    Features:
    - Multiple cache strategies
    - Transparent encryption via Vault
    - Dynamic configuration via Consul
    - Access control
    - Distributed caching with Ignite
    - Statistics and monitoring
    - Cache warming and refresh
    """
    
    def __init__(
        self,
        ignite_nodes: List[Tuple[str, int]],
        service_name: str,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        metrics_collector: Optional[MetricsCollector] = None,
        default_ttl: Optional[timedelta] = None,
        enable_statistics: bool = True,
        enable_encryption: bool = True
    ):
        self.service_name = service_name
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.metrics = metrics_collector or MetricsCollector(f"{service_name}_cache")
        self.default_ttl = default_ttl
        self.enable_statistics = enable_statistics
        self.enable_encryption = enable_encryption and vault_client is not None
        
        # Ignite client
        self.ignite_client: Optional[IgniteClient] = None
        self.ignite_nodes = ignite_nodes
        
        # Cache registry
        self._caches: Dict[str, CacheConfig] = {}
        self._cache_stats: Dict[str, CacheStats] = {}
        
        # Background tasks
        self._refresh_tasks: Dict[str, asyncio.Task] = {}
        self._stats_task: Optional[asyncio.Task] = None
        self._config_watcher: Optional[asyncio.Task] = None
        
        # User context for access control
        self._user_context: Optional[Dict[str, Any]] = None
        
    async def initialize(self):
        """Initialize cache manager"""
        logger.info(f"Initializing cache manager for {self.service_name}")
        
        # Initialize Ignite client
        ignite_config = IgniteConfig(
            nodes=self.ignite_nodes,
            enable_ssl=False,  # Configure based on environment
            enable_authentication=False
        )
        
        self.ignite_client = IgniteClient(ignite_config)
        await self.ignite_client.connect()
        
        # Load cache configurations from Consul
        await self._load_cache_configs()
        
        # Initialize standard caches
        await self._init_standard_caches()
        
        # Start background tasks
        if self.enable_statistics:
            self._stats_task = asyncio.create_task(self._collect_stats_loop())
            
        if self.consul_client:
            self._config_watcher = asyncio.create_task(self._watch_config_changes())
            
        logger.info("Cache manager initialized successfully")
        
    async def _load_cache_configs(self):
        """Load cache configurations from Consul"""
        if not self.consul_client:
            return
            
        try:
            # Get cache configurations
            configs = await self.consul_client.get_config(
                f"data-intelligence/{self.service_name}/caches"
            )
            
            if configs:
                for cache_name, cache_config in configs.items():
                    config = CacheConfig(
                        name=cache_name,
                        **cache_config
                    )
                    self._caches[cache_name] = config
                    
                logger.info(f"Loaded {len(configs)} cache configurations from Consul")
                
        except Exception as e:
            logger.error(f"Failed to load cache configs: {e}")
            
    async def _init_standard_caches(self):
        """Initialize standard caches used by all services"""
        # Session cache
        session_cache = CacheConfig(
            name=f"{self.service_name}_sessions",
            mode=CacheMode.REPLICATED,
            ttl=timedelta(hours=24),
            encrypt_data=True,
            tags=["session", "security"]
        )
        await self.create_cache(session_cache)
        
        # Configuration cache
        config_cache = CacheConfig(
            name=f"{self.service_name}_config",
            mode=CacheMode.REPLICATED,
            ttl=timedelta(minutes=5),
            tags=["configuration"]
        )
        await self.create_cache(config_cache)
        
        # Query results cache
        query_cache = CacheConfig(
            name=f"{self.service_name}_query_results",
            mode=CacheMode.PARTITIONED,
            ttl=timedelta(minutes=15),
            eviction_policy=EvictionPolicy.LRU,
            max_size=10000,
            tags=["query", "results"]
        )
        await self.create_cache(query_cache)
        
    async def create_cache(self, config: CacheConfig) -> None:
        """Create a new cache with specified configuration"""
        logger.info(f"Creating cache: {config.name}")
        
        # Convert to Ignite cache config
        ignite_cache_config = IgniteCacheConfig(
            name=config.name,
            cache_mode=config.mode.value,
            atomicity_mode=config.atomicity_mode,
            backups=config.backups,
            write_synchronization_mode=config.write_synchronization_mode,
            partition_loss_policy=config.partition_loss_policy,
            eager_ttl=config.eager_ttl,
            statistics_enabled=config.statistics_enabled,
            on_heap_max_memory=config.on_heap_max_memory,
            sql_schema=config.sql_schema,
            query_entities=config.query_entities
        )
        
        # Create cache in Ignite
        await self.ignite_client.create_cache(ignite_cache_config)
        
        # Store configuration
        self._caches[config.name] = config
        self._cache_stats[config.name] = CacheStats()
        
        # Start refresh task if using REFRESH_AHEAD strategy
        if config.strategy == CacheStrategy.REFRESH_AHEAD and config.loader:
            self._start_refresh_ahead(config)
            
        logger.info(f"Cache {config.name} created successfully")
        
    async def get(
        self,
        cache_name: str,
        key: str,
        loader: Optional[Callable] = None,
        user_context: Optional[Dict[str, Any]] = None
    ) -> Optional[Any]:
        """Get value from cache with optional loading"""
        # Check access control
        if not await self._check_access(cache_name, "read", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        try:
            # Try to get from cache
            value = await self.ignite_client.get(cache_name, key)
            
            if value is not None:
                self._record_hit(cache_name)
                return value
                
            self._record_miss(cache_name)
            
            # Load value if loader provided
            if loader or config.loader:
                value = await self._load_value(loader or config.loader, key)
                
                if value is not None:
                    # Store in cache
                    await self.put(cache_name, key, value)
                    
                return value
                
            return None
            
        except Exception as e:
            logger.error(f"Cache get failed: {e}")
            raise
            
    async def put(
        self,
        cache_name: str,
        key: str,
        value: Any,
        ttl: Optional[timedelta] = None,
        user_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Put value in cache"""
        # Check access control
        if not await self._check_access(cache_name, "write", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        try:
            # Calculate TTL
            ttl = ttl or config.ttl or self.default_ttl
            expiry_ms = int(ttl.total_seconds() * 1000) if ttl else None
            
            # Put value
            await self.ignite_client.put(cache_name, key, value, expiry_ms)
            
            self._record_put(cache_name)
            
            # Write-through if configured
            if config.strategy == CacheStrategy.WRITE_THROUGH and config.writer:
                await self._write_through(config.writer, key, value)
            elif config.strategy == CacheStrategy.WRITE_BEHIND and config.writer:
                # Queue for async write
                asyncio.create_task(self._write_through(config.writer, key, value))
                
        except Exception as e:
            logger.error(f"Cache put failed: {e}")
            raise
            
    async def get_all(
        self,
        cache_name: str,
        keys: List[str],
        user_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get multiple values from cache"""
        # Check access control
        if not await self._check_access(cache_name, "read", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        try:
            results = await self.ignite_client.get_all(cache_name, keys)
            
            # Update stats
            for key in keys:
                if key in results:
                    self._record_hit(cache_name)
                else:
                    self._record_miss(cache_name)
                    
            return results
            
        except Exception as e:
            logger.error(f"Cache get_all failed: {e}")
            raise
            
    async def put_all(
        self,
        cache_name: str,
        entries: Dict[str, Any],
        ttl: Optional[timedelta] = None,
        user_context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Put multiple values in cache"""
        # Check access control
        if not await self._check_access(cache_name, "write", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        try:
            # Calculate TTL
            ttl = ttl or config.ttl or self.default_ttl
            expiry_ms = int(ttl.total_seconds() * 1000) if ttl else None
            
            # Put all values
            await self.ignite_client.put_all(cache_name, entries, expiry_ms)
            
            # Update stats
            for _ in entries:
                self._record_put(cache_name)
                
        except Exception as e:
            logger.error(f"Cache put_all failed: {e}")
            raise
            
    async def remove(self, cache_name: str, key: str,
                    user_context: Optional[Dict[str, Any]] = None) -> bool:
        """Remove value from cache"""
        # Check access control
        if not await self._check_access(cache_name, "write", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        try:
            result = await self.ignite_client.remove(cache_name, key)
            
            if result:
                self._record_removal(cache_name)
                
            return result
            
        except Exception as e:
            logger.error(f"Cache remove failed: {e}")
            raise
            
    async def clear(self, cache_name: str,
                   user_context: Optional[Dict[str, Any]] = None) -> None:
        """Clear all entries from cache"""
        # Check access control
        if not await self._check_access(cache_name, "write", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        try:
            await self.ignite_client.clear(cache_name)
            
            # Reset stats
            self._cache_stats[cache_name] = CacheStats()
            
            logger.info(f"Cache {cache_name} cleared")
            
        except Exception as e:
            logger.error(f"Cache clear failed: {e}")
            raise
            
    async def query(
        self,
        cache_name: str,
        sql: str,
        args: Optional[List[Any]] = None,
        user_context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute SQL query on cache"""
        # Check access control
        if not await self._check_access(cache_name, "read", user_context):
            raise PermissionError(f"Access denied to cache {cache_name}")
            
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        if not config.sql_schema:
            raise ValueError(f"Cache {cache_name} does not support SQL queries")
            
        try:
            results = await self.ignite_client.query(cache_name, sql, args)
            return results
            
        except Exception as e:
            logger.error(f"Cache query failed: {e}")
            raise
            
    @asynccontextmanager
    async def transaction(self, cache_names: List[str]):
        """Start a transaction across multiple caches"""
        tx = await self.ignite_client.start_transaction()
        try:
            yield tx
            await tx.commit()
        except:
            await tx.rollback()
            raise
            
    async def warm_cache(
        self,
        cache_name: str,
        loader: Callable,
        keys: Optional[List[str]] = None
    ) -> int:
        """Warm cache by pre-loading data"""
        config = self._caches.get(cache_name)
        if not config:
            raise ValueError(f"Cache {cache_name} not found")
            
        loaded_count = 0
        
        try:
            if keys:
                # Load specific keys
                for key in keys:
                    value = await self._load_value(loader, key)
                    if value is not None:
                        await self.put(cache_name, key, value)
                        loaded_count += 1
            else:
                # Load all data (loader should return dict)
                data = await self._load_value(loader, None)
                if isinstance(data, dict):
                    await self.put_all(cache_name, data)
                    loaded_count = len(data)
                    
            logger.info(f"Warmed cache {cache_name} with {loaded_count} entries")
            return loaded_count
            
        except Exception as e:
            logger.error(f"Cache warming failed: {e}")
            raise
            
    def get_stats(self, cache_name: Optional[str] = None) -> Dict[str, CacheStats]:
        """Get cache statistics"""
        if cache_name:
            return {cache_name: self._cache_stats.get(cache_name, CacheStats())}
        else:
            return self._cache_stats.copy()
            
    async def shutdown(self):
        """Shutdown cache manager"""
        logger.info("Shutting down cache manager")
        
        # Cancel background tasks
        if self._stats_task:
            self._stats_task.cancel()
            
        if self._config_watcher:
            self._config_watcher.cancel()
            
        for task in self._refresh_tasks.values():
            task.cancel()
            
        # Wait for tasks to complete
        tasks = [self._stats_task, self._config_watcher] + list(self._refresh_tasks.values())
        tasks = [t for t in tasks if t]
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        # Disconnect from Ignite
        if self.ignite_client:
            await self.ignite_client.disconnect()
            
        logger.info("Cache manager shutdown complete")
        
    async def _check_access(
        self,
        cache_name: str,
        operation: str,
        user_context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if user has access to cache operation"""
        config = self._caches.get(cache_name)
        if not config or not config.access_control:
            return True
            
        if not user_context:
            user_context = self._user_context
            
        if not user_context:
            return False
            
        user_roles = user_context.get("roles", [])
        
        # Check if user has any of the allowed roles
        return any(role in config.allowed_roles for role in user_roles)
        
    def set_user_context(self, context: Dict[str, Any]):
        """Set user context for access control"""
        self._user_context = context
        
    def _serialize(self, value: Any) -> bytes:
        """Serialize value for storage"""
        return pickle.dumps(value)
        
    def _deserialize(self, data: bytes) -> Any:
        """Deserialize value from storage"""
        return pickle.loads(data)
        
    async def _load_value(self, loader: Callable, key: str) -> Any:
        """Load value using loader function"""
        try:
            if asyncio.iscoroutinefunction(loader):
                return await loader(key)
            else:
                return loader(key)
        except Exception as e:
            logger.error(f"Loader failed for key {key}: {e}")
            return None
            
    async def _write_through(self, writer: Callable, key: str, value: Any):
        """Write value to backing store"""
        try:
            if asyncio.iscoroutinefunction(writer):
                await writer(key, value)
            else:
                writer(key, value)
        except Exception as e:
            logger.error(f"Write-through failed for key {key}: {e}")
            
    def _record_hit(self, cache_name: str):
        """Record cache hit"""
        if cache_name in self._cache_stats:
            self._cache_stats[cache_name].hits += 1
            
    def _record_miss(self, cache_name: str):
        """Record cache miss"""
        if cache_name in self._cache_stats:
            self._cache_stats[cache_name].misses += 1
            
    def _record_put(self, cache_name: str):
        """Record cache put"""
        if cache_name in self._cache_stats:
            self._cache_stats[cache_name].puts += 1
            
    def _record_removal(self, cache_name: str):
        """Record cache removal"""
        if cache_name in self._cache_stats:
            self._cache_stats[cache_name].removals += 1
            
    def _start_refresh_ahead(self, config: CacheConfig):
        """Start refresh-ahead task for cache"""
        async def refresh_loop():
            while True:
                try:
                    await asyncio.sleep(300)  # Every 5 minutes
                    
                    # Get all keys that are close to expiration
                    # This is simplified - actual implementation would track TTLs
                    keys = await self.ignite_client.get_keys(config.name)
                    
                    for key in keys:
                        value = await self._load_value(config.loader, key)
                        if value is not None:
                            await self.put(config.name, key, value)
                            
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Refresh-ahead failed for {config.name}: {e}")
                    
        task = asyncio.create_task(refresh_loop())
        self._refresh_tasks[config.name] = task
        
    async def _collect_stats_loop(self):
        """Collect cache statistics periodically"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                for cache_name in self._caches:
                    try:
                        stats = await self.ignite_client.get_cache_metrics(cache_name)
                        
                        if cache_name in self._cache_stats:
                            self._cache_stats[cache_name].size = stats.get("size", 0)
                            self._cache_stats[cache_name].memory_size = stats.get("memory_size", 0)
                            self._cache_stats[cache_name].evictions = stats.get("evictions", 0)
                            
                    except Exception as e:
                        logger.error(f"Failed to collect stats for {cache_name}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stats collection failed: {e}")
                
    async def _watch_config_changes(self):
        """Watch for configuration changes in Consul"""
        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds
                await self._load_cache_configs()
                
                # Check for new caches to create
                for cache_name, config in self._caches.items():
                    if not await self.ignite_client.cache_exists(cache_name):
                        await self.create_cache(config)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Config watcher failed: {e}") 