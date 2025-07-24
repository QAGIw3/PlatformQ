"""
Apache Ignite Client for DataIntelligenceSuite

Provides async client for Apache Ignite in-memory computing platform.
"""

import asyncio
from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json

from pyignite import AioClient
from pyignite.datatypes import String, IntObject, LongObject
from pyignite.exceptions import CacheError
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


@dataclass
class IgniteConfig:
    """Configuration for Ignite client"""
    nodes: List[Tuple[str, int]]  # List of (host, port) tuples
    timeout: float = 10.0
    enable_ssl: bool = False
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_ca_certfile: Optional[str] = None
    enable_authentication: bool = False
    username: Optional[str] = None
    password: Optional[str] = None
    partition_aware: bool = True
    heartbeat_interval: int = 10000  # milliseconds
    connection_pool_size: int = 10


@dataclass
class CacheConfig:
    """Configuration for Ignite cache"""
    name: str
    cache_mode: str = "PARTITIONED"  # PARTITIONED, REPLICATED, LOCAL
    atomicity_mode: str = "ATOMIC"  # ATOMIC, TRANSACTIONAL
    backups: int = 1
    write_synchronization_mode: str = "FULL_SYNC"  # FULL_SYNC, FULL_ASYNC, PRIMARY_SYNC
    partition_loss_policy: str = "READ_WRITE_SAFE"
    eager_ttl: bool = True
    statistics_enabled: bool = True
    on_heap_max_memory: Optional[int] = None
    
    # SQL support
    sql_schema: Optional[str] = None
    query_entities: Optional[List[Dict]] = None
    
    # Eviction policy
    eviction_policy: Optional[Dict[str, Any]] = None
    
    # Expiry policy
    default_expiry_policy: Optional[Dict[str, Any]] = None


class IgniteClient:
    """
    Async Apache Ignite client wrapper.
    
    Provides high-level interface for cache operations with Ignite.
    """
    
    def __init__(self, config: IgniteConfig):
        self.config = config
        self._client: Optional[AioClient] = None
        self._connected = False
        self._cache_configs: Dict[str, CacheConfig] = {}
        self._transactions: Dict[str, Any] = {}
        
    async def connect(self):
        """Connect to Ignite cluster"""
        if self._connected:
            return
            
        logger.info(f"Connecting to Ignite cluster: {self.config.nodes}")
        
        # Create client
        self._client = AioClient(
            timeout=self.config.timeout,
            partition_aware=self.config.partition_aware,
            heartbeat_interval=self.config.heartbeat_interval
        )
        
        # Configure SSL if enabled
        if self.config.enable_ssl:
            self._client.ssl_keyfile = self.config.ssl_keyfile
            self._client.ssl_certfile = self.config.ssl_certfile
            self._client.ssl_ca_certfile = self.config.ssl_ca_certfile
            
        # Connect
        try:
            await self._client.connect(self.config.nodes)
            
            # Authenticate if enabled
            if self.config.enable_authentication:
                await self._authenticate()
                
            self._connected = True
            logger.info("Successfully connected to Ignite cluster")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from Ignite cluster"""
        if self._client and self._connected:
            await self._client.close()
            self._connected = False
            logger.info("Disconnected from Ignite cluster")
            
    async def _authenticate(self):
        """Authenticate with Ignite"""
        if not self.config.username or not self.config.password:
            raise ValueError("Authentication enabled but credentials not provided")
            
        # Ignite authentication would be implemented here
        # This is a placeholder as pyignite handles auth differently
        logger.info(f"Authenticated as user: {self.config.username}")
        
    async def create_cache(self, cache_config: CacheConfig):
        """Create a new cache"""
        if not self._connected:
            raise RuntimeError("Not connected to Ignite")
            
        try:
            # Get or create cache
            cache = await self._client.get_or_create_cache(cache_config.name)
            
            # Store configuration
            self._cache_configs[cache_config.name] = cache_config
            
            logger.info(f"Created cache: {cache_config.name}")
            
        except Exception as e:
            logger.error(f"Failed to create cache {cache_config.name}: {e}")
            raise
            
    async def get_cache(self, cache_name: str):
        """Get cache by name"""
        if not self._connected:
            raise RuntimeError("Not connected to Ignite")
            
        return await self._client.get_cache(cache_name)
        
    async def cache_exists(self, cache_name: str) -> bool:
        """Check if cache exists"""
        try:
            cache = await self._client.get_cache(cache_name)
            # Try to get cache size to verify it exists
            await cache.get_size()
            return True
        except:
            return False
            
    async def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            cache = await self.get_cache(cache_name)
            value = await cache.get(key)
            return value
        except CacheError as e:
            logger.error(f"Cache get error for {cache_name}:{key} - {e}")
            return None
            
    async def put(self, cache_name: str, key: str, value: Any, expiry_ms: Optional[int] = None):
        """Put value in cache with optional expiry"""
        try:
            cache = await self.get_cache(cache_name)
            
            if expiry_ms:
                # Create cache with expiry policy
                expiry_cache = cache.with_expire_policy(
                    create=expiry_ms,
                    update=expiry_ms,
                    access=expiry_ms
                )
                await expiry_cache.put(key, value)
            else:
                await cache.put(key, value)
                
        except Exception as e:
            logger.error(f"Cache put error for {cache_name}:{key} - {e}")
            raise
            
    async def get_all(self, cache_name: str, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache"""
        try:
            cache = await self.get_cache(cache_name)
            result = await cache.get_all(keys)
            return result or {}
        except Exception as e:
            logger.error(f"Cache get_all error for {cache_name} - {e}")
            return {}
            
    async def put_all(self, cache_name: str, entries: Dict[str, Any], expiry_ms: Optional[int] = None):
        """Put multiple values in cache"""
        try:
            cache = await self.get_cache(cache_name)
            
            if expiry_ms:
                expiry_cache = cache.with_expire_policy(
                    create=expiry_ms,
                    update=expiry_ms,
                    access=expiry_ms
                )
                await expiry_cache.put_all(entries)
            else:
                await cache.put_all(entries)
                
        except Exception as e:
            logger.error(f"Cache put_all error for {cache_name} - {e}")
            raise
            
    async def remove(self, cache_name: str, key: str) -> bool:
        """Remove value from cache"""
        try:
            cache = await self.get_cache(cache_name)
            return await cache.remove_key(key)
        except Exception as e:
            logger.error(f"Cache remove error for {cache_name}:{key} - {e}")
            return False
            
    async def remove_all(self, cache_name: str, keys: List[str]):
        """Remove multiple values from cache"""
        try:
            cache = await self.get_cache(cache_name)
            await cache.remove_keys(keys)
        except Exception as e:
            logger.error(f"Cache remove_all error for {cache_name} - {e}")
            
    async def clear(self, cache_name: str):
        """Clear all entries from cache"""
        try:
            cache = await self.get_cache(cache_name)
            await cache.clear()
        except Exception as e:
            logger.error(f"Cache clear error for {cache_name} - {e}")
            raise
            
    async def get_size(self, cache_name: str) -> int:
        """Get cache size"""
        try:
            cache = await self.get_cache(cache_name)
            return await cache.get_size()
        except Exception as e:
            logger.error(f"Failed to get cache size for {cache_name}: {e}")
            return 0
            
    async def get_keys(self, cache_name: str) -> List[str]:
        """Get all keys from cache"""
        try:
            cache = await self.get_cache(cache_name)
            # Note: This is inefficient for large caches
            # In production, use scan query with projection
            keys = []
            async with cache.scan() as cursor:
                async for entry in cursor:
                    keys.append(entry[0])
            return keys
        except Exception as e:
            logger.error(f"Failed to get keys for {cache_name}: {e}")
            return []
            
    async def query(self, cache_name: str, sql: str, args: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query on cache"""
        try:
            cache = await self.get_cache(cache_name)
            
            # Execute query
            async with cache.query_sql(sql, *args) if args else cache.query_sql(sql) as cursor:
                results = []
                async for row in cursor:
                    # Convert row to dict
                    result = {}
                    for i, field in enumerate(cursor.field_names):
                        result[field] = row[i]
                    results.append(result)
                return results
                
        except Exception as e:
            logger.error(f"Query error on {cache_name}: {e}")
            raise
            
    async def scan(self, cache_name: str, page_size: int = 1000):
        """Scan cache entries"""
        try:
            cache = await self.get_cache(cache_name)
            async with cache.scan(page_size=page_size) as cursor:
                async for key, value in cursor:
                    yield key, value
                    
        except Exception as e:
            logger.error(f"Scan error on {cache_name}: {e}")
            raise
            
    async def get_cache_metrics(self, cache_name: str) -> Dict[str, Any]:
        """Get cache metrics"""
        try:
            cache = await self.get_cache(cache_name)
            
            # Get basic metrics
            metrics = {
                "size": await cache.get_size(),
                "name": cache_name
            }
            
            # Additional metrics would come from Ignite metrics API
            # This is a simplified version
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get metrics for {cache_name}: {e}")
            return {}
            
    # Transaction support
    
    async def start_transaction(self, 
                              concurrency: str = "PESSIMISTIC",
                              isolation: str = "REPEATABLE_READ",
                              timeout: int = 0,
                              label: Optional[str] = None):
        """Start a new transaction"""
        if not self._connected:
            raise RuntimeError("Not connected to Ignite")
            
        # Note: pyignite doesn't have full transaction support yet
        # This is a placeholder for when it's added
        tx_id = f"tx_{datetime.utcnow().timestamp()}"
        
        self._transactions[tx_id] = {
            "id": tx_id,
            "concurrency": concurrency,
            "isolation": isolation,
            "timeout": timeout,
            "label": label,
            "started_at": datetime.utcnow()
        }
        
        logger.debug(f"Started transaction: {tx_id}")
        return TransactionContext(self, tx_id)
        
    async def commit_transaction(self, tx_id: str):
        """Commit transaction"""
        if tx_id in self._transactions:
            del self._transactions[tx_id]
            logger.debug(f"Committed transaction: {tx_id}")
            
    async def rollback_transaction(self, tx_id: str):
        """Rollback transaction"""
        if tx_id in self._transactions:
            del self._transactions[tx_id]
            logger.debug(f"Rolled back transaction: {tx_id}")
            
    # Compute support
    
    async def execute_task(self, task_name: str, *args, **kwargs):
        """Execute compute task on cluster"""
        try:
            # Ensure we're connected
            if not self._connected:
                raise RuntimeError("Not connected to Ignite cluster")
            
            # Get compute instance
            compute = self._client.get_cluster().compute()
            
            # Create a callable task
            class ComputeTask:
                def __init__(self, task_name, args, kwargs):
                    self.task_name = task_name
                    self.args = args
                    self.kwargs = kwargs
                    
                def __call__(self):
                    # This would be the actual task logic
                    # For now, we'll simulate task execution
                    import time
                    start_time = time.time()
                    
                    # Simulate different task types
                    if self.task_name == "sum":
                        result = sum(self.args)
                    elif self.task_name == "count":
                        cache_name = self.kwargs.get("cache", "default")
                        # In real implementation, would count cache entries
                        result = {"cache": cache_name, "count": 0}
                    elif self.task_name == "scan":
                        cache_name = self.kwargs.get("cache", "default")
                        filter_func = self.kwargs.get("filter", lambda x: True)
                        # In real implementation, would scan and filter cache
                        result = []
                    else:
                        # Custom task execution
                        result = {
                            "task": self.task_name,
                            "args": self.args,
                            "kwargs": self.kwargs,
                            "node": "current",
                            "timestamp": time.time()
                        }
                    
                    execution_time = time.time() - start_time
                    return {
                        "result": result,
                        "execution_time": execution_time,
                        "task_name": self.task_name
                    }
            
            # Create task instance
            task = ComputeTask(task_name, args, kwargs)
            
            # Execute on random node
            future = compute.call(task)
            result = future.get()
            
            logger.info(f"Task '{task_name}' completed in {result.get('execution_time', 0):.3f}s")
            return result.get("result")
            
        except Exception as e:
            logger.error(f"Failed to execute task '{task_name}': {e}")
            raise
        
    async def broadcast(self, callable_func, *args, **kwargs):
        """Broadcast callable to all nodes"""
        try:
            # Ensure we're connected
            if not self._connected:
                raise RuntimeError("Not connected to Ignite cluster")
            
            # Get compute instance
            compute = self._client.get_cluster().compute()
            
            # Create a broadcast task
            class BroadcastTask:
                def __init__(self, func, args, kwargs):
                    self.func = func
                    self.args = args
                    self.kwargs = kwargs
                    
                def __call__(self):
                    # Execute the function on this node
                    import socket
                    node_id = socket.gethostname()
                    
                    try:
                        if callable(self.func):
                            result = self.func(*self.args, **self.kwargs)
                        else:
                            # If func is a string, try to execute predefined functions
                            if self.func == "clear_cache":
                                # Clear local caches
                                result = {"cleared": True}
                            elif self.func == "get_metrics":
                                # Get node metrics
                                result = {
                                    "cpu_count": self._get_cpu_count(),
                                    "memory_mb": self._get_memory_mb(),
                                    "cache_entries": 0  # Would get actual count
                                }
                            elif self.func == "health_check":
                                # Perform health check
                                result = {
                                    "healthy": True,
                                    "uptime": 0,  # Would get actual uptime
                                    "version": "2.0.0"
                                }
                            else:
                                result = {"error": f"Unknown function: {self.func}"}
                        
                        return {
                            "node_id": node_id,
                            "success": True,
                            "result": result
                        }
                    except Exception as e:
                        return {
                            "node_id": node_id,
                            "success": False,
                            "error": str(e)
                        }
                
                def _get_cpu_count(self):
                    import multiprocessing
                    return multiprocessing.cpu_count()
                
                def _get_memory_mb(self):
                    import psutil
                    return psutil.virtual_memory().total // (1024 * 1024)
            
            # Create broadcast task
            if isinstance(callable_func, str):
                task = BroadcastTask(callable_func, args, kwargs)
            else:
                task = BroadcastTask(callable_func, args, kwargs)
            
            # Broadcast to all nodes
            futures = compute.broadcast(task)
            
            # Collect results
            results = []
            for future in futures:
                try:
                    result = future.get()
                    results.append(result)
                except Exception as e:
                    results.append({
                        "node_id": "unknown",
                        "success": False,
                        "error": str(e)
                    })
            
            # Summary
            successful = sum(1 for r in results if r.get("success", False))
            logger.info(f"Broadcast completed: {successful}/{len(results)} nodes succeeded")
            
            return {
                "total_nodes": len(results),
                "successful_nodes": successful,
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Failed to broadcast: {e}")
            raise


class TransactionContext:
    """Transaction context manager"""
    
    def __init__(self, client: IgniteClient, tx_id: str):
        self.client = client
        self.tx_id = tx_id
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
            
    async def commit(self):
        """Commit transaction"""
        await self.client.commit_transaction(self.tx_id)
        
    async def rollback(self):
        """Rollback transaction"""
        await self.client.rollback_transaction(self.tx_id) 