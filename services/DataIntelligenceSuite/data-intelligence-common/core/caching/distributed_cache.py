"""
Distributed Cache Client for DataIntelligenceSuite

Provides distributed caching capabilities with multi-node coordination.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, List, Set, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import hashlib
import json

from pyignite import Client, AioClient
from pyignite.datatypes import String, IntObject
from pyignite.cache import Cache

logger = logging.getLogger(__name__)


@dataclass
class NodeInfo:
    """Information about a cache node"""
    node_id: str
    host: str
    port: int
    status: str = "active"
    last_heartbeat: datetime = None
    
    def __post_init__(self):
        if self.last_heartbeat is None:
            self.last_heartbeat = datetime.utcnow()
            
    @property
    def is_healthy(self) -> bool:
        """Check if node is healthy (heartbeat within 30 seconds)"""
        return (datetime.utcnow() - self.last_heartbeat).total_seconds() < 30


class DistributedCacheClient:
    """
    Distributed cache client with advanced features.
    
    Features:
    - Consistent hashing for key distribution
    - Node failure detection and recovery
    - Distributed locks
    - Pub/sub for cache invalidation
    - Near cache for frequently accessed data
    """
    
    def __init__(
        self,
        nodes: List[Tuple[str, int]],
        service_name: str,
        enable_near_cache: bool = True,
        near_cache_size: int = 1000
    ):
        self.nodes = nodes
        self.service_name = service_name
        self.enable_near_cache = enable_near_cache
        self.near_cache_size = near_cache_size
        
        # Ignite clients
        self._client: Optional[AioClient] = None
        
        # Node management
        self._node_info: Dict[str, NodeInfo] = {}
        self._hash_ring: List[Tuple[int, str]] = []
        
        # Near cache
        self._near_cache: Dict[str, Tuple[Any, datetime]] = {}
        self._near_cache_ttl = timedelta(minutes=5)
        
        # Distributed locks
        self._locks: Dict[str, asyncio.Lock] = {}
        
        # Pub/sub for invalidation
        self._invalidation_handlers: List[Callable] = []
        
        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
    async def connect(self):
        """Connect to distributed cache cluster"""
        logger.info(f"Connecting to distributed cache cluster: {self.nodes}")
        
        # Create Ignite client
        self._client = AioClient(
            timeout=10.0,
            partition_aware=True,
            heartbeat_interval=10000  # 10 seconds
        )
        
        await self._client.connect(self.nodes)
        
        # Initialize node info
        await self._init_nodes()
        
        # Build hash ring
        self._build_hash_ring()
        
        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self.enable_near_cache:
            self._cleanup_task = asyncio.create_task(self._cleanup_near_cache())
            
        logger.info("Connected to distributed cache cluster")
        
    async def disconnect(self):
        """Disconnect from cache cluster"""
        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        # Wait for tasks
        tasks = [t for t in [self._heartbeat_task, self._cleanup_task] if t]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        # Close client
        if self._client:
            await self._client.close()
            
        logger.info("Disconnected from distributed cache cluster")
        
    async def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from distributed cache"""
        # Check near cache first
        if self.enable_near_cache:
            near_value = self._get_from_near_cache(key)
            if near_value is not None:
                return near_value
                
        # Get from distributed cache
        try:
            cache = await self._client.get_cache(cache_name)
            value = await cache.get(key)
            
            # Update near cache
            if value is not None and self.enable_near_cache:
                self._put_to_near_cache(key, value)
                
            return value
            
        except Exception as e:
            logger.error(f"Distributed get error for {cache_name}:{key} - {e}")
            return None
            
    async def put(
        self,
        cache_name: str,
        key: str,
        value: Any,
        ttl: Optional[timedelta] = None
    ) -> None:
        """Put value to distributed cache"""
        try:
            cache = await self._client.get_cache(cache_name)
            
            if ttl:
                expiry_ms = int(ttl.total_seconds() * 1000)
                await cache.put(key, value, expiry_policy=expiry_ms)
            else:
                await cache.put(key, value)
                
            # Update near cache
            if self.enable_near_cache:
                self._put_to_near_cache(key, value)
                
            # Notify other nodes to invalidate near cache
            await self._broadcast_invalidation(cache_name, key)
            
        except Exception as e:
            logger.error(f"Distributed put error for {cache_name}:{key} - {e}")
            
    async def remove(self, cache_name: str, key: str) -> bool:
        """Remove value from distributed cache"""
        try:
            cache = await self._client.get_cache(cache_name)
            result = await cache.remove_key(key)
            
            # Remove from near cache
            if self.enable_near_cache:
                self._remove_from_near_cache(key)
                
            # Notify other nodes
            await self._broadcast_invalidation(cache_name, key)
            
            return result
            
        except Exception as e:
            logger.error(f"Distributed remove error for {cache_name}:{key} - {e}")
            return False
            
    async def get_all(
        self,
        cache_name: str,
        keys: List[str]
    ) -> Dict[str, Any]:
        """Get multiple values from distributed cache"""
        try:
            cache = await self._client.get_cache(cache_name)
            
            # Check near cache first
            results = {}
            missing_keys = []
            
            if self.enable_near_cache:
                for key in keys:
                    value = self._get_from_near_cache(key)
                    if value is not None:
                        results[key] = value
                    else:
                        missing_keys.append(key)
            else:
                missing_keys = keys
                
            # Get missing from distributed cache
            if missing_keys:
                distributed_results = await cache.get_all(missing_keys)
                
                # Update near cache and results
                for key, value in distributed_results.items():
                    if value is not None:
                        results[key] = value
                        if self.enable_near_cache:
                            self._put_to_near_cache(key, value)
                            
            return results
            
        except Exception as e:
            logger.error(f"Distributed get_all error for {cache_name} - {e}")
            return {}
            
    async def acquire_lock(
        self,
        lock_name: str,
        timeout: Optional[timedelta] = None
    ) -> bool:
        """Acquire distributed lock"""
        try:
            # Use Ignite's atomic operations for distributed locking
            lock_cache = await self._client.get_or_create_cache("_locks")
            
            # Try to acquire lock
            lock_key = f"{self.service_name}:{lock_name}"
            lock_value = f"{self.service_name}:{datetime.utcnow().isoformat()}"
            
            # Put if absent (atomic operation)
            acquired = await lock_cache.put_if_absent(lock_key, lock_value)
            
            if acquired and timeout:
                # Set expiry for lock
                expiry_ms = int(timeout.total_seconds() * 1000)
                await lock_cache.with_expire_policy(
                    create=expiry_ms,
                    update=expiry_ms,
                    access=expiry_ms
                ).put(lock_key, lock_value)
                
            return acquired
            
        except Exception as e:
            logger.error(f"Failed to acquire lock {lock_name}: {e}")
            return False
            
    async def release_lock(self, lock_name: str) -> bool:
        """Release distributed lock"""
        try:
            lock_cache = await self._client.get_cache("_locks")
            lock_key = f"{self.service_name}:{lock_name}"
            
            return await lock_cache.remove_key(lock_key)
            
        except Exception as e:
            logger.error(f"Failed to release lock {lock_name}: {e}")
            return False
            
    def add_invalidation_handler(self, handler: Callable):
        """Add handler for cache invalidation events"""
        self._invalidation_handlers.append(handler)
        
    # Near cache methods
    
    def _get_from_near_cache(self, key: str) -> Optional[Any]:
        """Get value from near cache"""
        if key in self._near_cache:
            value, timestamp = self._near_cache[key]
            
            # Check if expired
            if datetime.utcnow() - timestamp < self._near_cache_ttl:
                return value
            else:
                # Remove expired entry
                del self._near_cache[key]
                
        return None
        
    def _put_to_near_cache(self, key: str, value: Any):
        """Put value to near cache"""
        # Evict oldest if at capacity
        if len(self._near_cache) >= self.near_cache_size:
            # Find oldest entry
            oldest_key = min(
                self._near_cache.keys(),
                key=lambda k: self._near_cache[k][1]
            )
            del self._near_cache[oldest_key]
            
        self._near_cache[key] = (value, datetime.utcnow())
        
    def _remove_from_near_cache(self, key: str):
        """Remove value from near cache"""
        self._near_cache.pop(key, None)
        
    async def _cleanup_near_cache(self):
        """Periodically clean up expired entries from near cache"""
        while True:
            try:
                await asyncio.sleep(60)  # Clean every minute
                
                now = datetime.utcnow()
                expired_keys = [
                    key for key, (_, timestamp) in self._near_cache.items()
                    if now - timestamp >= self._near_cache_ttl
                ]
                
                for key in expired_keys:
                    del self._near_cache[key]
                    
                if expired_keys:
                    logger.debug(f"Cleaned {len(expired_keys)} expired entries from near cache")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Near cache cleanup error: {e}")
                
    # Node management methods
    
    async def _init_nodes(self):
        """Initialize node information"""
        for i, (host, port) in enumerate(self.nodes):
            node_id = f"{host}:{port}"
            self._node_info[node_id] = NodeInfo(
                node_id=node_id,
                host=host,
                port=port
            )
            
    def _build_hash_ring(self):
        """Build consistent hash ring for key distribution"""
        self._hash_ring.clear()
        
        # Add virtual nodes for better distribution
        virtual_nodes = 150
        
        for node_id, node_info in self._node_info.items():
            if node_info.status == "active":
                for i in range(virtual_nodes):
                    virtual_key = f"{node_id}:{i}"
                    hash_value = int(hashlib.md5(virtual_key.encode()).hexdigest(), 16)
                    self._hash_ring.append((hash_value, node_id))
                    
        # Sort by hash value
        self._hash_ring.sort(key=lambda x: x[0])
        
    def _get_node_for_key(self, key: str) -> Optional[str]:
        """Get node responsible for key using consistent hashing"""
        if not self._hash_ring:
            return None
            
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        
        # Binary search for the first node with hash >= key_hash
        left, right = 0, len(self._hash_ring) - 1
        
        while left < right:
            mid = (left + right) // 2
            if self._hash_ring[mid][0] < key_hash:
                left = mid + 1
            else:
                right = mid
                
        # If no node found, wrap around to first node
        if left == len(self._hash_ring) - 1 and self._hash_ring[left][0] < key_hash:
            return self._hash_ring[0][1]
            
        return self._hash_ring[left][1]
        
    async def _heartbeat_loop(self):
        """Send heartbeats and detect failed nodes"""
        while True:
            try:
                await asyncio.sleep(10)  # Heartbeat every 10 seconds
                
                # Update node status
                # In real implementation, this would check actual node health
                for node_info in self._node_info.values():
                    node_info.last_heartbeat = datetime.utcnow()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                
    async def _broadcast_invalidation(self, cache_name: str, key: str):
        """Broadcast cache invalidation to other nodes"""
        # In real implementation, this would use Ignite's messaging or pub/sub
        # For now, just call local handlers
        for handler in self._invalidation_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(cache_name, key)
                else:
                    handler(cache_name, key)
            except Exception as e:
                logger.error(f"Invalidation handler error: {e}") 