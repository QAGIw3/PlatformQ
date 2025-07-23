"""
Cache Management for Integration Hub.
"""

import asyncio
from typing import Dict, List, Any, Optional, Set, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict, OrderedDict

from data_intelligence_common.core.events import EventBus

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class EvictionPolicy(str, Enum):
    """Cache eviction policies."""
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    FIFO = "fifo"  # First In First Out
    TTL = "ttl"  # Time To Live based
    CUSTOM = "custom"  # Custom policy


@dataclass
class CacheEntry:
    """Individual cache entry."""
    key: str
    value: Any
    created_at: datetime = field(default_factory=datetime.utcnow)
    accessed_at: datetime = field(default_factory=datetime.utcnow)
    access_count: int = 0
    ttl_seconds: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if entry is expired."""
        if self.ttl_seconds is None:
            return False
        
        expiry_time = self.created_at + timedelta(seconds=self.ttl_seconds)
        return datetime.utcnow() > expiry_time
    
    def touch(self):
        """Update access time and count."""
        self.accessed_at = datetime.utcnow()
        self.access_count += 1


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    puts: int = 0
    deletes: int = 0
    size: int = 0
    memory_bytes: int = 0
    
    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class CacheManager:
    """
    Advanced cache management with multiple eviction policies.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        max_size: int = 10000,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
        ttl_seconds: Optional[int] = None
    ):
        self.event_bus = event_bus
        self.max_size = max_size
        self.eviction_policy = eviction_policy
        self.default_ttl = ttl_seconds
        
        # Cache storage
        self.cache: Dict[str, CacheEntry] = {}
        self.access_order: OrderedDict = OrderedDict()  # For LRU
        self.frequency_map: Dict[str, int] = defaultdict(int)  # For LFU
        
        # Cache statistics
        self.stats = CacheStats()
        
        # Eviction handlers
        self.eviction_handlers: Dict[EvictionPolicy, Callable] = {
            EvictionPolicy.LRU: self._evict_lru,
            EvictionPolicy.LFU: self._evict_lfu,
            EvictionPolicy.FIFO: self._evict_fifo,
            EvictionPolicy.TTL: self._evict_expired
        }
        
        # Custom eviction function
        self.custom_eviction_func: Optional[Callable] = None
        
        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stats_task: Optional[asyncio.Task] = None
        
        logger.info(f"Cache Manager initialized with {eviction_policy.value} policy")
        
    async def initialize(self):
        """Initialize cache manager."""
        # Subscribe to events
        await self.event_bus.subscribe("cache.clear", self._handle_clear_cache)
        await self.event_bus.subscribe("cache.invalidate", self._handle_invalidate)
        
        # Start background tasks
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())
        self._stats_task = asyncio.create_task(self._report_stats())
        
        logger.info("Cache Manager ready")
        
    async def cleanup(self):
        """Cleanup cache manager resources."""
        # Cancel background tasks
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._stats_task:
            self._stats_task.cancel()
        
        # Clear cache
        self.cache.clear()
        self.access_order.clear()
        self.frequency_map.clear()
        
        logger.info("Cache Manager cleaned up")
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        entry = self.cache.get(key)
        
        if entry is None:
            self.stats.misses += 1
            return None
        
        # Check expiration
        if entry.is_expired():
            await self.delete(key)
            self.stats.expirations += 1
            self.stats.misses += 1
            return None
        
        # Update access info
        entry.touch()
        self.stats.hits += 1
        
        # Update access order for LRU
        if self.eviction_policy == EvictionPolicy.LRU:
            self.access_order.move_to_end(key)
        
        # Update frequency for LFU
        if self.eviction_policy == EvictionPolicy.LFU:
            self.frequency_map[key] = entry.access_count
        
        return entry.value
        
    async def put(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Put value into cache."""
        try:
            # Check if we need to evict
            if len(self.cache) >= self.max_size and key not in self.cache:
                await self._evict()
            
            # Create or update entry
            entry = CacheEntry(
                key=key,
                value=value,
                ttl_seconds=ttl_seconds or self.default_ttl,
                metadata=metadata or {}
            )
            
            # Update cache
            self.cache[key] = entry
            self.stats.puts += 1
            self.stats.size = len(self.cache)
            
            # Update access order for LRU
            if self.eviction_policy == EvictionPolicy.LRU:
                self.access_order[key] = True
                self.access_order.move_to_end(key)
            
            # Update frequency for LFU
            if self.eviction_policy == EvictionPolicy.LFU:
                self.frequency_map[key] = 1
            
            # Publish event
            await self.event_bus.publish("cache.entry.put", {
                "key": key,
                "ttl": ttl_seconds,
                "has_metadata": bool(metadata)
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Error putting key {key}: {e}")
            return False
            
    async def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        if key not in self.cache:
            return False
        
        try:
            # Remove from cache
            del self.cache[key]
            self.stats.deletes += 1
            self.stats.size = len(self.cache)
            
            # Remove from access order
            if key in self.access_order:
                del self.access_order[key]
            
            # Remove from frequency map
            if key in self.frequency_map:
                del self.frequency_map[key]
            
            # Publish event
            await self.event_bus.publish("cache.entry.deleted", {"key": key})
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            return False
            
    async def clear(self) -> int:
        """Clear all entries from cache."""
        count = len(self.cache)
        
        self.cache.clear()
        self.access_order.clear()
        self.frequency_map.clear()
        
        self.stats.size = 0
        
        # Publish event
        await self.event_bus.publish("cache.cleared", {"count": count})
        
        logger.info(f"Cleared {count} entries from cache")
        return count
        
    async def get_multiple(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache."""
        results = {}
        
        for key in keys:
            value = await self.get(key)
            if value is not None:
                results[key] = value
        
        return results
        
    async def put_multiple(
        self,
        entries: Dict[str, Any],
        ttl_seconds: Optional[int] = None
    ) -> int:
        """Put multiple entries into cache."""
        success_count = 0
        
        for key, value in entries.items():
            if await self.put(key, value, ttl_seconds):
                success_count += 1
        
        return success_count
        
    async def delete_multiple(self, keys: List[str]) -> int:
        """Delete multiple entries from cache."""
        success_count = 0
        
        for key in keys:
            if await self.delete(key):
                success_count += 1
        
        return success_count
        
    async def _evict(self):
        """Evict entries based on policy."""
        handler = self.eviction_handlers.get(self.eviction_policy)
        
        if handler:
            await handler()
        elif self.eviction_policy == EvictionPolicy.CUSTOM and self.custom_eviction_func:
            await self.custom_eviction_func(self)
        else:
            # Default to LRU
            await self._evict_lru()
            
    async def _evict_lru(self):
        """Evict least recently used entry."""
        if not self.access_order:
            return
        
        # Get least recently used key
        key = next(iter(self.access_order))
        
        # Delete entry
        await self.delete(key)
        self.stats.evictions += 1
        
        logger.debug(f"Evicted LRU entry: {key}")
        
    async def _evict_lfu(self):
        """Evict least frequently used entry."""
        if not self.frequency_map:
            return
        
        # Find least frequently used key
        key = min(self.frequency_map, key=self.frequency_map.get)
        
        # Delete entry
        await self.delete(key)
        self.stats.evictions += 1
        
        logger.debug(f"Evicted LFU entry: {key}")
        
    async def _evict_fifo(self):
        """Evict first in (oldest) entry."""
        if not self.cache:
            return
        
        # Find oldest entry
        oldest_key = None
        oldest_time = None
        
        for key, entry in self.cache.items():
            if oldest_time is None or entry.created_at < oldest_time:
                oldest_time = entry.created_at
                oldest_key = key
        
        # Delete entry
        if oldest_key:
            await self.delete(oldest_key)
            self.stats.evictions += 1
            
            logger.debug(f"Evicted FIFO entry: {oldest_key}")
            
    async def _evict_expired(self):
        """Evict expired entries."""
        expired_keys = []
        
        for key, entry in self.cache.items():
            if entry.is_expired():
                expired_keys.append(key)
        
        # Delete expired entries
        for key in expired_keys:
            await self.delete(key)
            self.stats.expirations += 1
        
        if expired_keys:
            logger.debug(f"Evicted {len(expired_keys)} expired entries")
            
    async def _cleanup_expired(self):
        """Background task to cleanup expired entries."""
        while True:
            try:
                await self._evict_expired()
                
                # Sleep for 60 seconds
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(60)
                
    async def _report_stats(self):
        """Background task to report cache statistics."""
        while True:
            try:
                # Calculate memory usage (approximate)
                memory_bytes = 0
                for entry in self.cache.values():
                    # Rough estimate of memory usage
                    memory_bytes += len(json.dumps(entry.value)) if isinstance(entry.value, (dict, list)) else 100
                
                self.stats.memory_bytes = memory_bytes
                
                # Publish stats
                await self.event_bus.publish("cache.stats", {
                    "size": self.stats.size,
                    "hits": self.stats.hits,
                    "misses": self.stats.misses,
                    "hit_rate": self.stats.hit_rate,
                    "evictions": self.stats.evictions,
                    "expirations": self.stats.expirations,
                    "memory_bytes": memory_bytes
                })
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reporting stats: {e}")
                await asyncio.sleep(300)
                
    async def _handle_clear_cache(self, event_data: Dict[str, Any]):
        """Handle cache clear event."""
        try:
            await self.clear()
        except Exception as e:
            logger.error(f"Error handling clear cache: {e}")
            
    async def _handle_invalidate(self, event_data: Dict[str, Any]):
        """Handle cache invalidation event."""
        try:
            keys = event_data.get("keys", [])
            pattern = event_data.get("pattern")
            
            if keys:
                await self.delete_multiple(keys)
            elif pattern:
                # Pattern-based invalidation
                matching_keys = [k for k in self.cache.keys() if pattern in k]
                await self.delete_multiple(matching_keys)
                
        except Exception as e:
            logger.error(f"Error handling invalidation: {e}")
            
    def register_custom_eviction(self, func: Callable):
        """Register custom eviction function."""
        self.custom_eviction_func = func
        logger.info("Registered custom eviction function")
        
    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self.stats
        
    def get_keys(self, pattern: Optional[str] = None) -> List[str]:
        """Get cache keys, optionally filtered by pattern."""
        keys = list(self.cache.keys())
        
        if pattern:
            keys = [k for k in keys if pattern in k]
        
        return keys
        
    def get_entry_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a cache entry."""
        entry = self.cache.get(key)
        
        if not entry:
            return None
        
        return {
            "key": key,
            "created_at": entry.created_at.isoformat(),
            "accessed_at": entry.accessed_at.isoformat(),
            "access_count": entry.access_count,
            "ttl_seconds": entry.ttl_seconds,
            "is_expired": entry.is_expired(),
            "metadata": entry.metadata
        } 