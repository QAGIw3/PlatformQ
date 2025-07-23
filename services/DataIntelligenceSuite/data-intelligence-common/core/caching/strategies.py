"""
Unified cache strategies and patterns for DataIntelligenceSuite.

Consolidates cache strategies from various modules into a single location.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Callable, List, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
from collections import deque

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class CacheStrategy(str, Enum):
    """Unified cache strategies enum - single source of truth"""
    CACHE_ASIDE = "cache_aside"          # Application manages cache
    READ_THROUGH = "read_through"        # Cache loads on miss
    WRITE_THROUGH = "write_through"      # Write to cache and source
    WRITE_BEHIND = "write_behind"        # Write to cache, async to source
    REFRESH_AHEAD = "refresh_ahead"      # Proactive refresh before expiry


class EvictionPolicy(str, Enum):
    """Cache eviction policies"""
    LRU = "LRU"        # Least Recently Used
    LFU = "LFU"        # Least Frequently Used
    FIFO = "FIFO"      # First In First Out
    RANDOM = "RANDOM"  # Random eviction
    TTL = "TTL"        # Time-based eviction


class CacheMode(str, Enum):
    """Cache modes for different use cases"""
    REPLICATED = "REPLICATED"    # Full replication across nodes
    PARTITIONED = "PARTITIONED"  # Data partitioned across nodes
    LOCAL = "LOCAL"              # Node-local cache
    NEAR = "NEAR"                # Near cache for frequently accessed data


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    created_at: datetime = field(default_factory=datetime.utcnow)
    accessed_at: datetime = field(default_factory=datetime.utcnow)
    ttl: Optional[timedelta] = None
    access_count: int = 0
    size_bytes: Optional[int] = None
    
    @property
    def is_expired(self) -> bool:
        """Check if entry is expired"""
        if self.ttl is None:
            return False
        return datetime.utcnow() > self.created_at + self.ttl
        
    @property
    def time_to_refresh(self) -> bool:
        """Check if entry should be refreshed (80% of TTL)"""
        if self.ttl is None:
            return False
        elapsed = datetime.utcnow() - self.created_at
        return elapsed > self.ttl * 0.8
        
    def touch(self):
        """Update access time and count"""
        self.accessed_at = datetime.utcnow()
        self.access_count += 1


class BaseCacheStrategy(ABC):
    """Base class for cache strategies"""
    
    def __init__(self, cache_manager, cache_name: str):
        self.cache_manager = cache_manager
        self.cache_name = cache_name
        self._metrics = {
            "hits": 0,
            "misses": 0,
            "puts": 0,
            "evictions": 0
        }
        
    @abstractmethod
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get value with strategy"""
        pass
        
    @abstractmethod
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put value with strategy"""
        pass
        
    @abstractmethod
    async def remove(self, key: str) -> bool:
        """Remove value with strategy"""
        pass
        
    async def clear(self) -> None:
        """Clear all entries"""
        await self.cache_manager.clear(self.cache_name)
        
    def get_metrics(self) -> Dict[str, int]:
        """Get strategy metrics"""
        return self._metrics.copy()


class CacheAsideStrategy(BaseCacheStrategy):
    """
    Cache-aside (lazy loading) pattern.
    
    Application manages cache population.
    """
    
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with cache-aside pattern"""
        # Try cache first
        value = await self.cache_manager.get(self.cache_name, key)
        
        if value is not None:
            self._metrics["hits"] += 1
            return value
            
        self._metrics["misses"] += 1
        
        # Load from source if loader provided
        if loader:
            value = await loader(key) if asyncio.iscoroutinefunction(loader) else loader(key)
            if value is not None:
                await self.put(key, value)
            return value
            
        return None
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with cache-aside pattern"""
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        self._metrics["puts"] += 1
        
    async def remove(self, key: str) -> bool:
        """Remove with cache-aside pattern"""
        result = await self.cache_manager.remove(self.cache_name, key)
        if result:
            self._metrics["evictions"] += 1
        return result


class ReadThroughStrategy(BaseCacheStrategy):
    """
    Read-through pattern.
    
    Cache loads missing data automatically.
    """
    
    def __init__(self, cache_manager, cache_name: str, loader: Callable):
        super().__init__(cache_manager, cache_name)
        self.loader = loader
        
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with read-through pattern"""
        # Try cache first
        value = await self.cache_manager.get(self.cache_name, key)
        
        if value is not None:
            self._metrics["hits"] += 1
            return value
            
        self._metrics["misses"] += 1
        
        # Use configured loader or provided one
        loader = loader or self.loader
        value = await loader(key) if asyncio.iscoroutinefunction(loader) else loader(key)
        
        if value is not None:
            await self.put(key, value)
            
        return value
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with read-through pattern"""
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        self._metrics["puts"] += 1
        
    async def remove(self, key: str) -> bool:
        """Remove with read-through pattern"""
        result = await self.cache_manager.remove(self.cache_name, key)
        if result:
            self._metrics["evictions"] += 1
        return result


class WriteThroughStrategy(BaseCacheStrategy):
    """
    Write-through pattern.
    
    Writes go to cache and backing store synchronously.
    """
    
    def __init__(self, cache_manager, cache_name: str, writer: Callable):
        super().__init__(cache_manager, cache_name)
        self.writer = writer
        
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with write-through pattern"""
        value = await self.cache_manager.get(self.cache_name, key)
        if value is not None:
            self._metrics["hits"] += 1
        else:
            self._metrics["misses"] += 1
        return value
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with write-through pattern"""
        # Write to backing store first
        if asyncio.iscoroutinefunction(self.writer):
            await self.writer(key, value)
        else:
            self.writer(key, value)
            
        # Then update cache
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        self._metrics["puts"] += 1
        
    async def remove(self, key: str) -> bool:
        """Remove with write-through pattern"""
        # Remove from backing store first
        if asyncio.iscoroutinefunction(self.writer):
            await self.writer(key, None)  # None indicates deletion
        else:
            self.writer(key, None)
            
        # Then remove from cache
        result = await self.cache_manager.remove(self.cache_name, key)
        if result:
            self._metrics["evictions"] += 1
        return result


class WriteBehindStrategy(BaseCacheStrategy):
    """
    Write-behind (write-back) pattern.
    
    Writes go to cache immediately, backing store updated asynchronously.
    """
    
    def __init__(self, cache_manager, cache_name: str, writer: Callable,
                 write_delay: timedelta = timedelta(seconds=5),
                 batch_size: int = 100):
        super().__init__(cache_manager, cache_name)
        self.writer = writer
        self.write_delay = write_delay
        self.batch_size = batch_size
        self.write_queue: Dict[str, Tuple[Any, datetime]] = {}
        self.write_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start write-behind task"""
        if not self.write_task:
            self.write_task = asyncio.create_task(self._write_loop())
            logger.info(f"Started write-behind task for cache {self.cache_name}")
            
    async def stop(self):
        """Stop write-behind task"""
        if self.write_task:
            self.write_task.cancel()
            try:
                await self.write_task
            except asyncio.CancelledError:
                pass
                
        # Flush remaining writes
        await self._flush_writes()
        logger.info(f"Stopped write-behind task for cache {self.cache_name}")
        
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with write-behind pattern"""
        # Check write queue first
        if key in self.write_queue:
            value, _ = self.write_queue[key]
            if value is not None:  # Not a deletion
                self._metrics["hits"] += 1
                return value
                
        value = await self.cache_manager.get(self.cache_name, key)
        if value is not None:
            self._metrics["hits"] += 1
        else:
            self._metrics["misses"] += 1
        return value
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with write-behind pattern"""
        # Update cache immediately
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        self._metrics["puts"] += 1
        
        # Queue for write to backing store
        self.write_queue[key] = (value, datetime.utcnow())
        
    async def remove(self, key: str) -> bool:
        """Remove with write-behind pattern"""
        # Remove from cache immediately
        result = await self.cache_manager.remove(self.cache_name, key)
        if result:
            self._metrics["evictions"] += 1
            
        # Queue for removal from backing store
        self.write_queue[key] = (None, datetime.utcnow())  # None indicates deletion
        
        return result
        
    async def _write_loop(self):
        """Background task to write to backing store"""
        while True:
            try:
                await asyncio.sleep(self.write_delay.total_seconds())
                await self._flush_writes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in write-behind loop: {e}")
                
    async def _flush_writes(self):
        """Flush pending writes to backing store"""
        if not self.write_queue:
            return
            
        # Get items to write (up to batch size)
        items_to_write = []
        for key, (value, timestamp) in list(self.write_queue.items())[:self.batch_size]:
            items_to_write.append((key, value))
            del self.write_queue[key]
            
        # Write items
        for key, value in items_to_write:
            try:
                if asyncio.iscoroutinefunction(self.writer):
                    await self.writer(key, value)
                else:
                    self.writer(key, value)
            except Exception as e:
                logger.error(f"Failed to write key {key} to backing store: {e}")
                # Re-queue on failure
                self.write_queue[key] = (value, datetime.utcnow())


class RefreshAheadStrategy(BaseCacheStrategy):
    """
    Refresh-ahead pattern.
    
    Proactively refreshes entries before they expire.
    """
    
    def __init__(self, cache_manager, cache_name: str, loader: Callable,
                 refresh_threshold: float = 0.8,
                 refresh_interval: timedelta = timedelta(seconds=30)):
        super().__init__(cache_manager, cache_name)
        self.loader = loader
        self.refresh_threshold = refresh_threshold
        self.refresh_interval = refresh_interval
        self._refresh_queue: Set[str] = set()
        self._refresh_task: Optional[asyncio.Task] = None
        self._entry_metadata: Dict[str, CacheEntry] = {}
        
    async def start(self):
        """Start refresh-ahead task"""
        if not self._refresh_task:
            self._refresh_task = asyncio.create_task(self._refresh_loop())
            logger.info(f"Started refresh-ahead task for cache {self.cache_name}")
            
    async def stop(self):
        """Stop refresh-ahead task"""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            logger.info(f"Stopped refresh-ahead task for cache {self.cache_name}")
            
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with refresh-ahead pattern"""
        value = await self.cache_manager.get(self.cache_name, key)
        
        if value is not None:
            self._metrics["hits"] += 1
            
            # Check if needs refresh
            if key in self._entry_metadata:
                entry = self._entry_metadata[key]
                entry.touch()
                
                if entry.time_to_refresh:
                    self._refresh_queue.add(key)
                    
            return value
            
        self._metrics["misses"] += 1
        
        # Load value
        loader = loader or self.loader
        value = await loader(key) if asyncio.iscoroutinefunction(loader) else loader(key)
        
        if value is not None:
            await self.put(key, value)
            
        return value
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with refresh-ahead pattern"""
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        self._metrics["puts"] += 1
        
        # Track metadata
        self._entry_metadata[key] = CacheEntry(
            key=key,
            value=value,
            ttl=ttl
        )
        
    async def remove(self, key: str) -> bool:
        """Remove with refresh-ahead pattern"""
        result = await self.cache_manager.remove(self.cache_name, key)
        if result:
            self._metrics["evictions"] += 1
            
        # Remove from tracking
        self._entry_metadata.pop(key, None)
        self._refresh_queue.discard(key)
        
        return result
        
    async def _refresh_loop(self):
        """Background task to refresh entries"""
        while True:
            try:
                await asyncio.sleep(self.refresh_interval.total_seconds())
                await self._refresh_entries()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in refresh-ahead loop: {e}")
                
    async def _refresh_entries(self):
        """Refresh entries that are close to expiration"""
        if not self._refresh_queue:
            return
            
        # Process refresh queue
        keys_to_refresh = list(self._refresh_queue)
        self._refresh_queue.clear()
        
        for key in keys_to_refresh:
            try:
                # Load fresh value
                value = await self.loader(key) if asyncio.iscoroutinefunction(self.loader) else self.loader(key)
                
                if value is not None:
                    # Get TTL from metadata
                    ttl = None
                    if key in self._entry_metadata:
                        ttl = self._entry_metadata[key].ttl
                        
                    # Update cache
                    await self.put(key, value, ttl)
                    logger.debug(f"Refreshed cache entry for key: {key}")
                    
            except Exception as e:
                logger.error(f"Failed to refresh key {key}: {e}")


# Strategy factory
def create_cache_strategy(
    strategy: CacheStrategy,
    cache_manager,
    cache_name: str,
    **kwargs
) -> BaseCacheStrategy:
    """Create cache strategy instance"""
    if strategy == CacheStrategy.CACHE_ASIDE:
        return CacheAsideStrategy(cache_manager, cache_name)
        
    elif strategy == CacheStrategy.READ_THROUGH:
        loader = kwargs.get("loader")
        if not loader:
            raise ValueError("READ_THROUGH strategy requires a loader function")
        return ReadThroughStrategy(cache_manager, cache_name, loader)
        
    elif strategy == CacheStrategy.WRITE_THROUGH:
        writer = kwargs.get("writer")
        if not writer:
            raise ValueError("WRITE_THROUGH strategy requires a writer function")
        return WriteThroughStrategy(cache_manager, cache_name, writer)
        
    elif strategy == CacheStrategy.WRITE_BEHIND:
        writer = kwargs.get("writer")
        if not writer:
            raise ValueError("WRITE_BEHIND strategy requires a writer function")
        write_delay = kwargs.get("write_delay", timedelta(seconds=5))
        batch_size = kwargs.get("batch_size", 100)
        return WriteBehindStrategy(cache_manager, cache_name, writer, write_delay, batch_size)
        
    elif strategy == CacheStrategy.REFRESH_AHEAD:
        loader = kwargs.get("loader")
        if not loader:
            raise ValueError("REFRESH_AHEAD strategy requires a loader function")
        refresh_threshold = kwargs.get("refresh_threshold", 0.8)
        refresh_interval = kwargs.get("refresh_interval", timedelta(seconds=30))
        return RefreshAheadStrategy(cache_manager, cache_name, loader, refresh_threshold, refresh_interval)
        
    else:
        raise ValueError(f"Unknown cache strategy: {strategy}") 