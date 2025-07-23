"""
Cache patterns for DataIntelligenceSuite

Implements common caching patterns and strategies.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, Callable, List, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class CachePattern(Enum):
    """Common cache patterns"""
    CACHE_ASIDE = "cache_aside"
    READ_THROUGH = "read_through"
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    REFRESH_AHEAD = "refresh_ahead"
    CACHE_WARMING = "cache_warming"


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    created_at: datetime
    accessed_at: datetime
    ttl: Optional[timedelta] = None
    access_count: int = 0
    
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


class CacheStrategy(ABC):
    """Base class for cache strategies"""
    
    def __init__(self, cache_manager, cache_name: str):
        self.cache_manager = cache_manager
        self.cache_name = cache_name
        
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


class CacheAsideStrategy(CacheStrategy):
    """
    Cache-aside (lazy loading) pattern.
    
    Application manages cache population.
    """
    
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with cache-aside pattern"""
        # Try cache first
        value = await self.cache_manager.get(self.cache_name, key)
        
        if value is not None:
            return value
            
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
        
    async def remove(self, key: str) -> bool:
        """Remove with cache-aside pattern"""
        return await self.cache_manager.remove(self.cache_name, key)


class ReadThroughStrategy(CacheStrategy):
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
            return value
            
        # Use configured loader or provided one
        loader = loader or self.loader
        value = await loader(key) if asyncio.iscoroutinefunction(loader) else loader(key)
        
        if value is not None:
            await self.put(key, value)
            
        return value
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with read-through pattern"""
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        
    async def remove(self, key: str) -> bool:
        """Remove with read-through pattern"""
        return await self.cache_manager.remove(self.cache_name, key)


class WriteThroughStrategy(CacheStrategy):
    """
    Write-through pattern.
    
    Writes go to cache and backing store synchronously.
    """
    
    def __init__(self, cache_manager, cache_name: str, writer: Callable):
        super().__init__(cache_manager, cache_name)
        self.writer = writer
        
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with write-through pattern"""
        return await self.cache_manager.get(self.cache_name, key)
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with write-through pattern"""
        # Write to backing store first
        if asyncio.iscoroutinefunction(self.writer):
            await self.writer(key, value)
        else:
            self.writer(key, value)
            
        # Then update cache
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        
    async def remove(self, key: str) -> bool:
        """Remove with write-through pattern"""
        # Remove from backing store first
        if asyncio.iscoroutinefunction(self.writer):
            await self.writer(key, None)  # None indicates deletion
        else:
            self.writer(key, None)
            
        # Then remove from cache
        return await self.cache_manager.remove(self.cache_name, key)


class WriteBehindStrategy(CacheStrategy):
    """
    Write-behind (write-back) pattern.
    
    Writes go to cache immediately, backing store updated asynchronously.
    """
    
    def __init__(self, cache_manager, cache_name: str, writer: Callable, 
                 write_delay: timedelta = timedelta(seconds=5)):
        super().__init__(cache_manager, cache_name)
        self.writer = writer
        self.write_delay = write_delay
        self.write_queue: Dict[str, Any] = {}
        self.write_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start write-behind task"""
        if not self.write_task:
            self.write_task = asyncio.create_task(self._write_loop())
            
    async def stop(self):
        """Stop write-behind task"""
        if self.write_task:
            self.write_task.cancel()
            await self.write_task
            
        # Flush remaining writes
        await self._flush_writes()
        
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with write-behind pattern"""
        # Check write queue first
        if key in self.write_queue:
            return self.write_queue[key]
            
        return await self.cache_manager.get(self.cache_name, key)
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with write-behind pattern"""
        # Update cache immediately
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        
        # Queue for write to backing store
        self.write_queue[key] = value
        
    async def remove(self, key: str) -> bool:
        """Remove with write-behind pattern"""
        # Remove from cache immediately
        result = await self.cache_manager.remove(self.cache_name, key)
        
        # Queue for removal from backing store
        self.write_queue[key] = None  # None indicates deletion
        
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
                logger.error(f"Write-behind error: {e}")
                
    async def _flush_writes(self):
        """Flush pending writes to backing store"""
        if not self.write_queue:
            return
            
        # Copy and clear queue
        writes = self.write_queue.copy()
        self.write_queue.clear()
        
        # Write to backing store
        for key, value in writes.items():
            try:
                if asyncio.iscoroutinefunction(self.writer):
                    await self.writer(key, value)
                else:
                    self.writer(key, value)
            except Exception as e:
                logger.error(f"Failed to write {key} to backing store: {e}")
                # Re-queue failed writes
                self.write_queue[key] = value


class RefreshAheadStrategy(CacheStrategy):
    """
    Refresh-ahead pattern.
    
    Proactively refreshes cache entries before expiration.
    """
    
    def __init__(self, cache_manager, cache_name: str, loader: Callable,
                 refresh_threshold: float = 0.8):
        super().__init__(cache_manager, cache_name)
        self.loader = loader
        self.refresh_threshold = refresh_threshold
        self.refresh_queue: Set[str] = set()
        self.refresh_task: Optional[asyncio.Task] = None
        self.entries: Dict[str, CacheEntry] = {}
        
    async def start(self):
        """Start refresh-ahead task"""
        if not self.refresh_task:
            self.refresh_task = asyncio.create_task(self._refresh_loop())
            
    async def stop(self):
        """Stop refresh-ahead task"""
        if self.refresh_task:
            self.refresh_task.cancel()
            await self.refresh_task
            
    async def get(self, key: str, loader: Optional[Callable] = None) -> Optional[Any]:
        """Get with refresh-ahead pattern"""
        value = await self.cache_manager.get(self.cache_name, key)
        
        if value is not None:
            # Update access time
            if key in self.entries:
                self.entries[key].accessed_at = datetime.utcnow()
                self.entries[key].access_count += 1
                
                # Check if refresh needed
                if self.entries[key].time_to_refresh:
                    self.refresh_queue.add(key)
                    
            return value
            
        # Load if not found
        loader = loader or self.loader
        value = await loader(key) if asyncio.iscoroutinefunction(loader) else loader(key)
        
        if value is not None:
            await self.put(key, value)
            
        return value
        
    async def put(self, key: str, value: Any, ttl: Optional[timedelta] = None) -> None:
        """Put with refresh-ahead pattern"""
        await self.cache_manager.put(self.cache_name, key, value, ttl)
        
        # Track entry
        self.entries[key] = CacheEntry(
            key=key,
            value=value,
            created_at=datetime.utcnow(),
            accessed_at=datetime.utcnow(),
            ttl=ttl
        )
        
    async def remove(self, key: str) -> bool:
        """Remove with refresh-ahead pattern"""
        # Remove from tracking
        self.entries.pop(key, None)
        self.refresh_queue.discard(key)
        
        return await self.cache_manager.remove(self.cache_name, key)
        
    async def _refresh_loop(self):
        """Background task to refresh entries"""
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                # Find entries needing refresh
                now = datetime.utcnow()
                for key, entry in self.entries.items():
                    if entry.time_to_refresh and key not in self.refresh_queue:
                        self.refresh_queue.add(key)
                        
                # Refresh queued entries
                if self.refresh_queue:
                    await self._refresh_entries()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Refresh-ahead error: {e}")
                
    async def _refresh_entries(self):
        """Refresh entries in queue"""
        # Copy and clear queue
        keys = list(self.refresh_queue)
        self.refresh_queue.clear()
        
        for key in keys:
            try:
                # Reload from source
                value = await self.loader(key) if asyncio.iscoroutinefunction(self.loader) else self.loader(key)
                
                if value is not None:
                    # Get current TTL
                    ttl = self.entries[key].ttl if key in self.entries else None
                    
                    # Update cache
                    await self.cache_manager.put(self.cache_name, key, value, ttl)
                    
                    # Update tracking
                    if key in self.entries:
                        self.entries[key].value = value
                        self.entries[key].created_at = datetime.utcnow()
                        
                    logger.debug(f"Refreshed cache entry: {key}")
                    
            except Exception as e:
                logger.error(f"Failed to refresh {key}: {e}") 