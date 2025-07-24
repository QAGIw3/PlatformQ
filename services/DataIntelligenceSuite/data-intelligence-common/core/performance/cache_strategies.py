"""
Advanced Caching Strategies

Provides sophisticated caching strategies for performance optimization.
"""

from typing import Dict, Any, Optional, List, Union, Callable, TypeVar, Generic
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import asyncio
import hashlib
import pickle
import json
from collections import OrderedDict
import threading
import weakref

from ..caching import CacheManager
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


class CacheStrategy(str, Enum):
    """Cache strategies"""
    LRU = "lru"              # Least Recently Used
    LFU = "lfu"              # Least Frequently Used
    FIFO = "fifo"            # First In First Out
    TTL = "ttl"              # Time To Live
    ADAPTIVE = "adaptive"    # Adaptive replacement
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"
    REFRESH_AHEAD = "refresh_ahead"


class EvictionPolicy(str, Enum):
    """Cache eviction policies"""
    SIZE_BASED = "size_based"
    TIME_BASED = "time_based"
    FREQUENCY_BASED = "frequency_based"
    ADAPTIVE = "adaptive"


@dataclass
class CacheConfig:
    """Cache configuration"""
    strategy: CacheStrategy = CacheStrategy.LRU
    max_size: int = 1000
    ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Eviction settings
    eviction_policy: EvictionPolicy = EvictionPolicy.SIZE_BASED
    eviction_batch_size: int = 10
    
    # Performance settings
    enable_stats: bool = True
    enable_compression: bool = False
    compression_threshold: int = 1024  # bytes
    
    # Refresh settings
    refresh_interval: Optional[timedelta] = None
    refresh_jitter: float = 0.1  # 10% jitter
    
    # Persistence
    enable_persistence: bool = False
    persistence_path: Optional[str] = None


@dataclass
class CacheEntry(Generic[V]):
    """Cache entry with metadata"""
    key: str
    value: V
    created_at: datetime
    accessed_at: datetime
    access_count: int = 0
    size: int = 0
    ttl: Optional[timedelta] = None
    
    def is_expired(self) -> bool:
        """Check if entry is expired"""
        if self.ttl is None:
            return False
        return datetime.utcnow() > self.created_at + self.ttl
        
    def touch(self):
        """Update access time and count"""
        self.accessed_at = datetime.utcnow()
        self.access_count += 1


class CacheStats:
    """Cache statistics"""
    
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0
        self.refreshes = 0
        self._lock = threading.Lock()
        
    def record_hit(self):
        with self._lock:
            self.hits += 1
            
    def record_miss(self):
        with self._lock:
            self.misses += 1
            
    def record_eviction(self):
        with self._lock:
            self.evictions += 1
            
    def record_expiration(self):
        with self._lock:
            self.expirations += 1
            
    def record_refresh(self):
        with self._lock:
            self.refreshes += 1
            
    def get_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0
        
    def get_stats(self) -> Dict[str, Any]:
        """Get all statistics"""
        with self._lock:
            return {
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': self.get_hit_rate(),
                'evictions': self.evictions,
                'expirations': self.expirations,
                'refreshes': self.refreshes
            }


class BaseCacheStrategy(ABC):
    """Base cache strategy interface"""
    
    @abstractmethod
    async def get(self, key: K) -> Optional[V]:
        """Get value from cache"""
        pass
        
    @abstractmethod
    async def put(self, key: K, value: V, ttl: Optional[timedelta] = None):
        """Put value in cache"""
        pass
        
    @abstractmethod
    async def evict(self, count: int = 1) -> List[K]:
        """Evict entries from cache"""
        pass
        
    @abstractmethod
    async def clear(self):
        """Clear all cache entries"""
        pass


class LRUCache(BaseCacheStrategy[K, V]):
    """Least Recently Used cache implementation"""
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self._cache: OrderedDict[K, CacheEntry[V]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._stats = CacheStats() if config.enable_stats else None
        
    async def get(self, key: K) -> Optional[V]:
        """Get value from cache"""
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                if self._stats:
                    self._stats.record_miss()
                return None
                
            # Check expiration
            if entry.is_expired():
                del self._cache[key]
                if self._stats:
                    self._stats.record_expiration()
                return None
                
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            entry.touch()
            
            if self._stats:
                self._stats.record_hit()
                
            return entry.value
            
    async def put(self, key: K, value: V, ttl: Optional[timedelta] = None):
        """Put value in cache"""
        async with self._lock:
            # Calculate size
            size = self._calculate_size(value)
            
            # Create entry
            entry = CacheEntry(
                key=str(key),
                value=value,
                created_at=datetime.utcnow(),
                accessed_at=datetime.utcnow(),
                size=size,
                ttl=ttl or self.config.ttl
            )
            
            # Add to cache
            self._cache[key] = entry
            self._cache.move_to_end(key)
            
            # Evict if needed
            while len(self._cache) > self.config.max_size:
                await self.evict()
                
    async def evict(self, count: int = 1) -> List[K]:
        """Evict least recently used entries"""
        evicted = []
        
        async with self._lock:
            for _ in range(min(count, len(self._cache))):
                if self._cache:
                    # Remove oldest (first) item
                    key, entry = self._cache.popitem(last=False)
                    evicted.append(key)
                    
                    if self._stats:
                        self._stats.record_eviction()
                        
        return evicted
        
    async def clear(self):
        """Clear all cache entries"""
        async with self._lock:
            self._cache.clear()
            
    def _calculate_size(self, value: V) -> int:
        """Calculate approximate size of value"""
        try:
            return len(pickle.dumps(value))
        except:
            return 0


class LFUCache(BaseCacheStrategy[K, V]):
    """Least Frequently Used cache implementation"""
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self._cache: Dict[K, CacheEntry[V]] = {}
        self._frequency_list: Dict[int, OrderedDict[K, bool]] = {}
        self._min_frequency = 0
        self._lock = asyncio.Lock()
        self._stats = CacheStats() if config.enable_stats else None
        
    async def get(self, key: K) -> Optional[V]:
        """Get value from cache"""
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                if self._stats:
                    self._stats.record_miss()
                return None
                
            # Check expiration
            if entry.is_expired():
                await self._remove_entry(key, entry)
                if self._stats:
                    self._stats.record_expiration()
                return None
                
            # Update frequency
            self._update_frequency(key, entry)
            
            if self._stats:
                self._stats.record_hit()
                
            return entry.value
            
    async def put(self, key: K, value: V, ttl: Optional[timedelta] = None):
        """Put value in cache"""
        async with self._lock:
            # Update existing entry
            if key in self._cache:
                entry = self._cache[key]
                entry.value = value
                entry.ttl = ttl or self.config.ttl
                self._update_frequency(key, entry)
                return
                
            # Evict if at capacity
            if len(self._cache) >= self.config.max_size:
                await self.evict()
                
            # Create new entry
            entry = CacheEntry(
                key=str(key),
                value=value,
                created_at=datetime.utcnow(),
                accessed_at=datetime.utcnow(),
                access_count=1,
                size=self._calculate_size(value),
                ttl=ttl or self.config.ttl
            )
            
            self._cache[key] = entry
            
            # Add to frequency list
            if 1 not in self._frequency_list:
                self._frequency_list[1] = OrderedDict()
            self._frequency_list[1][key] = True
            self._min_frequency = 1
            
    async def evict(self, count: int = 1) -> List[K]:
        """Evict least frequently used entries"""
        evicted = []
        
        async with self._lock:
            for _ in range(count):
                if not self._cache:
                    break
                    
                # Get least frequent list
                if self._min_frequency in self._frequency_list:
                    freq_list = self._frequency_list[self._min_frequency]
                    
                    if freq_list:
                        # Remove oldest from least frequent
                        key = next(iter(freq_list))
                        entry = self._cache[key]
                        
                        await self._remove_entry(key, entry)
                        evicted.append(key)
                        
                        if self._stats:
                            self._stats.record_eviction()
                            
        return evicted
        
    async def clear(self):
        """Clear all cache entries"""
        async with self._lock:
            self._cache.clear()
            self._frequency_list.clear()
            self._min_frequency = 0
            
    def _update_frequency(self, key: K, entry: CacheEntry[V]):
        """Update entry frequency"""
        old_freq = entry.access_count
        entry.touch()
        new_freq = entry.access_count
        
        # Remove from old frequency list
        if old_freq in self._frequency_list and key in self._frequency_list[old_freq]:
            del self._frequency_list[old_freq][key]
            
            # Clean up empty frequency list
            if not self._frequency_list[old_freq]:
                del self._frequency_list[old_freq]
                
                # Update min frequency if needed
                if old_freq == self._min_frequency:
                    self._min_frequency = min(self._frequency_list.keys()) if self._frequency_list else 0
                    
        # Add to new frequency list
        if new_freq not in self._frequency_list:
            self._frequency_list[new_freq] = OrderedDict()
        self._frequency_list[new_freq][key] = True
        
    async def _remove_entry(self, key: K, entry: CacheEntry[V]):
        """Remove entry from cache and frequency list"""
        del self._cache[key]
        
        freq = entry.access_count
        if freq in self._frequency_list and key in self._frequency_list[freq]:
            del self._frequency_list[freq][key]
            
            if not self._frequency_list[freq]:
                del self._frequency_list[freq]
                
                if freq == self._min_frequency:
                    self._min_frequency = min(self._frequency_list.keys()) if self._frequency_list else 0
                    
    def _calculate_size(self, value: V) -> int:
        """Calculate approximate size of value"""
        try:
            return len(pickle.dumps(value))
        except:
            return 0


class AdaptiveCache(BaseCacheStrategy[K, V]):
    """
    Adaptive Replacement Cache (ARC) implementation.
    
    Balances between recency and frequency.
    """
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self.c = config.max_size  # Cache size
        self.p = 0  # Target size for T1
        
        # Ghost lists (only keys)
        self.b1: OrderedDict[K, bool] = OrderedDict()  # LRU ghost
        self.b2: OrderedDict[K, bool] = OrderedDict()  # LFU ghost
        
        # Cache lists
        self.t1: OrderedDict[K, CacheEntry[V]] = OrderedDict()  # LRU
        self.t2: OrderedDict[K, CacheEntry[V]] = OrderedDict()  # LFU
        
        self._lock = asyncio.Lock()
        self._stats = CacheStats() if config.enable_stats else None
        
    async def get(self, key: K) -> Optional[V]:
        """Get value from cache"""
        async with self._lock:
            # Check T1 (recent)
            if key in self.t1:
                entry = self.t1[key]
                
                if entry.is_expired():
                    del self.t1[key]
                    if self._stats:
                        self._stats.record_expiration()
                    return None
                    
                # Move to T2 (frequent)
                del self.t1[key]
                self.t2[key] = entry
                entry.touch()
                
                if self._stats:
                    self._stats.record_hit()
                    
                return entry.value
                
            # Check T2 (frequent)
            if key in self.t2:
                entry = self.t2[key]
                
                if entry.is_expired():
                    del self.t2[key]
                    if self._stats:
                        self._stats.record_expiration()
                    return None
                    
                # Move to end of T2
                self.t2.move_to_end(key)
                entry.touch()
                
                if self._stats:
                    self._stats.record_hit()
                    
                return entry.value
                
            if self._stats:
                self._stats.record_miss()
                
            return None
            
    async def put(self, key: K, value: V, ttl: Optional[timedelta] = None):
        """Put value in cache"""
        async with self._lock:
            # Case 1: Key in T1 ∪ T2 (cache hit)
            if key in self.t1 or key in self.t2:
                # Update value
                if key in self.t1:
                    self.t1[key].value = value
                    self.t1[key].ttl = ttl or self.config.ttl
                else:
                    self.t2[key].value = value
                    self.t2[key].ttl = ttl or self.config.ttl
                return
                
            # Case 2: Key in B1 (recent ghost)
            if key in self.b1:
                # Adapt p
                delta = 1 if len(self.b1) >= len(self.b2) else len(self.b2) // len(self.b1)
                self.p = min(self.p + delta, self.c)
                
                # Replace
                await self._replace(key, self.p)
                
                # Move from B1 to T2
                del self.b1[key]
                entry = self._create_entry(key, value, ttl)
                self.t2[key] = entry
                
                return
                
            # Case 3: Key in B2 (frequent ghost)
            if key in self.b2:
                # Adapt p
                delta = 1 if len(self.b2) >= len(self.b1) else len(self.b1) // len(self.b2)
                self.p = max(self.p - delta, 0)
                
                # Replace
                await self._replace(key, self.p)
                
                # Move from B2 to T2
                del self.b2[key]
                entry = self._create_entry(key, value, ttl)
                self.t2[key] = entry
                
                return
                
            # Case 4: Key not in cache or ghosts
            if len(self.t1) + len(self.t2) >= self.c:
                # Cache full, need to evict
                if len(self.t1) + len(self.b1) >= self.c:
                    # Evict from B1
                    if self.b1:
                        self.b1.popitem(last=False)
                elif len(self.t1) + len(self.t2) + len(self.b1) + len(self.b2) >= 2 * self.c:
                    # Evict from B2
                    if self.b2:
                        self.b2.popitem(last=False)
                        
                # Replace
                await self._replace(key, self.p)
                
            # Add to T1
            entry = self._create_entry(key, value, ttl)
            self.t1[key] = entry
            
    async def evict(self, count: int = 1) -> List[K]:
        """Evict entries based on adaptive policy"""
        evicted = []
        
        async with self._lock:
            for _ in range(count):
                if len(self.t1) + len(self.t2) == 0:
                    break
                    
                # Decide which list to evict from
                if len(self.t1) >= max(1, self.p):
                    # Evict from T1
                    if self.t1:
                        key, entry = self.t1.popitem(last=False)
                        self.b1[key] = True
                        evicted.append(key)
                else:
                    # Evict from T2
                    if self.t2:
                        key, entry = self.t2.popitem(last=False)
                        self.b2[key] = True
                        evicted.append(key)
                        
                if self._stats:
                    self._stats.record_eviction()
                    
        return evicted
        
    async def clear(self):
        """Clear all cache entries"""
        async with self._lock:
            self.t1.clear()
            self.t2.clear()
            self.b1.clear()
            self.b2.clear()
            self.p = 0
            
    async def _replace(self, key: K, p: int):
        """Replace entry in cache"""
        if len(self.t1) >= max(1, p):
            # Evict from T1
            if self.t1:
                old_key, _ = self.t1.popitem(last=False)
                self.b1[old_key] = True
        else:
            # Evict from T2
            if self.t2:
                old_key, _ = self.t2.popitem(last=False)
                self.b2[old_key] = True
                
    def _create_entry(self, key: K, value: V, ttl: Optional[timedelta]) -> CacheEntry[V]:
        """Create cache entry"""
        return CacheEntry(
            key=str(key),
            value=value,
            created_at=datetime.utcnow(),
            accessed_at=datetime.utcnow(),
            size=self._calculate_size(value),
            ttl=ttl or self.config.ttl
        )
        
    def _calculate_size(self, value: V) -> int:
        """Calculate approximate size of value"""
        try:
            return len(pickle.dumps(value))
        except:
            return 0


class RefreshAheadCache(BaseCacheStrategy[K, V]):
    """
    Refresh-ahead cache that proactively refreshes entries.
    """
    
    def __init__(
        self,
        config: CacheConfig,
        refresh_func: Callable[[K], asyncio.Future[V]]
    ):
        self.config = config
        self.refresh_func = refresh_func
        self._base_cache = LRUCache(config)
        self._refresh_tasks: Dict[K, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        
    async def get(self, key: K) -> Optional[V]:
        """Get value from cache"""
        value = await self._base_cache.get(key)
        
        if value is not None:
            # Check if refresh needed
            async with self._lock:
                entry = self._base_cache._cache.get(key)
                if entry:
                    age = datetime.utcnow() - entry.created_at
                    ttl = entry.ttl or self.config.ttl
                    
                    # Refresh if > 80% of TTL
                    if age > ttl * 0.8 and key not in self._refresh_tasks:
                        task = asyncio.create_task(self._refresh_entry(key))
                        self._refresh_tasks[key] = task
                        
        return value
        
    async def put(self, key: K, value: V, ttl: Optional[timedelta] = None):
        """Put value in cache"""
        await self._base_cache.put(key, value, ttl)
        
    async def evict(self, count: int = 1) -> List[K]:
        """Evict entries from cache"""
        evicted = await self._base_cache.evict(count)
        
        # Cancel refresh tasks for evicted keys
        async with self._lock:
            for key in evicted:
                if key in self._refresh_tasks:
                    self._refresh_tasks[key].cancel()
                    del self._refresh_tasks[key]
                    
        return evicted
        
    async def clear(self):
        """Clear all cache entries"""
        await self._base_cache.clear()
        
        # Cancel all refresh tasks
        async with self._lock:
            for task in self._refresh_tasks.values():
                task.cancel()
            self._refresh_tasks.clear()
            
    async def _refresh_entry(self, key: K):
        """Refresh a cache entry"""
        try:
            # Add jitter to prevent thundering herd
            if self.config.refresh_jitter > 0:
                import random
                jitter = random.uniform(0, self.config.refresh_jitter)
                await asyncio.sleep(jitter)
                
            # Refresh value
            value = await self.refresh_func(key)
            
            # Update cache
            await self.put(key, value)
            
            if self._base_cache._stats:
                self._base_cache._stats.record_refresh()
                
        except Exception as e:
            logger.error(f"Failed to refresh cache entry {key}: {e}")
        finally:
            # Remove from refresh tasks
            async with self._lock:
                self._refresh_tasks.pop(key, None)


class CacheFactory:
    """Factory for creating cache instances"""
    
    @staticmethod
    def create(
        config: CacheConfig,
        refresh_func: Optional[Callable] = None
    ) -> BaseCacheStrategy:
        """Create cache instance based on strategy"""
        if config.strategy == CacheStrategy.LRU:
            return LRUCache(config)
        elif config.strategy == CacheStrategy.LFU:
            return LFUCache(config)
        elif config.strategy == CacheStrategy.ADAPTIVE:
            return AdaptiveCache(config)
        elif config.strategy == CacheStrategy.REFRESH_AHEAD:
            if refresh_func is None:
                raise ValueError("Refresh function required for refresh-ahead cache")
            return RefreshAheadCache(config, refresh_func)
        else:
            raise ValueError(f"Unknown cache strategy: {config.strategy}")


# Export main components
__all__ = [
    'CacheStrategy',
    'EvictionPolicy',
    'CacheConfig',
    'CacheEntry',
    'CacheStats',
    'BaseCacheStrategy',
    'LRUCache',
    'LFUCache',
    'AdaptiveCache',
    'RefreshAheadCache',
    'CacheFactory'
] 