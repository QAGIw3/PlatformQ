"""
Pricing Cache Layer

Multi-level caching system for high-performance pricing calculations
with memoization, TTL management, and predictive pre-caching.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from functools import lru_cache, wraps
import hashlib
import pickle
import numpy as np
from collections import OrderedDict
import threading
import redis
import msgpack

from platformq_shared.cache import CacheManager
from ignite import Cache as IgniteCache

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    created_at: datetime
    ttl: int  # seconds
    hit_count: int = 0
    last_accessed: datetime = field(default_factory=datetime.utcnow)
    computation_time: float = 0.0  # milliseconds
    
    def is_expired(self) -> bool:
        """Check if entry has expired"""
        if self.ttl == 0:  # No expiration
            return False
        return datetime.utcnow() > self.created_at + timedelta(seconds=self.ttl)
        
    def access(self):
        """Update access statistics"""
        self.hit_count += 1
        self.last_accessed = datetime.utcnow()


class LRUCache:
    """Thread-safe LRU cache implementation"""
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self.cache = OrderedDict()
        self.lock = threading.RLock()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
        
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get item from cache"""
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                entry = self.cache.pop(key)
                self.cache[key] = entry
                entry.access()
                self.stats['hits'] += 1
                return entry
            else:
                self.stats['misses'] += 1
                return None
                
    def put(self, key: str, entry: CacheEntry):
        """Put item in cache"""
        with self.lock:
            # Remove if already exists
            if key in self.cache:
                self.cache.pop(key)
                
            # Add to end
            self.cache[key] = entry
            
            # Evict if over capacity
            if len(self.cache) > self.max_size:
                # Remove least recently used
                oldest_key = next(iter(self.cache))
                self.cache.pop(oldest_key)
                self.stats['evictions'] += 1
                
    def clear(self):
        """Clear cache"""
        with self.lock:
            self.cache.clear()
            
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        with self.lock:
            total = self.stats['hits'] + self.stats['misses']
            hit_rate = self.stats['hits'] / total if total > 0 else 0
            return {
                **self.stats,
                'size': len(self.cache),
                'hit_rate': hit_rate
            }


class PricingCache:
    """Multi-level pricing cache with L1 (memory), L2 (Redis), L3 (Ignite)"""
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        ignite_config: Optional[Dict[str, Any]] = None,
        l1_size: int = 10000,
        default_ttl: int = 300  # 5 minutes
    ):
        # L1: In-memory LRU cache
        self.l1_cache = LRUCache(max_size=l1_size)
        
        # L2: Redis cache
        self.redis_client = redis.from_url(redis_url, decode_responses=False)
        
        # L3: Apache Ignite cache
        self.ignite_cache = None
        if ignite_config:
            try:
                from pyignite import Client
                self.ignite_client = Client()
                self.ignite_client.connect(ignite_config.get('host', 'localhost'), 
                                         ignite_config.get('port', 10800))
                self.ignite_cache = self.ignite_client.get_or_create_cache('pricing_cache')
            except Exception as e:
                logger.warning(f"Failed to connect to Ignite: {e}")
                
        self.default_ttl = default_ttl
        
        # Computation memoization
        self._memoized = {}
        
        # Predictive pre-caching
        self.access_patterns = OrderedDict()
        self.prefetch_queue = asyncio.Queue()
        
        # Background tasks
        self.background_tasks = []
        
    async def start(self):
        """Start background tasks"""
        # Start TTL cleaner
        task = asyncio.create_task(self._ttl_cleaner())
        self.background_tasks.append(task)
        
        # Start prefetch worker
        task = asyncio.create_task(self._prefetch_worker())
        self.background_tasks.append(task)
        
        logger.info("Pricing cache started")
        
    async def stop(self):
        """Stop background tasks"""
        for task in self.background_tasks:
            task.cancel()
            
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Close connections
        self.redis_client.close()
        if self.ignite_cache:
            self.ignite_client.close()
            
        logger.info("Pricing cache stopped")
        
    def cache_key(self, func_name: str, *args, **kwargs) -> str:
        """Generate cache key from function and arguments"""
        # Create hashable representation
        key_data = {
            'func': func_name,
            'args': args,
            'kwargs': sorted(kwargs.items())
        }
        
        # Use msgpack for efficient serialization
        serialized = msgpack.packb(key_data, use_bin_type=True)
        
        # Create hash
        return hashlib.sha256(serialized).hexdigest()
        
    async def get(
        self,
        func_name: str,
        args: tuple,
        kwargs: dict
    ) -> Optional[Any]:
        """Get value from cache (checks all levels)"""
        key = self.cache_key(func_name, *args, **kwargs)
        
        # Record access pattern
        self._record_access(key)
        
        # Check L1 (memory)
        entry = self.l1_cache.get(key)
        if entry and not entry.is_expired():
            logger.debug(f"L1 cache hit for {key}")
            return entry.value
            
        # Check L2 (Redis)
        try:
            redis_data = self.redis_client.get(f"pricing:{key}")
            if redis_data:
                entry = pickle.loads(redis_data)
                if not entry.is_expired():
                    logger.debug(f"L2 cache hit for {key}")
                    # Promote to L1
                    self.l1_cache.put(key, entry)
                    return entry.value
        except Exception as e:
            logger.error(f"Redis get error: {e}")
            
        # Check L3 (Ignite)
        if self.ignite_cache:
            try:
                ignite_data = self.ignite_cache.get(key)
                if ignite_data:
                    entry = pickle.loads(ignite_data)
                    if not entry.is_expired():
                        logger.debug(f"L3 cache hit for {key}")
                        # Promote to L1 and L2
                        self.l1_cache.put(key, entry)
                        self.redis_client.setex(
                            f"pricing:{key}",
                            entry.ttl,
                            pickle.dumps(entry)
                        )
                        return entry.value
            except Exception as e:
                logger.error(f"Ignite get error: {e}")
                
        logger.debug(f"Cache miss for {key}")
        return None
        
    async def put(
        self,
        func_name: str,
        args: tuple,
        kwargs: dict,
        value: Any,
        ttl: Optional[int] = None,
        computation_time: float = 0.0
    ):
        """Put value in cache (all levels)"""
        key = self.cache_key(func_name, *args, **kwargs)
        ttl = ttl or self.default_ttl
        
        # Create entry
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=datetime.utcnow(),
            ttl=ttl,
            computation_time=computation_time
        )
        
        # Store in L1
        self.l1_cache.put(key, entry)
        
        # Store in L2 (Redis)
        try:
            self.redis_client.setex(
                f"pricing:{key}",
                ttl,
                pickle.dumps(entry)
            )
        except Exception as e:
            logger.error(f"Redis put error: {e}")
            
        # Store in L3 (Ignite)
        if self.ignite_cache:
            try:
                self.ignite_cache.put(key, pickle.dumps(entry))
            except Exception as e:
                logger.error(f"Ignite put error: {e}")
                
    def memoize(
        self,
        ttl: Optional[int] = None,
        key_func: Optional[Callable] = None
    ):
        """Decorator for memoizing pricing functions"""
        def decorator(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                # Get from cache
                cached = await self.get(func.__name__, args, kwargs)
                if cached is not None:
                    return cached
                    
                # Compute value
                start_time = datetime.utcnow()
                result = await func(*args, **kwargs)
                computation_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                # Store in cache
                await self.put(
                    func.__name__,
                    args,
                    kwargs,
                    result,
                    ttl=ttl,
                    computation_time=computation_time
                )
                
                return result
                
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                # For sync functions, use L1 cache only
                key = self.cache_key(func.__name__, *args, **kwargs)
                
                entry = self.l1_cache.get(key)
                if entry and not entry.is_expired():
                    return entry.value
                    
                # Compute value
                start_time = datetime.utcnow()
                result = func(*args, **kwargs)
                computation_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                # Store in L1
                entry = CacheEntry(
                    key=key,
                    value=result,
                    created_at=datetime.utcnow(),
                    ttl=ttl or self.default_ttl,
                    computation_time=computation_time
                )
                self.l1_cache.put(key, entry)
                
                return result
                
            # Return appropriate wrapper
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
                
        return decorator
        
    def _record_access(self, key: str):
        """Record access pattern for predictive caching"""
        now = datetime.utcnow()
        
        if key in self.access_patterns:
            pattern = self.access_patterns.pop(key)
            pattern['accesses'].append(now)
            pattern['count'] += 1
        else:
            pattern = {
                'accesses': [now],
                'count': 1
            }
            
        self.access_patterns[key] = pattern
        
        # Limit pattern history
        if len(self.access_patterns) > 1000:
            # Remove oldest
            self.access_patterns.popitem(last=False)
            
    async def prefetch(self, func_name: str, args_list: List[Tuple]):
        """Prefetch multiple values"""
        tasks = []
        
        for args in args_list:
            # Check if already cached
            cached = await self.get(func_name, args, {})
            if cached is None:
                # Add to prefetch queue
                await self.prefetch_queue.put((func_name, args))
                
    async def _prefetch_worker(self):
        """Background worker for prefetching"""
        while True:
            try:
                # Get prefetch request
                func_name, args = await self.prefetch_queue.get()
                
                # This would call the actual pricing function
                # For now, just log
                logger.debug(f"Prefetching {func_name} with args {args}")
                
                await asyncio.sleep(0.1)  # Rate limit
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Prefetch error: {e}")
                
    async def _ttl_cleaner(self):
        """Background task to clean expired entries"""
        while True:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Clean L1 cache
                expired_keys = []
                for key, entry in self.l1_cache.cache.items():
                    if entry.is_expired():
                        expired_keys.append(key)
                        
                for key in expired_keys:
                    self.l1_cache.cache.pop(key, None)
                    
                if expired_keys:
                    logger.info(f"Cleaned {len(expired_keys)} expired L1 entries")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"TTL cleaner error: {e}")
                
    def invalidate(self, pattern: Optional[str] = None):
        """Invalidate cache entries"""
        if pattern:
            # Invalidate by pattern
            # L1
            keys_to_remove = [k for k in self.l1_cache.cache.keys() if pattern in k]
            for key in keys_to_remove:
                self.l1_cache.cache.pop(key, None)
                
            # L2
            try:
                for key in self.redis_client.scan_iter(match=f"pricing:*{pattern}*"):
                    self.redis_client.delete(key)
            except Exception as e:
                logger.error(f"Redis invalidate error: {e}")
                
            logger.info(f"Invalidated entries matching pattern: {pattern}")
        else:
            # Clear all
            self.l1_cache.clear()
            try:
                self.redis_client.flushdb()
            except Exception as e:
                logger.error(f"Redis flush error: {e}")
                
            if self.ignite_cache:
                try:
                    self.ignite_cache.clear()
                except Exception as e:
                    logger.error(f"Ignite clear error: {e}")
                    
            logger.info("Cleared all cache entries")
            
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        stats = {
            'l1': self.l1_cache.get_stats(),
            'l2': {},
            'l3': {},
            'prefetch_queue_size': self.prefetch_queue.qsize()
        }
        
        # Redis stats
        try:
            info = self.redis_client.info()
            stats['l2'] = {
                'keys': self.redis_client.dbsize(),
                'memory_used_mb': info.get('used_memory', 0) / (1024 * 1024),
                'hit_rate': info.get('keyspace_hits', 0) / 
                           (info.get('keyspace_hits', 0) + info.get('keyspace_misses', 1))
            }
        except Exception as e:
            logger.error(f"Redis stats error: {e}")
            
        # Ignite stats
        if self.ignite_cache:
            try:
                stats['l3'] = {
                    'size': self.ignite_cache.get_size()
                }
            except Exception as e:
                logger.error(f"Ignite stats error: {e}")
                
        return stats


# Specific pricing calculators with caching

class CachedBlackScholes:
    """Black-Scholes pricing with intelligent caching"""
    
    def __init__(self, cache: PricingCache):
        self.cache = cache
        
    @lru_cache(maxsize=10000)
    def _norm_cdf(self, x: float) -> float:
        """Cached normal CDF calculation"""
        from scipy.stats import norm
        return norm.cdf(x)
        
    async def calculate_option_price(
        self,
        spot: float,
        strike: float,
        time_to_maturity: float,
        volatility: float,
        risk_free_rate: float,
        option_type: str = "CALL"
    ) -> float:
        """Calculate option price with caching"""
        
        # Check cache first
        @self.cache.memoize(ttl=300)  # 5 minute TTL
        async def _calculate():
            # Black-Scholes calculation
            from math import log, sqrt, exp
            
            d1 = (log(spot / strike) + (risk_free_rate + 0.5 * volatility ** 2) * time_to_maturity) / (volatility * sqrt(time_to_maturity))
            d2 = d1 - volatility * sqrt(time_to_maturity)
            
            if option_type == "CALL":
                price = spot * self._norm_cdf(d1) - strike * exp(-risk_free_rate * time_to_maturity) * self._norm_cdf(d2)
            else:
                price = strike * exp(-risk_free_rate * time_to_maturity) * self._norm_cdf(-d2) - spot * self._norm_cdf(-d1)
                
            return price
            
        return await _calculate()
        
    async def calculate_greeks(
        self,
        spot: float,
        strike: float,
        time_to_maturity: float,
        volatility: float,
        risk_free_rate: float,
        option_type: str = "CALL"
    ) -> Dict[str, float]:
        """Calculate option Greeks with caching"""
        
        @self.cache.memoize(ttl=300)
        async def _calculate():
            from math import log, sqrt, exp, pi
            from scipy.stats import norm
            
            d1 = (log(spot / strike) + (risk_free_rate + 0.5 * volatility ** 2) * time_to_maturity) / (volatility * sqrt(time_to_maturity))
            d2 = d1 - volatility * sqrt(time_to_maturity)
            
            # Delta
            if option_type == "CALL":
                delta = norm.cdf(d1)
            else:
                delta = norm.cdf(d1) - 1
                
            # Gamma
            gamma = norm.pdf(d1) / (spot * volatility * sqrt(time_to_maturity))
            
            # Theta
            if option_type == "CALL":
                theta = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(time_to_maturity)) -
                        risk_free_rate * strike * exp(-risk_free_rate * time_to_maturity) * norm.cdf(d2))
            else:
                theta = (-spot * norm.pdf(d1) * volatility / (2 * sqrt(time_to_maturity)) +
                        risk_free_rate * strike * exp(-risk_free_rate * time_to_maturity) * norm.cdf(-d2))
                        
            # Vega
            vega = spot * norm.pdf(d1) * sqrt(time_to_maturity)
            
            # Rho
            if option_type == "CALL":
                rho = strike * time_to_maturity * exp(-risk_free_rate * time_to_maturity) * norm.cdf(d2)
            else:
                rho = -strike * time_to_maturity * exp(-risk_free_rate * time_to_maturity) * norm.cdf(-d2)
                
            return {
                'delta': delta,
                'gamma': gamma,
                'theta': theta / 365,  # Per day
                'vega': vega / 100,    # Per 1% vol change
                'rho': rho / 100       # Per 1% rate change
            }
            
        return await _calculate()


class CachedVolatilitySurface:
    """Volatility surface with intelligent caching and interpolation"""
    
    def __init__(self, cache: PricingCache):
        self.cache = cache
        self.surface_cache = {}  # In-memory surface cache
        
    async def get_implied_volatility(
        self,
        strike: float,
        time_to_maturity: float,
        surface_id: str = "default"
    ) -> float:
        """Get implied volatility with caching and interpolation"""
        
        # Try exact match first
        @self.cache.memoize(ttl=600)  # 10 minute TTL
        async def _get_exact():
            # This would fetch from database or calculate
            # For now, return a dummy value
            return 0.2 + 0.1 * abs(strike - 100) / 100
            
        # Check if we need interpolation
        cache_key = f"{surface_id}:{strike:.2f}:{time_to_maturity:.4f}"
        
        if cache_key in self.surface_cache:
            return self.surface_cache[cache_key]
            
        # Get surrounding points for interpolation
        vol = await _get_exact()
        
        # Cache the result
        self.surface_cache[cache_key] = vol
        
        # Limit cache size
        if len(self.surface_cache) > 10000:
            # Remove oldest entries
            keys_to_remove = list(self.surface_cache.keys())[:1000]
            for key in keys_to_remove:
                del self.surface_cache[key]
                
        return vol 