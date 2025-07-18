"""
Optimized Apache Ignite Integration

High-performance caching and distributed computing with connection pooling
"""

from typing import Dict, List, Optional, Any, Callable, Set, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import asyncio
import logging
import json
from dataclasses import dataclass
from collections import defaultdict
import pyignite
from pyignite.datatypes import (
    String, IntObject, LongObject, 
    DecimalObject, TimestampObject, BoolObject
)
from functools import wraps

from app.engines.performance_optimizer import (
    IgnitePerformanceOptimizer,
    CacheConfig,
    OptimizedMarketDataCache,
    OptimizedOrderBookCache
)

logger = logging.getLogger(__name__)


class OptimizedIgniteCache:
    """Optimized Ignite cache with advanced features"""
    
    def __init__(self, nodes: List[str] = None):
        self.nodes = nodes or ['ignite:10800']
        self.optimizer = IgnitePerformanceOptimizer()
        self.initialized = False
        
        # Specialized caches
        self.market_data_cache: Optional[OptimizedMarketDataCache] = None
        self.orderbook_cache: Optional[OptimizedOrderBookCache] = None
        
        # Batch operation queues
        self.write_batch: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.batch_size = 1000
        self.batch_interval = 0.1  # seconds
        
        # Performance tracking
        self.operation_times: Dict[str, List[float]] = defaultdict(list)
        
    async def initialize(self):
        """Initialize optimized cache system"""
        if self.initialized:
            return
            
        try:
            # Initialize optimizer
            await self.optimizer.initialize(self.nodes)
            
            # Create specialized caches
            self.market_data_cache = OptimizedMarketDataCache(self.optimizer)
            self.orderbook_cache = OptimizedOrderBookCache(self.optimizer)
            
            # Setup additional optimized caches
            await self._setup_optimized_caches()
            
            # Start batch processor
            asyncio.create_task(self._batch_processor())
            
            self.initialized = True
            logger.info("Optimized Ignite cache initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Ignite: {e}")
            raise
            
    async def _setup_optimized_caches(self):
        """Setup additional optimized caches"""
        
        # Options pricing cache
        options_config = CacheConfig(
            name="options_pricing",
            mode="REPLICATED",
            expire_after_write=30,  # 30 seconds
            near_cache_enabled=True,
            max_size=100000
        )
        await self.optimizer.create_cache(options_config)
        
        # Margin cache with SQL
        margin_config = CacheConfig(
            name="margin_accounts",
            mode="PARTITIONED",
            backups=2,
            query_entities=[
                self._create_margin_query_entity()
            ],
            indexes=[
                {"name": "idx_user_margin", "fields": ["user_id"]},
                {"name": "idx_margin_ratio", "fields": ["margin_ratio"]}
            ]
        )
        await self.optimizer.create_cache(margin_config)
        
        # Volume statistics cache
        volume_config = CacheConfig(
            name="volume_stats",
            mode="PARTITIONED",
            expire_after_write=300,  # 5 minutes
            write_behind=True,
            write_behind_flush_frequency=2000
        )
        await self.optimizer.create_cache(volume_config)
        
    def _create_margin_query_entity(self):
        """Create query entity for margin accounts"""
        from pyignite.query import QueryEntity, QueryField
        
        return QueryEntity(
            table_name="margin_accounts",
            key_type=String,
            value_type=None,
            query_fields=[
                QueryField(name="user_id", type_name=String.__name__),
                QueryField(name="total_collateral", type_name=DecimalObject.__name__),
                QueryField(name="used_margin", type_name=DecimalObject.__name__),
                QueryField(name="margin_ratio", type_name=DecimalObject.__name__),
                QueryField(name="is_liquidatable", type_name=BoolObject.__name__),
                QueryField(name="updated_at", type_name=TimestampObject.__name__)
            ]
        )
        
    # Optimized get/set operations
    
    async def get(self, key: str, cache_name: str = "default") -> Optional[Any]:
        """Optimized get with near cache"""
        return await self._timed_operation(
            "get",
            self.optimizer.caches.get(cache_name, self.optimizer.caches.get("default")).get,
            key
        )
        
    async def set(self, key: str, value: Any, cache_name: str = "default", 
                  ttl: Optional[int] = None):
        """Optimized set with batching"""
        # Add to batch
        self.write_batch[cache_name][key] = (value, ttl)
        
        # Flush if batch is full
        if len(self.write_batch[cache_name]) >= self.batch_size:
            await self._flush_batch(cache_name)
            
    async def get_all(self, keys: List[str], cache_name: str = "default") -> Dict[str, Any]:
        """Batch get operation"""
        return await self._timed_operation(
            "get_all",
            self.optimizer.batch_get,
            cache_name, keys
        )
        
    async def set_all(self, entries: Dict[str, Any], cache_name: str = "default",
                      ttl: Optional[int] = None):
        """Batch set operation"""
        await self._timed_operation(
            "set_all",
            self.optimizer.batch_put,
            cache_name, entries, ttl
        )
        
    # Compute operations
    
    async def compute_on_key(self, key: str, func: Callable, 
                            cache_name: str = "default") -> Any:
        """Execute computation on node holding the key"""
        return await self.optimizer.affinity_compute(cache_name, key, func)
        
    async def map_reduce(self, data: List[Any], map_func: Callable, 
                        reduce_func: Callable) -> Any:
        """Distributed map-reduce operation"""
        return await self.optimizer.map_reduce(map_func, reduce_func, data)
        
    # SQL operations
    
    async def query(self, sql: str, params: Optional[List] = None,
                   cache_name: str = "default") -> List[Dict]:
        """Execute SQL query"""
        return await self._timed_operation(
            "query",
            self.optimizer.execute_sql,
            cache_name, sql, params
        )
        
    async def query_continuous(self, sql: str, callback: Callable,
                             cache_name: str = "default"):
        """Setup continuous query for real-time updates"""
        return await self.optimizer.continuous_query(
            cache_name, sql, callback, initial_query=True
        )
        
    # Specialized operations
    
    async def update_market_price(self, instrument_id: str, price: Decimal,
                                 volume: Decimal):
        """Update market price with optimization"""
        if self.market_data_cache:
            await self.market_data_cache.update_price(
                instrument_id, price, volume, datetime.utcnow()
            )
            
    async def update_order_book(self, instrument_id: str,
                               bids: List[Tuple[Decimal, Decimal]],
                               asks: List[Tuple[Decimal, Decimal]]):
        """Update order book with optimization"""
        if self.orderbook_cache:
            await self.orderbook_cache.update_order_book(
                instrument_id, bids, asks
            )
            
    async def get_margin_accounts_at_risk(self, threshold: Decimal = Decimal("0.8")):
        """Get margin accounts at risk using SQL"""
        sql = """
        SELECT user_id, margin_ratio, total_collateral, used_margin
        FROM margin_accounts
        WHERE margin_ratio > ?
        ORDER BY margin_ratio DESC
        """
        
        return await self.query(sql, [float(threshold)], "margin_accounts")
        
    async def calculate_position_var(self, positions: List[Dict]) -> Decimal:
        """Calculate VaR using distributed compute"""
        
        def calculate_var_chunk(position_chunk):
            # Simplified VaR calculation
            total_var = 0.0
            for pos in position_chunk:
                position_value = pos['quantity'] * pos['price']
                volatility = pos.get('volatility', 0.3)
                var_95 = position_value * volatility * 1.645
                total_var += var_95
            return total_var
            
        def sum_var(var_results):
            return sum(var_results)
            
        var_result = await self.map_reduce(
            positions, calculate_var_chunk, sum_var
        )
        
        return Decimal(str(var_result))
        
    # Performance monitoring
    
    async def _timed_operation(self, operation: str, func: Callable, *args, **kwargs):
        """Execute operation with timing"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Get connection from pool for operation
            conn = self.optimizer.get_connection()
            result = await asyncio.get_event_loop().run_in_executor(
                None, func, *args, **kwargs
            )
            self.optimizer.return_connection(conn)
            
            # Track timing
            elapsed = asyncio.get_event_loop().time() - start_time
            self.operation_times[operation].append(elapsed)
            
            # Keep only recent timings
            if len(self.operation_times[operation]) > 1000:
                self.operation_times[operation] = self.operation_times[operation][-1000:]
                
            return result
            
        except Exception as e:
            logger.error(f"Operation {operation} failed: {e}")
            raise
            
    async def _batch_processor(self):
        """Process write batches periodically"""
        while True:
            try:
                await asyncio.sleep(self.batch_interval)
                
                # Flush all batches
                for cache_name in list(self.write_batch.keys()):
                    if self.write_batch[cache_name]:
                        await self._flush_batch(cache_name)
                        
            except asyncio.CancelledError:
                # Flush remaining on shutdown
                for cache_name in list(self.write_batch.keys()):
                    if self.write_batch[cache_name]:
                        await self._flush_batch(cache_name)
                break
            except Exception as e:
                logger.error(f"Batch processor error: {e}")
                
    async def _flush_batch(self, cache_name: str):
        """Flush write batch for cache"""
        if not self.write_batch[cache_name]:
            return
            
        # Separate by TTL
        no_ttl_entries = {}
        ttl_entries = defaultdict(dict)
        
        for key, (value, ttl) in self.write_batch[cache_name].items():
            if ttl is None:
                no_ttl_entries[key] = value
            else:
                ttl_entries[ttl][key] = value
                
        # Batch put without TTL
        if no_ttl_entries:
            await self.set_all(no_ttl_entries, cache_name)
            
        # Batch put with TTL
        for ttl, entries in ttl_entries.items():
            await self.set_all(entries, cache_name, ttl)
            
        # Clear batch
        self.write_batch[cache_name].clear()
        
    async def warmup_cache(self, cache_name: str, warmup_func: Callable):
        """Warm up cache with initial data"""
        await self.optimizer.cache_warmup(cache_name, warmup_func)
        
    async def get_cache_stats(self, cache_name: str = None) -> Dict[str, Any]:
        """Get cache statistics"""
        if cache_name:
            return await self.optimizer.get_cache_stats(cache_name)
            
        # Get stats for all caches
        all_stats = {}
        for name in self.optimizer.cache_configs:
            all_stats[name] = await self.optimizer.get_cache_stats(name)
            
        # Add operation timing stats
        timing_stats = {}
        for op, times in self.operation_times.items():
            if times:
                timing_stats[op] = {
                    'count': len(times),
                    'avg_ms': sum(times) / len(times) * 1000,
                    'min_ms': min(times) * 1000,
                    'max_ms': max(times) * 1000
                }
                
        return {
            'cache_stats': all_stats,
            'operation_timings': timing_stats,
            'performance_summary': self.optimizer.get_performance_summary()
        }
        
    async def optimize_query(self, sql: str, cache_name: str = "default") -> Dict[str, Any]:
        """Analyze and optimize SQL query"""
        return await self.optimizer.optimize_query(cache_name, sql)
        
    async def clear_cache(self, cache_name: str):
        """Clear specific cache"""
        await self.optimizer.clear_cache(cache_name)
        
    async def shutdown(self):
        """Shutdown cache system"""
        # Flush all batches
        for cache_name in list(self.write_batch.keys()):
            if self.write_batch[cache_name]:
                await self._flush_batch(cache_name)
                
        # Shutdown optimizer
        await self.optimizer.shutdown()


# Singleton instance
_optimized_cache_instance: Optional[OptimizedIgniteCache] = None


async def get_optimized_cache() -> OptimizedIgniteCache:
    """Get or create optimized cache instance"""
    global _optimized_cache_instance
    
    if _optimized_cache_instance is None:
        _optimized_cache_instance = OptimizedIgniteCache()
        await _optimized_cache_instance.initialize()
        
    return _optimized_cache_instance


# Decorators for caching

def cached(cache_name: str = "default", ttl: Optional[int] = None,
           key_func: Optional[Callable] = None):
    """Decorator for caching function results"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache = await get_optimized_cache()
            
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                import hashlib
                key_data = f"{func.__name__}:{args}:{kwargs}"
                cache_key = hashlib.md5(key_data.encode()).hexdigest()
                
            # Try cache first
            cached_result = await cache.get(cache_key, cache_name)
            if cached_result is not None:
                return cached_result
                
            # Execute function
            result = await func(*args, **kwargs)
            
            # Cache result
            await cache.set(cache_key, result, cache_name, ttl)
            
            return result
            
        return wrapper
    return decorator


def cache_invalidate(cache_name: str = "default", key_func: Optional[Callable] = None):
    """Decorator to invalidate cache on function execution"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache = await get_optimized_cache()
            
            # Execute function first
            result = await func(*args, **kwargs)
            
            # Generate cache key to invalidate
            if key_func:
                cache_key = key_func(*args, **kwargs)
                await cache.set(cache_key, None, cache_name, ttl=1)  # Expire immediately
                
            return result
            
        return wrapper
    return decorator 