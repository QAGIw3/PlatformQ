"""
Performance Optimization Engine using Apache Ignite

Implements advanced caching, distributed computing, and query optimization
"""

from decimal import Decimal
from typing import Dict, List, Optional, Any, Set, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import logging
from functools import wraps
import hashlib
import json
from collections import defaultdict
import pyignite
from pyignite.datatypes import String, DecimalObject, TimestampObject
from pyignite.cache import Cache
from pyignite.query import Query, QueryEntity, QueryField

logger = logging.getLogger(__name__)


@dataclass
class CacheConfig:
    """Configuration for cache region"""
    name: str
    mode: str = "PARTITIONED"  # PARTITIONED, REPLICATED, LOCAL
    backups: int = 1
    expire_after_write: Optional[int] = None  # seconds
    expire_after_access: Optional[int] = None  # seconds
    eviction_policy: str = "LRU"  # LRU, FIFO, SORTED
    max_size: Optional[int] = None
    sql_schema: Optional[str] = None
    query_entities: List[QueryEntity] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    near_cache_enabled: bool = False
    read_through: bool = False
    write_through: bool = True
    write_behind: bool = False
    write_behind_flush_size: int = 10000
    write_behind_flush_frequency: int = 5000  # ms


class IgnitePerformanceOptimizer:
    """Advanced performance optimization using Ignite features"""
    
    def __init__(self, ignite_client: pyignite.Client = None):
        self.client = ignite_client or pyignite.Client()
        self.caches: Dict[str, Cache] = {}
        self.cache_configs: Dict[str, CacheConfig] = {}
        self.compute_pool = None
        
        # Performance metrics
        self.cache_hits = defaultdict(int)
        self.cache_misses = defaultdict(int)
        self.query_times = defaultdict(list)
        
        # Connection pool
        self.connection_pool = []
        self.pool_size = 10
        
        # Batch processors
        self.batch_queues: Dict[str, List] = defaultdict(list)
        self.batch_processors: Dict[str, asyncio.Task] = {}
        
    async def initialize(self, nodes: List[str] = None):
        """Initialize Ignite connection and caches"""
        if nodes is None:
            nodes = ['127.0.0.1:10800']
            
        # Connect to Ignite cluster
        self.client.connect(nodes)
        
        # Initialize connection pool
        for _ in range(self.pool_size):
            client = pyignite.Client()
            client.connect(nodes)
            self.connection_pool.append(client)
            
        # Setup default cache configurations
        await self._setup_default_caches()
        
        # Start batch processors
        await self._start_batch_processors()
        
        logger.info(f"Ignite performance optimizer initialized with {len(nodes)} nodes")
        
    async def _setup_default_caches(self):
        """Setup default cache configurations"""
        
        # Market data cache - replicated for fast reads
        market_data_config = CacheConfig(
            name="market_data",
            mode="REPLICATED",
            expire_after_write=300,  # 5 minutes
            near_cache_enabled=True,
            query_entities=[
                QueryEntity(
                    table_name="market_data",
                    key_type=String,
                    value_type=None,
                    query_fields=[
                        QueryField(name="instrument_id", type_name=String.__name__),
                        QueryField(name="price", type_name=DecimalObject.__name__),
                        QueryField(name="volume", type_name=DecimalObject.__name__),
                        QueryField(name="timestamp", type_name=TimestampObject.__name__)
                    ]
                )
            ],
            indexes=[
                {"name": "idx_instrument_time", "fields": ["instrument_id", "timestamp"]}
            ]
        )
        await self.create_cache(market_data_config)
        
        # Order book cache - partitioned with backups
        orderbook_config = CacheConfig(
            name="orderbook",
            mode="PARTITIONED",
            backups=2,
            expire_after_access=60,  # 1 minute
            max_size=1000000,
            near_cache_enabled=True
        )
        await self.create_cache(orderbook_config)
        
        # User positions cache - partitioned by user
        positions_config = CacheConfig(
            name="positions",
            mode="PARTITIONED",
            backups=1,
            write_through=True,
            query_entities=[
                QueryEntity(
                    table_name="positions",
                    key_type=String,
                    value_type=None,
                    query_fields=[
                        QueryField(name="user_id", type_name=String.__name__),
                        QueryField(name="instrument_id", type_name=String.__name__),
                        QueryField(name="quantity", type_name=DecimalObject.__name__),
                        QueryField(name="entry_price", type_name=DecimalObject.__name__)
                    ]
                )
            ],
            indexes=[
                {"name": "idx_user", "fields": ["user_id"]},
                {"name": "idx_user_instrument", "fields": ["user_id", "instrument_id"]}
            ]
        )
        await self.create_cache(positions_config)
        
        # Volatility surface cache
        vol_surface_config = CacheConfig(
            name="volatility_surface",
            mode="REPLICATED",
            expire_after_write=60,  # 1 minute
            near_cache_enabled=True
        )
        await self.create_cache(vol_surface_config)
        
        # Settlement cache - persistent
        settlement_config = CacheConfig(
            name="settlements",
            mode="PARTITIONED",
            backups=2,
            write_through=True,
            write_behind=True,
            write_behind_flush_frequency=1000  # 1 second
        )
        await self.create_cache(settlement_config)
        
    async def create_cache(self, config: CacheConfig) -> Cache:
        """Create or get cache with configuration"""
        if config.name in self.caches:
            return self.caches[config.name]
            
        # Create cache configuration
        cache_config = {
            'name': config.name,
            'cache_mode': config.mode,
            'backups': config.backups,
            'atomicity_mode': 'TRANSACTIONAL',
            'write_synchronization_mode': 'FULL_SYNC'
        }
        
        # Add expiry policies
        if config.expire_after_write:
            cache_config['expiry_policy'] = {
                'create': config.expire_after_write * 1000,
                'update': config.expire_after_write * 1000
            }
            
        # Add eviction policy
        if config.max_size:
            cache_config['eviction_policy'] = {
                'max_size': config.max_size,
                'type': config.eviction_policy
            }
            
        # Create cache
        cache = self.client.create_cache(cache_config)
        
        # Configure SQL if needed
        if config.query_entities:
            cache.query_entities = config.query_entities
            
        # Create indexes
        for index in config.indexes:
            cache.query_index_create(**index)
            
        self.caches[config.name] = cache
        self.cache_configs[config.name] = config
        
        return cache
        
    def cache_decorator(self, cache_name: str, key_func: Optional[Callable] = None, 
                       ttl: Optional[int] = None):
        """Decorator for caching function results"""
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Generate cache key
                if key_func:
                    cache_key = key_func(*args, **kwargs)
                else:
                    # Default key generation
                    key_data = f"{func.__name__}:{args}:{kwargs}"
                    cache_key = hashlib.md5(key_data.encode()).hexdigest()
                    
                # Try to get from cache
                cache = self.caches.get(cache_name)
                if cache:
                    try:
                        cached_value = cache.get(cache_key)
                        if cached_value is not None:
                            self.cache_hits[cache_name] += 1
                            return cached_value
                    except Exception as e:
                        logger.error(f"Cache get error: {e}")
                        
                # Cache miss - execute function
                self.cache_misses[cache_name] += 1
                result = await func(*args, **kwargs)
                
                # Store in cache
                if cache and result is not None:
                    try:
                        if ttl:
                            cache.put(cache_key, result, ttl=ttl)
                        else:
                            cache.put(cache_key, result)
                    except Exception as e:
                        logger.error(f"Cache put error: {e}")
                        
                return result
                
            return wrapper
        return decorator
        
    async def batch_get(self, cache_name: str, keys: List[str]) -> Dict[str, Any]:
        """Batch get operation for multiple keys"""
        cache = self.caches.get(cache_name)
        if not cache:
            return {}
            
        # Use Ignite's getAll for efficiency
        return cache.get_all(keys)
        
    async def batch_put(self, cache_name: str, entries: Dict[str, Any], 
                       ttl: Optional[int] = None):
        """Batch put operation for multiple entries"""
        cache = self.caches.get(cache_name)
        if not cache:
            return
            
        # Add to batch queue
        batch_key = f"{cache_name}_batch"
        self.batch_queues[batch_key].extend(entries.items())
        
        # Process immediately if queue is large
        if len(self.batch_queues[batch_key]) > 1000:
            await self._process_batch(cache_name)
            
    async def _process_batch(self, cache_name: str):
        """Process batched operations"""
        batch_key = f"{cache_name}_batch"
        
        if not self.batch_queues[batch_key]:
            return
            
        cache = self.caches.get(cache_name)
        if not cache:
            return
            
        # Get all entries
        entries = dict(self.batch_queues[batch_key])
        self.batch_queues[batch_key].clear()
        
        # Batch put
        try:
            cache.put_all(entries)
            logger.debug(f"Processed batch of {len(entries)} entries for {cache_name}")
        except Exception as e:
            logger.error(f"Batch put error: {e}")
            
    async def execute_sql(self, cache_name: str, sql: str, 
                         params: Optional[List] = None) -> List[Dict]:
        """Execute SQL query on cache"""
        cache = self.caches.get(cache_name)
        if not cache:
            return []
            
        start_time = datetime.utcnow()
        
        try:
            # Create query
            query = Query(sql)
            if params:
                query.args = params
                
            # Execute query
            cursor = cache.query(query)
            results = []
            
            for row in cursor:
                results.append(dict(row))
                
            # Track performance
            elapsed = (datetime.utcnow() - start_time).total_seconds()
            self.query_times[cache_name].append(elapsed)
            
            return results
            
        except Exception as e:
            logger.error(f"SQL query error: {e}")
            return []
            
    async def continuous_query(self, cache_name: str, sql: str, 
                             callback: Callable, initial_query: bool = True):
        """Setup continuous query for real-time updates"""
        cache = self.caches.get(cache_name)
        if not cache:
            return None
            
        # Create continuous query
        query = cache.query_continuous(sql)
        
        # Set callback
        query.listener = callback
        
        # Include initial results
        if initial_query:
            query.include_initial = True
            
        # Start query
        query.start()
        
        return query
        
    async def compute_task(self, task_func: Callable, data: Any, 
                          nodes: Optional[List[str]] = None):
        """Execute compute task on Ignite compute grid"""
        if not self.compute_pool:
            self.compute_pool = self.client.get_compute()
            
        # Execute task on specific nodes or all
        if nodes:
            compute = self.compute_pool.for_nodes(nodes)
        else:
            compute = self.compute_pool
            
        # Run task
        return await asyncio.get_event_loop().run_in_executor(
            None, compute.call, task_func, data
        )
        
    async def map_reduce(self, map_func: Callable, reduce_func: Callable, 
                        data: List[Any]) -> Any:
        """Execute map-reduce operation on compute grid"""
        if not self.compute_pool:
            self.compute_pool = self.client.get_compute()
            
        # Split data for parallel processing
        chunk_size = max(1, len(data) // self.pool_size)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        
        # Map phase - parallel execution
        map_tasks = []
        for chunk in chunks:
            task = self.compute_task(map_func, chunk)
            map_tasks.append(task)
            
        map_results = await asyncio.gather(*map_tasks)
        
        # Reduce phase
        return await self.compute_task(reduce_func, map_results)
        
    async def affinity_compute(self, cache_name: str, key: str, 
                             compute_func: Callable) -> Any:
        """Execute compute function on node holding the cache key"""
        cache = self.caches.get(cache_name)
        if not cache:
            return None
            
        # Get affinity compute for key
        compute = self.client.get_compute().for_key(cache_name, key)
        
        # Execute function on affinity node
        return await asyncio.get_event_loop().run_in_executor(
            None, compute.call, compute_func, key
        )
        
    async def create_near_cache(self, cache_name: str, max_size: int = 10000):
        """Create near cache for frequently accessed data"""
        config = self.cache_configs.get(cache_name)
        if not config:
            return
            
        # Enable near cache
        config.near_cache_enabled = True
        
        # Configure near cache
        cache = self.caches[cache_name]
        cache.with_near_cache(
            eviction_policy='LRU',
            eviction_policy_max_size=max_size,
            ttl=60  # 1 minute
        )
        
    async def optimize_query(self, cache_name: str, sql: str) -> Dict[str, Any]:
        """Analyze and optimize SQL query"""
        cache = self.caches.get(cache_name)
        if not cache:
            return {}
            
        # Explain query
        explain_sql = f"EXPLAIN {sql}"
        plan = await self.execute_sql(cache_name, explain_sql)
        
        # Analyze plan
        optimization_hints = []
        
        # Check for missing indexes
        if any('TABLE SCAN' in str(p) for p in plan):
            optimization_hints.append("Consider adding index for better performance")
            
        # Check for join order
        if 'JOIN' in sql.upper():
            optimization_hints.append("Ensure smaller table is on the right side of join")
            
        return {
            "query_plan": plan,
            "optimization_hints": optimization_hints,
            "estimated_cost": self._estimate_query_cost(plan)
        }
        
    def _estimate_query_cost(self, plan: List[Dict]) -> float:
        """Estimate query cost from execution plan"""
        # Simplified cost estimation
        cost = 0.0
        
        for step in plan:
            if 'TABLE SCAN' in str(step):
                cost += 100.0
            elif 'INDEX SCAN' in str(step):
                cost += 10.0
            elif 'JOIN' in str(step):
                cost += 50.0
                
        return cost
        
    async def cache_warmup(self, cache_name: str, warmup_func: Callable):
        """Warm up cache with initial data"""
        cache = self.caches.get(cache_name)
        if not cache:
            return
            
        # Get warm-up data
        warmup_data = await warmup_func()
        
        # Batch load into cache
        if isinstance(warmup_data, dict):
            await self.batch_put(cache_name, warmup_data)
        else:
            logger.warning(f"Warmup data for {cache_name} must be a dictionary")
            
    async def get_cache_stats(self, cache_name: str) -> Dict[str, Any]:
        """Get cache statistics"""
        cache = self.caches.get(cache_name)
        if not cache:
            return {}
            
        metrics = cache.get_metrics()
        
        total_requests = self.cache_hits[cache_name] + self.cache_misses[cache_name]
        hit_rate = (self.cache_hits[cache_name] / total_requests * 100 
                   if total_requests > 0 else 0)
        
        return {
            "size": metrics.get('CacheSize', 0),
            "hits": self.cache_hits[cache_name],
            "misses": self.cache_misses[cache_name],
            "hit_rate": hit_rate,
            "avg_get_time": metrics.get('AverageGetTime', 0),
            "avg_put_time": metrics.get('AveragePutTime', 0),
            "evictions": metrics.get('CacheEvictions', 0)
        }
        
    async def clear_cache(self, cache_name: str):
        """Clear all entries from cache"""
        cache = self.caches.get(cache_name)
        if cache:
            cache.clear()
            
    async def destroy_cache(self, cache_name: str):
        """Destroy cache completely"""
        if cache_name in self.caches:
            self.caches[cache_name].destroy()
            del self.caches[cache_name]
            del self.cache_configs[cache_name]
            
    async def _start_batch_processors(self):
        """Start background batch processors"""
        for cache_name in self.cache_configs:
            processor = asyncio.create_task(self._batch_processor_loop(cache_name))
            self.batch_processors[f"{cache_name}_batch"] = processor
            
    async def _batch_processor_loop(self, cache_name: str):
        """Background loop for processing batches"""
        batch_key = f"{cache_name}_batch"
        
        while True:
            try:
                await asyncio.sleep(1)  # Process every second
                
                if self.batch_queues[batch_key]:
                    await self._process_batch(cache_name)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Batch processor error for {cache_name}: {e}")
                
    def get_connection(self) -> pyignite.Client:
        """Get connection from pool"""
        if self.connection_pool:
            return self.connection_pool.pop()
        return self.client
        
    def return_connection(self, conn: pyignite.Client):
        """Return connection to pool"""
        if len(self.connection_pool) < self.pool_size:
            self.connection_pool.append(conn)
            
    async def shutdown(self):
        """Shutdown optimizer and cleanup"""
        # Cancel batch processors
        for processor in self.batch_processors.values():
            processor.cancel()
            
        # Close connections
        for conn in self.connection_pool:
            conn.close()
            
        self.client.close()
        
        logger.info("Ignite performance optimizer shutdown complete")
        
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get overall performance summary"""
        total_hits = sum(self.cache_hits.values())
        total_misses = sum(self.cache_misses.values())
        total_requests = total_hits + total_misses
        
        avg_query_times = {}
        for cache_name, times in self.query_times.items():
            if times:
                avg_query_times[cache_name] = sum(times) / len(times)
                
        return {
            "total_cache_hits": total_hits,
            "total_cache_misses": total_misses,
            "overall_hit_rate": (total_hits / total_requests * 100 
                               if total_requests > 0 else 0),
            "cache_hit_rates": {
                name: (self.cache_hits[name] / 
                      (self.cache_hits[name] + self.cache_misses[name]) * 100
                      if (self.cache_hits[name] + self.cache_misses[name]) > 0 else 0)
                for name in self.cache_configs
            },
            "avg_query_times": avg_query_times,
            "active_caches": len(self.caches),
            "connection_pool_size": len(self.connection_pool)
        }


class OptimizedMarketDataCache:
    """Optimized market data caching with Ignite"""
    
    def __init__(self, optimizer: IgnitePerformanceOptimizer):
        self.optimizer = optimizer
        self.cache_name = "market_data"
        
    @property
    def cache(self):
        return self.optimizer.caches.get(self.cache_name)
        
    async def update_price(self, instrument_id: str, price: Decimal, 
                          volume: Decimal, timestamp: datetime):
        """Update market price with optimized caching"""
        
        # Create cache key
        key = f"{instrument_id}:latest"
        
        # Update latest price
        data = {
            "instrument_id": instrument_id,
            "price": float(price),
            "volume": float(volume),
            "timestamp": timestamp.isoformat()
        }
        
        # Use affinity compute for local update
        await self.optimizer.affinity_compute(
            self.cache_name, key,
            lambda k: self.cache.put(k, data)
        )
        
        # Also store time series data
        ts_key = f"{instrument_id}:{timestamp.timestamp()}"
        await self.optimizer.batch_put(self.cache_name, {ts_key: data})
        
    async def get_latest_price(self, instrument_id: str) -> Optional[Dict]:
        """Get latest price with caching"""
        key = f"{instrument_id}:latest"
        return self.cache.get(key)
        
    async def get_price_history(self, instrument_id: str, 
                               start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get price history using SQL query"""
        
        sql = """
        SELECT price, volume, timestamp 
        FROM market_data 
        WHERE instrument_id = ? 
        AND timestamp >= ? 
        AND timestamp <= ?
        ORDER BY timestamp DESC
        """
        
        return await self.optimizer.execute_sql(
            self.cache_name, sql,
            [instrument_id, start_time.isoformat(), end_time.isoformat()]
        )
        
    async def get_aggregated_stats(self, instrument_id: str, 
                                  period: timedelta) -> Dict[str, float]:
        """Get aggregated statistics using compute grid"""
        
        end_time = datetime.utcnow()
        start_time = end_time - period
        
        # Use map-reduce for aggregation
        price_data = await self.get_price_history(instrument_id, start_time, end_time)
        
        if not price_data:
            return {}
            
        # Map function - extract prices
        def map_prices(data_chunk):
            return [float(d['price']) for d in data_chunk]
            
        # Reduce function - calculate stats
        def reduce_stats(mapped_results):
            all_prices = []
            for result in mapped_results:
                all_prices.extend(result)
                
            if not all_prices:
                return {}
                
            return {
                'min': min(all_prices),
                'max': max(all_prices),
                'avg': sum(all_prices) / len(all_prices),
                'count': len(all_prices)
            }
            
        return await self.optimizer.map_reduce(
            map_prices, reduce_stats, price_data
        )


class OptimizedOrderBookCache:
    """Optimized order book caching"""
    
    def __init__(self, optimizer: IgnitePerformanceOptimizer):
        self.optimizer = optimizer
        self.cache_name = "orderbook"
        
    async def update_order_book(self, instrument_id: str, 
                               bids: List[Tuple[Decimal, Decimal]], 
                               asks: List[Tuple[Decimal, Decimal]]):
        """Update order book with batching"""
        
        # Prepare batch update
        updates = {}
        
        # Store top N levels
        for i, (price, qty) in enumerate(bids[:20]):
            key = f"{instrument_id}:bid:{i}"
            updates[key] = {"price": float(price), "quantity": float(qty)}
            
        for i, (price, qty) in enumerate(asks[:20]):
            key = f"{instrument_id}:ask:{i}"
            updates[key] = {"price": float(price), "quantity": float(qty)}
            
        # Store summary
        updates[f"{instrument_id}:summary"] = {
            "best_bid": float(bids[0][0]) if bids else 0,
            "best_ask": float(asks[0][0]) if asks else 0,
            "bid_depth": len(bids),
            "ask_depth": len(asks),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Batch update
        await self.optimizer.batch_put(self.cache_name, updates, ttl=60)
        
    @staticmethod
    def orderbook_key_func(instrument_id: str, side: str, level: int) -> str:
        """Generate cache key for order book level"""
        return f"{instrument_id}:{side}:{level}" 