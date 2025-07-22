"""
Digital Integration Hub (DIH) with Apache Ignite

Provides an in-memory data integration layer that aggregates hot data from
multiple sources, enabling low-latency API access with ACID transactions.
"""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import json
from collections import defaultdict
import hashlib
import uuid

from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import (
    String, IntObject, LongObject, FloatObject, 
    BoolObject, TimestampObject, BinaryObject,
    CollectionObject, MapObject
)
from pyignite.queries import Query, SqlFieldsQuery, CacheConfiguration
from pyignite.cache import Cache
from pyignite.exceptions import CacheError
from pyignite.transaction import TransactionConcurrency, TransactionIsolation

logger = logging.getLogger(__name__)


class DataSource(Enum):
    """Supported data sources for DIH"""
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"
    REST_API = "rest_api"
    PULSAR_STREAM = "pulsar_stream"
    REDIS = "redis"
    JANUSGRAPH = "janusgraph"


class CacheStrategy(Enum):
    """Cache update strategies"""
    WRITE_THROUGH = "write_through"      # Write to cache and source
    WRITE_BEHIND = "write_behind"        # Write to cache, async to source
    READ_THROUGH = "read_through"        # Read from source if not in cache
    REFRESH_AHEAD = "refresh_ahead"      # Proactive refresh before expiry


class ConsistencyLevel(Enum):
    """Data consistency levels"""
    EVENTUAL = "eventual"
    STRONG = "strong"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    CONSISTENT_PREFIX = "consistent_prefix"


@dataclass
class DataEntity:
    """Generic data entity in the DIH"""
    entity_id: str
    entity_type: str
    data: Dict[str, Any]
    version: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    ttl_seconds: Optional[int] = None
    source: Optional[DataSource] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheRegion:
    """Configuration for a cache region"""
    name: str
    cache_mode: str = "PARTITIONED"  # PARTITIONED, REPLICATED, LOCAL
    backups: int = 1
    atomicity_mode: str = "TRANSACTIONAL"  # ATOMIC, TRANSACTIONAL
    cache_strategy: CacheStrategy = CacheStrategy.WRITE_THROUGH
    eviction_policy: str = "LRU"  # LRU, FIFO, RANDOM
    eviction_max_size: int = 10000000  # 10M entries
    expiry_policy: Optional[Dict[str, int]] = None  # TTL settings
    indexes: List[Tuple[str, str]] = field(default_factory=list)  # [(field, type)]
    sql_schema: Optional[str] = None
    query_parallelism: int = 4
    rebalance_mode: str = "SYNC"  # SYNC, ASYNC, NONE


@dataclass
class DataSourceConfig:
    """Configuration for a data source"""
    source_type: DataSource
    connection_params: Dict[str, Any]
    sync_interval_seconds: Optional[int] = None
    batch_size: int = 1000
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    transform_function: Optional[str] = None  # Python function path


@dataclass
class APIEndpoint:
    """API endpoint configuration"""
    path: str
    method: str = "GET"
    cache_regions: List[str] = field(default_factory=list)
    query_template: Optional[str] = None
    cache_key_pattern: Optional[str] = None
    ttl_seconds: Optional[int] = 300  # 5 minutes default
    auth_required: bool = True


class DigitalIntegrationHub:
    """
    Main DIH implementation using Apache Ignite
    """
    
    def __init__(self, 
                 ignite_nodes: List[Tuple[str, int]],
                 default_consistency: ConsistencyLevel = ConsistencyLevel.STRONG):
        self.ignite_nodes = ignite_nodes
        self.default_consistency = default_consistency
        
        # Ignite client
        self.client: Optional[Client] = None
        
        # Cache configurations
        self.cache_regions: Dict[str, CacheRegion] = {}
        self.caches: Dict[str, Cache] = {}
        
        # Data source configurations
        self.data_sources: Dict[str, DataSourceConfig] = {}
        self.source_connectors: Dict[str, Any] = {}
        
        # API endpoints
        self.api_endpoints: Dict[str, APIEndpoint] = {}
        
        # Sync tasks
        self.sync_tasks: Dict[str, asyncio.Task] = {}
        
        # Metrics
        self.metrics = defaultdict(lambda: defaultdict(int))
        
    async def initialize(self):
        """Initialize the DIH"""
        logger.info("Initializing Digital Integration Hub...")
        
        # Connect to Ignite cluster
        self.client = Client()
        self.client.connect(self.ignite_nodes)
        
        # Initialize default cache regions
        await self._initialize_default_regions()
        
        # Start metrics collection
        asyncio.create_task(self._collect_metrics())
        
        logger.info("DIH initialized successfully")
        
    async def shutdown(self):
        """Shutdown the DIH"""
        logger.info("Shutting down Digital Integration Hub...")
        
        # Cancel sync tasks
        for task in self.sync_tasks.values():
            task.cancel()
            
        # Close cache connections
        for cache in self.caches.values():
            cache.close()
            
        # Disconnect from Ignite
        if self.client:
            self.client.close()
            
        logger.info("DIH shutdown complete")
        
    async def _initialize_default_regions(self):
        """Initialize default cache regions"""
        # User session cache
        await self.create_cache_region(CacheRegion(
            name="user_sessions",
            cache_mode="REPLICATED",
            cache_strategy=CacheStrategy.WRITE_THROUGH,
            expiry_policy={"create": 3600, "access": 3600},  # 1 hour TTL
            indexes=[("user_id", "STRING"), ("tenant_id", "STRING")]
        ))
        
        # Asset metadata cache
        await self.create_cache_region(CacheRegion(
            name="asset_metadata",
            cache_mode="PARTITIONED",
            backups=2,
            cache_strategy=CacheStrategy.READ_THROUGH,
            eviction_max_size=50000000,  # 50M entries
            indexes=[("asset_id", "STRING"), ("asset_type", "STRING"), ("owner_id", "STRING")]
        ))
        
        # Real-time metrics cache
        await self.create_cache_region(CacheRegion(
            name="realtime_metrics",
            cache_mode="PARTITIONED",
            atomicity_mode="ATOMIC",
            cache_strategy=CacheStrategy.WRITE_BEHIND,
            expiry_policy={"create": 300},  # 5 minute TTL
            indexes=[("metric_name", "STRING"), ("timestamp", "LONG")]
        ))
        
        # Transaction cache
        await self.create_cache_region(CacheRegion(
            name="transactions",
            cache_mode="PARTITIONED",
            backups=2,
            atomicity_mode="TRANSACTIONAL",
            cache_strategy=CacheStrategy.WRITE_THROUGH,
            indexes=[("tx_id", "STRING"), ("user_id", "STRING"), ("status", "STRING")]
        ))
        
    async def create_cache_region(self, region: CacheRegion) -> Cache:
        """Create a new cache region"""
        logger.info(f"Creating cache region: {region.name}")
        
        # Cache configuration
        cache_config = CacheConfiguration(
            name=region.name,
            cache_mode=region.cache_mode,
            backups=region.backups,
            atomicity_mode=region.atomicity_mode,
            eviction_policy_factory={
                "type": region.eviction_policy,
                "max_size": region.eviction_max_size
            }
        )
        
        # Set expiry policy if defined
        if region.expiry_policy:
            cache_config.expiry_policy = region.expiry_policy
            
        # Set SQL schema if defined
        if region.sql_schema:
            cache_config.sql_schema = region.sql_schema
            
        # Create cache
        cache = self.client.get_or_create_cache(cache_config)
        
        # Create indexes
        for field_name, field_type in region.indexes:
            index_name = f"idx_{region.name}_{field_name}"
            sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {region.name} ({field_name})"
            cache.query(SqlFieldsQuery(sql))
            
        # Store configuration and cache
        self.cache_regions[region.name] = region
        self.caches[region.name] = cache
        
        return cache
        
    async def register_data_source(self, 
                                  source_name: str,
                                  config: DataSourceConfig,
                                  target_regions: List[str]):
        """Register a data source for synchronization"""
        logger.info(f"Registering data source: {source_name}")
        
        # Store configuration
        self.data_sources[source_name] = config
        
        # Initialize connector
        connector = await self._create_connector(config)
        self.source_connectors[source_name] = connector
        
        # Start sync task if interval specified
        if config.sync_interval_seconds:
            task = asyncio.create_task(
                self._sync_data_source(source_name, target_regions)
            )
            self.sync_tasks[source_name] = task
            
    async def _create_connector(self, config: DataSourceConfig) -> Any:
        """Create a connector for a data source"""
        if config.source_type == DataSource.CASSANDRA:
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider
            
            auth = None
            if "username" in config.connection_params:
                auth = PlainTextAuthProvider(
                    username=config.connection_params["username"],
                    password=config.connection_params["password"]
                )
                
            cluster = Cluster(
                config.connection_params.get("hosts", ["cassandra"]),
                auth_provider=auth
            )
            return cluster.connect()
            
        elif config.source_type == DataSource.ELASTICSEARCH:
            from elasticsearch import AsyncElasticsearch
            
            return AsyncElasticsearch(
                hosts=config.connection_params.get("hosts", ["elasticsearch:9200"]),
                **config.connection_params.get("options", {})
            )
            
        elif config.source_type == DataSource.POSTGRESQL:
            import asyncpg
            
            return await asyncpg.create_pool(
                **config.connection_params
            )
            
        elif config.source_type == DataSource.REST_API:
            import httpx
            
            return httpx.AsyncClient(
                base_url=config.connection_params.get("base_url"),
                headers=config.connection_params.get("headers", {})
            )
            
        else:
            raise ValueError(f"Unsupported data source: {config.source_type}")
            
    async def _sync_data_source(self, source_name: str, target_regions: List[str]):
        """Sync data from source to cache regions"""
        config = self.data_sources[source_name]
        connector = self.source_connectors[source_name]
        
        while True:
            try:
                logger.debug(f"Syncing data source: {source_name}")
                
                # Fetch data from source
                data = await self._fetch_from_source(connector, config)
                
                # Transform if needed
                if config.transform_function:
                    data = await self._apply_transform(data, config.transform_function)
                    
                # Write to target regions
                for region_name in target_regions:
                    cache = self.caches.get(region_name)
                    if cache:
                        await self._write_to_cache(cache, data, config)
                        
                self.metrics[source_name]["sync_success"] += 1
                
            except Exception as e:
                logger.error(f"Error syncing {source_name}: {e}")
                self.metrics[source_name]["sync_errors"] += 1
                
            # Wait for next sync
            await asyncio.sleep(config.sync_interval_seconds)
            
    async def register_api_endpoint(self, endpoint: APIEndpoint):
        """Register an API endpoint"""
        logger.info(f"Registering API endpoint: {endpoint.path}")
        self.api_endpoints[endpoint.path] = endpoint
        
    async def execute_api_query(self, 
                               endpoint_path: str,
                               params: Dict[str, Any],
                               user_context: Optional[Dict[str, Any]] = None) -> Any:
        """Execute an API query through the DIH"""
        endpoint = self.api_endpoints.get(endpoint_path)
        if not endpoint:
            raise ValueError(f"Unknown endpoint: {endpoint_path}")
            
        # Generate cache key
        cache_key = self._generate_cache_key(endpoint, params)
        
        # Check cache first
        for region_name in endpoint.cache_regions:
            cache = self.caches.get(region_name)
            if cache:
                try:
                    result = cache.get(cache_key)
                    if result:
                        self.metrics[endpoint_path]["cache_hits"] += 1
                        return result
                except CacheError:
                    pass
                    
        self.metrics[endpoint_path]["cache_misses"] += 1
        
        # Execute query
        result = await self._execute_query(endpoint, params, user_context)
        
        # Cache result
        if result and endpoint.ttl_seconds:
            for region_name in endpoint.cache_regions:
                cache = self.caches.get(region_name)
                if cache:
                    try:
                        cache.put(cache_key, result, ttl=endpoint.ttl_seconds)
                    except CacheError as e:
                        logger.error(f"Error caching result: {e}")
                        
        return result
        
    async def execute_transaction(self,
                                 operations: List[Tuple[str, str, Any]],
                                 isolation: TransactionIsolation = TransactionIsolation.REPEATABLE_READ,
                                 concurrency: TransactionConcurrency = TransactionConcurrency.PESSIMISTIC,
                                 timeout: int = 5000) -> bool:
        """
        Execute ACID transaction across cache regions
        
        Args:
            operations: List of (region, operation, data) tuples
            isolation: Transaction isolation level
            concurrency: Transaction concurrency mode
            timeout: Transaction timeout in milliseconds
            
        Returns:
            True if transaction committed successfully
        """
        tx = None
        try:
            # Start transaction
            tx = self.client.tx_start(
                concurrency=concurrency,
                isolation=isolation,
                timeout=timeout
            )
            
            # Execute operations
            for region_name, operation, data in operations:
                cache = self.caches.get(region_name)
                if not cache:
                    raise ValueError(f"Unknown cache region: {region_name}")
                    
                if operation == "put":
                    cache.put(data["key"], data["value"])
                elif operation == "put_all":
                    cache.put_all(data)
                elif operation == "remove":
                    cache.remove(data["key"])
                elif operation == "remove_all":
                    cache.remove_all(data["keys"])
                elif operation == "update":
                    existing = cache.get(data["key"])
                    if existing:
                        updated = {**existing, **data["updates"]}
                        cache.put(data["key"], updated)
                else:
                    raise ValueError(f"Unknown operation: {operation}")
                    
            # Commit transaction
            tx.commit()
            self.metrics["transactions"]["committed"] += 1
            return True
            
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            if tx:
                tx.rollback()
            self.metrics["transactions"]["rolled_back"] += 1
            raise
            
    async def query_cross_region(self,
                                sql_query: str,
                                params: Optional[List[Any]] = None,
                                page_size: int = 1000) -> List[Dict[str, Any]]:
        """Execute SQL query across cache regions"""
        query = SqlFieldsQuery(sql_query)
        
        if params:
            query.args = params
            
        query.page_size = page_size
        
        # Execute query
        cursor = self.client.sql(query)
        
        # Fetch results
        results = []
        for row in cursor:
            # Convert row to dict (assuming first row has field names)
            if not results and hasattr(cursor, 'field_names'):
                field_names = cursor.field_names
            else:
                field_names = [f"col_{i}" for i in range(len(row))]
                
            results.append(dict(zip(field_names, row)))
            
        return results
        
    async def create_continuous_query(self,
                                     region_name: str,
                                     filter_sql: str,
                                     callback: Any,
                                     initial_query: bool = True) -> str:
        """Create a continuous query for real-time updates"""
        cache = self.caches.get(region_name)
        if not cache:
            raise ValueError(f"Unknown cache region: {region_name}")
            
        # Generate query ID
        query_id = f"cq_{region_name}_{uuid.uuid4().hex[:8]}"
        
        # Create continuous query
        # Note: PyIgnite doesn't have direct CQ support, would need Java client
        # This is a placeholder for the pattern
        logger.info(f"Created continuous query: {query_id}")
        
        return query_id
        
    async def bulk_load(self,
                       region_name: str,
                       data: List[Tuple[Any, Any]],
                       batch_size: int = 10000) -> Dict[str, Any]:
        """Bulk load data into cache region"""
        cache = self.caches.get(region_name)
        if not cache:
            raise ValueError(f"Unknown cache region: {region_name}")
            
        start_time = datetime.utcnow()
        loaded_count = 0
        error_count = 0
        
        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = dict(data[i:i + batch_size])
            
            try:
                cache.put_all(batch)
                loaded_count += len(batch)
            except Exception as e:
                logger.error(f"Error loading batch: {e}")
                error_count += len(batch)
                
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        return {
            "loaded_count": loaded_count,
            "error_count": error_count,
            "duration_seconds": duration,
            "throughput": loaded_count / duration if duration > 0 else 0
        }
        
    async def get_cache_statistics(self, region_name: Optional[str] = None) -> Dict[str, Any]:
        """Get cache statistics"""
        if region_name:
            cache = self.caches.get(region_name)
            if not cache:
                raise ValueError(f"Unknown cache region: {region_name}")
                
            # Get cache metrics
            size = cache.get_size()
            
            return {
                "region": region_name,
                "size": size,
                "configuration": self.cache_regions[region_name].__dict__
            }
        else:
            # Get all cache statistics
            stats = {}
            for name, cache in self.caches.items():
                stats[name] = {
                    "size": cache.get_size(),
                    "configuration": self.cache_regions[name].__dict__
                }
            return stats
            
    async def invalidate_cache(self, 
                             region_name: str,
                             pattern: Optional[str] = None):
        """Invalidate cache entries"""
        cache = self.caches.get(region_name)
        if not cache:
            raise ValueError(f"Unknown cache region: {region_name}")
            
        if pattern:
            # Invalidate by pattern (would need scan query)
            logger.info(f"Invalidating cache entries matching: {pattern}")
            # Placeholder for pattern-based invalidation
        else:
            # Clear entire cache
            cache.clear()
            logger.info(f"Cleared cache region: {region_name}")
            
    def _generate_cache_key(self, endpoint: APIEndpoint, params: Dict[str, Any]) -> str:
        """Generate cache key for API endpoint"""
        if endpoint.cache_key_pattern:
            # Use custom pattern
            return endpoint.cache_key_pattern.format(**params)
        else:
            # Generate from params
            param_str = json.dumps(params, sort_keys=True)
            return f"{endpoint.path}:{hashlib.md5(param_str.encode()).hexdigest()}"
            
    async def _execute_query(self, 
                           endpoint: APIEndpoint,
                           params: Dict[str, Any],
                           user_context: Optional[Dict[str, Any]]) -> Any:
        """Execute the actual query"""
        if endpoint.query_template:
            # Execute SQL query
            query = endpoint.query_template.format(**params)
            return await self.query_cross_region(query)
        else:
            # Custom query logic would go here
            return None
            
    async def _fetch_from_source(self, connector: Any, config: DataSourceConfig) -> List[Dict[str, Any]]:
        """Fetch data from source"""
        # Implementation depends on source type
        # This is a placeholder
        return []
        
    async def _apply_transform(self, data: List[Dict[str, Any]], transform_function: str) -> List[Dict[str, Any]]:
        """Apply transformation function to data"""
        # Dynamic import and execution of transform function
        # This is a placeholder
        return data
        
    async def _write_to_cache(self, cache: Cache, data: List[Dict[str, Any]], config: DataSourceConfig):
        """Write data to cache"""
        # Convert to cache entries
        entries = {}
        for item in data:
            key = item.get("id") or item.get("key")
            if key:
                entries[key] = item
                
        # Bulk put
        if entries:
            cache.put_all(entries)
            
    async def _collect_metrics(self):
        """Collect DIH metrics"""
        while True:
            try:
                # Collect cache metrics
                for name, cache in self.caches.items():
                    self.metrics["cache_sizes"][name] = cache.get_size()
                    
                # Log metrics periodically
                logger.debug(f"DIH Metrics: {dict(self.metrics)}")
                
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                
            await asyncio.sleep(60)  # Collect every minute 