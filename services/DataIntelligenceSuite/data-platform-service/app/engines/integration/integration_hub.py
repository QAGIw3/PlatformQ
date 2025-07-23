"""
Digital Integration Hub for high-performance data integration.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import uuid
from collections import defaultdict

from pyignite import Client
from pyignite.datatypes import String, IntObject, LongObject
from pyignite.queries import SqlFieldsQuery, CacheConfiguration
from pyignite.cache import Cache
from pyignite.transaction import TransactionConcurrency, TransactionIsolation

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager, CacheConfig, CacheStrategy
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class DataSource(str, Enum):
    """Supported data sources for integration."""
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"
    REST_API = "rest_api"
    PULSAR_STREAM = "pulsar_stream"
    JANUSGRAPH = "janusgraph"
    MINIO = "minio"
    TRINO = "trino"


class ConsistencyLevel(str, Enum):
    """Data consistency levels."""
    EVENTUAL = "eventual"
    STRONG = "strong"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    CONSISTENT_PREFIX = "consistent_prefix"


@dataclass
class DataEntity:
    """Generic data entity in the integration hub."""
    entity_id: str
    entity_type: str
    data: Dict[str, Any]
    version: int = 1
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    ttl_seconds: Optional[int] = None
    source: Optional[DataSource] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "data": self.data,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "ttl_seconds": self.ttl_seconds,
            "source": self.source.value if self.source else None,
            "metadata": self.metadata
        }


@dataclass
class CacheRegion:
    """Configuration for a cache region."""
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
    write_synchronization_mode: str = "PRIMARY_SYNC"  # FULL_SYNC, FULL_ASYNC, PRIMARY_SYNC
    
    def to_config(self) -> CacheConfiguration:
        """Convert to Ignite cache configuration."""
        config = CacheConfiguration()
        config.name = self.name
        config.cache_mode = self.cache_mode
        config.backups = self.backups
        config.atomicity_mode = self.atomicity_mode
        config.write_synchronization_mode = self.write_synchronization_mode
        config.query_parallelism = self.query_parallelism
        config.rebalance_mode = self.rebalance_mode
        
        # Set eviction policy
        if self.eviction_policy and self.eviction_max_size:
            config.eviction_policy = {
                "policy": self.eviction_policy,
                "max_size": self.eviction_max_size
            }
        
        # Set expiry policy
        if self.expiry_policy:
            config.expiry_policy = self.expiry_policy
        
        return config


class IntegrationHub:
    """
    High-performance data integration hub using Apache Ignite.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        ignite_client: Optional[IgniteClient] = None,
        ignite_nodes: Optional[List[Tuple[str, int]]] = None,
        default_consistency: ConsistencyLevel = ConsistencyLevel.STRONG
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        self.ignite_nodes = ignite_nodes or [("localhost", 10800)]
        self.default_consistency = default_consistency
        
        # Cache regions
        self.cache_regions: Dict[str, CacheRegion] = {}
        self.cache_instances: Dict[str, Cache] = {}
        
        # Data source configurations
        self.data_sources: Dict[str, Dict[str, Any]] = {}
        
        # Transaction management
        self.active_transactions: Dict[str, Any] = {}
        
        # Metrics
        self.metrics = defaultdict(int)
        
        # Background tasks
        self._monitor_task: Optional[asyncio.Task] = None
        self._sync_task: Optional[asyncio.Task] = None
        
        logger.info("Integration Hub initialized")
        
    async def initialize(self):
        """Initialize the integration hub."""
        # Connect to Ignite if not using injected client
        if not self.ignite_client:
            self.ignite_client = Client()
            self.ignite_client.connect(self.ignite_nodes)
        
        # Create default cache regions
        await self._create_default_regions()
        
        # Subscribe to events
        await self.event_bus.subscribe("integration.cache.invalidate", self._handle_cache_invalidate)
        await self.event_bus.subscribe("integration.sync.request", self._handle_sync_request)
        
        # Start background tasks
        self._monitor_task = asyncio.create_task(self._monitor_caches())
        self._sync_task = asyncio.create_task(self._sync_data_sources())
        
        logger.info("Integration Hub initialized successfully")
        
    async def cleanup(self):
        """Cleanup integration hub resources."""
        # Cancel background tasks
        if self._monitor_task:
            self._monitor_task.cancel()
        if self._sync_task:
            self._sync_task.cancel()
        
        # Close cache instances
        for cache in self.cache_instances.values():
            cache.destroy()
        
        # Disconnect from Ignite
        if self.ignite_client and hasattr(self.ignite_client, 'close'):
            self.ignite_client.close()
        
        logger.info("Integration Hub cleaned up")
        
    async def create_cache_region(
        self,
        region: CacheRegion
    ) -> Cache:
        """Create a new cache region."""
        try:
            # Store region configuration
            self.cache_regions[region.name] = region
            
            # Create Ignite cache
            cache_config = region.to_config()
            cache = self.ignite_client.create_cache(cache_config)
            self.cache_instances[region.name] = cache
            
            # Create indexes if specified
            if region.indexes:
                for field_name, field_type in region.indexes:
                    await self._create_index(cache, field_name, field_type)
            
            # Publish event
            await self.event_bus.publish("integration.cache.created", {
                "region": region.name,
                "config": region.__dict__
            })
            
            logger.info(f"Created cache region: {region.name}")
            return cache
            
        except Exception as e:
            logger.error(f"Error creating cache region {region.name}: {e}")
            raise
            
    async def get_cache(self, region_name: str) -> Optional[Cache]:
        """Get cache instance by region name."""
        if region_name in self.cache_instances:
            return self.cache_instances[region_name]
        
        # Try to get existing cache
        try:
            cache = self.ignite_client.get_cache(region_name)
            self.cache_instances[region_name] = cache
            return cache
        except:
            return None
            
    async def put_entity(
        self,
        region_name: str,
        entity: DataEntity,
        consistency: Optional[ConsistencyLevel] = None
    ) -> bool:
        """Put entity into cache."""
        cache = await self.get_cache(region_name)
        if not cache:
            raise ValueError(f"Cache region {region_name} not found")
        
        consistency = consistency or self.default_consistency
        
        try:
            # Handle consistency level
            if consistency == ConsistencyLevel.STRONG:
                # Use transaction for strong consistency
                with self.ignite_client.tx_start(
                    concurrency=TransactionConcurrency.PESSIMISTIC,
                    isolation=TransactionIsolation.REPEATABLE_READ
                ):
                    cache.put(entity.entity_id, entity.to_dict())
            else:
                # Direct put for eventual consistency
                cache.put(entity.entity_id, entity.to_dict())
            
            # Update metrics
            self.metrics["puts"] += 1
            
            # Publish event
            await self.event_bus.publish("integration.entity.put", {
                "region": region_name,
                "entity_id": entity.entity_id,
                "entity_type": entity.entity_type,
                "version": entity.version
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Error putting entity {entity.entity_id}: {e}")
            self.metrics["put_errors"] += 1
            return False
            
    async def get_entity(
        self,
        region_name: str,
        entity_id: str,
        consistency: Optional[ConsistencyLevel] = None
    ) -> Optional[DataEntity]:
        """Get entity from cache."""
        cache = await self.get_cache(region_name)
        if not cache:
            return None
        
        try:
            data = cache.get(entity_id)
            if data:
                # Convert back to DataEntity
                entity = DataEntity(
                    entity_id=data["entity_id"],
                    entity_type=data["entity_type"],
                    data=data["data"],
                    version=data["version"],
                    created_at=datetime.fromisoformat(data["created_at"]),
                    updated_at=datetime.fromisoformat(data["updated_at"]),
                    ttl_seconds=data.get("ttl_seconds"),
                    source=DataSource(data["source"]) if data.get("source") else None,
                    metadata=data.get("metadata", {})
                )
                
                self.metrics["gets"] += 1
                self.metrics["hits"] += 1
                return entity
            else:
                self.metrics["gets"] += 1
                self.metrics["misses"] += 1
                
                # Handle read-through if configured
                region = self.cache_regions.get(region_name)
                if region and region.cache_strategy == CacheStrategy.READ_THROUGH:
                    entity = await self._read_through(region_name, entity_id)
                    if entity:
                        await self.put_entity(region_name, entity)
                    return entity
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting entity {entity_id}: {e}")
            self.metrics["get_errors"] += 1
            return None
            
    async def query_entities(
        self,
        region_name: str,
        query: str,
        params: Optional[List[Any]] = None,
        limit: int = 100
    ) -> List[DataEntity]:
        """Query entities using SQL."""
        cache = await self.get_cache(region_name)
        if not cache:
            return []
        
        try:
            # Execute SQL query
            sql_query = SqlFieldsQuery(query)
            if params:
                sql_query.args = params
            sql_query.page_size = limit
            
            cursor = cache.query(sql_query)
            
            entities = []
            for row in cursor:
                # Assuming first column is the key
                entity_data = cache.get(row[0])
                if entity_data:
                    entity = DataEntity(
                        entity_id=entity_data["entity_id"],
                        entity_type=entity_data["entity_type"],
                        data=entity_data["data"],
                        version=entity_data["version"],
                        created_at=datetime.fromisoformat(entity_data["created_at"]),
                        updated_at=datetime.fromisoformat(entity_data["updated_at"]),
                        ttl_seconds=entity_data.get("ttl_seconds"),
                        source=DataSource(entity_data["source"]) if entity_data.get("source") else None,
                        metadata=entity_data.get("metadata", {})
                    )
                    entities.append(entity)
            
            self.metrics["queries"] += 1
            return entities
            
        except Exception as e:
            logger.error(f"Error querying entities: {e}")
            self.metrics["query_errors"] += 1
            return []
            
    async def aggregate_data(
        self,
        region_names: List[str],
        aggregation_key: str,
        aggregation_func: str = "COUNT"
    ) -> Dict[str, Any]:
        """Aggregate data across multiple cache regions."""
        results = {}
        
        for region_name in region_names:
            cache = await self.get_cache(region_name)
            if not cache:
                continue
            
            try:
                # Build aggregation query
                query = f"SELECT {aggregation_key}, {aggregation_func}(*) FROM {region_name} GROUP BY {aggregation_key}"
                sql_query = SqlFieldsQuery(query)
                
                cursor = cache.query(sql_query)
                
                for row in cursor:
                    key = row[0]
                    value = row[1]
                    
                    if key not in results:
                        results[key] = 0
                    
                    if aggregation_func == "COUNT":
                        results[key] += value
                    elif aggregation_func == "SUM":
                        results[key] += value
                    elif aggregation_func == "MAX":
                        results[key] = max(results[key], value)
                    elif aggregation_func == "MIN":
                        results[key] = min(results[key], value) if results[key] else value
                        
            except Exception as e:
                logger.error(f"Error aggregating data from {region_name}: {e}")
        
        return results
        
    async def begin_transaction(
        self,
        transaction_id: Optional[str] = None,
        concurrency: TransactionConcurrency = TransactionConcurrency.PESSIMISTIC,
        isolation: TransactionIsolation = TransactionIsolation.REPEATABLE_READ,
        timeout: int = 5000
    ) -> str:
        """Begin a new transaction."""
        transaction_id = transaction_id or str(uuid.uuid4())
        
        try:
            tx = self.ignite_client.tx_start(
                concurrency=concurrency,
                isolation=isolation,
                timeout=timeout
            )
            
            self.active_transactions[transaction_id] = {
                "tx": tx,
                "started_at": datetime.utcnow(),
                "operations": []
            }
            
            logger.info(f"Started transaction: {transaction_id}")
            return transaction_id
            
        except Exception as e:
            logger.error(f"Error starting transaction: {e}")
            raise
            
    async def commit_transaction(self, transaction_id: str) -> bool:
        """Commit a transaction."""
        if transaction_id not in self.active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        try:
            tx_info = self.active_transactions[transaction_id]
            tx_info["tx"].commit()
            
            # Publish event
            await self.event_bus.publish("integration.transaction.committed", {
                "transaction_id": transaction_id,
                "operations": tx_info["operations"],
                "duration_ms": (datetime.utcnow() - tx_info["started_at"]).total_seconds() * 1000
            })
            
            del self.active_transactions[transaction_id]
            return True
            
        except Exception as e:
            logger.error(f"Error committing transaction {transaction_id}: {e}")
            return False
            
    async def rollback_transaction(self, transaction_id: str) -> bool:
        """Rollback a transaction."""
        if transaction_id not in self.active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        try:
            tx_info = self.active_transactions[transaction_id]
            tx_info["tx"].rollback()
            
            # Publish event
            await self.event_bus.publish("integration.transaction.rolled_back", {
                "transaction_id": transaction_id,
                "operations": tx_info["operations"]
            })
            
            del self.active_transactions[transaction_id]
            return True
            
        except Exception as e:
            logger.error(f"Error rolling back transaction {transaction_id}: {e}")
            return False
            
    async def register_data_source(
        self,
        name: str,
        source_type: DataSource,
        connection_params: Dict[str, Any],
        sync_config: Optional[Dict[str, Any]] = None
    ):
        """Register a data source for integration."""
        self.data_sources[name] = {
            "type": source_type,
            "connection": connection_params,
            "sync": sync_config or {},
            "registered_at": datetime.utcnow()
        }
        
        # Publish event
        await self.event_bus.publish("integration.datasource.registered", {
            "name": name,
            "type": source_type.value
        })
        
        logger.info(f"Registered data source: {name} ({source_type.value})")
        
    async def _create_default_regions(self):
        """Create default cache regions."""
        default_regions = [
            CacheRegion(
                name="session-cache",
                cache_mode="PARTITIONED",
                backups=1,
                eviction_policy="LRU",
                eviction_max_size=100000,
                expiry_policy={"type": "created", "duration": 3600}  # 1 hour TTL
            ),
            CacheRegion(
                name="metadata-cache",
                cache_mode="REPLICATED",
                eviction_policy="LRU",
                eviction_max_size=50000
            ),
            CacheRegion(
                name="metrics-cache",
                cache_mode="PARTITIONED",
                backups=0,
                eviction_policy="FIFO",
                eviction_max_size=1000000,
                expiry_policy={"type": "created", "duration": 60}  # 1 minute TTL
            ),
            CacheRegion(
                name="transaction-cache",
                cache_mode="PARTITIONED",
                backups=2,
                atomicity_mode="TRANSACTIONAL",
                write_synchronization_mode="FULL_SYNC"
            )
        ]
        
        for region in default_regions:
            try:
                await self.create_cache_region(region)
            except Exception as e:
                logger.warning(f"Could not create default region {region.name}: {e}")
                
    async def _create_index(self, cache: Cache, field_name: str, field_type: str):
        """Create index on cache field."""
        try:
            # Create index using SQL
            index_query = f"CREATE INDEX idx_{cache.name}_{field_name} ON {cache.name}({field_name})"
            sql_query = SqlFieldsQuery(index_query)
            cache.query(sql_query)
            
            logger.info(f"Created index on {cache.name}.{field_name}")
            
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            
    async def _read_through(self, region_name: str, entity_id: str) -> Optional[DataEntity]:
        """Read entity from data source."""
        # This would implement read-through logic
        # For now, return None
        return None
        
    async def _monitor_caches(self):
        """Monitor cache health and metrics."""
        while True:
            try:
                for region_name, cache in self.cache_instances.items():
                    try:
                        # Get cache metrics
                        size = cache.get_size()
                        
                        # Report metrics
                        await self.event_bus.publish("integration.metrics", {
                            "region": region_name,
                            "size": size,
                            "hits": self.metrics.get("hits", 0),
                            "misses": self.metrics.get("misses", 0),
                            "puts": self.metrics.get("puts", 0),
                            "gets": self.metrics.get("gets", 0)
                        })
                        
                    except Exception as e:
                        logger.error(f"Error monitoring cache {region_name}: {e}")
                
                # Reset metrics
                self.metrics.clear()
                
                # Sleep for 60 seconds
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache monitoring: {e}")
                await asyncio.sleep(60)
                
    async def _sync_data_sources(self):
        """Sync data from registered sources."""
        while True:
            try:
                for source_name, source_config in self.data_sources.items():
                    sync_config = source_config.get("sync", {})
                    
                    if sync_config.get("enabled", False):
                        interval = sync_config.get("interval_seconds", 300)
                        last_sync = sync_config.get("last_sync")
                        
                        if not last_sync or (datetime.utcnow() - last_sync).seconds >= interval:
                            # Trigger sync
                            await self.event_bus.publish("integration.sync.trigger", {
                                "source": source_name,
                                "type": source_config["type"].value
                            })
                            
                            source_config["sync"]["last_sync"] = datetime.utcnow()
                
                # Sleep for 30 seconds
                await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in data sync: {e}")
                await asyncio.sleep(30)
                
    async def _handle_cache_invalidate(self, event_data: Dict[str, Any]):
        """Handle cache invalidation event."""
        try:
            region_name = event_data.get("region")
            entity_ids = event_data.get("entity_ids", [])
            
            cache = await self.get_cache(region_name)
            if cache:
                for entity_id in entity_ids:
                    cache.remove(entity_id)
                    
                logger.info(f"Invalidated {len(entity_ids)} entries in {region_name}")
                
        except Exception as e:
            logger.error(f"Error handling cache invalidation: {e}")
            
    async def _handle_sync_request(self, event_data: Dict[str, Any]):
        """Handle data sync request."""
        try:
            source_name = event_data.get("source")
            
            if source_name in self.data_sources:
                # This would trigger actual sync logic
                logger.info(f"Processing sync request for {source_name}")
                
        except Exception as e:
            logger.error(f"Error handling sync request: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get integration hub statistics."""
        stats = {
            "cache_regions": len(self.cache_regions),
            "data_sources": len(self.data_sources),
            "active_transactions": len(self.active_transactions),
            "metrics": dict(self.metrics)
        }
        
        # Add cache sizes
        cache_sizes = {}
        for region_name, cache in self.cache_instances.items():
            try:
                cache_sizes[region_name] = cache.get_size()
            except:
                cache_sizes[region_name] = -1
        
        stats["cache_sizes"] = cache_sizes
        
        return stats 