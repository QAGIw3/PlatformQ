"""
Apache Ignite implementation of Digital Integration Hub.

Provides Ignite-specific DIH features including distributed caching,
SQL queries, and ACID transactions.
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import asyncio
import json

from pyignite import Client
from pyignite.queries import SqlFieldsQuery
from pyignite.cache import Cache
from pyignite.exceptions import CacheError
from pyignite.transaction import TransactionConcurrency, TransactionIsolation

from ..core.integration import (
    BaseDigitalIntegrationHub,
    CacheRegion,
    DataSourceConfig,
    ConsistencyLevel
)
from ..core.integration.data_source_manager import (
    BaseDataSourceManager,
    ConnectionPool,
    DataSourceConnection
)
from ..core.integration.cdc_processor import BaseCDCProcessor, CDCEvent
from ..monitoring import StructuredLogger
from ..core.caching import CacheManager
from .ignite_client import IgniteClient, IgniteConfig, CacheConfig

logger = StructuredLogger.get_logger(__name__)


class IgniteDataSourceManager(BaseDataSourceManager):
    """Ignite-specific data source manager"""
    
    async def _initialize_impl(self):
        """Initialize Ignite-specific components"""
        pass
        
    async def _shutdown_impl(self):
        """Shutdown Ignite-specific components"""
        pass
        
    async def _create_connection_impl(
        self,
        name: str,
        source_type: str,
        connection_params: Dict[str, Any],
        pool_config: ConnectionPool
    ) -> Any:
        """Create connection based on source type"""
        if source_type == "postgresql":
            import asyncpg
            return await asyncpg.create_pool(
                **connection_params,
                min_size=pool_config.min_size,
                max_size=pool_config.max_size,
                command_timeout=pool_config.acquire_timeout
            )
            
        elif source_type == "cassandra":
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider
            
            auth = None
            if "username" in connection_params:
                auth = PlainTextAuthProvider(
                    username=connection_params["username"],
                    password=connection_params["password"]
                )
                
            cluster = Cluster(
                connection_params.get("contact_points", ["cassandra"]),
                auth_provider=auth
            )
            return cluster.connect()
            
        elif source_type == "elasticsearch":
            from elasticsearch import AsyncElasticsearch
            return AsyncElasticsearch(**connection_params)
            
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
            
    async def _acquire_connection(self, name: str) -> DataSourceConnection:
        """Acquire connection from pool"""
        # Return the pool/connection directly
        return self._connections[name]
        
    async def _release_connection(self, name: str, conn: DataSourceConnection):
        """Release connection back to pool"""
        # Connection pools handle this automatically
        pass
        
    async def _close_connection_impl(self, name: str):
        """Close connection"""
        conn = self._connections.get(name)
        if conn:
            if hasattr(conn, "close"):
                await conn.close()
            elif hasattr(conn, "shutdown"):
                conn.shutdown()
                
    async def _health_check_impl(self, name: str):
        """Perform health check"""
        config = self._configs[name]
        
        if config["source_type"] == "postgresql":
            conn = self._connections[name]
            await conn.fetchval("SELECT 1")
            
        elif config["source_type"] == "cassandra":
            conn = self._connections[name]
            conn.execute("SELECT now() FROM system.local")
            
        elif config["source_type"] == "elasticsearch":
            conn = self._connections[name]
            await conn.info()


class IgniteCDCProcessor(BaseCDCProcessor):
    """Ignite-specific CDC processor"""
    
    def __init__(self, *args, ignite_client: Client, **kwargs):
        super().__init__(*args, **kwargs)
        self.ignite_client = ignite_client
        
    async def _initialize_impl(self):
        """Initialize Ignite CDC components"""
        # Create CDC state cache
        cache_config = {
            "name": f"cdc_state_{self.source_name}",
            "cache_mode": "REPLICATED",
            "atomicity_mode": "TRANSACTIONAL"
        }
        self.state_cache = self.ignite_client.get_or_create_cache(cache_config)
        
    async def _shutdown_impl(self):
        """Shutdown Ignite CDC components"""
        if hasattr(self, "state_cache"):
            self.state_cache.close()
            
    async def _capture_events(self) -> List[CDCEvent]:
        """Capture events from source"""
        # This would be implemented based on the specific source
        # For now, return empty list
        return []
        
    async def _save_position_impl(self, key: str, position):
        """Save position to Ignite"""
        self.state_cache.put(key, position.to_dict())
        
    async def _load_position_impl(self, key: str):
        """Load position from Ignite"""
        data = self.state_cache.get(key)
        if data:
            from ..core.integration.cdc_processor import CDCPosition
            return CDCPosition(
                source=data["source"],
                position=data["position"],
                timestamp=datetime.fromisoformat(data["timestamp"]),
                metadata=data.get("metadata", {})
            )
        return None


class IgniteDigitalIntegrationHub(BaseDigitalIntegrationHub):
    """
    Apache Ignite implementation of Digital Integration Hub.
    
    Features:
    - Distributed in-memory caching
    - SQL query support
    - ACID transactions
    - Continuous queries
    - Native Ignite integration
    """
    
    def __init__(
        self,
        ignite_nodes: List[Tuple[str, int]],
        default_consistency: ConsistencyLevel = ConsistencyLevel.STRONG,
        cache_manager: Optional[CacheManager] = None
    ):
        super().__init__(default_consistency, cache_manager)
        self.ignite_nodes = ignite_nodes
        self.ignite_client: Optional[IgniteClient] = None
        self.caches: Dict[str, Cache] = {}
        
        # Initialize managers
        self.data_source_manager = IgniteDataSourceManager()
        
    async def _initialize_impl(self):
        """Initialize Ignite-specific components"""
        # Create Ignite client
        config = IgniteConfig(
            nodes=self.ignite_nodes,
            use_ssl=False,
            timeout=10.0
        )
        
        self.ignite_client = IgniteClient(config)
        await self.ignite_client.connect()
        
        # Initialize data source manager
        await self.data_source_manager.initialize()
        
        logger.info("Ignite DIH initialized")
        
    async def _shutdown_impl(self):
        """Shutdown Ignite-specific components"""
        # Close caches
        for cache in self.caches.values():
            cache.close()
            
        # Shutdown managers
        await self.data_source_manager.shutdown()
        
        # Disconnect from Ignite
        if self.ignite_client:
            await self.ignite_client.disconnect()
            
        logger.info("Ignite DIH shutdown")
        
    async def _create_cache_impl(self, region: CacheRegion):
        """Create Ignite cache"""
        # Convert to Ignite cache config
        cache_config = CacheConfig(
            name=region.name,
            cache_mode=region.cache_mode,
            backups=region.backups,
            atomicity_mode=region.atomicity_mode,
            eviction_policy=region.eviction_policy,
            eviction_max_size=region.eviction_max_size,
            statistics_enabled=region.statistics_enabled,
            eager_ttl=region.eager_ttl
        )
        
        # Set TTL if specified
        if region.ttl_seconds:
            cache_config.expiry_policy = {
                "type": "CreatedExpiryPolicy",
                "duration": region.ttl_seconds * 1000  # Convert to milliseconds
            }
            
        # Create cache
        cache = await self.ignite_client.create_cache(region.name, cache_config)
        
        # Create indexes
        if region.indexes:
            for field_name, field_type in region.indexes:
                await self._create_index(region.name, field_name, field_type)
                
        # Store cache reference
        self.caches[region.name] = cache
        
    async def _create_index(self, cache_name: str, field_name: str, field_type: str):
        """Create index on cache field"""
        index_name = f"idx_{cache_name}_{field_name}"
        sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {cache_name} ({field_name})"
        
        query = SqlFieldsQuery(sql)
        cursor = self.ignite_client.client.sql(query)
        cursor.get_all()  # Execute query
        
    async def _connect_data_source(self, source_name: str, config: DataSourceConfig):
        """Connect to data source"""
        await self.data_source_manager.register_data_source(
            name=source_name,
            source_type=config.source_type.value,
            connection_params=config.connection_params,
            pool_config=ConnectionPool(
                min_size=1,
                max_size=config.connection_pool_size,
                acquire_timeout=config.fetch_timeout
            ),
            vault_role=config.vault_role
        )
        
    async def _sync_data_source(
        self,
        source_name: str,
        target_regions: List[str]
    ) -> int:
        """Sync data from source to cache regions"""
        config = self.data_sources[source_name]
        
        # Execute query on source
        results = await self.data_source_manager.execute_query(
            source_name,
            "SELECT * FROM data LIMIT 1000",  # Placeholder query
            timeout=config.fetch_timeout
        )
        
        # Load into target regions
        loaded_count = 0
        for region_name in target_regions:
            cache = self.caches.get(region_name)
            if cache:
                # Batch put
                batch = {}
                for row in results:
                    key = row.get("id", loaded_count)
                    batch[key] = row
                    
                    if len(batch) >= config.batch_size:
                        cache.put_all(batch)
                        loaded_count += len(batch)
                        batch = {}
                        
                # Put remaining
                if batch:
                    cache.put_all(batch)
                    loaded_count += len(batch)
                    
        return loaded_count
        
    async def _get_impl(self, region_name: str, key: str) -> Optional[Any]:
        """Get from Ignite cache"""
        cache = self.caches.get(region_name)
        if cache:
            try:
                return cache.get(key)
            except CacheError:
                return None
        return None
        
    async def _put_impl(
        self,
        region_name: str,
        key: str,
        value: Any,
        ttl_seconds: Optional[int]
    ):
        """Put to Ignite cache"""
        cache = self.caches.get(region_name)
        if cache:
            if ttl_seconds:
                # Ignite doesn't support per-entry TTL directly
                # Would need to use expiry policy
                cache.put(key, value)
            else:
                cache.put(key, value)
                
    async def _remove_impl(self, region_name: str, key: str):
        """Remove from Ignite cache"""
        cache = self.caches.get(region_name)
        if cache:
            cache.remove(key)
            
    async def _query_impl(
        self,
        query: str,
        params: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Execute SQL query on Ignite"""
        sql_query = SqlFieldsQuery(query)
        
        if params:
            # Convert params to args
            sql_query.args = list(params.values())
            
        cursor = self.ignite_client.client.sql(sql_query)
        
        # Get field names
        field_names = []
        if hasattr(cursor, "_field_names"):
            field_names = cursor._field_names
            
        # Fetch results
        results = []
        for row in cursor:
            if field_names:
                results.append(dict(zip(field_names, row)))
            else:
                results.append({"row": row})
                
        return results
        
    async def execute_transaction(
        self,
        operations: List[Tuple[str, str, Any]],
        isolation: TransactionIsolation = TransactionIsolation.REPEATABLE_READ,
        concurrency: TransactionConcurrency = TransactionConcurrency.PESSIMISTIC,
        timeout: int = 5000
    ) -> bool:
        """
        Execute ACID transaction across cache regions.
        
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
            tx = self.ignite_client.client.tx_start(
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
                elif operation == "update":
                    existing = cache.get(data["key"])
                    if existing:
                        updated = {**existing, **data["updates"]}
                        cache.put(data["key"], updated)
                else:
                    raise ValueError(f"Unknown operation: {operation}")
                    
            # Commit transaction
            tx.commit()
            
            # Update metrics
            metrics = self.metrics.get("transactions", self.metrics["transactions"])
            metrics.sync_success_count += 1
            
            return True
            
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            if tx:
                tx.rollback()
                
            # Update metrics
            metrics = self.metrics.get("transactions", self.metrics["transactions"])
            metrics.sync_error_count += 1
            
            raise
            
    async def create_continuous_query(
        self,
        region_name: str,
        filter_sql: str,
        callback: Any,
        initial_query: bool = True
    ) -> str:
        """
        Create a continuous query for real-time updates.
        
        Note: PyIgnite doesn't have direct continuous query support.
        This would need to be implemented using Ignite thin client
        or thick client with Java interop.
        """
        logger.warning("Continuous queries not supported in PyIgnite")
        return f"cq_{region_name}_placeholder" 