"""
Base Data Integration Hub (DIH) implementation with caching and monitoring.

Provides a foundation for building data integration services.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Callable, Set, Union, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
import json
from enum import Enum

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import MetricsCollector, StructuredLogger
from ...utils.converters import DataConverter
from ..events import EventBus, Event, create_data_event
from ..caching import CacheManager, CacheConfig, CacheStrategy

logger = StructuredLogger.get_logger(__name__)


class DataSource(str, Enum):
    """Supported data sources for integration"""
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    POSTGRESQL = "postgresql"
    MONGODB = "mongodb"
    REST_API = "rest_api"
    PULSAR_STREAM = "pulsar_stream"
    IGNITE = "ignite"
    JANUSGRAPH = "janusgraph"
    MINIO = "minio"
    TRINO = "trino"
    DRUID = "druid"


class ConsistencyLevel(str, Enum):
    """Data consistency levels"""
    EVENTUAL = "eventual"
    STRONG = "strong"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    CONSISTENT_PREFIX = "consistent_prefix"


@dataclass
class DataEntity:
    """Generic data entity for integration"""
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
        """Convert to dictionary"""
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
    """Configuration for a cache region"""
    name: str
    cache_mode: str = "PARTITIONED"  # PARTITIONED, REPLICATED, LOCAL
    backups: int = 1
    atomicity_mode: str = "ATOMIC"  # ATOMIC, TRANSACTIONAL
    cache_strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
    eviction_policy: str = "LRU"  # LRU, LFU, FIFO, RANDOM
    eviction_max_size: int = 10000000  # 10M entries
    ttl_seconds: Optional[int] = None
    indexes: List[Tuple[str, str]] = field(default_factory=list)  # [(field, type)]
    sql_schema: Optional[str] = None
    query_parallelism: int = 4
    rebalance_mode: str = "SYNC"  # SYNC, ASYNC, NONE
    
    # Security
    encrypt_data: bool = False
    access_control: bool = False
    allowed_roles: List[str] = field(default_factory=list)
    
    # Performance
    statistics_enabled: bool = True
    eager_ttl: bool = True
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataSourceConfig:
    """Configuration for a data source"""
    source_type: DataSource
    connection_params: Dict[str, Any]
    sync_interval_seconds: Optional[int] = None
    batch_size: int = 1000
    consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL
    transform_function: Optional[str] = None  # Python function path
    
    # Security
    vault_role: str = "readonly"
    encrypt_in_transit: bool = True
    
    # Performance
    connection_pool_size: int = 10
    fetch_timeout: int = 30
    retry_policy: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IntegrationMetrics:
    """Metrics for data integration"""
    sync_success_count: int = 0
    sync_error_count: int = 0
    records_processed: int = 0
    bytes_transferred: int = 0
    last_sync_time: Optional[datetime] = None
    average_sync_duration: Optional[float] = None
    cache_hit_rate: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "sync_success_count": self.sync_success_count,
            "sync_error_count": self.sync_error_count,
            "records_processed": self.records_processed,
            "bytes_transferred": self.bytes_transferred,
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "average_sync_duration": self.average_sync_duration,
            "cache_hit_rate": self.cache_hit_rate
        }


class BaseDigitalIntegrationHub(ABC):
    """
    Abstract base class for Digital Integration Hub implementations.
    
    Provides patterns for:
    - Multi-source data integration
    - Cache management
    - Data synchronization
    - Query federation
    """
    
    def __init__(
        self,
        default_consistency: ConsistencyLevel = ConsistencyLevel.STRONG,
        cache_manager: Optional[CacheManager] = None
    ):
        self.default_consistency = default_consistency
        self.cache_manager = cache_manager
        
        # Storage
        self.cache_regions: Dict[str, CacheRegion] = {}
        self.data_sources: Dict[str, DataSourceConfig] = {}
        
        # Metrics
        self.metrics: Dict[str, IntegrationMetrics] = {}
        
        # Tasks
        self._sync_tasks: Dict[str, asyncio.Task] = {}
        self._initialized = False
        
    async def initialize(self):
        """Initialize the integration hub"""
        if self._initialized:
            return
            
        logger.info("Initializing Digital Integration Hub")
        
        # Initialize implementation
        await self._initialize_impl()
        
        # Start monitoring
        asyncio.create_task(self._monitor_metrics())
        
        self._initialized = True
        logger.info("Digital Integration Hub initialized")
        
    async def shutdown(self):
        """Shutdown the integration hub"""
        logger.info("Shutting down Digital Integration Hub")
        
        # Cancel sync tasks
        for task in self._sync_tasks.values():
            task.cancel()
            
        await asyncio.gather(*self._sync_tasks.values(), return_exceptions=True)
        
        # Shutdown implementation
        await self._shutdown_impl()
        
        self._initialized = False
        logger.info("Digital Integration Hub shutdown complete")
        
    @abstractmethod
    async def _initialize_impl(self):
        """Initialize implementation-specific components"""
        pass
        
    @abstractmethod
    async def _shutdown_impl(self):
        """Shutdown implementation-specific components"""
        pass
        
    async def create_cache_region(self, region: CacheRegion) -> None:
        """
        Create a new cache region.
        
        Args:
            region: Cache region configuration
        """
        logger.info(f"Creating cache region: {region.name}")
        
        # Store configuration
        self.cache_regions[region.name] = region
        
        # Create implementation-specific cache
        await self._create_cache_impl(region)
        
        # Initialize metrics
        self.metrics[region.name] = IntegrationMetrics()
        
        logger.info(f"Cache region created: {region.name}")
        
    @abstractmethod
    async def _create_cache_impl(self, region: CacheRegion):
        """Create implementation-specific cache"""
        pass
        
    async def register_data_source(
        self,
        source_name: str,
        config: DataSourceConfig,
        target_regions: List[str]
    ):
        """
        Register a data source for synchronization.
        
        Args:
            source_name: Unique source identifier
            config: Data source configuration
            target_regions: Cache regions to sync to
        """
        logger.info(f"Registering data source: {source_name}")
        
        # Validate target regions
        for region in target_regions:
            if region not in self.cache_regions:
                raise ValueError(f"Unknown cache region: {region}")
                
        # Store configuration
        self.data_sources[source_name] = config
        
        # Initialize connection
        await self._connect_data_source(source_name, config)
        
        # Start sync if configured
        if config.sync_interval_seconds:
            task = asyncio.create_task(
                self._sync_data_source_loop(source_name, target_regions)
            )
            self._sync_tasks[source_name] = task
            
        logger.info(f"Data source registered: {source_name}")
        
    @abstractmethod
    async def _connect_data_source(self, source_name: str, config: DataSourceConfig):
        """Connect to data source"""
        pass
        
    async def _sync_data_source_loop(self, source_name: str, target_regions: List[str]):
        """Synchronization loop for data source"""
        config = self.data_sources[source_name]
        metrics = self.metrics.get(source_name, IntegrationMetrics())
        
        while True:
            try:
                start_time = datetime.utcnow()
                
                # Perform sync
                records = await self._sync_data_source(source_name, target_regions)
                
                # Update metrics
                metrics.sync_success_count += 1
                metrics.records_processed += records
                metrics.last_sync_time = datetime.utcnow()
                
                duration = (datetime.utcnow() - start_time).total_seconds()
                if metrics.average_sync_duration:
                    metrics.average_sync_duration = (
                        metrics.average_sync_duration * 0.9 + duration * 0.1
                    )
                else:
                    metrics.average_sync_duration = duration
                    
                logger.debug(
                    f"Synced {records} records from {source_name} in {duration:.2f}s"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error syncing {source_name}: {e}")
                metrics.sync_error_count += 1
                
            # Wait for next sync
            await asyncio.sleep(config.sync_interval_seconds)
            
    @abstractmethod
    async def _sync_data_source(
        self,
        source_name: str,
        target_regions: List[str]
    ) -> int:
        """
        Sync data from source to target regions.
        
        Returns:
            Number of records synced
        """
        pass
        
    async def get(self, region_name: str, key: str) -> Optional[Any]:
        """
        Get value from cache region.
        
        Args:
            region_name: Cache region name
            key: Cache key
            
        Returns:
            Cached value or None
        """
        region = self.cache_regions.get(region_name)
        if not region:
            raise ValueError(f"Unknown cache region: {region_name}")
            
        return await self._get_impl(region_name, key)
        
    @abstractmethod
    async def _get_impl(self, region_name: str, key: str) -> Optional[Any]:
        """Implementation-specific get"""
        pass
        
    async def put(
        self,
        region_name: str,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None
    ):
        """
        Put value into cache region.
        
        Args:
            region_name: Cache region name
            key: Cache key
            value: Value to cache
            ttl_seconds: Optional TTL override
        """
        region = self.cache_regions.get(region_name)
        if not region:
            raise ValueError(f"Unknown cache region: {region_name}")
            
        await self._put_impl(region_name, key, value, ttl_seconds or region.ttl_seconds)
        
    @abstractmethod
    async def _put_impl(
        self,
        region_name: str,
        key: str,
        value: Any,
        ttl_seconds: Optional[int]
    ):
        """Implementation-specific put"""
        pass
        
    async def remove(self, region_name: str, key: str):
        """
        Remove value from cache region.
        
        Args:
            region_name: Cache region name
            key: Cache key
        """
        region = self.cache_regions.get(region_name)
        if not region:
            raise ValueError(f"Unknown cache region: {region_name}")
            
        await self._remove_impl(region_name, key)
        
    @abstractmethod
    async def _remove_impl(self, region_name: str, key: str):
        """Implementation-specific remove"""
        pass
        
    async def query_cross_region(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute query across cache regions.
        
        Args:
            query: Query string (implementation-specific)
            params: Query parameters
            
        Returns:
            Query results
        """
        return await self._query_impl(query, params)
        
    @abstractmethod
    async def _query_impl(
        self,
        query: str,
        params: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Implementation-specific query"""
        pass
        
    async def get_metrics(
        self,
        source_or_region: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get integration metrics.
        
        Args:
            source_or_region: Specific source/region or None for all
            
        Returns:
            Metrics dictionary
        """
        if source_or_region:
            metrics = self.metrics.get(source_or_region)
            return metrics.to_dict() if metrics else {}
        else:
            return {
                name: metrics.to_dict()
                for name, metrics in self.metrics.items()
            }
            
    async def _monitor_metrics(self):
        """Monitor and log metrics periodically"""
        while self._initialized:
            try:
                # Log summary metrics
                total_syncs = sum(m.sync_success_count for m in self.metrics.values())
                total_errors = sum(m.sync_error_count for m in self.metrics.values())
                total_records = sum(m.records_processed for m in self.metrics.values())
                
                logger.debug(
                    f"DIH Metrics - Syncs: {total_syncs}, Errors: {total_errors}, "
                    f"Records: {total_records}"
                )
                
                await asyncio.sleep(60)  # Log every minute
                
            except Exception as e:
                logger.error(f"Error monitoring metrics: {e}")
                await asyncio.sleep(60) 