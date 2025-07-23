"""
Data Integration Engine

Provides high-performance data integration, caching, and synchronization.
"""

from .integration_hub import (
    IntegrationHub,
    DataSource,
    CacheStrategy,
    ConsistencyLevel,
    DataEntity,
    CacheRegion
)
from .cache_manager import (
    CacheManager,
    CacheEntry,
    CacheStats,
    EvictionPolicy
)
from .sync_engine import (
    SyncEngine,
    SyncMode,
    SyncStatus,
    SyncJob,
    SyncResult
)
from .data_aggregator import (
    DataAggregator,
    AggregationType,
    AggregationRule,
    AggregationResult
)

__all__ = [
    # Integration Hub
    "IntegrationHub",
    "DataSource",
    "CacheStrategy",
    "ConsistencyLevel",
    "DataEntity",
    "CacheRegion",
    
    # Cache Manager
    "CacheManager",
    "CacheEntry",
    "CacheStats",
    "EvictionPolicy",
    
    # Sync Engine
    "SyncEngine",
    "SyncMode",
    "SyncStatus",
    "SyncJob",
    "SyncResult",
    
    # Data Aggregator
    "DataAggregator",
    "AggregationType",
    "AggregationRule",
    "AggregationResult"
] 