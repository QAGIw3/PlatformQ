"""
Data Integration Framework

Provides patterns and abstractions for building data integration services.
"""

from .base_dih import (
    DataSource,
    CacheStrategy,
    ConsistencyLevel,
    DataEntity,
    CacheRegion,
    DataSourceConfig,
    BaseDigitalIntegrationHub
)

from .data_source_manager import (
    BaseDataSourceManager,
    DataSourceConnection,
    ConnectionPool,
    ConnectionState,
    ConnectionHealth
)

from .cdc_processor import (
    CDCEvent,
    CDCEventType,
    CDCSourceType,
    CDCPosition,
    CDCMetrics,
    BaseCDCProcessor,
    CDCHandler
)

from .cache_patterns import (
    CacheWarmer,
    CacheOptimizer,
    CacheStatistics,
    WarmingStrategy,
    WarmingTask,
    OptimizationGoal,
    OptimizationRecommendation
)

__all__ = [
    # Base DIH
    "DataSource",
    "CacheStrategy", 
    "ConsistencyLevel",
    "DataEntity",
    "CacheRegion",
    "DataSourceConfig",
    "BaseDigitalIntegrationHub",
    
    # Data Source Management
    "BaseDataSourceManager",
    "DataSourceConnection",
    "ConnectionPool",
    "ConnectionState",
    "ConnectionHealth",
    
    # CDC
    "CDCEvent",
    "CDCEventType",
    "CDCSourceType",
    "CDCPosition",
    "CDCMetrics",
    "BaseCDCProcessor",
    "CDCHandler",
    
    # Cache Patterns
    "CacheWarmer",
    "CacheOptimizer",
    "CacheStatistics",
    "WarmingStrategy",
    "WarmingTask",
    "OptimizationGoal",
    "OptimizationRecommendation"
] 