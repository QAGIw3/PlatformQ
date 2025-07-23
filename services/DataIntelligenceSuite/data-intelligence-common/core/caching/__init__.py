"""
Unified Caching Framework for DataIntelligenceSuite

Provides comprehensive caching capabilities with multiple strategies and patterns.
"""

# Cache strategies and enums
from .strategies import (
    CacheStrategy,
    EvictionPolicy,
    CacheMode,
    CacheEntry,
    BaseCacheStrategy,
    CacheAsideStrategy,
    ReadThroughStrategy,
    WriteThroughStrategy,
    WriteBehindStrategy,
    RefreshAheadStrategy,
    create_cache_strategy
)

# Cache manager
from .cache_manager import (
    CacheManager,
    CacheConfig,
    CacheStats
)

# Cache decorators
from .cache_decorators import (
    cached,
    cache_result,
    invalidate_cache,
    cache_key_generator
)

# Cache patterns
from .cache_patterns import (
    CachePattern,
    MultiLevelCache,
    CacheWarmer,
    CacheInvalidator
)

# Distributed cache
from .distributed_cache import (
    DistributedCache,
    DistributedCacheConfig,
    CacheNode,
    ConsistentHashRing
)

__all__ = [
    # Strategies and enums
    "CacheStrategy",
    "EvictionPolicy", 
    "CacheMode",
    "CacheEntry",
    "BaseCacheStrategy",
    "CacheAsideStrategy",
    "ReadThroughStrategy",
    "WriteThroughStrategy",
    "WriteBehindStrategy",
    "RefreshAheadStrategy",
    "create_cache_strategy",
    
    # Manager
    "CacheManager",
    "CacheConfig",
    "CacheStats",
    
    # Decorators
    "cached",
    "cache_result",
    "invalidate_cache",
    "cache_key_generator",
    
    # Patterns
    "CachePattern",
    "MultiLevelCache",
    "CacheWarmer",
    "CacheInvalidator",
    
    # Distributed
    "DistributedCache",
    "DistributedCacheConfig",
    "CacheNode",
    "ConsistentHashRing"
] 