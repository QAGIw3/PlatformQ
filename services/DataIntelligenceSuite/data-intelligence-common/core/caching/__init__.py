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
    cache_aside,
    cache_invalidate,
    invalidate_cache,
    cache_key_generator,
    memoize,
    CacheContext
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
    DistributedCacheClient,
    NodeInfo
)

# Cache utilities
from .cache_utils import (
    generate_cache_key,
    hash_key,
    parse_ttl,
    serialize_value,
    deserialize_value,
    estimate_size,
    is_cache_key_valid,
    normalize_cache_key,
    create_key_pattern,
    match_keys,
    chunk_keys,
    create_cache_key_builder,
    CacheKeyBuilder,
    calculate_hit_rate,
    format_cache_stats
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
    "cache_aside",
    "cache_invalidate",
    "invalidate_cache",
    "cache_key_generator",
    "memoize",
    "CacheContext",
    
    # Patterns
    "CachePattern",
    "MultiLevelCache",
    "CacheWarmer",
    "CacheInvalidator",
    
    # Distributed
    "DistributedCacheClient",
    "NodeInfo",
    
    # Utilities
    "generate_cache_key",
    "hash_key",
    "parse_ttl",
    "serialize_value",
    "deserialize_value",
    "estimate_size",
    "is_cache_key_valid",
    "normalize_cache_key",
    "create_key_pattern",
    "match_keys",
    "chunk_keys",
    "create_cache_key_builder",
    "CacheKeyBuilder",
    "calculate_hit_rate",
    "format_cache_stats"
] 