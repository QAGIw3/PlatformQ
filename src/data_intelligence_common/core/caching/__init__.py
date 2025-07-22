"""
Unified caching framework for DataIntelligenceSuite using Apache Ignite.

This module provides a consistent caching interface across all services.
"""

from .cache_manager import CacheManager, CacheConfig
from .cache_strategies import (
    CacheStrategy,
    CacheAsideStrategy,
    ReadThroughStrategy,
    WriteThroughStrategy,
    WriteBehindStrategy
)
from .cache_decorators import cache, cache_async, invalidate_cache
from .distributed_cache import DistributedCacheClient
from .cache_metrics import CacheMetrics

__all__ = [
    "CacheManager",
    "CacheConfig",
    "CacheStrategy",
    "CacheAsideStrategy",
    "ReadThroughStrategy",
    "WriteThroughStrategy",
    "WriteBehindStrategy",
    "cache",
    "cache_async",
    "invalidate_cache",
    "DistributedCacheClient",
    "CacheMetrics"
] 