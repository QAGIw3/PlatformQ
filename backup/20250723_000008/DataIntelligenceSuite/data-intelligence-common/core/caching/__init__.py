"""
Unified Caching Framework for DataIntelligenceSuite

Provides a consistent caching layer using Apache Ignite across all services.
"""

from .cache_manager import CacheManager, CacheConfig
from .cache_decorators import cached, cache_aside, cache_invalidate
from .cache_patterns import CachePattern, CacheStrategy
from .distributed_cache import DistributedCacheClient

__all__ = [
    "CacheManager",
    "CacheConfig",
    "cached",
    "cache_aside",
    "cache_invalidate",
    "CachePattern",
    "CacheStrategy",
    "DistributedCacheClient",
] 