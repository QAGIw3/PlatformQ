"""State management with Apache Ignite."""

from .ignite_manager import IgniteStateManager
from .cache_config import CacheConfig, CacheType

__all__ = [
    "IgniteStateManager",
    "CacheConfig",
    "CacheType"
] 