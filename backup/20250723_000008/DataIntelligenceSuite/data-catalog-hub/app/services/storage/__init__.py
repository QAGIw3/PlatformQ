"""
Storage Adapters for Data Catalog Hub

Storage backend implementations including caching and persistence.
"""

from .ignite_cache_adapter import IgniteCacheAdapter

__all__ = [
    'IgniteCacheAdapter'
] 