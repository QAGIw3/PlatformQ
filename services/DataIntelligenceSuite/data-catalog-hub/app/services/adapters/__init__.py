"""
Adapters for Data Catalog Hub

Provides backward compatibility adapters for migrating to new architecture.
"""

from .legacy_search_adapter import LegacySearchAdapter

__all__ = [
    'LegacySearchAdapter'
] 