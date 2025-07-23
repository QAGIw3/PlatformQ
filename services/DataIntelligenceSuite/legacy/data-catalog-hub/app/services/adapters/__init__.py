"""
Adapter Services

Provides compatibility adapters for migration.
"""

from .search_migration_adapter import (
    VectorSearchServiceAdapter,
    ESVectorSearchServiceAdapter,
    HybridSearchServiceAdapter
)

__all__ = [
    'VectorSearchServiceAdapter',
    'ESVectorSearchServiceAdapter',
    'HybridSearchServiceAdapter'
] 