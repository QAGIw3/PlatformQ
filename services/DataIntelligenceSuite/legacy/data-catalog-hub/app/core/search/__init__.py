"""
Search functionality for Data Catalog Hub

Provides core search engine, catalog-specific search integration,
and unified search across all platform services.
"""

from .engine import SearchEngine
from .catalog_integration import CatalogSearchIntegration, SearchIntent, CatalogSearchResult
from .unified_integration import UnifiedSearchIntegration, ServiceRegistry

__all__ = [
    'SearchEngine',
    'CatalogSearchIntegration',
    'SearchIntent',
    'CatalogSearchResult',
    'UnifiedSearchIntegration',
    'ServiceRegistry'
] 