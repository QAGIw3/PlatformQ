"""
Search and indexing services for Data Catalog Hub

This module provides both legacy services (for backward compatibility)
and new consolidated services following improved architecture.
"""

# Legacy services (maintained for backward compatibility)
from .indexer import IndexingService
from .es_vector_search import ElasticsearchVectorService
from .hybrid_search import HybridSearchService
from .query_understanding import QueryUnderstandingService
from .ai_search_enhancement import AISearchEnhancement
from .search_analytics import SearchAnalyticsService
from .vector_search import VectorSearchService
from .enhanced_vector_search import EnhancedVectorSearchService

# New consolidated architecture
from .search_orchestrator import SearchOrchestrator
from .interfaces import SearchResult, SearchOptions
from .adapters import LegacySearchAdapter
from .ai import EmbeddingManager, UnifiedQueryAnalyzer
from .storage import IgniteCacheAdapter
from .strategies import TextSearchStrategy, ExactMatchStrategy

__all__ = [
    # Legacy exports
    'IndexingService',
    'ElasticsearchVectorService',
    'HybridSearchService',
    'QueryUnderstandingService',
    'AISearchEnhancement',
    'SearchAnalyticsService',
    'VectorSearchService',
    'EnhancedVectorSearchService',
    # New consolidated exports
    'SearchOrchestrator',
    'SearchResult',
    'SearchOptions',
    'LegacySearchAdapter',
    'EmbeddingManager',
    'UnifiedQueryAnalyzer',
    'IgniteCacheAdapter',
    'TextSearchStrategy',
    'ExactMatchStrategy'
] 