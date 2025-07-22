"""
Unified Search Service

Consolidates all search functionality into a single service.
"""

from typing import List, Dict, Any, Optional, Tuple
import asyncio
import logging
from datetime import datetime

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions, ServiceResult
from app.services.ai import UnifiedQueryAnalyzer, EmbeddingManager
from app.services.storage import IgniteCacheAdapter
from app.services.search.strategies import (
    TextSearchStrategy,
    VectorSearchStrategy,
    HybridSearchStrategy,
    ExactMatchStrategy
)
from app.core.search import CatalogSearchIntegration
from app.events import EventBus

logger = logging.getLogger(__name__)


class UnifiedSearchService:
    """
    Unified search service that orchestrates all search operations.
    
    This service consolidates the functionality from:
    - SearchOrchestrator
    - Multiple vector search implementations
    - Search analytics
    - AI-powered search
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        query_analyzer: UnifiedQueryAnalyzer,
        embedding_manager: EmbeddingManager,
        cache_adapter: IgniteCacheAdapter,
        catalog_search: CatalogSearchIntegration,
        event_bus: EventBus
    ):
        self.es_client = es_client
        self.query_analyzer = query_analyzer
        self.embedding_manager = embedding_manager
        self.cache_adapter = cache_adapter
        self.catalog_search = catalog_search
        self.event_bus = event_bus
        
        # Initialize search strategies
        self.strategies = {
            'text': TextSearchStrategy(es_client),
            'vector': VectorSearchStrategy(es_client, embedding_manager),
            'hybrid': HybridSearchStrategy(es_client, embedding_manager),
            'exact': ExactMatchStrategy(es_client)
        }
        
        self._initialized = False
        
    async def initialize(self):
        """Initialize the search service"""
        if self._initialized:
            return
            
        # Initialize components
        await self.query_analyzer.initialize()
        await self.embedding_manager.initialize()
        
        # Initialize strategies
        for strategy in self.strategies.values():
            if hasattr(strategy, 'initialize'):
                await strategy.initialize()
                
        self._initialized = True
        logger.info("Unified search service initialized")
        
    async def search(
        self,
        query: str,
        options: Optional[SearchOptions] = None,
        strategy: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Main search method that handles all search operations.
        
        Args:
            query: Search query
            options: Search options
            strategy: Force specific strategy (auto-detect if None)
            user_id: User ID for personalization
            session_id: Session ID for tracking
            
        Returns:
            ServiceResult with search results
        """
        try:
            if not self._initialized:
                await self.initialize()
                
            start_time = datetime.utcnow()
            
            # Default options
            if options is None:
                options = SearchOptions()
                
            # Analyze query
            query_analysis = await self.query_analyzer.analyze(
                query,
                context={
                    'user_id': user_id,
                    'session_id': session_id,
                    'tenant_id': options.tenant_id
                }
            )
            
            # Select strategy
            if strategy is None:
                strategy = self._select_strategy(query_analysis)
                
            if strategy not in self.strategies:
                return ServiceResult.failure(
                    error=f"Unknown search strategy: {strategy}"
                )
                
            # Check cache
            cache_key = self._generate_cache_key(
                query,
                options,
                strategy,
                user_id
            )
            cached_results = await self._get_cached_results(cache_key)
            if cached_results:
                return ServiceResult.success(cached_results)
                
            # Execute search
            search_strategy = self.strategies[strategy]
            results = await search_strategy.search(
                query_analysis.get('enhanced_query', query),
                options
            )
            
            # Enhance results with catalog metadata
            enhanced_results = await self._enhance_results(
                results,
                query_analysis
            )
            
            # Calculate metrics
            duration_ms = int(
                (datetime.utcnow() - start_time).total_seconds() * 1000
            )
            
            # Build response
            response = {
                'query': query,
                'results': enhanced_results,
                'total': len(enhanced_results),
                'strategy': strategy,
                'query_analysis': query_analysis,
                'duration_ms': duration_ms,
                'session_id': session_id
            }
            
            # Cache results
            await self._cache_results(cache_key, response)
            
            # Track analytics
            await self._track_search(
                query,
                response,
                user_id,
                session_id
            )
            
            return ServiceResult.success(response)
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return ServiceResult.failure(
                error="Search operation failed",
                details={"error": str(e)}
            )
            
    async def index_document(
        self,
        doc_id: str,
        content: Dict[str, Any],
        doc_type: str = "text",
        tenant_id: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Index a document for search"""
        try:
            # Generate embeddings if needed
            embeddings = None
            if doc_type in ['text', 'code']:
                text_content = content.get('content', '')
                if text_content:
                    embeddings = await self.embedding_manager.embed(
                        text_content,
                        model_type=doc_type
                    )
                    
            # Index in Elasticsearch
            index_name = f"catalog_{doc_type}"
            if tenant_id:
                index_name = f"{index_name}_{tenant_id}"
                
            doc_body = {
                **content,
                'doc_id': doc_id,
                'doc_type': doc_type,
                'indexed_at': datetime.utcnow().isoformat()
            }
            
            if embeddings is not None:
                doc_body['embeddings'] = embeddings.tolist()
                
            await self.es_client.index(
                index=index_name,
                id=doc_id,
                body=doc_body
            )
            
            return ServiceResult.success({
                'doc_id': doc_id,
                'index': index_name,
                'indexed': True
            })
            
        except Exception as e:
            logger.error(f"Failed to index document {doc_id}: {e}")
            return ServiceResult.failure(
                error="Failed to index document",
                details={"error": str(e)}
            )
            
    async def delete_document(
        self,
        doc_id: str,
        doc_type: str = "text",
        tenant_id: Optional[str] = None
    ) -> ServiceResult[bool]:
        """Delete a document from search index"""
        try:
            index_name = f"catalog_{doc_type}"
            if tenant_id:
                index_name = f"{index_name}_{tenant_id}"
                
            await self.es_client.delete(
                index=index_name,
                id=doc_id
            )
            
            # Clear from cache
            await self.cache_adapter.delete_pattern(f"*{doc_id}*")
            
            return ServiceResult.success(True)
            
        except Exception as e:
            logger.error(f"Failed to delete document {doc_id}: {e}")
            return ServiceResult.failure(
                error="Failed to delete document",
                details={"error": str(e)}
            )
            
    async def get_suggestions(
        self,
        prefix: str,
        context: Optional[Dict[str, Any]] = None,
        limit: int = 10
    ) -> ServiceResult[List[str]]:
        """Get search suggestions"""
        try:
            # Use Elasticsearch completion suggester
            response = await self.es_client.search(
                index="catalog_*",
                body={
                    "suggest": {
                        "text": prefix,
                        "completion": {
                            "field": "suggest",
                            "size": limit,
                            "skip_duplicates": True
                        }
                    }
                }
            )
            
            suggestions = []
            for option in response.get('suggest', {}).get('completion', []):
                for suggestion in option.get('options', []):
                    suggestions.append(suggestion['text'])
                    
            return ServiceResult.success(suggestions[:limit])
            
        except Exception as e:
            logger.error(f"Failed to get suggestions: {e}")
            return ServiceResult.failure(
                error="Failed to get suggestions",
                details={"error": str(e)}
            )
            
    # Private helper methods
    
    def _select_strategy(self, query_analysis: Dict[str, Any]) -> str:
        """Select search strategy based on query analysis"""
        intent = query_analysis.get('intent', {})
        entities = query_analysis.get('entities', {})
        
        # Exact match for IDs
        if entities.get('uuid') or entities.get('exact_match'):
            return 'exact'
            
        # Vector search for semantic queries
        if intent.get('semantic_search') or intent.get('find_similar'):
            return 'vector'
            
        # Hybrid for complex queries
        if len(entities) > 2 or intent.get('complex_query'):
            return 'hybrid'
            
        # Default to text search
        return 'text'
        
    def _generate_cache_key(
        self,
        query: str,
        options: SearchOptions,
        strategy: str,
        user_id: Optional[str]
    ) -> str:
        """Generate cache key for search results"""
        import hashlib
        import json
        
        cache_data = {
            'query': query,
            'options': options.__dict__,
            'strategy': strategy,
            'user_id': user_id
        }
        
        cache_str = json.dumps(cache_data, sort_keys=True)
        return f"search:{hashlib.md5(cache_str.encode()).hexdigest()}"
        
    async def _get_cached_results(
        self,
        cache_key: str
    ) -> Optional[Dict[str, Any]]:
        """Get cached search results"""
        return await self.cache_adapter.get(cache_key)
        
    async def _cache_results(
        self,
        cache_key: str,
        results: Dict[str, Any]
    ):
        """Cache search results"""
        await self.cache_adapter.set(
            cache_key,
            results,
            ttl=300  # 5 minutes
        )
        
    async def _enhance_results(
        self,
        results: List[SearchResult],
        query_analysis: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Enhance search results with catalog metadata"""
        enhanced = []
        
        for result in results:
            # Get additional metadata from catalog
            catalog_data = await self.catalog_search.get_entity_details(
                result.id
            )
            
            enhanced_result = {
                'id': result.id,
                'type': result.type,
                'title': result.title,
                'description': result.description,
                'score': result.score,
                'highlights': result.highlights,
                'metadata': {
                    **result.metadata,
                    'catalog': catalog_data
                }
            }
            
            enhanced.append(enhanced_result)
            
        return enhanced
        
    async def _track_search(
        self,
        query: str,
        response: Dict[str, Any],
        user_id: Optional[str],
        session_id: Optional[str]
    ):
        """Track search analytics"""
        # Emit search event
        await self.event_bus.publish({
            'event_type': 'search_performed',
            'data': {
                'query': query,
                'user_id': user_id,
                'session_id': session_id,
                'results_count': response['total'],
                'strategy': response['strategy'],
                'duration_ms': response['duration_ms'],
                'timestamp': datetime.utcnow().isoformat()
            }
        }) 