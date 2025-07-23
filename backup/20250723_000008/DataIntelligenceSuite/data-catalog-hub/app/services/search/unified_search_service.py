"""
Unified Search Service

Consolidates all search functionality into a single service.
"""

from typing import List, Dict, Any, Optional, Tuple
import asyncio
import logging
from datetime import datetime
import uuid

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
            
    async def vector_search(
        self,
        query: str,
        entity_types: Optional[List[str]] = None,
        embedding_fields: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        threshold: float = 0.7,
        limit: int = 20
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Vector similarity search using embeddings.
        
        Args:
            query: Text to convert to embedding
            entity_types: Limit to specific entity types
            embedding_fields: Fields containing embeddings to search
            filters: Additional filter criteria
            threshold: Minimum similarity score (0-1)
            limit: Maximum results
            
        Returns:
            ServiceResult with vector search results
        """
        try:
            if not self._initialized:
                await self.initialize()
                
            start_time = datetime.utcnow()
            
            # Create search options
            options = SearchOptions(
                limit=limit,
                filters=filters or {},
                include_metadata=True
            )
            
            # Add entity type filter if specified
            if entity_types:
                options.filters['entity_type'] = entity_types
                
            # Set embedding fields
            if embedding_fields:
                options.metadata = {'embedding_fields': embedding_fields}
                
            # Force vector strategy
            strategy = self.strategies['vector']
            
            # Generate query embedding
            query_embedding = await self.embedding_manager.embed(query)
            
            # Execute vector search
            results = await strategy.search(query, options)
            
            # Filter by threshold
            filtered_results = []
            for result in results:
                if result.score >= threshold:
                    filtered_results.append(result)
                    
            # Calculate metrics
            duration_ms = int(
                (datetime.utcnow() - start_time).total_seconds() * 1000
            )
            
            # Format response
            response = {
                'query': query,
                'results': [self._format_result(r) for r in filtered_results],
                'total': len(filtered_results),
                'threshold': threshold,
                'embedding_fields': embedding_fields or ['text_embedding'],
                'entity_types': entity_types,
                'metrics': {
                    'duration_ms': duration_ms,
                    'results_before_threshold': len(results),
                    'results_after_threshold': len(filtered_results)
                }
            }
            
            # Emit analytics event
            await self.event_bus.emit(
                'search.vector_search',
                {
                    'query': query,
                    'result_count': len(filtered_results),
                    'duration_ms': duration_ms,
                    'threshold': threshold,
                    'entity_types': entity_types
                }
            )
            
            return ServiceResult.success(response)
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="Vector search failed",
                details={'error': str(e)}
            )
            
    async def find_similar(
        self,
        entity_id: str,
        limit: int = 10,
        entity_types: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Find entities similar to a given entity.
        
        Args:
            entity_id: Entity to find similar items for
            limit: Maximum results
            entity_types: Limit to specific entity types
            filters: Additional filter criteria
            
        Returns:
            ServiceResult with similar entities
        """
        try:
            if not self._initialized:
                await self.initialize()
                
            # Create search options
            options = SearchOptions(
                limit=limit,
                filters=filters or {}
            )
            
            if entity_types:
                options.filters['entity_type'] = entity_types
                
            # Use vector strategy's find_similar method
            strategy = self.strategies['vector']
            
            if not hasattr(strategy, 'find_similar'):
                return ServiceResult.failure(
                    error="Find similar not supported by current strategy"
                )
                
            results = await strategy.find_similar(entity_id, options)
            
            # Format response
            response = {
                'source_entity': entity_id,
                'similar_entities': [self._format_result(r) for r in results],
                'total': len(results),
                'entity_types': entity_types
            }
            
            return ServiceResult.success(response)
            
        except Exception as e:
            logger.error(f"Find similar failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="Find similar failed",
                details={'error': str(e)}
            )
        
    async def unified_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 10,
        from_: int = 0,
        facets: Optional[List[str]] = None,
        sort: Optional[Dict[str, str]] = None,
        tenant_id: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Alias for the main search method with additional parameters.
        
        Args:
            query: Search query
            filters: Search filters
            size: Number of results
            from_: Offset for pagination
            facets: Fields to generate facets for
            sort: Sort criteria
            tenant_id: Tenant ID for filtering
            
        Returns:
            ServiceResult with search results
        """
        options = SearchOptions(
            limit=size,
            offset=from_,
            filters=filters or {},
            sort=sort,
            include_facets=facets is not None,
            tenant_id=tenant_id
        )
        
        if facets:
            options.metadata = {'facet_fields': facets}
            
        return await self.search(query, options)
        
    async def text_search(
        self,
        query: str,
        fields: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        fuzziness: str = "AUTO",
        operator: str = "OR",
        limit: int = 20
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Traditional text-based search.
        
        Args:
            query: Search query
            fields: Fields to search in
            filters: Additional filters
            fuzziness: Fuzzy matching level
            operator: Query operator (AND/OR)
            limit: Maximum results
            
        Returns:
            ServiceResult with text search results
        """
        try:
            if not self._initialized:
                await self.initialize()
                
            options = SearchOptions(
                limit=limit,
                filters=filters or {},
                metadata={
                    'search_fields': fields,
                    'fuzziness': fuzziness,
                    'operator': operator
                }
            )
            
            # Force text strategy
            strategy = self.strategies['text']
            results = await strategy.search(query, options)
            
            response = {
                'query': query,
                'results': [self._format_result(r) for r in results],
                'total': len(results),
                'fields': fields,
                'strategy': 'text'
            }
            
            return ServiceResult.success(response)
            
        except Exception as e:
            logger.error(f"Text search failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="Text search failed",
                details={'error': str(e)}
            )
            
    async def hybrid_search(
        self,
        query: str,
        text_weight: float = 0.5,
        vector_weight: float = 0.5,
        filters: Optional[Dict[str, Any]] = None,
        entity_types: Optional[List[str]] = None,
        limit: int = 20
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Hybrid search combining text and vector approaches.
        
        Args:
            query: Search query
            text_weight: Weight for text search (0-1)
            vector_weight: Weight for vector search (0-1)
            filters: Additional filters
            entity_types: Limit to specific entity types
            limit: Maximum results
            
        Returns:
            ServiceResult with hybrid search results
        """
        try:
            if not self._initialized:
                await self.initialize()
                
            # Normalize weights
            total_weight = text_weight + vector_weight
            text_weight = text_weight / total_weight
            vector_weight = vector_weight / total_weight
            
            options = SearchOptions(
                limit=limit,
                filters=filters or {},
                metadata={
                    'text_weight': text_weight,
                    'vector_weight': vector_weight
                }
            )
            
            if entity_types:
                options.filters['entity_type'] = entity_types
                
            # Force hybrid strategy
            strategy = self.strategies['hybrid']
            results = await strategy.search(query, options)
            
            response = {
                'query': query,
                'results': [self._format_result(r) for r in results],
                'total': len(results),
                'weights': {
                    'text': text_weight,
                    'vector': vector_weight
                },
                'strategy': 'hybrid'
            }
            
            return ServiceResult.success(response)
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="Hybrid search failed",
                details={'error': str(e)}
            )
            
    async def ai_powered_search(
        self,
        query: str,
        use_rag: bool = True,
        include_explanations: bool = True,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10
    ) -> ServiceResult[Dict[str, Any]]:
        """
        AI-powered search with query understanding and RAG.
        
        Args:
            query: Natural language query
            use_rag: Whether to use RAG for answer generation
            include_explanations: Include result explanations
            filters: Additional filters
            limit: Maximum results
            
        Returns:
            ServiceResult with AI-enhanced search results
        """
        try:
            if not self._initialized:
                await self.initialize()
                
            start_time = datetime.utcnow()
            
            # Analyze query with AI
            query_analysis = await self.query_analyzer.analyze(
                query,
                context={'use_rag': use_rag}
            )
            
            # Create search options
            options = SearchOptions(
                limit=limit,
                filters=filters or {},
                include_metadata=True,
                metadata={
                    'include_explanations': include_explanations,
                    'use_rag': use_rag
                }
            )
            
            # Determine best strategy based on analysis
            strategy_name = self._select_strategy(query_analysis)
            strategy = self.strategies[strategy_name]
            
            # Execute search with enhanced query
            enhanced_query = query_analysis.get('enhanced_query', query)
            results = await strategy.search(enhanced_query, options)
            
            # Generate AI explanations if requested
            if include_explanations:
                for result in results:
                    result.explanation = await self._generate_explanation(
                        query,
                        result,
                        query_analysis
                    )
                    
            # Generate RAG response if requested
            rag_response = None
            if use_rag and results:
                rag_response = await self._generate_rag_response(
                    query,
                    results,
                    query_analysis
                )
                
            duration_ms = int(
                (datetime.utcnow() - start_time).total_seconds() * 1000
            )
            
            response = {
                'query': query,
                'enhanced_query': enhanced_query,
                'results': [self._format_result(r) for r in results],
                'total': len(results),
                'query_analysis': query_analysis,
                'rag_response': rag_response,
                'strategy': strategy_name,
                'metrics': {
                    'duration_ms': duration_ms
                }
            }
            
            return ServiceResult.success(response)
            
        except Exception as e:
            logger.error(f"AI-powered search failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="AI-powered search failed",
                details={'error': str(e)}
            )
        
    async def get_facets(
        self,
        query: Optional[str] = None,
        fields: List[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Get facets/aggregations for search refinement.
        
        Args:
            query: Optional search query
            fields: Fields to generate facets for
            filters: Current filters
            limit: Max values per facet
            
        Returns:
            ServiceResult with facet data
        """
        try:
            if not fields:
                fields = ['entity_type', 'classification', 'tags', 'owner']
                
            # Build aggregation query
            aggs = {}
            for field in fields:
                aggs[field] = {
                    "terms": {
                        "field": f"{field}.keyword",
                        "size": limit
                    }
                }
                
            query_body = {
                "size": 0,
                "aggs": aggs
            }
            
            if query:
                query_body["query"] = {
                    "multi_match": {
                        "query": query,
                        "fields": ["name^3", "title^3", "description^2", "content"]
                    }
                }
                
            if filters:
                query_body["query"] = {
                    "bool": {
                        "must": query_body.get("query", {"match_all": {}}),
                        "filter": [{"term": {k: v}} for k, v in filters.items()]
                    }
                }
                
            # Execute aggregation
            response = await self.es_client.search(
                index="catalog_*",
                body=query_body
            )
            
            # Format facets
            facets = {}
            for field, agg_data in response.get("aggregations", {}).items():
                facets[field] = [
                    {"value": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in agg_data.get("buckets", [])
                ]
                
            return ServiceResult.success({
                "facets": facets,
                "query": query,
                "fields": fields
            })
            
        except Exception as e:
            logger.error(f"Get facets failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="Failed to get facets",
                details={'error': str(e)}
            )
            
    async def find_related(
        self,
        entity_id: str,
        relationship_types: Optional[List[str]] = None,
        limit: int = 20
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Find entities related to a given entity.
        
        Args:
            entity_id: Source entity ID
            relationship_types: Types of relationships to follow
            limit: Maximum results
            
        Returns:
            ServiceResult with related entities
        """
        try:
            # This would integrate with Atlas lineage/relationships
            # For now, using a simplified approach
            
            # Get the source entity
            source_entity = await self.catalog_search.get_entity(entity_id)
            if not source_entity:
                return ServiceResult.failure(error="Entity not found")
                
            # Find related entities through various means
            related = []
            
            # 1. Direct relationships (lineage, etc.)
            if hasattr(self.catalog_search, 'get_related_entities'):
                direct_related = await self.catalog_search.get_related_entities(
                    entity_id,
                    relationship_types=relationship_types
                )
                related.extend(direct_related)
                
            # 2. Similar entities (vector similarity)
            similar_result = await self.find_similar(entity_id, limit=limit//2)
            if similar_result.success:
                related.extend(similar_result.data.get('similar_entities', []))
                
            # 3. Same classification/tags
            if source_entity.get('classifications'):
                for classification in source_entity['classifications']:
                    search_result = await self.search(
                        f"classification:{classification}",
                        SearchOptions(limit=limit//4)
                    )
                    if search_result.success:
                        related.extend(search_result.data.get('results', []))
                        
            # Deduplicate and limit
            seen_ids = {entity_id}
            unique_related = []
            for item in related:
                item_id = item.get('id') or item.get('guid')
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    unique_related.append(item)
                    if len(unique_related) >= limit:
                        break
                        
            return ServiceResult.success({
                'source_entity': entity_id,
                'related_entities': unique_related,
                'total': len(unique_related)
            })
            
        except Exception as e:
            logger.error(f"Find related failed: {e}", exc_info=True)
            return ServiceResult.failure(
                error="Failed to find related entities",
                details={'error': str(e)}
            )
            
    async def get_recent_searches(
        self,
        user_id: Optional[str] = None,
        limit: int = 10
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get recent search queries."""
        try:
            cache_key = f"recent_searches:{user_id or 'global'}"
            recent = await self.cache_adapter.get(cache_key) or []
            return ServiceResult.success(recent[:limit])
        except Exception as e:
            logger.error(f"Get recent searches failed: {e}")
            return ServiceResult.failure(error="Failed to get recent searches")
            
    async def get_popular_searches(
        self,
        time_range: str = "24h",
        limit: int = 10
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get popular/trending searches."""
        try:
            # Would integrate with analytics
            # For now, return cached popular searches
            cache_key = f"popular_searches:{time_range}"
            popular = await self.cache_adapter.get(cache_key) or []
            return ServiceResult.success(popular[:limit])
        except Exception as e:
            logger.error(f"Get popular searches failed: {e}")
            return ServiceResult.failure(error="Failed to get popular searches")
            
    async def save_search(
        self,
        user_id: str,
        name: str,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        alert_enabled: bool = False
    ) -> ServiceResult[Dict[str, Any]]:
        """Save a search query for later use."""
        try:
            saved_search = {
                'id': str(uuid.uuid4()),
                'user_id': user_id,
                'name': name,
                'query': query,
                'filters': filters,
                'alert_enabled': alert_enabled,
                'created_at': datetime.utcnow().isoformat()
            }
            
            # Store in cache (would use persistent storage)
            cache_key = f"saved_searches:{user_id}"
            saved_searches = await self.cache_adapter.get(cache_key) or []
            saved_searches.append(saved_search)
            await self.cache_adapter.set(cache_key, saved_searches)
            
            return ServiceResult.success(saved_search)
        except Exception as e:
            logger.error(f"Save search failed: {e}")
            return ServiceResult.failure(error="Failed to save search")
            
    async def get_saved_searches(
        self,
        user_id: str
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get user's saved searches."""
        try:
            cache_key = f"saved_searches:{user_id}"
            saved_searches = await self.cache_adapter.get(cache_key) or []
            return ServiceResult.success(saved_searches)
        except Exception as e:
            logger.error(f"Get saved searches failed: {e}")
            return ServiceResult.failure(error="Failed to get saved searches")
            
    async def explain_result(
        self,
        result_id: str,
        query: str
    ) -> ServiceResult[Dict[str, Any]]:
        """Explain why a result matched a query."""
        try:
            # Get the result
            result = await self.es_client.get(
                index="catalog_*",
                id=result_id
            )
            
            # Get explanation from Elasticsearch
            explain_response = await self.es_client.explain(
                index=result['_index'],
                id=result_id,
                body={
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": ["name^3", "title^3", "description^2", "content"]
                        }
                    }
                }
            )
            
            # Generate human-readable explanation
            explanation = {
                'result_id': result_id,
                'query': query,
                'matched': explain_response.get('matched', False),
                'score': explain_response.get('explanation', {}).get('value', 0),
                'details': self._format_explanation(explain_response.get('explanation', {}))
            }
            
            return ServiceResult.success(explanation)
            
        except Exception as e:
            logger.error(f"Explain result failed: {e}")
            return ServiceResult.failure(
                error="Failed to explain result",
                details={'error': str(e)}
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

    # Helper methods
    
    async def _generate_explanation(
        self,
        query: str,
        result: SearchResult,
        query_analysis: Dict[str, Any]
    ) -> str:
        """Generate AI explanation for why a result matched."""
        try:
            # Simple explanation based on metadata
            explanations = []
            
            if result.score > 0.9:
                explanations.append("Very high relevance match")
            elif result.score > 0.7:
                explanations.append("High relevance match")
                
            if query_analysis.get('entities'):
                matched_entities = []
                for entity_type, entities in query_analysis['entities'].items():
                    if any(e in str(result.metadata) for e in entities):
                        matched_entities.append(entity_type)
                if matched_entities:
                    explanations.append(f"Matches {', '.join(matched_entities)}")
                    
            return "; ".join(explanations) if explanations else "Relevant match"
            
        except Exception as e:
            logger.error(f"Generate explanation failed: {e}")
            return "Match found"
            
    async def _generate_rag_response(
        self,
        query: str,
        results: List[SearchResult],
        query_analysis: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Generate RAG response from search results."""
        try:
            # Extract context from results
            context_docs = []
            for result in results[:5]:  # Top 5 results
                context_docs.append({
                    'content': result.content,
                    'metadata': result.metadata,
                    'score': result.score
                })
                
            # Would integrate with LLM here
            # For now, return a structured response
            return {
                'answer': f"Based on the search results for '{query}', here are the key findings...",
                'sources': [r.id for r in results[:5]],
                'confidence': sum(r.score for r in results[:5]) / len(results[:5]) if results else 0
            }
            
        except Exception as e:
            logger.error(f"Generate RAG response failed: {e}")
            return None
            
    def _format_explanation(self, explanation: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format Elasticsearch explanation into readable format."""
        details = []
        
        if 'description' in explanation:
            details.append({
                'description': explanation['description'],
                'value': explanation.get('value', 0)
            })
            
        if 'details' in explanation:
            for detail in explanation['details']:
                details.extend(self._format_explanation(detail))
                
        return details 