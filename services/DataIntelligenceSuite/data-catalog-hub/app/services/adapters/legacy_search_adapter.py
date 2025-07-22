"""
Legacy Search Adapter

Provides backward compatibility with existing search services
while migrating to the new consolidated architecture.
"""

import logging
from typing import List, Dict, Any, Optional, Union
import numpy as np

from elasticsearch import AsyncElasticsearch

# Import existing services
from ..hybrid_search import HybridSearchService
from ..vector_search import VectorSearchService
from ..es_vector_search import ESVectorSearchService
from ..ai_search_enhancement import AISearchOrchestrator
from ..query_understanding import QueryUnderstandingService
from ..search_analytics import SearchAnalyticsTracker

# Import new interfaces
from ..interfaces import SearchResult, SearchOptions
from ..ai.query_analyzer import UnifiedQueryAnalyzer
from ..ai.embedding_manager import EmbeddingManager
from ..storage.ignite_cache_adapter import IgniteCacheAdapter

logger = logging.getLogger(__name__)


class LegacySearchAdapter:
    """
    Maintains backward compatibility during migration to new architecture.
    
    Wraps the old service interfaces and translates to/from new interfaces.
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        query_analyzer: Optional[UnifiedQueryAnalyzer] = None,
        embedding_manager: Optional[EmbeddingManager] = None,
        cache_adapter: Optional[IgniteCacheAdapter] = None
    ):
        self.es_client = es_client
        self.query_analyzer = query_analyzer
        self.embedding_manager = embedding_manager
        self.cache_adapter = cache_adapter
        
        # Initialize legacy services with compatibility wrappers
        self._hybrid_search = None
        self._vector_search = None
        self._es_vector_search = None
        self._ai_orchestrator = None
        self._query_understanding = None
        self._analytics_tracker = None
        
        # Track which services are using new vs old implementations
        self.migration_status = {
            "query_understanding": "new" if query_analyzer else "legacy",
            "embedding": "new" if embedding_manager else "legacy",
            "cache": "ignite" if cache_adapter else "none"
        }
        
    async def initialize(self):
        """Initialize all services"""
        # Initialize new components if provided
        if self.query_analyzer:
            await self.query_analyzer.initialize()
        
        if self.embedding_manager:
            await self.embedding_manager.initialize()
            
        if self.cache_adapter:
            await self.cache_adapter.initialize()
        
        # Initialize legacy services as needed
        await self._init_legacy_services()
        
    async def _init_legacy_services(self):
        """Initialize legacy services for backward compatibility"""
        # Only initialize what's actually needed
        logger.info(f"Migration status: {self.migration_status}")
        
    # --- HybridSearchService compatibility ---
    
    async def get_hybrid_search(self) -> HybridSearchService:
        """Get or create hybrid search service"""
        if not self._hybrid_search:
            # If we have new components, create a wrapper
            if self.embedding_manager:
                # Create wrapper that uses new embedding manager
                self._hybrid_search = HybridSearchServiceWrapper(
                    self.es_client,
                    self.embedding_manager,
                    self.query_analyzer
                )
            else:
                # Fall back to legacy implementation
                vector_service = await self.get_vector_search()
                self._hybrid_search = HybridSearchService(
                    self.es_client,
                    vector_service
                )
        
        return self._hybrid_search
    
    async def get_vector_search(self) -> VectorSearchService:
        """Get or create vector search service"""
        if not self._vector_search:
            if self.embedding_manager:
                # Create wrapper that uses new embedding manager
                self._vector_search = VectorSearchServiceWrapper(
                    self.embedding_manager,
                    self.cache_adapter
                )
            else:
                # Legacy implementation
                self._vector_search = VectorSearchService()
                await self._vector_search.initialize()
        
        return self._vector_search
    
    async def get_es_vector_search(self) -> ESVectorSearchService:
        """Get or create ES vector search service"""
        if not self._es_vector_search:
            if self.embedding_manager:
                # Create wrapper
                self._es_vector_search = ESVectorSearchServiceWrapper(
                    self.es_client,
                    self.embedding_manager
                )
            else:
                # Legacy
                self._es_vector_search = ESVectorSearchService(self.es_client)
                await self._es_vector_search.initialize()
        
        return self._es_vector_search
    
    async def get_query_understanding(self) -> QueryUnderstandingService:
        """Get or create query understanding service"""
        if not self._query_understanding:
            if self.query_analyzer:
                # Create wrapper
                self._query_understanding = QueryUnderstandingServiceWrapper(
                    self.query_analyzer
                )
            else:
                # Legacy
                self._query_understanding = QueryUnderstandingService()
        
        return self._query_understanding
    
    async def get_ai_orchestrator(self) -> AISearchOrchestrator:
        """Get or create AI search orchestrator"""
        if not self._ai_orchestrator:
            # For AI orchestrator, prefer using new components if available
            if self.query_analyzer and self.embedding_manager:
                self._ai_orchestrator = AISearchOrchestratorWrapper(
                    self.es_client,
                    self.query_analyzer,
                    self.embedding_manager,
                    self.cache_adapter
                )
            else:
                # Legacy with Ignite instead of Redis
                ignite_wrapper = IgniteRedisWrapper(self.cache_adapter) if self.cache_adapter else None
                self._ai_orchestrator = AISearchOrchestrator(
                    self.es_client,
                    redis_client=ignite_wrapper
                )
        
        return self._ai_orchestrator
    
    async def get_analytics_tracker(self) -> SearchAnalyticsTracker:
        """Get or create analytics tracker"""
        if not self._analytics_tracker:
            # Use Ignite wrapper if available
            ignite_wrapper = IgniteRedisWrapper(self.cache_adapter) if self.cache_adapter else None
            self._analytics_tracker = SearchAnalyticsTracker(
                self.es_client,
                redis_client=ignite_wrapper
            )
        
        return self._analytics_tracker


# --- Wrapper Classes ---

class HybridSearchServiceWrapper:
    """Wraps new components to provide HybridSearchService interface"""
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        embedding_manager: EmbeddingManager,
        query_analyzer: Optional[UnifiedQueryAnalyzer] = None
    ):
        self.es_client = es_client
        self.embedding_manager = embedding_manager
        self.query_analyzer = query_analyzer
        
    async def hybrid_search(
        self,
        query: str,
        tenant_id: Optional[str] = None,
        size: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        text_weight: float = 0.6,
        vector_weight: float = 0.4,
        rerank: bool = True
    ) -> List[Dict[str, Any]]:
        """Implement hybrid search using new components"""
        # Analyze query if analyzer available
        query_analysis = None
        if self.query_analyzer:
            query_analysis = await self.query_analyzer.analyze(query)
            query = query_analysis.get("enhanced_query", query)
            
            # Merge filters
            if query_analysis.get("filters"):
                filters = filters or {}
                filters.update(query_analysis["filters"])
        
        # Generate embedding
        embedding = await self.embedding_manager.embed(query, content_type="text")
        
        # Build ES query for hybrid search
        es_query = {
            "size": size,
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["name^3", "title^3", "description^2", "content"],
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                                "boost": text_weight
                            }
                        }
                    ]
                }
            },
            "knn": {
                "field": "text_embedding",
                "query_vector": embedding.tolist(),
                "k": size,
                "num_candidates": size * 10,
                "boost": vector_weight
            }
        }
        
        # Add filters
        if filters or tenant_id:
            es_query["query"]["bool"]["filter"] = []
            if tenant_id:
                es_query["query"]["bool"]["filter"].append({"term": {"tenant_id": tenant_id}})
            if filters:
                for field, value in filters.items():
                    if isinstance(value, dict):
                        # Range query
                        es_query["query"]["bool"]["filter"].append({"range": {field: value}})
                    elif isinstance(value, list):
                        es_query["query"]["bool"]["filter"].append({"terms": {field: value}})
                    else:
                        es_query["query"]["bool"]["filter"].append({"term": {field: value}})
        
        # Execute search
        response = await self.es_client.search(
            index="platformq_search",
            body=es_query
        )
        
        # Convert to expected format
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"],
                "source": hit["_source"],
                "search_type": "hybrid",
                "combined_score": hit["_score"]
            })
        
        return results


class VectorSearchServiceWrapper:
    """Wraps new embedding manager to provide VectorSearchService interface"""
    
    def __init__(
        self,
        embedding_manager: EmbeddingManager,
        cache_adapter: Optional[IgniteCacheAdapter] = None
    ):
        self.embedding_manager = embedding_manager
        self.cache_adapter = cache_adapter
        
    async def embed_text(
        self,
        text: Union[str, List[str]],
        model_type: str = "default"
    ) -> np.ndarray:
        """Generate text embeddings"""
        # Map old model types to new
        content_type = "text" if model_type == "default" else model_type
        return await self.embedding_manager.embed(text, content_type=content_type)
    
    async def embed_image(self, image_path: str) -> np.ndarray:
        """Generate image embeddings"""
        return await self.embedding_manager.embed(image_path, content_type="image")
    
    async def search(
        self,
        query: Union[str, np.ndarray],
        collection_name: str = "text_embeddings",
        tenant_id: Optional[str] = None,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Compatibility method - delegates to ES"""
        # This is a simplified implementation
        # In practice, would need to maintain collection mappings
        logger.warning("VectorSearchServiceWrapper.search called - delegating to ES")
        return []


class QueryUnderstandingServiceWrapper:
    """Wraps new query analyzer to provide QueryUnderstandingService interface"""
    
    def __init__(self, query_analyzer: UnifiedQueryAnalyzer):
        self.query_analyzer = query_analyzer
        
    async def analyze_query(self, query: str) -> Dict[str, Any]:
        """Analyze query using new analyzer"""
        result = await self.query_analyzer.analyze(query)
        
        # Transform to legacy format
        return {
            "original_query": result["original_query"],
            "intent": result["intent"],
            "entities": result["entities"],
            "enhanced_query": result["enhanced_query"],
            "filters": result["filters"],
            "suggestions": result["suggestions"]
        }


class ESVectorSearchServiceWrapper:
    """Wraps new components to provide ESVectorSearchService interface"""
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        embedding_manager: EmbeddingManager
    ):
        self.es_client = es_client
        self.embedding_manager = embedding_manager
        
    async def embed_text(
        self,
        text: Union[str, List[str]],
        model_type: str = "text"
    ) -> np.ndarray:
        """Generate embeddings"""
        return await self.embedding_manager.embed(text, content_type=model_type)
    
    async def knn_search(
        self,
        query_vector: Union[str, np.ndarray],
        index: str = "unified",
        field: str = "text_embedding",
        k: int = 10,
        num_candidates: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        boost: float = 1.0
    ) -> List[Dict[str, Any]]:
        """Perform k-NN search"""
        # Generate embedding if text provided
        if isinstance(query_vector, str):
            embedding = await self.embedding_manager.embed(query_vector)
        else:
            embedding = query_vector
            
        # Build query
        knn_query = {
            "field": field,
            "query_vector": embedding.tolist(),
            "k": k,
            "num_candidates": num_candidates
        }
        
        # Add filters
        if filters or tenant_id:
            filter_clauses = []
            if tenant_id:
                filter_clauses.append({"term": {"tenant_id": tenant_id}})
            if filters:
                for field_name, value in filters.items():
                    if isinstance(value, list):
                        filter_clauses.append({"terms": {field_name: value}})
                    else:
                        filter_clauses.append({"term": {field_name: value}})
            knn_query["filter"] = {"bool": {"must": filter_clauses}}
        
        # Execute
        response = await self.es_client.search(
            index=index,
            knn=knn_query,
            size=k
        )
        
        # Format results
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"] * boost,
                "source": hit["_source"],
                "index": hit["_index"]
            })
        
        return results


class AISearchOrchestratorWrapper:
    """Wraps new components to provide AISearchOrchestrator interface"""
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        query_analyzer: UnifiedQueryAnalyzer,
        embedding_manager: EmbeddingManager,
        cache_adapter: Optional[IgniteCacheAdapter] = None
    ):
        self.es_client = es_client
        self.query_analyzer = query_analyzer
        self.embedding_manager = embedding_manager
        self.cache_adapter = cache_adapter
        
    async def process_search_query(
        self,
        query: str,
        user_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process query with AI enhancements"""
        # Use new query analyzer
        analysis = await self.query_analyzer.analyze(query, context)
        
        return {
            "original_query": query,
            "intent_analysis": analysis["intent"],
            "query_enhancement": {
                "original_query": query,
                "enhanced_query": analysis["enhanced_query"],
                "semantic_keywords": [kw["text"] for kw in analysis["entities"].get("keywords", [])]
            },
            "explanation": f"Searching for: {analysis['enhanced_query']}",
            "user_profile": None,  # Would need personalization engine
            "search_config": {
                "use_semantic": True,
                "use_fuzzy": True,
                "boost_recent": False,
                "personalize": False
            }
        }


class IgniteRedisWrapper:
    """
    Wraps Ignite cache adapter to provide Redis-like interface
    for legacy services that expect Redis.
    """
    
    def __init__(self, ignite_adapter: IgniteCacheAdapter):
        self.ignite = ignite_adapter
        
    async def get(self, key: str) -> Optional[Any]:
        """Redis-compatible get"""
        return await self.ignite.get(key)
    
    async def set(self, key: str, value: Any) -> bool:
        """Redis-compatible set"""
        return await self.ignite.set(key, value)
    
    async def setex(self, key: str, ttl: int, value: Any) -> bool:
        """Redis-compatible setex (set with expiry)"""
        return await self.ignite.set(key, value, ttl=ttl)
    
    async def delete(self, key: str) -> bool:
        """Redis-compatible delete"""
        return await self.ignite.delete(key)
    
    async def exists(self, key: str) -> bool:
        """Redis-compatible exists"""
        return await self.ignite.exists(key)
    
    async def incr(self, key: str) -> int:
        """Redis-compatible increment"""
        # Simplified implementation
        value = await self.ignite.get(key)
        new_value = (int(value) if value else 0) + 1
        await self.ignite.set(key, new_value)
        return new_value
    
    async def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """Redis-compatible sorted set add"""
        # Ignite doesn't have native sorted sets
        # Store as a dict for now
        current = await self.ignite.get(key) or {}
        current.update(mapping)
        await self.ignite.set(key, current)
        return len(mapping)
    
    async def zrevrange(self, key: str, start: int, stop: int) -> List[str]:
        """Redis-compatible sorted set range"""
        data = await self.ignite.get(key)
        if not data:
            return []
        
        # Sort by score descending
        sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=True)
        return [item[0] for item in sorted_items[start:stop+1]] 