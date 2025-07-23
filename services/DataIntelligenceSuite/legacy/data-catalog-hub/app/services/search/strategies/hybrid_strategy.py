"""
Hybrid Search Strategy

Combines text and vector search for optimal results.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
import asyncio
from collections import defaultdict
import numpy as np

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions
from app.services.ai import EmbeddingManager
from .base import BaseSearchStrategy

logger = logging.getLogger(__name__)


class HybridSearchStrategy(BaseSearchStrategy):
    """
    Hybrid search combining text (BM25) and vector (k-NN) search.
    
    This strategy provides the best of both worlds:
    - Exact keyword matching from text search
    - Semantic understanding from vector search
    - Configurable weighting between approaches
    - Result fusion and re-ranking
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        embedding_manager: EmbeddingManager
    ):
        super().__init__(es_client)
        self.embedding_manager = embedding_manager
        
        # Default weights
        self.default_text_weight = 0.4
        self.default_vector_weight = 0.6
        
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute hybrid search combining text and vector approaches."""
        try:
            # Get weights from options or use defaults
            text_weight = getattr(options, 'text_weight', self.default_text_weight)
            vector_weight = getattr(options, 'vector_weight', self.default_vector_weight)
            
            # Normalize weights
            total_weight = text_weight + vector_weight
            text_weight = text_weight / total_weight
            vector_weight = vector_weight / total_weight
            
            # Build both queries in parallel
            text_query_task = self._build_text_query(query, options)
            vector_query_task = self._build_vector_query(query, options)
            
            text_query, vector_query = await asyncio.gather(
                text_query_task,
                vector_query_task
            )
            
            # Determine search strategy based on ES version
            use_combined = getattr(options, 'use_combined_query', True)
            
            if use_combined:
                # Use combined query (ES 8.4+)
                results = await self._execute_combined_search(
                    text_query,
                    vector_query,
                    text_weight,
                    vector_weight,
                    options
                )
            else:
                # Execute searches separately and merge
                results = await self._execute_separate_searches(
                    text_query,
                    vector_query,
                    text_weight,
                    vector_weight,
                    options
                )
                
            return results
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            raise
            
    async def _build_text_query(
        self,
        query: str,
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build text search component."""
        # Search fields with boosts
        search_fields = [
            "name^5",
            "title^5",
            "description^3",
            "tags^3",
            "content^1"
        ]
        
        # Build multi-match query
        text_query = {
            "multi_match": {
                "query": query,
                "fields": search_fields,
                "type": "best_fields",
                "operator": "OR",
                "fuzziness": "AUTO",
                "prefix_length": 2
            }
        }
        
        return text_query
        
    async def _build_vector_query(
        self,
        query: str,
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build vector search component."""
        # Generate embedding
        query_embedding = await self.embedding_manager.embed_text(query)
        
        # k-NN parameters
        k = getattr(options, 'k', options.size)
        num_candidates = getattr(options, 'num_candidates', k * 10)
        
        # Build k-NN query
        vector_query = {
            "field": "text_embedding",
            "query_vector": query_embedding.tolist(),
            "k": k,
            "num_candidates": num_candidates
        }
        
        return vector_query
        
    async def _execute_combined_search(
        self,
        text_query: Dict[str, Any],
        vector_query: Dict[str, Any],
        text_weight: float,
        vector_weight: float,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute combined text + vector search in single query."""
        # Build combined query
        es_query = {
            "size": options.size,
            "from": options.from_offset,
            "query": {
                "bool": {
                    "should": [
                        {
                            "function_score": {
                                "query": text_query,
                                "boost": text_weight
                            }
                        }
                    ],
                    "minimum_should_match": 0
                }
            },
            "knn": vector_query
        }
        
        # Add kNN boost
        es_query["knn"]["boost"] = vector_weight
        
        # Add filters
        filters = self._build_filters(options)
        if filters:
            es_query["query"]["bool"]["filter"] = filters
            es_query["knn"]["filter"] = {"bool": {"must": filters}}
            
        # Apply additional boosting
        if options.boost_recent:
            es_query["query"] = await self._boost_by_recency(
                es_query["query"],
                decay_days=30
            )
            
        if options.boost_quality:
            es_query["query"] = await self._boost_by_quality(
                es_query["query"],
                boost_factor=1.5
            )
            
        # Add highlighting for text matches
        if options.include_highlights:
            es_query["highlight"] = {
                "fields": {
                    "name": {"number_of_fragments": 0},
                    "title": {"number_of_fragments": 0},
                    "description": {"fragment_size": 150},
                    "content": {"fragment_size": 150}
                }
            }
            
        # Get index
        index = self._get_index_name()
        
        # Execute search
        response = await self.es_client.search(
            index=index,
            body=es_query
        )
        
        # Convert results
        results = self._convert_hits_to_results(
            response["hits"]["hits"],
            search_type="hybrid"
        )
        
        # Add metadata
        for result in results:
            result.metadata = {
                "search_type": "hybrid_combined",
                "text_weight": text_weight,
                "vector_weight": vector_weight,
                "total_hits": response["hits"]["total"]["value"],
                "took_ms": response.get("took", 0)
            }
            
        return results
        
    async def _execute_separate_searches(
        self,
        text_query: Dict[str, Any],
        vector_query: Dict[str, Any],
        text_weight: float,
        vector_weight: float,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute text and vector searches separately and merge results."""
        # Get more results than needed for better fusion
        expanded_size = options.size * 3
        
        # Build text search query
        text_es_query = {
            "size": expanded_size,
            "query": text_query
        }
        
        # Build vector search query
        vector_es_query = {
            "size": expanded_size,
            "knn": vector_query
        }
        
        # Add filters to both
        filters = self._build_filters(options)
        if filters:
            text_es_query["query"] = {
                "bool": {
                    "must": text_query,
                    "filter": filters
                }
            }
            vector_es_query["knn"]["filter"] = {"bool": {"must": filters}}
            
        # Get index
        index = self._get_index_name()
        
        # Execute both searches in parallel
        text_task = self.es_client.search(index=index, body=text_es_query)
        vector_task = self.es_client.search(index=index, body=vector_es_query)
        
        text_response, vector_response = await asyncio.gather(
            text_task,
            vector_task
        )
        
        # Convert to result objects
        text_results = self._convert_hits_to_results(
            text_response["hits"]["hits"],
            search_type="text"
        )
        
        vector_results = self._convert_hits_to_results(
            vector_response["hits"]["hits"],
            search_type="vector"
        )
        
        # Merge and re-rank results
        merged_results = self._merge_results(
            text_results,
            vector_results,
            text_weight,
            vector_weight
        )
        
        # Apply re-ranking if requested
        if getattr(options, 'rerank', True):
            merged_results = await self._rerank_results(
                merged_results,
                query,
                options
            )
            
        # Return top results
        return merged_results[:options.size]
        
    def _merge_results(
        self,
        text_results: List[SearchResult],
        vector_results: List[SearchResult],
        text_weight: float,
        vector_weight: float
    ) -> List[SearchResult]:
        """Merge and score results from text and vector searches."""
        # Create score maps
        text_scores = {r.id: r.score for r in text_results}
        vector_scores = {r.id: r.score for r in vector_results}
        
        # Normalize scores
        max_text_score = max(text_scores.values()) if text_scores else 1.0
        max_vector_score = max(vector_scores.values()) if vector_scores else 1.0
        
        # Combine all unique IDs
        all_ids = set(text_scores.keys()) | set(vector_scores.keys())
        
        # Calculate combined scores
        merged_results = []
        result_map = {}
        
        # Build result map
        for result in text_results + vector_results:
            if result.id not in result_map:
                result_map[result.id] = result
                
        # Calculate hybrid scores
        for doc_id in all_ids:
            # Get normalized scores
            text_score = (text_scores.get(doc_id, 0) / max_text_score) * text_weight
            vector_score = (vector_scores.get(doc_id, 0) / max_vector_score) * vector_weight
            
            # Combined score
            hybrid_score = text_score + vector_score
            
            # Get result object
            result = result_map[doc_id]
            result.score = hybrid_score
            result.search_type = "hybrid"
            
            # Add score breakdown
            result.metadata = result.metadata or {}
            result.metadata.update({
                "text_score": text_scores.get(doc_id, 0),
                "vector_score": vector_scores.get(doc_id, 0),
                "normalized_text_score": text_score,
                "normalized_vector_score": vector_score,
                "hybrid_score": hybrid_score
            })
            
            merged_results.append(result)
            
        # Sort by hybrid score
        merged_results.sort(key=lambda x: x.score, reverse=True)
        
        return merged_results
        
    async def _rerank_results(
        self,
        results: List[SearchResult],
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Apply advanced re-ranking to results."""
        # Simple re-ranking based on additional factors
        # Could be enhanced with cross-encoder models
        
        for result in results:
            # Boost exact matches
            if result.name and query.lower() in result.name.lower():
                result.score *= 1.5
                
            # Boost if query terms appear in description
            if result.description and query.lower() in result.description.lower():
                result.score *= 1.2
                
            # Apply quality boost
            if result.quality_score:
                result.score *= (1 + result.quality_score * 0.3)
                
        # Re-sort
        results.sort(key=lambda x: x.score, reverse=True)
        
        return results 