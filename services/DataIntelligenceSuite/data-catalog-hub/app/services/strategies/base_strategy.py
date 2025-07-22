"""
Base Search Strategy

Provides foundation for all search strategy implementations.
"""

import logging
from typing import List, Dict, Any, Optional
from abc import ABC

from elasticsearch import AsyncElasticsearch

from ..interfaces import SearchStrategy, SearchResult, SearchOptions

logger = logging.getLogger(__name__)


class BaseSearchStrategy(SearchStrategy):
    """
    Base implementation with common functionality for search strategies
    """
    
    def __init__(self, es_client: AsyncElasticsearch, index_name: str = "platformq_search"):
        self.es_client = es_client
        self.index_name = index_name
        
    def _build_filters(
        self,
        options: SearchOptions
    ) -> Optional[List[Dict[str, Any]]]:
        """Build filter clauses from search options"""
        filter_clauses = []
        
        if options.tenant_id:
            filter_clauses.append({"term": {"tenant_id": options.tenant_id}})
            
        if options.filters:
            for field, value in options.filters.items():
                if isinstance(value, dict):
                    # Range query
                    filter_clauses.append({"range": {field: value}})
                elif isinstance(value, list):
                    # Terms query
                    filter_clauses.append({"terms": {field: value}})
                else:
                    # Term query
                    filter_clauses.append({"term": {field: value}})
                    
        if options.boost_recent:
            # Add recency boost
            filter_clauses.append({
                "range": {
                    "created_at": {
                        "gte": "now-30d"
                    }
                }
            })
            
        return filter_clauses if filter_clauses else None
    
    def _convert_hits_to_results(
        self,
        hits: List[Dict[str, Any]],
        search_type: str = "unknown"
    ) -> List[SearchResult]:
        """Convert Elasticsearch hits to SearchResult objects"""
        results = []
        
        for hit in hits:
            result = SearchResult(
                id=hit["_id"],
                score=hit["_score"],
                source=hit["_source"],
                index=hit.get("_index", self.index_name),
                search_type=search_type
            )
            
            # Add highlights if available
            if "highlight" in hit:
                result.highlights = hit["highlight"]
                
            # Add explanation if available
            if "_explanation" in hit:
                result.explanation = str(hit["_explanation"])
                
            results.append(result)
            
        return results
    
    async def index(
        self,
        doc_id: str,
        content: Dict[str, Any],
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Index a document"""
        if tenant_id:
            content["tenant_id"] = tenant_id
            
        response = await self.es_client.index(
            index=self.index_name,
            id=doc_id,
            document=content
        )
        
        return {
            "id": response["_id"],
            "index": response["_index"],
            "result": response["result"],
            "version": response["_version"]
        }
    
    async def delete(
        self,
        doc_id: str,
        tenant_id: Optional[str] = None
    ) -> bool:
        """Delete a document"""
        try:
            # If tenant_id provided, verify ownership first
            if tenant_id:
                doc = await self.es_client.get(
                    index=self.index_name,
                    id=doc_id,
                    _source=["tenant_id"]
                )
                if doc["_source"].get("tenant_id") != tenant_id:
                    logger.warning(f"Tenant {tenant_id} tried to delete doc {doc_id} owned by {doc['_source'].get('tenant_id')}")
                    return False
                    
            await self.es_client.delete(
                index=self.index_name,
                id=doc_id
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete document {doc_id}: {e}")
            return False


class TextSearchStrategy(BaseSearchStrategy):
    """
    Traditional text-based search using Elasticsearch BM25
    """
    
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute text-based search"""
        try:
            # Build the query
            es_query = {
                "size": options.size,
                "from": options.from_offset,
                "query": {
                    "bool": {
                        "must": {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "name^3",
                                    "title^3",
                                    "description^2",
                                    "content",
                                    "tags^2",
                                    "metadata.keywords^2"
                                ],
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                                "prefix_length": 2
                            }
                        }
                    }
                }
            }
            
            # Add filters
            filters = self._build_filters(options)
            if filters:
                es_query["query"]["bool"]["filter"] = filters
                
            # Add highlighting
            if options.include_highlights:
                es_query["highlight"] = {
                    "fields": {
                        "name": {"number_of_fragments": 0},
                        "title": {"number_of_fragments": 0},
                        "description": {"fragment_size": 150},
                        "content": {"fragment_size": 150}
                    }
                }
                
            # Add explanation
            if options.include_explanations:
                es_query["explain"] = True
                
            # Execute search
            response = await self.es_client.search(
                index=self.index_name,
                body=es_query
            )
            
            # Convert results
            results = self._convert_hits_to_results(
                response["hits"]["hits"],
                search_type="text"
            )
            
            # Add metadata
            for result in results:
                result.metadata = {
                    "total_hits": response["hits"]["total"]["value"],
                    "max_score": response["hits"]["max_score"],
                    "took_ms": response["took"]
                }
                
            return results
            
        except Exception as e:
            logger.error(f"Text search failed: {e}")
            raise


class ExactMatchStrategy(TextSearchStrategy):
    """
    Exact match search strategy for finding specific items
    """
    
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute exact match search"""
        try:
            # First try exact term match on key fields
            es_query = {
                "size": options.size,
                "from": options.from_offset,
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"name.keyword": {"value": query, "boost": 10}}},
                            {"term": {"title.keyword": {"value": query, "boost": 10}}},
                            {"term": {"id": {"value": query, "boost": 10}}},
                            {"term": {"entity_id": {"value": query, "boost": 10}}}
                        ],
                        "minimum_should_match": 1
                    }
                }
            }
            
            # Add filters
            filters = self._build_filters(options)
            if filters:
                es_query["query"]["bool"]["filter"] = filters
                
            # Execute exact search
            response = await self.es_client.search(
                index=self.index_name,
                body=es_query
            )
            
            # If we got results, return them
            if response["hits"]["total"]["value"] > 0:
                return self._convert_hits_to_results(
                    response["hits"]["hits"],
                    search_type="exact"
                )
                
            # Otherwise fall back to text search
            return await super().search(query, options)
            
        except Exception as e:
            logger.error(f"Exact match search failed: {e}")
            # Fall back to text search
            return await super().search(query, options) 