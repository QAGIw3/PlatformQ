"""
Exact Match Search Strategy

Strategy for finding exact matches by ID, name, or other unique identifiers.
"""

import logging
from typing import List, Dict, Any, Optional

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions
from .base import BaseSearchStrategy

logger = logging.getLogger(__name__)


class ExactMatchStrategy(BaseSearchStrategy):
    """
    Exact match search strategy for finding specific items.
    
    This strategy is optimized for:
    - Finding entities by exact ID or GUID
    - Exact name matches
    - Qualified name lookups
    - Quick existence checks
    """
    
    def __init__(self, es_client: AsyncElasticsearch):
        super().__init__(es_client)
        
        # Fields to check for exact matches
        self.exact_match_fields = [
            "_id",
            "guid",
            "qualified_name",
            "name.keyword",
            "entity_id",
            "external_id"
        ]
        
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute exact match search."""
        try:
            # First try direct document lookup if query looks like an ID
            if self._looks_like_id(query):
                direct_result = await self._try_direct_lookup(query, options)
                if direct_result:
                    return direct_result
                    
            # Build exact match query
            es_query = await self._build_exact_match_query(query, options)
            
            # Get appropriate index
            index = self._get_index_name(
                options.filters.get('entity_type') if options.filters else None
            )
            
            # Execute search
            response = await self.es_client.search(
                index=index,
                body=es_query
            )
            
            # Convert results
            results = self._convert_hits_to_results(
                response["hits"]["hits"],
                search_type="exact"
            )
            
            # If no exact matches found and fuzzy is enabled, fall back to fuzzy search
            if not results and getattr(options, 'fallback_to_fuzzy', True):
                results = await self._fuzzy_fallback_search(query, options)
                
            return results
            
        except Exception as e:
            logger.error(f"Exact match search failed: {e}")
            raise
            
    def _looks_like_id(self, query: str) -> bool:
        """Check if query looks like an ID."""
        # Common ID patterns
        if len(query) == 36 and query.count('-') == 4:  # UUID
            return True
        if query.isalnum() and len(query) in [24, 32]:  # MongoDB ObjectId or hash
            return True
        if query.startswith(('ent_', 'guid_', 'id_')):  # Prefixed IDs
            return True
        return False
        
    async def _try_direct_lookup(
        self,
        doc_id: str,
        options: SearchOptions
    ) -> Optional[List[SearchResult]]:
        """Try direct document lookup by ID."""
        try:
            index = self._get_index_name()
            
            # Try get by ID
            response = await self.es_client.get(
                index=index,
                id=doc_id,
                _source=True,
                ignore=[404]
            )
            
            if response.get("found"):
                # Check tenant access
                if options.tenant_id:
                    source_tenant = response["_source"].get("tenant_id")
                    if source_tenant and source_tenant != options.tenant_id:
                        return []
                        
                # Convert to result
                result = SearchResult(
                    id=response["_id"],
                    score=1.0,  # Perfect match
                    source=response["_source"],
                    index=response["_index"],
                    search_type="exact_id"
                )
                
                # Extract fields
                self._extract_result_fields(result, response["_source"])
                
                return [result]
                
        except Exception as e:
            logger.debug(f"Direct lookup failed for {doc_id}: {e}")
            
        return None
        
    async def _build_exact_match_query(
        self,
        query: str,
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build query for exact matching."""
        # Base query
        es_query = {
            "size": options.size,
            "from": options.from_offset
        }
        
        # Build should clauses for different exact match types
        should_clauses = []
        
        # Exact term matches on various fields
        for field in self.exact_match_fields:
            should_clauses.append({
                "term": {
                    field: {
                        "value": query,
                        "boost": 10.0 if field == "_id" else 5.0
                    }
                }
            })
            
        # Also try lowercase version for keyword fields
        should_clauses.append({
            "term": {
                "name.keyword": {
                    "value": query.lower(),
                    "boost": 4.0
                }
            }
        })
        
        # Prefix match on qualified name
        should_clauses.append({
            "prefix": {
                "qualified_name": {
                    "value": query,
                    "boost": 3.0
                }
            }
        })
        
        # Build main query
        main_query = {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
        
        # Apply filters
        filters = self._build_filters(options)
        if filters:
            es_query["query"] = {
                "bool": {
                    "must": main_query,
                    "filter": filters
                }
            }
        else:
            es_query["query"] = main_query
            
        # Sort by score (exact matches will score highest)
        es_query["sort"] = ["_score", "_doc"]
        
        return es_query
        
    async def _fuzzy_fallback_search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Fallback to fuzzy search if no exact matches."""
        # Build fuzzy query
        es_query = {
            "size": options.size,
            "from": options.from_offset,
            "query": {
                "bool": {
                    "should": [
                        {
                            "fuzzy": {
                                "name": {
                                    "value": query,
                                    "fuzziness": "AUTO",
                                    "prefix_length": 2,
                                    "boost": 2.0
                                }
                            }
                        },
                        {
                            "match": {
                                "name": {
                                    "query": query,
                                    "fuzziness": "AUTO",
                                    "operator": "AND"
                                }
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        
        # Apply filters
        filters = self._build_filters(options)
        if filters:
            es_query["query"]["bool"]["filter"] = filters
            
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
            search_type="fuzzy_fallback"
        )
        
        return results
        
    def _extract_result_fields(
        self,
        result: SearchResult,
        source: Dict[str, Any]
    ):
        """Extract key fields from source into result object."""
        result.name = source.get("name", "")
        result.description = source.get("description", "")
        result.entity_type = source.get("entity_type", "")
        result.owner = source.get("owner", "")
        result.tags = source.get("tags", [])
        result.classifications = source.get("classifications", [])
        result.quality_score = source.get("quality_metrics", {}).get("overall_score")
        result.qualified_name = source.get("qualified_name", "") 