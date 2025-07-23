"""
Text Search Strategy

Traditional text-based search using Elasticsearch BM25.
"""

import logging
from typing import List, Dict, Any, Optional

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions
from .base import BaseSearchStrategy

logger = logging.getLogger(__name__)


class TextSearchStrategy(BaseSearchStrategy):
    """
    Text-based search strategy using Elasticsearch's BM25 algorithm.
    
    This strategy performs traditional keyword-based search with support for:
    - Multi-field search with field boosting
    - Fuzzy matching
    - Phrase matching
    - Highlighting
    - Faceted search
    """
    
    def __init__(self, es_client: AsyncElasticsearch):
        super().__init__(es_client)
        
        # Default field boosts
        self.default_field_boosts = {
            "name": 5.0,
            "title": 5.0,
            "display_name": 4.0,
            "description": 3.0,
            "tags": 3.0,
            "business_metadata.business_terms": 3.0,
            "classifications.name": 2.5,
            "owner": 2.0,
            "qualified_name": 2.0,
            "content": 1.0,
            "technical_metadata": 1.0
        }
        
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute text-based search."""
        try:
            # Build search query
            es_query = await self._build_text_query(query, options)
            
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
                search_type="text"
            )
            
            # Add search metadata
            for result in results:
                result.metadata = {
                    "total_hits": response["hits"]["total"]["value"],
                    "max_score": response["hits"].get("max_score", 0),
                    "took_ms": response.get("took", 0)
                }
                
            return results
            
        except Exception as e:
            logger.error(f"Text search failed: {e}")
            raise
            
    async def _build_text_query(
        self,
        query: str,
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build Elasticsearch query for text search."""
        # Base query structure
        es_query = {
            "size": options.size,
            "from": options.from_offset
        }
        
        # Build search fields with boosts
        search_fields = []
        field_boosts = getattr(options, 'field_boosts', self.default_field_boosts)
        
        for field, boost in field_boosts.items():
            search_fields.append(f"{field}^{boost}")
            
        # Main query
        main_query = {
            "multi_match": {
                "query": query,
                "fields": search_fields,
                "type": "best_fields",
                "operator": "OR",
                "fuzziness": "AUTO",
                "prefix_length": 2,
                "max_expansions": 50,
                "zero_terms_query": "none"
            }
        }
        
        # Check for phrase search
        if '"' in query:
            # Extract phrases
            import re
            phrases = re.findall(r'"([^"]*)"', query)
            if phrases:
                # Use phrase matching
                main_query = {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": phrase,
                                    "fields": search_fields,
                                    "type": "phrase",
                                    "slop": 2
                                }
                            }
                            for phrase in phrases
                        ]
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
            
        # Apply boosting
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
            
        if getattr(options, 'boost_usage', True):
            es_query["query"] = await self._boost_by_usage(
                es_query["query"],
                boost_factor=1.2
            )
            
        # Add highlighting
        if options.include_highlights:
            es_query["highlight"] = {
                "fields": {
                    "name": {"number_of_fragments": 0},
                    "title": {"number_of_fragments": 0},
                    "description": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    },
                    "content": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    },
                    "tags": {"number_of_fragments": 0}
                },
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"]
            }
            
        # Add aggregations for facets
        if options.include_facets:
            es_query["aggs"] = {
                "entity_types": {
                    "terms": {
                        "field": "entity_type",
                        "size": 20
                    }
                },
                "owners": {
                    "terms": {
                        "field": "owner.keyword",
                        "size": 20
                    }
                },
                "classifications": {
                    "nested": {
                        "path": "classifications"
                    },
                    "aggs": {
                        "names": {
                            "terms": {
                                "field": "classifications.name",
                                "size": 20
                            }
                        }
                    }
                },
                "tags": {
                    "terms": {
                        "field": "tags.keyword",
                        "size": 30
                    }
                },
                "quality_ranges": {
                    "range": {
                        "field": "quality_metrics.overall_score",
                        "ranges": [
                            {"from": 0, "to": 0.5, "key": "low"},
                            {"from": 0.5, "to": 0.8, "key": "medium"},
                            {"from": 0.8, "to": 1.0, "key": "high"}
                        ]
                    }
                }
            }
            
        # Add explanation
        if options.include_explanations:
            es_query["explain"] = True
            
        # Add sorting
        sort_field = getattr(options, 'sort_by', '_score')
        sort_order = getattr(options, 'sort_order', 'desc')
        
        if sort_field != '_score':
            es_query["sort"] = [
                {sort_field: {"order": sort_order}},
                "_score"
            ]
            
        return es_query 