"""
Base Search Strategy

Foundation for all search strategy implementations.
"""

import logging
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions, ServiceResult

logger = logging.getLogger(__name__)


class BaseSearchStrategy(ABC):
    """
    Base class for all search strategies.
    """
    
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client
        self.index_prefix = "catalog"
        
    @abstractmethod
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute search with this strategy."""
        pass
        
    def _build_filters(
        self,
        options: SearchOptions
    ) -> Optional[List[Dict[str, Any]]]:
        """Build Elasticsearch filter clauses from search options."""
        filter_clauses = []
        
        # Tenant filter
        if options.tenant_id:
            filter_clauses.append({"term": {"tenant_id": options.tenant_id}})
            
        # Custom filters
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
                    
        # Entity type filter
        if hasattr(options, 'entity_types') and options.entity_types:
            filter_clauses.append({"terms": {"entity_type": options.entity_types}})
            
        # Classification filter
        if hasattr(options, 'classifications') and options.classifications:
            filter_clauses.append({
                "nested": {
                    "path": "classifications",
                    "query": {
                        "terms": {"classifications.name": options.classifications}
                    }
                }
            })
            
        # Date range filter
        if hasattr(options, 'date_range') and options.date_range:
            date_filter = {}
            if options.date_range.get('from'):
                date_filter['gte'] = options.date_range['from']
            if options.date_range.get('to'):
                date_filter['lte'] = options.date_range['to']
            if date_filter:
                filter_clauses.append({"range": {"created_at": date_filter}})
                
        # Quality score filter
        if hasattr(options, 'min_quality_score') and options.min_quality_score:
            filter_clauses.append({
                "range": {
                    "quality_metrics.overall_score": {
                        "gte": options.min_quality_score
                    }
                }
            })
            
        return filter_clauses if filter_clauses else None
    
    def _convert_hits_to_results(
        self,
        hits: List[Dict[str, Any]],
        search_type: str = "unknown"
    ) -> List[SearchResult]:
        """Convert Elasticsearch hits to SearchResult objects."""
        results = []
        
        for hit in hits:
            result = SearchResult(
                id=hit["_id"],
                score=hit.get("_score", 0.0),
                source=hit["_source"],
                index=hit.get("_index", ""),
                search_type=search_type
            )
            
            # Add highlights if available
            if "highlight" in hit:
                result.highlights = hit["highlight"]
                
            # Add explanation if available
            if "_explanation" in hit:
                result.explanation = hit["_explanation"]
                
            # Extract key fields
            source = hit["_source"]
            result.name = source.get("name", "")
            result.description = source.get("description", "")
            result.entity_type = source.get("entity_type", "")
            result.owner = source.get("owner", "")
            result.tags = source.get("tags", [])
            result.classifications = source.get("classifications", [])
            result.quality_score = source.get("quality_metrics", {}).get("overall_score")
            
            results.append(result)
            
        return results
        
    def _get_index_name(self, entity_type: Optional[str] = None) -> str:
        """Get the appropriate index name."""
        if entity_type:
            return f"{self.index_prefix}_{entity_type.lower()}"
        return f"{self.index_prefix}_unified"
        
    async def _boost_by_quality(
        self,
        query: Dict[str, Any],
        boost_factor: float = 1.5
    ) -> Dict[str, Any]:
        """Add quality score boosting to query."""
        return {
            "function_score": {
                "query": query,
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "quality_metrics.overall_score",
                            "factor": boost_factor,
                            "modifier": "sqrt",
                            "missing": 0.5
                        }
                    }
                ],
                "score_mode": "multiply"
            }
        }
        
    async def _boost_by_recency(
        self,
        query: Dict[str, Any],
        decay_days: int = 30
    ) -> Dict[str, Any]:
        """Add recency boosting to query."""
        return {
            "function_score": {
                "query": query,
                "functions": [
                    {
                        "exp": {
                            "modified_at": {
                                "origin": "now",
                                "scale": f"{decay_days}d",
                                "decay": 0.5
                            }
                        }
                    }
                ],
                "score_mode": "multiply"
            }
        }
        
    async def _boost_by_usage(
        self,
        query: Dict[str, Any],
        boost_factor: float = 1.2
    ) -> Dict[str, Any]:
        """Add usage/popularity boosting to query."""
        return {
            "function_score": {
                "query": query,
                "functions": [
                    {
                        "field_value_factor": {
                            "field": "usage_metrics.access_count",
                            "factor": boost_factor,
                            "modifier": "log1p",
                            "missing": 1
                        }
                    }
                ],
                "score_mode": "multiply"
            }
        } 