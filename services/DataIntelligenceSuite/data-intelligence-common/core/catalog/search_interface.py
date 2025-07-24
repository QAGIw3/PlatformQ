"""
Search interface for catalog.

Provides advanced search capabilities with faceting, filtering, and relevance ranking.
"""

import re
import uuid
from abc import ABC
from typing import Any, Dict, List, Optional, Set, Union, Tuple
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
import asyncio
from collections import defaultdict
import json

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class SearchIntent(str, Enum):
    """Search intent types"""
    NAVIGATION = "navigation"  # Looking for specific item
    EXPLORATION = "exploration"  # Browsing/discovering
    INVESTIGATION = "investigation"  # Deep analysis
    MONITORING = "monitoring"  # Tracking changes


class SearchOperator(str, Enum):
    """Search operators"""
    AND = "and"
    OR = "or"
    NOT = "not"
    EXACT = "exact"
    FUZZY = "fuzzy"
    WILDCARD = "wildcard"
    RANGE = "range"
    EXISTS = "exists"


class FacetType(str, Enum):
    """Facet types"""
    TERMS = "terms"
    RANGE = "range"
    DATE_HISTOGRAM = "date_histogram"
    NESTED = "nested"


@dataclass
class SearchFilter:
    """Search filter"""
    field: str
    operator: SearchOperator
    value: Any
    boost: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "field": self.field,
            "operator": self.operator.value,
            "value": self.value,
            "boost": self.boost
        }


@dataclass
class SearchFacet:
    """Search facet configuration"""
    field: str
    facet_type: FacetType = FacetType.TERMS
    size: int = 10
    min_count: int = 1
    config: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "field": self.field,
            "type": self.facet_type.value,
            "size": self.size,
            "min_count": self.min_count,
            "config": self.config
        }


@dataclass
class FacetValue:
    """Facet value result"""
    value: Any
    count: int
    selected: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "value": self.value,
            "count": self.count,
            "selected": self.selected
        }


@dataclass
class SearchQuery:
    """Search query"""
    query_text: str = ""
    filters: List[SearchFilter] = field(default_factory=list)
    facets: List[SearchFacet] = field(default_factory=list)
    sort_by: Optional[str] = None
    sort_order: str = "desc"
    offset: int = 0
    limit: int = 20
    intent: SearchIntent = SearchIntent.EXPLORATION
    include_fields: Optional[List[str]] = None
    exclude_fields: Optional[List[str]] = None
    highlight_fields: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "query_text": self.query_text,
            "filters": [f.to_dict() for f in self.filters],
            "facets": [f.to_dict() for f in self.facets],
            "sort_by": self.sort_by,
            "sort_order": self.sort_order,
            "offset": self.offset,
            "limit": self.limit,
            "intent": self.intent.value,
            "include_fields": self.include_fields,
            "exclude_fields": self.exclude_fields,
            "highlight_fields": self.highlight_fields
        }


@dataclass
class SearchHit:
    """Search result hit"""
    id: str
    score: float
    source: Dict[str, Any]
    highlights: Dict[str, List[str]] = field(default_factory=dict)
    explanations: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "score": self.score,
            "source": self.source,
            "highlights": self.highlights,
            "explanations": self.explanations
        }


@dataclass
class SearchResult:
    """Search result"""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    total_hits: int = 0
    hits: List[SearchHit] = field(default_factory=list)
    facets: Dict[str, List[FacetValue]] = field(default_factory=dict)
    took_ms: float = 0.0
    max_score: float = 0.0
    suggestions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "query_id": self.query_id,
            "total_hits": self.total_hits,
            "hits": [h.to_dict() for h in self.hits],
            "facets": {
                k: [v.to_dict() for v in values]
                for k, values in self.facets.items()
            },
            "took_ms": self.took_ms,
            "max_score": self.max_score,
            "suggestions": self.suggestions,
            "metadata": self.metadata
        }


class BaseSearchBackend(ABC):
    """Base search backend interface"""
    
    async def index(
        self,
        doc_id: str,
        document: Dict[str, Any],
        index_name: str = "catalog"
    ) -> bool:
        """Index document"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement index method"
        )
        
    async def search(
        self,
        query: SearchQuery,
        index_name: str = "catalog"
    ) -> SearchResult:
        """Execute search"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement search method"
        )
        
    async def delete(
        self,
        doc_id: str,
        index_name: str = "catalog"
    ) -> bool:
        """Delete document"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement delete method"
        )
        
    async def bulk_index(
        self,
        documents: List[Tuple[str, Dict[str, Any]]],
        index_name: str = "catalog"
    ) -> Dict[str, bool]:
        """Bulk index documents"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement bulk_index method"
        )


class InMemorySearchBackend(BaseSearchBackend):
    """Simple in-memory search backend for testing"""
    
    def __init__(self):
        self._indices: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        self._inverted_index: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
        
    async def index(
        self,
        doc_id: str,
        document: Dict[str, Any],
        index_name: str = "catalog"
    ) -> bool:
        """Index document"""
        # Store document
        self._indices[index_name][doc_id] = document
        
        # Build inverted index
        for field, value in document.items():
            if isinstance(value, str):
                # Tokenize and index
                tokens = self._tokenize(value)
                for token in tokens:
                    self._inverted_index[index_name][token.lower()].add(doc_id)
                    
        return True
        
    async def search(
        self,
        query: SearchQuery,
        index_name: str = "catalog"
    ) -> SearchResult:
        """Execute search"""
        start_time = datetime.utcnow()
        
        # Get matching documents
        matching_docs = set()
        
        if query.query_text:
            # Text search
            tokens = self._tokenize(query.query_text)
            for token in tokens:
                token_lower = token.lower()
                if token_lower in self._inverted_index[index_name]:
                    matching_docs.update(self._inverted_index[index_name][token_lower])
        else:
            # No query text, include all
            matching_docs = set(self._indices[index_name].keys())
            
        # Apply filters
        filtered_docs = []
        for doc_id in matching_docs:
            doc = self._indices[index_name].get(doc_id)
            if doc and self._matches_filters(doc, query.filters):
                filtered_docs.append((doc_id, doc))
                
        # Calculate scores
        scored_docs = []
        for doc_id, doc in filtered_docs:
            score = self._calculate_score(doc, query)
            scored_docs.append((score, doc_id, doc))
            
        # Sort by score
        scored_docs.sort(key=lambda x: x[0], reverse=True)
        
        # Apply pagination
        paginated = scored_docs[query.offset:query.offset + query.limit]
        
        # Build hits
        hits = []
        max_score = 0.0
        for score, doc_id, doc in paginated:
            max_score = max(max_score, score)
            
            # Apply field filtering
            filtered_doc = self._filter_fields(doc, query.include_fields, query.exclude_fields)
            
            # Generate highlights
            highlights = {}
            if query.highlight_fields and query.query_text:
                highlights = self._generate_highlights(doc, query.query_text, query.highlight_fields)
                
            hits.append(SearchHit(
                id=doc_id,
                score=score,
                source=filtered_doc,
                highlights=highlights
            ))
            
        # Generate facets
        facets = {}
        if query.facets:
            facets = self._generate_facets(filtered_docs, query.facets)
            
        # Calculate duration
        took_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        return SearchResult(
            total_hits=len(filtered_docs),
            hits=hits,
            facets=facets,
            took_ms=took_ms,
            max_score=max_score
        )
        
    async def delete(
        self,
        doc_id: str,
        index_name: str = "catalog"
    ) -> bool:
        """Delete document"""
        if doc_id in self._indices[index_name]:
            # Remove from document store
            doc = self._indices[index_name].pop(doc_id)
            
            # Remove from inverted index
            for field, value in doc.items():
                if isinstance(value, str):
                    tokens = self._tokenize(value)
                    for token in tokens:
                        token_lower = token.lower()
                        if token_lower in self._inverted_index[index_name]:
                            self._inverted_index[index_name][token_lower].discard(doc_id)
                            
            return True
        return False
        
    async def bulk_index(
        self,
        documents: List[Tuple[str, Dict[str, Any]]],
        index_name: str = "catalog"
    ) -> Dict[str, bool]:
        """Bulk index documents"""
        results = {}
        for doc_id, document in documents:
            results[doc_id] = await self.index(doc_id, document, index_name)
        return results
        
    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenization"""
        # Remove punctuation and split
        tokens = re.findall(r'\b\w+\b', text.lower())
        return tokens
        
    def _matches_filters(
        self,
        doc: Dict[str, Any],
        filters: List[SearchFilter]
    ) -> bool:
        """Check if document matches filters"""
        for filter in filters:
            field_value = doc.get(filter.field)
            
            if filter.operator == SearchOperator.EXISTS:
                if (filter.value and field_value is None) or (not filter.value and field_value is not None):
                    return False
            elif filter.operator == SearchOperator.EXACT:
                if field_value != filter.value:
                    return False
            elif filter.operator == SearchOperator.RANGE:
                if isinstance(filter.value, dict):
                    min_val = filter.value.get("min")
                    max_val = filter.value.get("max")
                    if min_val is not None and field_value < min_val:
                        return False
                    if max_val is not None and field_value > max_val:
                        return False
            # Add more operators as needed
            
        return True
        
    def _calculate_score(
        self,
        doc: Dict[str, Any],
        query: SearchQuery
    ) -> float:
        """Calculate document score"""
        score = 0.0
        
        if query.query_text:
            # Simple TF scoring
            query_tokens = set(self._tokenize(query.query_text))
            
            for field, value in doc.items():
                if isinstance(value, str):
                    doc_tokens = self._tokenize(value)
                    matches = sum(1 for token in doc_tokens if token in query_tokens)
                    if matches > 0:
                        score += matches / len(doc_tokens)
                        
        else:
            # No query text, use constant score
            score = 1.0
            
        return score
        
    def _filter_fields(
        self,
        doc: Dict[str, Any],
        include_fields: Optional[List[str]],
        exclude_fields: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Filter document fields"""
        if include_fields:
            return {k: v for k, v in doc.items() if k in include_fields}
        elif exclude_fields:
            return {k: v for k, v in doc.items() if k not in exclude_fields}
        else:
            return doc
            
    def _generate_highlights(
        self,
        doc: Dict[str, Any],
        query_text: str,
        highlight_fields: List[str]
    ) -> Dict[str, List[str]]:
        """Generate search highlights"""
        highlights = {}
        query_tokens = set(self._tokenize(query_text))
        
        for field in highlight_fields:
            if field in doc and isinstance(doc[field], str):
                # Simple highlighting
                text = doc[field]
                highlighted = []
                
                for token in query_tokens:
                    pattern = re.compile(r'\b' + re.escape(token) + r'\b', re.IGNORECASE)
                    text = pattern.sub(f"<em>{token}</em>", text)
                    
                if "<em>" in text:
                    # Extract snippets around highlights
                    snippets = []
                    for match in re.finditer(r'(.{0,50}<em>.*?</em>.{0,50})', text):
                        snippets.append(match.group(1))
                    highlights[field] = snippets[:3]  # Limit to 3 snippets
                    
        return highlights
        
    def _generate_facets(
        self,
        documents: List[Tuple[str, Dict[str, Any]]],
        facet_configs: List[SearchFacet]
    ) -> Dict[str, List[FacetValue]]:
        """Generate facets from documents"""
        facets = {}
        
        for config in facet_configs:
            if config.facet_type == FacetType.TERMS:
                # Count term frequencies
                value_counts = defaultdict(int)
                
                for _, doc in documents:
                    value = doc.get(config.field)
                    if value is not None:
                        if isinstance(value, list):
                            for v in value:
                                value_counts[v] += 1
                        else:
                            value_counts[value] += 1
                            
                # Sort by count and apply size limit
                sorted_values = sorted(
                    value_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:config.size]
                
                # Filter by min_count
                facet_values = [
                    FacetValue(value=v, count=c)
                    for v, c in sorted_values
                    if c >= config.min_count
                ]
                
                if facet_values:
                    facets[config.field] = facet_values
                    
        return facets


class CatalogSearch:
    """
    Catalog search interface.
    
    Features:
    - Multi-backend support
    - Query optimization
    - Result caching
    - Search analytics
    - Relevance tuning
    """
    
    def __init__(
        self,
        backend: Optional[BaseSearchBackend] = None,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.backend = backend or InMemorySearchBackend()
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Search history for analytics
        self._search_history: List[Dict[str, Any]] = []
        self._popular_queries: Dict[str, int] = defaultdict(int)
        
    async def search(
        self,
        query: SearchQuery,
        user_context: Optional[Dict[str, Any]] = None
    ) -> SearchResult:
        """Execute search query"""
        # Check cache
        cache_key = None
        if self.cache:
            cache_key = self._generate_cache_key(query)
            cached = await self.cache.get(cache_key)
            if cached:
                return self._dict_to_result(cached)
                
        # Optimize query
        optimized_query = self._optimize_query(query, user_context)
        
        # Execute search
        result = await self.backend.search(optimized_query)
        
        # Post-process results
        result = self._post_process_results(result, query, user_context)
        
        # Cache result
        if self.cache and cache_key:
            await self.cache.set(cache_key, result.to_dict(), ttl=300)
            
        # Track search
        self._track_search(query, result, user_context)
        
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="search.executed",
                source="catalog_search",
                data={
                    "query_id": result.query_id,
                    "query_text": query.query_text,
                    "total_hits": result.total_hits,
                    "took_ms": result.took_ms
                }
            ))
            
        return result
        
    async def index_entity(
        self,
        entity_id: str,
        entity: Dict[str, Any]
    ) -> bool:
        """Index catalog entity"""
        # Prepare document for indexing
        doc = self._prepare_document(entity)
        
        # Index in backend
        success = await self.backend.index(entity_id, doc)
        
        # Clear relevant caches
        if self.cache:
            await self._clear_search_caches()
            
        # Publish event
        if self.event_bus and success:
            await self.event_bus.publish(Event(
                type="search.indexed",
                source="catalog_search",
                data={"entity_id": entity_id}
            ))
            
        return success
        
    async def delete_entity(self, entity_id: str) -> bool:
        """Delete entity from search index"""
        success = await self.backend.delete(entity_id)
        
        # Clear caches
        if self.cache and success:
            await self._clear_search_caches()
            
        return success
        
    async def bulk_index(
        self,
        entities: List[Tuple[str, Dict[str, Any]]]
    ) -> Dict[str, bool]:
        """Bulk index entities"""
        # Prepare documents
        documents = [
            (entity_id, self._prepare_document(entity))
            for entity_id, entity in entities
        ]
        
        # Bulk index
        results = await self.backend.bulk_index(documents)
        
        # Clear caches
        if self.cache:
            await self._clear_search_caches()
            
        return results
        
    async def suggest(
        self,
        prefix: str,
        field: str = "name",
        limit: int = 10
    ) -> List[str]:
        """Get search suggestions"""
        # Simple prefix-based suggestions
        # In production, would use proper suggest index
        
        query = SearchQuery(
            query_text=f"{prefix}*",
            limit=limit,
            include_fields=[field]
        )
        
        result = await self.backend.search(query)
        
        suggestions = []
        for hit in result.hits:
            value = hit.source.get(field)
            if value and isinstance(value, str):
                suggestions.append(value)
                
        return suggestions
        
    def _optimize_query(
        self,
        query: SearchQuery,
        user_context: Optional[Dict[str, Any]]
    ) -> SearchQuery:
        """Optimize query based on context and patterns"""
        optimized = SearchQuery(**query.to_dict())
        
        # Add user context filters
        if user_context:
            # Filter by user's accessible entities
            if "accessible_entities" in user_context:
                optimized.filters.append(SearchFilter(
                    field="id",
                    operator=SearchOperator.EXISTS,
                    value=user_context["accessible_entities"]
                ))
                
        # Boost recent items for exploration
        if query.intent == SearchIntent.EXPLORATION and not query.sort_by:
            optimized.sort_by = "updated_at"
            optimized.sort_order = "desc"
            
        return optimized
        
    def _post_process_results(
        self,
        result: SearchResult,
        query: SearchQuery,
        user_context: Optional[Dict[str, Any]]
    ) -> SearchResult:
        """Post-process search results"""
        # Add query suggestions
        if result.total_hits == 0 and query.query_text:
            # Suggest alternative queries
            result.suggestions = self._generate_suggestions(query.query_text)
            
        # Personalize results based on user context
        if user_context and "preferences" in user_context:
            # Re-rank based on user preferences
            # This is a simplified example
            pass
            
        return result
        
    def _prepare_document(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare entity for indexing"""
        doc = entity.copy()
        
        # Add computed fields
        doc["_indexed_at"] = datetime.utcnow().isoformat()
        
        # Flatten nested structures for better search
        if "metadata" in doc and isinstance(doc["metadata"], dict):
            for key, value in doc["metadata"].items():
                doc[f"metadata_{key}"] = value
                
        return doc
        
    def _generate_cache_key(self, query: SearchQuery) -> str:
        """Generate cache key for query"""
        # Simple hash of query parameters
        import hashlib
        query_str = json.dumps(query.to_dict(), sort_keys=True)
        return f"search:{hashlib.md5(query_str.encode()).hexdigest()}"
        
    async def _clear_search_caches(self):
        """Clear search-related caches"""
        if self.cache:
            # Clear with pattern matching if supported
            # For now, just log
            logger.info("Clearing search caches")
            
    def _track_search(
        self,
        query: SearchQuery,
        result: SearchResult,
        user_context: Optional[Dict[str, Any]]
    ):
        """Track search for analytics"""
        # Record search
        search_record = {
            "query_id": result.query_id,
            "timestamp": datetime.utcnow(),
            "query_text": query.query_text,
            "filters": len(query.filters),
            "total_hits": result.total_hits,
            "took_ms": result.took_ms,
            "user_id": user_context.get("user_id") if user_context else None
        }
        
        self._search_history.append(search_record)
        
        # Track popular queries
        if query.query_text:
            self._popular_queries[query.query_text] += 1
            
        # Limit history size
        if len(self._search_history) > 10000:
            self._search_history = self._search_history[-5000:]
            
    def _generate_suggestions(self, query_text: str) -> List[str]:
        """Generate query suggestions"""
        suggestions = []
        
        # Check for typos
        # In production, would use proper spell checking
        
        # Suggest popular related queries
        for popular_query, count in self._popular_queries.items():
            if query_text.lower() in popular_query.lower() and popular_query != query_text:
                suggestions.append(popular_query)
                
        return suggestions[:5]
        
    def _dict_to_result(self, data: Dict[str, Any]) -> SearchResult:
        """Convert dictionary to SearchResult"""
        # Reconstruct hits
        hits = []
        for hit_data in data.get("hits", []):
            hits.append(SearchHit(
                id=hit_data["id"],
                score=hit_data["score"],
                source=hit_data["source"],
                highlights=hit_data.get("highlights", {}),
                explanations=hit_data.get("explanations")
            ))
            
        # Reconstruct facets
        facets = {}
        for field, values in data.get("facets", {}).items():
            facets[field] = [
                FacetValue(
                    value=v["value"],
                    count=v["count"],
                    selected=v.get("selected", False)
                )
                for v in values
            ]
            
        return SearchResult(
            query_id=data["query_id"],
            total_hits=data["total_hits"],
            hits=hits,
            facets=facets,
            took_ms=data["took_ms"],
            max_score=data["max_score"],
            suggestions=data.get("suggestions", []),
            metadata=data.get("metadata", {})
        )
        
    async def get_search_analytics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get search analytics"""
        # Filter history by date range
        filtered_history = self._search_history
        if start_date:
            filtered_history = [
                s for s in filtered_history
                if s["timestamp"] >= start_date
            ]
        if end_date:
            filtered_history = [
                s for s in filtered_history
                if s["timestamp"] <= end_date
            ]
            
        if not filtered_history:
            return {
                "total_searches": 0,
                "avg_response_time": 0,
                "zero_result_rate": 0,
                "popular_queries": []
            }
            
        # Calculate metrics
        total_searches = len(filtered_history)
        avg_response_time = sum(s["took_ms"] for s in filtered_history) / total_searches
        zero_results = sum(1 for s in filtered_history if s["total_hits"] == 0)
        zero_result_rate = zero_results / total_searches
        
        # Get top queries
        query_counts = defaultdict(int)
        for search in filtered_history:
            if search["query_text"]:
                query_counts[search["query_text"]] += 1
                
        popular_queries = sorted(
            query_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        return {
            "total_searches": total_searches,
            "avg_response_time": avg_response_time,
            "zero_result_rate": zero_result_rate,
            "popular_queries": [
                {"query": q, "count": c}
                for q, c in popular_queries
            ],
            "unique_users": len(set(
                s.get("user_id") for s in filtered_history
                if s.get("user_id")
            ))
        } 