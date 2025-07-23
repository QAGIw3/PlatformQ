"""
Search API Models

Request and response models for unified search operations.
"""

from typing import Dict, Any, Optional, List, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class SearchType(str, Enum):
    """Types of search available."""
    TEXT = "text"
    VECTOR = "vector"
    HYBRID = "hybrid"
    AI_POWERED = "ai_powered"
    GRAPH = "graph"


class SearchFilterType(str, Enum):
    """Types of search filters."""
    ENTITY_TYPE = "entity_type"
    CLASSIFICATION = "classification"
    OWNER = "owner"
    TAG = "tag"
    DATE_RANGE = "date_range"
    QUALITY_SCORE = "quality_score"
    CUSTOM = "custom"


class SearchResultType(str, Enum):
    """Types of search results."""
    ENTITY = "entity"
    GLOSSARY_TERM = "glossary_term"
    CLASSIFICATION = "classification"
    SCHEMA = "schema"
    PROCESS = "process"


class SortOrder(str, Enum):
    """Sort order options."""
    RELEVANCE = "relevance"
    NAME_ASC = "name_asc"
    NAME_DESC = "name_desc"
    DATE_ASC = "date_asc"
    DATE_DESC = "date_desc"
    POPULARITY = "popularity"


class SearchFilter(BaseModel):
    """Search filter definition."""
    type: SearchFilterType = Field(..., description="Filter type")
    field: Optional[str] = Field(None, description="Field to filter on")
    value: Any = Field(..., description="Filter value")
    operator: Optional[str] = Field("eq", description="Filter operator")
    
    @validator('operator')
    def validate_operator(cls, v):
        valid_operators = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'contains', 'regex']
        if v not in valid_operators:
            raise ValueError(f'operator must be one of {valid_operators}')
        return v


class SearchHighlight(BaseModel):
    """Search result highlight."""
    field: str = Field(..., description="Field with highlight")
    fragments: List[str] = Field(..., description="Highlighted fragments")
    score: Optional[float] = Field(None, description="Highlight score")


class SearchResult(BaseModel):
    """Individual search result."""
    guid: str = Field(..., description="Result GUID")
    name: str = Field(..., description="Result name")
    type: SearchResultType = Field(..., description="Result type")
    qualified_name: str = Field(..., description="Qualified name")
    
    # Basic info
    description: Optional[str] = Field(None, description="Description")
    owner: Optional[str] = Field(None, description="Owner")
    status: str = Field("active", description="Status")
    
    # Search metadata
    score: Optional[float] = Field(None, description="Relevance score")
    highlights: Optional[List[SearchHighlight]] = Field(None, description="Search highlights")
    explanation: Optional[Dict[str, Any]] = Field(None, description="Score explanation")
    
    # Additional context
    entity_type: Optional[str] = Field(None, description="Entity type for entities")
    classifications: List[str] = Field(default_factory=list, description="Classifications")
    tags: List[str] = Field(default_factory=list, description="Tags")
    
    # Relationships
    parent_guid: Optional[str] = Field(None, description="Parent entity")
    glossary_terms: List[str] = Field(default_factory=list, description="Associated terms")
    
    # Quality and usage
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Quality score")
    usage_score: Optional[float] = Field(None, ge=0.0, description="Usage/popularity score")
    
    # Timestamps
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    modified_at: Optional[datetime] = Field(None, description="Last modification")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class UnifiedSearchRequest(BaseModel):
    """Request model for unified search."""
    query: str = Field(..., min_length=1, description="Search query")
    search_types: Optional[List[SearchType]] = Field(None, description="Types of search")
    filters: Optional[List[SearchFilter]] = Field(None, description="Search filters")
    
    # Pagination
    limit: int = Field(20, ge=1, le=100, description="Result limit per type")
    offset: int = Field(0, ge=0, description="Result offset")
    
    # Options
    include_score: bool = Field(True, description="Include relevance scores")
    include_highlights: bool = Field(True, description="Include highlights")
    include_facets: bool = Field(False, description="Include search facets")
    
    # Sorting
    sort_by: SortOrder = Field(SortOrder.RELEVANCE, description="Sort order")
    
    # Advanced
    boost_recent: bool = Field(True, description="Boost recent results")
    boost_quality: bool = Field(True, description="Boost high quality results")
    personalized: bool = Field(False, description="Personalize results")


class UnifiedSearchResponse(BaseModel):
    """Response model for unified search."""
    query: str = Field(..., description="Original query")
    total_results: int = Field(..., description="Total results across all types")
    
    # Results by type
    results: Dict[str, List[SearchResult]] = Field(..., description="Results grouped by type")
    
    # Facets
    facets: Optional[Dict[str, List[SearchFacet]]] = Field(None, description="Search facets")
    
    # Query analysis
    query_analysis: Optional[Dict[str, Any]] = Field(None, description="Query analysis")
    suggestions: Optional[List[str]] = Field(None, description="Query suggestions")
    
    # Performance
    query_time_ms: int = Field(..., description="Query execution time")
    
    # Pagination
    has_more: Dict[str, bool] = Field(..., description="More results available by type")
    next_offset: Dict[str, int] = Field(..., description="Next offset by type")


class TextSearchRequest(BaseModel):
    """Request model for text search."""
    query: str = Field(..., min_length=1, description="Text query")
    entity_types: Optional[List[str]] = Field(None, description="Entity types")
    fields: Optional[List[str]] = Field(None, description="Fields to search")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    
    # Options
    fuzzy: bool = Field(False, description="Enable fuzzy matching")
    fuzzy_distance: int = Field(2, ge=0, le=3, description="Fuzzy distance")
    
    # Advanced
    operator: str = Field("OR", regex="^(AND|OR)$", description="Query operator")
    boost_fields: Optional[Dict[str, float]] = Field(None, description="Field boosts")
    
    # Pagination
    limit: int = Field(20, ge=1, le=100, description="Result limit")
    offset: int = Field(0, ge=0, description="Result offset")


class VectorSearchRequest(BaseModel):
    """Request model for vector search."""
    query: str = Field(..., description="Query for embedding")
    entity_types: Optional[List[str]] = Field(None, description="Entity types")
    embedding_fields: Optional[List[str]] = Field(None, description="Fields with embeddings")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    
    # Vector options
    threshold: float = Field(0.7, ge=0.0, le=1.0, description="Similarity threshold")
    algorithm: str = Field("cosine", regex="^(cosine|euclidean|dot)$", description="Similarity algorithm")
    
    # Results
    limit: int = Field(20, ge=1, le=100, description="Result limit")
    include_embeddings: bool = Field(False, description="Include embeddings in results")


class HybridSearchRequest(BaseModel):
    """Request model for hybrid search."""
    query: str = Field(..., description="Search query")
    text_weight: float = Field(0.5, ge=0.0, le=1.0, description="Text search weight")
    vector_weight: float = Field(0.5, ge=0.0, le=1.0, description="Vector search weight")
    
    # Scope
    entity_types: Optional[List[str]] = Field(None, description="Entity types")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    
    # Options
    rerank: bool = Field(True, description="Rerank combined results")
    normalize_scores: bool = Field(True, description="Normalize scores")
    
    # Results
    limit: int = Field(20, ge=1, le=100, description="Result limit")
    
    @validator('vector_weight')
    def validate_weights(cls, v, values):
        text_weight = values.get('text_weight', 0.5)
        if abs(text_weight + v - 1.0) > 0.001:
            raise ValueError('text_weight + vector_weight must equal 1.0')
        return v


class AISearchRequest(BaseModel):
    """Request model for AI-powered search."""
    natural_query: str = Field(..., description="Natural language query")
    
    # Options
    include_reasoning: bool = Field(True, description="Include AI reasoning")
    max_hops: int = Field(3, ge=1, le=5, description="Max graph traversal hops")
    
    # Context
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")
    conversation_history: Optional[List[Dict[str, str]]] = Field(None, description="Previous queries")
    
    # Filters
    entity_types: Optional[List[str]] = Field(None, description="Limit to entity types")
    time_range: Optional[Dict[str, datetime]] = Field(None, description="Time range filter")


class SearchFacet(BaseModel):
    """Search facet for filtering."""
    field: str = Field(..., description="Facet field")
    values: List[Dict[str, Any]] = Field(..., description="Facet values with counts")
    total_count: int = Field(..., description="Total distinct values")
    
    # Each value contains:
    # - value: Any
    # - count: int
    # - selected: bool


class SearchSuggestion(BaseModel):
    """Search suggestion/autocomplete."""
    text: str = Field(..., description="Suggestion text")
    score: float = Field(..., description="Suggestion score")
    type: str = Field(..., description="Suggestion type")
    
    # Additional info
    entity_count: Optional[int] = Field(None, description="Matching entities")
    recent_usage: Optional[int] = Field(None, description="Recent usage count")
    highlighted: Optional[str] = Field(None, description="Highlighted text")


class SavedSearch(BaseModel):
    """Saved search definition."""
    id: str = Field(..., description="Saved search ID")
    name: str = Field(..., description="Search name")
    query: str = Field(..., description="Search query")
    search_type: SearchType = Field(..., description="Search type")
    
    # Configuration
    filters: Optional[Dict[str, Any]] = Field(None, description="Saved filters")
    settings: Optional[Dict[str, Any]] = Field(None, description="Search settings")
    
    # Metadata
    created_by: str = Field(..., description="Creator")
    created_at: datetime = Field(..., description="Creation timestamp")
    last_used: Optional[datetime] = Field(None, description="Last usage")
    usage_count: int = Field(0, description="Usage count")
    
    # Sharing
    is_public: bool = Field(False, description="Public search")
    shared_with: List[str] = Field(default_factory=list, description="Shared users/groups")


class SearchExplainRequest(BaseModel):
    """Request to explain search results."""
    query: str = Field(..., description="Original query")
    result_guid: str = Field(..., description="Result to explain")
    search_type: SearchType = Field(..., description="Search type used")


class SearchExplainResponse(BaseModel):
    """Response explaining search result."""
    query: str = Field(..., description="Original query")
    result_guid: str = Field(..., description="Result GUID")
    
    # Explanation
    match_explanation: Dict[str, Any] = Field(..., description="Why result matched")
    score_breakdown: Dict[str, float] = Field(..., description="Score components")
    
    # Factors
    matching_fields: List[str] = Field(..., description="Fields that matched")
    boost_factors: Dict[str, float] = Field(..., description="Applied boosts")
    penalties: Dict[str, float] = Field(default_factory=dict, description="Applied penalties")
    
    # Debugging
    debug_info: Optional[Dict[str, Any]] = Field(None, description="Debug information") 