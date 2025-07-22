"""
Search and Discovery API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core import SearchEngine, AtlasClient
from platformq_shared.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["search"])

# Global dependencies
search_engine: Optional[SearchEngine] = None
atlas_client: Optional[AtlasClient] = None


def set_dependencies(search: SearchEngine, atlas: AtlasClient):
    """Set the global dependencies for this router"""
    global search_engine, atlas_client
    search_engine = search
    atlas_client = atlas


# Request/Response Models
class SearchRequest(BaseModel):
    """Simple search request"""
    query: str = Field(..., description="Search query")
    type_name: Optional[str] = Field(None, description="Filter by entity type")
    limit: int = Field(20, ge=1, le=100, description="Maximum results")
    offset: int = Field(0, ge=0, description="Result offset")
    exclude_deleted: bool = Field(True, description="Exclude deleted entities")


class AdvancedSearchRequest(BaseModel):
    """Advanced search with filters and facets"""
    query: str = Field("*", description="Search query, * for all")
    filters: Optional[Dict[str, Any]] = Field(None, description="Search filters")
    facets: Optional[List[str]] = Field(None, description="Facets to return")
    sort_by: Optional[str] = Field(None, description="Sort field")
    sort_order: Optional[str] = Field("desc", pattern="^(asc|desc)$")
    limit: int = Field(20, ge=1, le=100)
    offset: int = Field(0, ge=0)
    include_terms: bool = Field(False, description="Include glossary terms")


class SearchResult(BaseModel):
    """Search result item"""
    guid: str
    type_name: str
    qualified_name: str
    name: str
    description: Optional[str]
    owner: Optional[str]
    classifications: List[str]
    tags: List[str]
    score: float
    highlight: Optional[Dict[str, List[str]]]
    attributes: Dict[str, Any]


class SearchResponse(BaseModel):
    """Search response with results and metadata"""
    query: str
    total: int
    results: List[SearchResult]
    facets: Optional[Dict[str, Dict[str, int]]]
    execution_time_ms: int


class SuggestionResponse(BaseModel):
    """Search suggestions"""
    query: str
    suggestions: List[str]
    spell_corrections: Optional[List[str]]


class RelatedEntity(BaseModel):
    """Related entity information"""
    guid: str
    type_name: str
    name: str
    relationship: str
    score: float


class RecommendationResponse(BaseModel):
    """Entity recommendations"""
    recommendations: List[SearchResult]
    based_on: List[str]
    algorithm: str


# API Endpoints
@router.get("/search", response_model=SearchResponse)
async def search(
    query: str = Query(..., description="Search query"),
    type_name: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    classifications: Optional[List[str]] = Query(None, description="Filter by classifications"),
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    owner: Optional[str] = Query(None, description="Filter by owner"),
    exclude_deleted: bool = Query(True)
):
    """
    Perform full-text search across all entities
    
    - **query**: Search query string (supports wildcards)
    - **type_name**: Filter by specific entity type
    - **limit**: Maximum number of results
    - **offset**: Result offset for pagination
    - **classifications**: Filter by classifications
    - **tags**: Filter by tags
    - **owner**: Filter by owner
    - **exclude_deleted**: Exclude soft-deleted entities
    """
    try:
        start_time = datetime.utcnow()
        
        # Build filters
        filters = {}
        if type_name:
            filters["typeName"] = type_name
        if classifications:
            filters["classifications"] = classifications
        if tags:
            filters["tags"] = tags
        if owner:
            filters["owner"] = owner
            
        # Perform search
        result = await search_engine.search(
            query=query,
            filters=filters,
            limit=limit,
            offset=offset,
            exclude_deleted=exclude_deleted
        )
        
        # Calculate execution time
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Build response
        return SearchResponse(
            query=query,
            total=result["total"],
            results=[
                SearchResult(
                    guid=hit["guid"],
                    type_name=hit["typeName"],
                    qualified_name=hit["qualifiedName"],
                    name=hit["name"],
                    description=hit.get("description"),
                    owner=hit.get("owner"),
                    classifications=hit.get("classifications", []),
                    tags=hit.get("tags", []),
                    score=hit["_score"],
                    highlight=hit.get("_highlight"),
                    attributes=hit.get("attributes", {})
                )
                for hit in result["hits"]
            ],
            facets=result.get("facets"),
            execution_time_ms=execution_time
        )
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/advanced", response_model=SearchResponse)
async def advanced_search(request: AdvancedSearchRequest):
    """
    Advanced search with complex filters and faceting
    
    Supports:
    - Complex filter combinations
    - Faceted search
    - Custom sorting
    - Glossary term inclusion
    """
    try:
        start_time = datetime.utcnow()
        
        # Perform advanced search
        result = await search_engine.advanced_search(
            query=request.query,
            filters=request.filters,
            facets=request.facets,
            sort_by=request.sort_by,
            sort_order=request.sort_order,
            limit=request.limit,
            offset=request.offset,
            include_terms=request.include_terms
        )
        
        # Calculate execution time
        execution_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Build response
        return SearchResponse(
            query=request.query,
            total=result["total"],
            results=[
                SearchResult(
                    guid=hit["guid"],
                    type_name=hit["typeName"],
                    qualified_name=hit["qualifiedName"],
                    name=hit["name"],
                    description=hit.get("description"),
                    owner=hit.get("owner"),
                    classifications=hit.get("classifications", []),
                    tags=hit.get("tags", []),
                    score=hit["_score"],
                    highlight=hit.get("_highlight"),
                    attributes=hit.get("attributes", {})
                )
                for hit in result["hits"]
            ],
            facets=result.get("facets"),
            execution_time_ms=execution_time
        )
        
    except Exception as e:
        logger.error(f"Advanced search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/suggestions", response_model=SuggestionResponse)
async def get_suggestions(
    query: str = Query(..., description="Partial query for suggestions"),
    type_name: Optional[str] = Query(None, description="Limit suggestions to type"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Get search suggestions and spell corrections
    
    - **query**: Partial search query
    - **type_name**: Limit suggestions to specific entity type
    - **limit**: Maximum number of suggestions
    """
    try:
        # Get suggestions
        suggestions = await search_engine.get_suggestions(
            query=query,
            type_name=type_name,
            limit=limit
        )
        
        # Get spell corrections if query seems misspelled
        corrections = await search_engine.get_spell_corrections(query)
        
        return SuggestionResponse(
            query=query,
            suggestions=suggestions,
            spell_corrections=corrections if corrections else None
        )
        
    except Exception as e:
        logger.error(f"Suggestions error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/discovery/related/{guid}", response_model=List[RelatedEntity])
async def find_related_entities(
    guid: str,
    relationship_types: Optional[List[str]] = Query(None, description="Filter by relationship types"),
    limit: int = Query(20, ge=1, le=100)
):
    """
    Find entities related to the specified entity
    
    - **guid**: Entity GUID to find relationships for
    - **relationship_types**: Filter by specific relationship types
    - **limit**: Maximum number of related entities
    """
    try:
        # Get related entities
        related = await search_engine.find_related_entities(
            guid=guid,
            relationship_types=relationship_types,
            limit=limit
        )
        
        return [
            RelatedEntity(
                guid=entity["guid"],
                type_name=entity["typeName"],
                name=entity["name"],
                relationship=entity["relationship"],
                score=entity["score"]
            )
            for entity in related
        ]
        
    except Exception as e:
        logger.error(f"Related entities error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/discovery/recommendations", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: Optional[str] = Query(None, description="User ID for personalized recommendations"),
    based_on_guids: Optional[List[str]] = Query(None, description="Base recommendations on these entities"),
    type_names: Optional[List[str]] = Query(None, description="Limit to these entity types"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Get entity recommendations based on user history or specified entities
    
    - **user_id**: User ID for personalized recommendations
    - **based_on_guids**: Base recommendations on these entity GUIDs
    - **type_names**: Limit recommendations to these types
    - **limit**: Maximum number of recommendations
    """
    try:
        # Get recommendations
        recommendations = await search_engine.get_recommendations(
            user_id=user_id,
            based_on_guids=based_on_guids,
            type_names=type_names,
            limit=limit
        )
        
        # Convert to response format
        results = []
        for rec in recommendations["entities"]:
            results.append(SearchResult(
                guid=rec["guid"],
                type_name=rec["typeName"],
                qualified_name=rec["qualifiedName"],
                name=rec["name"],
                description=rec.get("description"),
                owner=rec.get("owner"),
                classifications=rec.get("classifications", []),
                tags=rec.get("tags", []),
                score=rec["score"],
                highlight=None,
                attributes=rec.get("attributes", {})
            ))
        
        return RecommendationResponse(
            recommendations=results,
            based_on=recommendations.get("basedOn", []),
            algorithm=recommendations.get("algorithm", "collaborative-filtering")
        )
        
    except Exception as e:
        logger.error(f"Recommendations error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/export")
async def export_search_results(request: AdvancedSearchRequest):
    """
    Export search results in various formats
    
    Returns search results in a format suitable for export (CSV, JSON, etc.)
    """
    try:
        # Perform search with higher limit for export
        export_request = request.copy()
        export_request.limit = min(request.limit * 10, 1000)  # Cap at 1000 for exports
        
        result = await search_engine.advanced_search(
            query=export_request.query,
            filters=export_request.filters,
            limit=export_request.limit,
            offset=export_request.offset
        )
        
        # Prepare export data
        export_data = {
            "metadata": {
                "query": export_request.query,
                "filters": export_request.filters,
                "total_results": result["total"],
                "exported_count": len(result["hits"]),
                "export_time": datetime.utcnow().isoformat()
            },
            "entities": [
                {
                    "guid": hit["guid"],
                    "type": hit["typeName"],
                    "qualified_name": hit["qualifiedName"],
                    "name": hit["name"],
                    "description": hit.get("description", ""),
                    "owner": hit.get("owner", ""),
                    "created_time": hit.get("createTime", ""),
                    "modified_time": hit.get("modifiedTime", ""),
                    "classifications": ",".join(hit.get("classifications", [])),
                    "tags": ",".join(hit.get("tags", []))
                }
                for hit in result["hits"]
            ]
        }
        
        return export_data
        
    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 