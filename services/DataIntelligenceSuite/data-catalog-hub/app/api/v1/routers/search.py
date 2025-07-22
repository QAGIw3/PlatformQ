"""
Search API Router

RESTful API endpoints for unified search operations.
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Body

from app.api.v1.dependencies import get_unified_search_service, get_current_user
from app.services.search import UnifiedSearchService
from app.services.search.models import SearchType, SearchFilter

router = APIRouter()


@router.post("")
async def unified_search(
    query: str = Body(..., description="Search query"),
    search_types: Optional[List[SearchType]] = Body(None, description="Types of search to perform"),
    filters: Optional[List[SearchFilter]] = Body(None, description="Search filters"),
    limit: int = Body(20, ge=1, le=100, description="Result limit per type"),
    offset: int = Body(0, ge=0, description="Result offset"),
    include_score: bool = Body(True, description="Include relevance scores"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Unified search across all catalog resources.
    
    - **query**: Search query (natural language or keywords)
    - **search_types**: Types to search (text, vector, hybrid, ai_powered)
    - **filters**: Filters for entity type, classification, etc.
    - **limit**: Maximum results per search type
    - **offset**: Pagination offset
    - **include_score**: Whether to include relevance scores
    """
    result = await search_service.unified_search(
        query=query,
        search_types=search_types,
        filters=filters,
        limit=limit,
        offset=offset,
        include_score=include_score
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/text")
async def text_search(
    query: str = Body(..., description="Text search query"),
    entity_types: Optional[List[str]] = Body(None, description="Entity types to search"),
    fields: Optional[List[str]] = Body(None, description="Fields to search in"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Additional filters"),
    fuzzy: bool = Body(False, description="Enable fuzzy matching"),
    limit: int = Body(20, ge=1, le=100, description="Result limit"),
    offset: int = Body(0, ge=0, description="Result offset"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Traditional text-based search.
    
    - **query**: Text to search for
    - **entity_types**: Limit to specific entity types
    - **fields**: Specific fields to search in
    - **filters**: Additional filter criteria
    - **fuzzy**: Enable fuzzy/approximate matching
    - **limit**: Maximum results
    - **offset**: Pagination offset
    """
    result = await search_service.text_search(
        query=query,
        entity_types=entity_types,
        fields=fields,
        filters=filters,
        fuzzy=fuzzy,
        limit=limit,
        offset=offset
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/vector")
async def vector_search(
    query: str = Body(..., description="Query for embedding"),
    entity_types: Optional[List[str]] = Body(None, description="Entity types to search"),
    embedding_fields: Optional[List[str]] = Body(None, description="Fields with embeddings"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Additional filters"),
    threshold: float = Body(0.7, ge=0.0, le=1.0, description="Similarity threshold"),
    limit: int = Body(20, ge=1, le=100, description="Result limit"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Vector similarity search using embeddings.
    
    - **query**: Text to convert to embedding
    - **entity_types**: Limit to specific entity types
    - **embedding_fields**: Fields containing embeddings to search
    - **filters**: Additional filter criteria
    - **threshold**: Minimum similarity score
    - **limit**: Maximum results
    """
    result = await search_service.vector_search(
        query=query,
        entity_types=entity_types,
        embedding_fields=embedding_fields,
        filters=filters,
        threshold=threshold,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/hybrid")
async def hybrid_search(
    query: str = Body(..., description="Search query"),
    text_weight: float = Body(0.5, ge=0.0, le=1.0, description="Weight for text search"),
    vector_weight: float = Body(0.5, ge=0.0, le=1.0, description="Weight for vector search"),
    entity_types: Optional[List[str]] = Body(None, description="Entity types to search"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Additional filters"),
    limit: int = Body(20, ge=1, le=100, description="Result limit"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Hybrid search combining text and vector approaches.
    
    - **query**: Search query
    - **text_weight**: Weight for text search results (0-1)
    - **vector_weight**: Weight for vector search results (0-1)
    - **entity_types**: Limit to specific entity types
    - **filters**: Additional filter criteria
    - **limit**: Maximum results
    """
    result = await search_service.hybrid_search(
        query=query,
        text_weight=text_weight,
        vector_weight=vector_weight,
        entity_types=entity_types,
        filters=filters,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/ai-powered")
async def ai_powered_search(
    natural_query: str = Body(..., description="Natural language query"),
    include_reasoning: bool = Body(True, description="Include AI reasoning"),
    max_hops: int = Body(3, ge=1, le=5, description="Maximum graph traversal hops"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service),
    current_user: dict = Depends(get_current_user)
):
    """
    AI-powered natural language search.
    
    - **natural_query**: Natural language question or search
    - **include_reasoning**: Include AI's reasoning process
    - **max_hops**: Maximum relationship hops to traverse
    """
    result = await search_service.ai_powered_search(
        natural_query=natural_query,
        include_reasoning=include_reasoning,
        max_hops=max_hops
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/suggestions")
async def get_search_suggestions(
    prefix: str = Query(..., min_length=2, description="Search prefix"),
    search_type: Optional[SearchType] = Query(None, description="Type of suggestions"),
    entity_types: Optional[List[str]] = Query(None, description="Entity types"),
    limit: int = Query(10, ge=1, le=50, description="Suggestion limit"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Get search suggestions/autocomplete.
    
    - **prefix**: Text prefix to get suggestions for
    - **search_type**: Type of search for context
    - **entity_types**: Limit to specific entity types
    - **limit**: Maximum suggestions
    """
    result = await search_service.get_suggestions(
        prefix=prefix,
        search_type=search_type,
        entity_types=entity_types,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/facets")
async def get_search_facets(
    query: Optional[str] = Body(None, description="Optional search query"),
    entity_types: Optional[List[str]] = Body(None, description="Entity types"),
    facet_fields: Optional[List[str]] = Body(None, description="Fields to get facets for"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Get search facets for filtering.
    
    - **query**: Optional query to get facets for
    - **entity_types**: Entity types to analyze
    - **facet_fields**: Specific fields to get facets for
    """
    result = await search_service.get_facets(
        query=query,
        entity_types=entity_types,
        facet_fields=facet_fields
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/related")
async def find_related_entities(
    entity_guid: str = Body(..., description="Source entity GUID"),
    relationship_types: Optional[List[str]] = Body(None, description="Types of relationships"),
    max_depth: int = Body(2, ge=1, le=5, description="Maximum traversal depth"),
    limit: int = Body(20, ge=1, le=100, description="Result limit"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Find entities related to a given entity.
    
    - **entity_guid**: Starting entity
    - **relationship_types**: Types of relationships to follow
    - **max_depth**: Maximum graph traversal depth
    - **limit**: Maximum results
    """
    result = await search_service.find_related(
        entity_guid=entity_guid,
        relationship_types=relationship_types,
        max_depth=max_depth,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/similar")
async def find_similar_entities(
    entity_guid: str = Body(..., description="Reference entity GUID"),
    similarity_method: str = Body("hybrid", regex="^(text|vector|hybrid|structural)$"),
    min_similarity: float = Body(0.7, ge=0.0, le=1.0, description="Minimum similarity"),
    limit: int = Body(10, ge=1, le=50, description="Result limit"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Find entities similar to a given entity.
    
    - **entity_guid**: Reference entity
    - **similarity_method**: Method to calculate similarity
    - **min_similarity**: Minimum similarity score
    - **limit**: Maximum results
    """
    result = await search_service.find_similar(
        entity_guid=entity_guid,
        similarity_method=similarity_method,
        min_similarity=min_similarity,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/recent")
async def get_recent_searches(
    limit: int = Query(10, ge=1, le=50, description="Number of recent searches"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service),
    current_user: dict = Depends(get_current_user)
):
    """Get user's recent searches."""
    result = await search_service.get_recent_searches(
        user_id=current_user["id"],
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/popular")
async def get_popular_searches(
    time_range_days: int = Query(7, ge=1, le=90, description="Time range in days"),
    limit: int = Query(20, ge=1, le=100, description="Result limit"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """Get popular searches across all users."""
    result = await search_service.get_popular_searches(
        time_range_days=time_range_days,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/save")
async def save_search(
    name: str = Body(..., description="Saved search name"),
    query: str = Body(..., description="Search query"),
    search_type: SearchType = Body(..., description="Type of search"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Search filters"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service),
    current_user: dict = Depends(get_current_user)
):
    """Save a search for later use."""
    result = await search_service.save_search(
        user_id=current_user["id"],
        name=name,
        query=query,
        search_type=search_type,
        filters=filters
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/saved")
async def get_saved_searches(
    search_service: UnifiedSearchService = Depends(get_unified_search_service),
    current_user: dict = Depends(get_current_user)
):
    """Get user's saved searches."""
    result = await search_service.get_saved_searches(user_id=current_user["id"])
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/explain")
async def explain_search_results(
    query: str = Body(..., description="Original search query"),
    result_guid: str = Body(..., description="Result entity GUID to explain"),
    search_type: SearchType = Body(..., description="Type of search performed"),
    search_service: UnifiedSearchService = Depends(get_unified_search_service)
):
    """
    Explain why a result matched a search query.
    
    - **query**: Original search query
    - **result_guid**: Entity GUID from search results
    - **search_type**: Type of search that was performed
    """
    result = await search_service.explain_result(
        query=query,
        result_guid=result_guid,
        search_type=search_type
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data 