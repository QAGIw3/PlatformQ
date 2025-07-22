"""
Glossary API Router

RESTful API endpoints for business glossary operations.
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body

from app.api.v1.dependencies import get_glossary_service, get_current_user
from app.services.catalog import GlossaryService
from app.core.glossary.models import TermStatus

router = APIRouter()


@router.post("")
async def create_glossary(
    name: str = Body(..., description="Glossary name"),
    short_description: str = Body(..., description="Short description"),
    long_description: Optional[str] = Body(None, description="Detailed description"),
    language: str = Body("en", description="Language code"),
    usage: Optional[str] = Body(None, description="Usage guidelines"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new glossary.
    
    - **name**: Unique glossary name
    - **short_description**: Brief description
    - **long_description**: Detailed description
    - **language**: Language code (default: en)
    - **usage**: Usage guidelines for terms
    """
    result = await glossary_service.create_glossary(
        name=name,
        short_description=short_description,
        long_description=long_description,
        language=language,
        usage=usage
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("")
async def list_glossaries(
    glossary_service: GlossaryService = Depends(get_glossary_service)
):
    """List all glossaries."""
    result = await glossary_service.list_glossaries()
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/terms")
async def create_term(
    name: str = Body(..., description="Term name"),
    definition: str = Body(..., description="Term definition"),
    glossary_guid: Optional[str] = Body(None, description="Parent glossary GUID"),
    abbreviation: Optional[str] = Body(None, description="Abbreviation"),
    usage: Optional[str] = Body(None, description="Usage guidelines"),
    examples: Optional[List[str]] = Body(None, description="Usage examples"),
    related_terms: Optional[List[str]] = Body(None, description="Related term names"),
    categories: Optional[List[str]] = Body(None, description="Category GUIDs"),
    status: TermStatus = Body(TermStatus.DRAFT, description="Initial status"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new glossary term.
    
    - **name**: Term name
    - **definition**: Clear definition of the term
    - **glossary_guid**: Parent glossary (uses default if not specified)
    - **abbreviation**: Common abbreviation
    - **usage**: How to use this term
    - **examples**: Real-world examples
    - **related_terms**: Names of related terms
    - **categories**: Category GUIDs for organization
    - **status**: Initial status (DRAFT, APPROVED, etc.)
    """
    result = await glossary_service.create_term(
        name=name,
        definition=definition,
        glossary_guid=glossary_guid,
        abbreviation=abbreviation,
        usage=usage,
        examples=examples,
        related_terms=related_terms,
        categories=categories,
        status=status
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.put("/terms/{term_guid}")
async def update_term(
    term_guid: str = Path(..., description="Term GUID"),
    updates: Dict[str, Any] = Body(..., description="Term updates"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Update an existing term."""
    result = await glossary_service.update_term(
        term_guid=term_guid,
        updates=updates
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/terms")
async def list_terms(
    glossary_guid: str = Query(..., description="Glossary GUID"),
    status: Optional[TermStatus] = Query(None, description="Filter by status"),
    category_guid: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(100, ge=1, le=1000, description="Result limit"),
    offset: int = Query(0, ge=0, description="Result offset"),
    glossary_service: GlossaryService = Depends(get_glossary_service)
):
    """List terms in a glossary."""
    result = await glossary_service.list_terms(
        glossary_guid=glossary_guid,
        status=status,
        category_guid=category_guid,
        limit=limit,
        offset=offset
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    terms, total = result.data
    return {
        "terms": terms,
        "total": total,
        "limit": limit,
        "offset": offset
    }


@router.post("/terms/{term_guid}/assign")
async def assign_term_to_entities(
    term_guid: str = Path(..., description="Term GUID"),
    entity_guids: List[str] = Body(..., description="Entity GUIDs"),
    semantic_assignment: bool = Body(False, description="Use AI for semantic assignment"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Assign term to entities.
    
    - **term_guid**: Business term to assign
    - **entity_guids**: List of entities to assign term to
    - **semantic_assignment**: Use AI to verify semantic correctness
    """
    result = await glossary_service.assign_term_to_entities(
        term_guid=term_guid,
        entity_guids=entity_guids,
        semantic_assignment=semantic_assignment
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.delete("/terms/{term_guid}/entities/{entity_guid}")
async def remove_term_from_entity(
    term_guid: str = Path(..., description="Term GUID"),
    entity_guid: str = Path(..., description="Entity GUID"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Remove term from entity."""
    result = await glossary_service.remove_term_from_entity(
        term_guid=term_guid,
        entity_guid=entity_guid
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return {"success": result.data}


@router.get("/terms/search")
async def search_terms(
    query: str = Query(..., description="Search query"),
    glossary_guid: Optional[str] = Query(None, description="Limit to glossary"),
    status: Optional[TermStatus] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100, description="Result limit"),
    glossary_service: GlossaryService = Depends(get_glossary_service)
):
    """Search for terms."""
    result = await glossary_service.search_terms(
        query=query,
        glossary_guid=glossary_guid,
        status=status,
        limit=limit
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/terms/{term_guid}/approve")
async def approve_term(
    term_guid: str = Path(..., description="Term GUID"),
    approver_notes: Optional[str] = Body(None, description="Approval notes"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Approve a draft term."""
    result = await glossary_service.approve_term(
        term_guid=term_guid,
        approver_notes=approver_notes
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/terms/{term_guid}/deprecate")
async def deprecate_term(
    term_guid: str = Path(..., description="Term GUID"),
    reason: str = Body(..., description="Deprecation reason"),
    replacement_guid: Optional[str] = Body(None, description="Replacement term GUID"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Deprecate a term."""
    result = await glossary_service.deprecate_term(
        term_guid=term_guid,
        reason=reason,
        replacement_guid=replacement_guid
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


# AI-Enhanced Endpoints

@router.post("/suggest-terms")
async def suggest_business_terms(
    technical_name: str = Body(..., description="Technical field/column name"),
    context: Optional[Dict[str, Any]] = Body(None, description="Additional context"),
    glossary_service: GlossaryService = Depends(get_glossary_service)
):
    """
    AI-powered suggestion of business terms for technical names.
    
    - **technical_name**: Technical column/field name
    - **context**: Additional context like schema, data type, table name
    """
    result = await glossary_service.suggest_business_terms(
        technical_name=technical_name,
        context=context
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/auto-map/{dataset_guid}")
async def create_automatic_mappings(
    dataset_guid: str = Path(..., description="Dataset GUID"),
    approval_required: bool = Query(True, description="Require approval for mappings"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Create automatic term mappings for a dataset.
    
    - **dataset_guid**: Dataset to create mappings for
    - **approval_required**: Whether mappings need manual approval
    """
    result = await glossary_service.create_automatic_mappings(
        dataset_guid=dataset_guid,
        approval_required=approval_required
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/terms/{term_guid}/usage")
async def analyze_term_usage(
    term_guid: str = Path(..., description="Term GUID"),
    time_range_days: int = Query(30, ge=1, le=365, description="Days of history"),
    glossary_service: GlossaryService = Depends(get_glossary_service)
):
    """Analyze term usage patterns."""
    result = await glossary_service.analyze_term_usage(
        term_guid=term_guid,
        time_range_days=time_range_days
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/recommend-terms")
async def recommend_new_terms(
    limit: int = Query(20, ge=1, le=100, description="Number of recommendations"),
    glossary_service: GlossaryService = Depends(get_glossary_service)
):
    """Recommend new terms based on usage patterns."""
    result = await glossary_service.recommend_new_terms(limit=limit)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/categories")
async def create_category(
    name: str = Body(..., description="Category name"),
    glossary_guid: str = Body(..., description="Parent glossary GUID"),
    short_description: str = Body(..., description="Short description"),
    long_description: Optional[str] = Body(None, description="Detailed description"),
    parent_category_guid: Optional[str] = Body(None, description="Parent category GUID"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Create a glossary category."""
    result = await glossary_service.create_category(
        name=name,
        glossary_guid=glossary_guid,
        short_description=short_description,
        long_description=long_description,
        parent_category_guid=parent_category_guid
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/import")
async def import_glossary(
    file_path: str = Body(..., description="Path to import file"),
    format: str = Body("csv", regex="^(csv|json|excel)$", description="File format"),
    glossary_guid: Optional[str] = Body(None, description="Target glossary"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Import glossary from file."""
    result = await glossary_service.import_glossary(
        file_path=file_path,
        format=format,
        glossary_guid=glossary_guid
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/export")
async def export_glossary(
    glossary_guid: str = Body(..., description="Glossary to export"),
    format: str = Body("json", regex="^(csv|json|excel)$", description="Export format"),
    include_relationships: bool = Body(True, description="Include relationships"),
    glossary_service: GlossaryService = Depends(get_glossary_service),
    current_user: dict = Depends(get_current_user)
):
    """Export glossary to file."""
    result = await glossary_service.export_glossary(
        glossary_guid=glossary_guid,
        format=format,
        include_relationships=include_relationships
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data 