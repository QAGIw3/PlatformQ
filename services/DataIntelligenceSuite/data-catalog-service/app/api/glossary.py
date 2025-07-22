"""
Business Glossary API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from app.core import GlossaryManager, AtlasClient, TermStatus
from app.core.business_glossary_enhanced import BusinessGlossaryEnhanced, TermMapping
from platformq_shared.logging import get_logger
from platformq_events import EventStream

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/glossary", tags=["glossary"])

# Global dependencies
glossary_manager: Optional[GlossaryManager] = None
atlas_client: Optional[AtlasClient] = None
business_glossary_enhanced: Optional[BusinessGlossaryEnhanced] = None
event_stream: Optional[EventStream] = None


def set_glossary_deps(**deps):
    """Set the global dependencies for this router"""
    global glossary_manager, atlas_client, business_glossary_enhanced, event_stream
    glossary_manager = deps.get("glossary_manager")
    atlas_client = deps.get("atlas_client")
    business_glossary_enhanced = deps.get("business_glossary_enhanced")
    event_stream = deps.get("event_stream")


# Request/Response Models
class GlossaryCreate(BaseModel):
    """Create glossary request"""
    name: str = Field(..., description="Glossary name")
    short_description: str = Field(..., description="Short description")
    long_description: Optional[str] = None
    language: str = Field("en", description="Language code")
    usage: Optional[str] = None


class GlossaryCategoryCreate(BaseModel):
    """Create glossary category request"""
    name: str = Field(..., description="Category name")
    glossary_guid: str = Field(..., description="Parent glossary GUID")
    short_description: str
    long_description: Optional[str] = None
    parent_category_guid: Optional[str] = None


class GlossaryTermCreate(BaseModel):
    """Create glossary term request"""
    name: str = Field(..., description="Term name")
    glossary_guid: str = Field(..., description="Parent glossary GUID")
    short_description: str
    long_description: Optional[str] = None
    abbreviation: Optional[str] = None
    examples: Optional[List[str]] = None
    usage: Optional[str] = None
    categories: Optional[List[str]] = Field(None, description="Category GUIDs")
    related_terms: Optional[List[str]] = Field(None, description="Related term GUIDs")
    synonyms: Optional[List[str]] = None
    antonyms: Optional[List[str]] = None
    see_also: Optional[List[str]] = None
    replaces: Optional[List[str]] = None
    valid_values: Optional[List[str]] = None
    preferred_terms: Optional[List[str]] = None
    is_a: Optional[List[str]] = None
    classified_by: Optional[List[str]] = None


class TermEntityAssignment(BaseModel):
    """Assign term to entities request"""
    term_guid: str
    entity_guids: List[str]
    semantic_assignment: bool = Field(False, description="Use semantic assignment")


class GlossaryInfo(BaseModel):
    """Glossary information"""
    guid: str
    name: str
    short_description: str
    long_description: Optional[str]
    language: str
    term_count: int
    category_count: int
    created_by: str
    created_date: datetime
    updated_by: Optional[str]
    updated_date: Optional[datetime]


class CategoryInfo(BaseModel):
    """Category information"""
    guid: str
    name: str
    qualified_name: str
    short_description: str
    long_description: Optional[str]
    parent_category_guid: Optional[str]
    child_category_count: int
    term_count: int


class TermInfo(BaseModel):
    """Term information"""
    guid: str
    name: str
    qualified_name: str
    short_description: str
    long_description: Optional[str]
    status: TermStatus
    abbreviation: Optional[str]
    examples: Optional[List[str]]
    usage: Optional[str]
    categories: List[str]
    assigned_entities: int
    created_by: str
    created_date: datetime


# API Endpoints
@router.get("/", response_model=List[GlossaryInfo])
async def list_glossaries():
    """List all glossaries"""
    try:
        glossaries = await glossary_manager.list_glossaries()
        
        return [
            GlossaryInfo(
                guid=g["guid"],
                name=g["name"],
                short_description=g["shortDescription"],
                long_description=g.get("longDescription"),
                language=g.get("language", "en"),
                term_count=g.get("termCount", 0),
                category_count=g.get("categoryCount", 0),
                created_by=g["createdBy"],
                created_date=datetime.fromtimestamp(g["createTime"] / 1000),
                updated_by=g.get("updatedBy"),
                updated_date=datetime.fromtimestamp(g["updateTime"] / 1000) if g.get("updateTime") else None
            )
            for g in glossaries
        ]
        
    except Exception as e:
        logger.error(f"Failed to list glossaries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=GlossaryInfo)
async def create_glossary(glossary: GlossaryCreate):
    """Create a new glossary"""
    try:
        result = await glossary_manager.create_glossary(
            name=glossary.name,
            short_description=glossary.short_description,
            long_description=glossary.long_description,
            language=glossary.language,
            usage=glossary.usage
        )
        
        return GlossaryInfo(
            guid=result["guid"],
            name=result["name"],
            short_description=result["shortDescription"],
            long_description=result.get("longDescription"),
            language=result.get("language", "en"),
            term_count=0,
            category_count=0,
            created_by=result["createdBy"],
            created_date=datetime.fromtimestamp(result["createTime"] / 1000),
            updated_by=None,
            updated_date=None
        )
        
    except Exception as e:
        logger.error(f"Failed to create glossary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{glossary_guid}/terms", response_model=List[TermInfo])
async def list_terms(
    glossary_guid: str = Path(..., description="Glossary GUID"),
    status: Optional[TermStatus] = Query(None, description="Filter by status"),
    category_guid: Optional[str] = Query(None, description="Filter by category"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List terms in a glossary"""
    try:
        terms = await glossary_manager.list_terms(
            glossary_guid=glossary_guid,
            status=status,
            category_guid=category_guid,
            limit=limit,
            offset=offset
        )
        
        return [
            TermInfo(
                guid=t["guid"],
                name=t["name"],
                qualified_name=t["qualifiedName"],
                short_description=t["shortDescription"],
                long_description=t.get("longDescription"),
                status=TermStatus(t.get("status", "DRAFT")),
                abbreviation=t.get("abbreviation"),
                examples=t.get("examples"),
                usage=t.get("usage"),
                categories=[c["guid"] for c in t.get("categories", [])],
                assigned_entities=t.get("assignedEntityCount", 0),
                created_by=t["createdBy"],
                created_date=datetime.fromtimestamp(t["createTime"] / 1000)
            )
            for t in terms
        ]
        
    except Exception as e:
        logger.error(f"Failed to list terms: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/terms", response_model=TermInfo)
async def create_term(term: GlossaryTermCreate):
    """Create a new glossary term"""
    try:
        result = await glossary_manager.create_term(
            name=term.name,
            glossary_guid=term.glossary_guid,
            short_description=term.short_description,
            long_description=term.long_description,
            abbreviation=term.abbreviation,
            examples=term.examples,
            usage=term.usage,
            categories=term.categories,
            related_terms=term.related_terms,
            synonyms=term.synonyms,
            antonyms=term.antonyms,
            see_also=term.see_also,
            replaces=term.replaces,
            valid_values=term.valid_values,
            preferred_terms=term.preferred_terms,
            is_a=term.is_a,
            classified_by=term.classified_by
        )
        
        return TermInfo(
            guid=result["guid"],
            name=result["name"],
            qualified_name=result["qualifiedName"],
            short_description=result["shortDescription"],
            long_description=result.get("longDescription"),
            status=TermStatus.DRAFT,
            abbreviation=result.get("abbreviation"),
            examples=result.get("examples"),
            usage=result.get("usage"),
            categories=[],
            assigned_entities=0,
            created_by=result["createdBy"],
            created_date=datetime.fromtimestamp(result["createTime"] / 1000)
        )
        
    except Exception as e:
        logger.error(f"Failed to create term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/terms/{term_guid}", response_model=TermInfo)
async def update_term(
    term_guid: str = Path(..., description="Term GUID"),
    updates: Dict[str, Any] = Body(..., description="Term updates")
):
    """Update a glossary term"""
    try:
        result = await glossary_manager.update_term(term_guid, updates)
        
        return TermInfo(
            guid=result["guid"],
            name=result["name"],
            qualified_name=result["qualifiedName"],
            short_description=result["shortDescription"],
            long_description=result.get("longDescription"),
            status=TermStatus(result.get("status", "DRAFT")),
            abbreviation=result.get("abbreviation"),
            examples=result.get("examples"),
            usage=result.get("usage"),
            categories=[c["guid"] for c in result.get("categories", [])],
            assigned_entities=result.get("assignedEntityCount", 0),
            created_by=result["createdBy"],
            created_date=datetime.fromtimestamp(result["createTime"] / 1000)
        )
        
    except Exception as e:
        logger.error(f"Failed to update term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/terms/{term_guid}")
async def delete_term(
    term_guid: str = Path(..., description="Term GUID"),
    force: bool = Query(False, description="Force delete even if assigned")
):
    """Delete a glossary term"""
    try:
        await glossary_manager.delete_term(term_guid, force=force)
        
        return {"status": "success", "message": f"Term {term_guid} deleted"}
        
    except Exception as e:
        logger.error(f"Failed to delete term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/terms/{term_guid}/assign", response_model=Dict[str, Any])
async def assign_term_to_entities(
    term_guid: str = Path(..., description="Term GUID"),
    assignment: TermEntityAssignment = Body(...)
):
    """
    Assign a glossary term to entities
    
    Terms can be assigned:
    - Directly to specific attributes
    - Semantically based on meaning
    """
    try:
        results = await glossary_manager.assign_term_to_entities(
            term_guid=assignment.term_guid,
            entity_guids=assignment.entity_guids,
            semantic_assignment=assignment.semantic_assignment
        )
        
        return {
            "term_guid": assignment.term_guid,
            "entities_assigned": len(results["assigned"]),
            "entities_failed": len(results["failed"]),
            "details": results
        }
        
    except Exception as e:
        logger.error(f"Failed to assign term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/terms/{term_guid}/assign/{entity_guid}")
async def remove_term_from_entity(
    term_guid: str = Path(..., description="Term GUID"),
    entity_guid: str = Path(..., description="Entity GUID")
):
    """Remove a glossary term from an entity"""
    try:
        await glossary_manager.remove_term_from_entity(term_guid, entity_guid)
        
        return {
            "status": "success",
            "message": f"Term {term_guid} removed from entity {entity_guid}"
        }
        
    except Exception as e:
        logger.error(f"Failed to remove term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/categories", response_model=CategoryInfo)
async def create_category(category: GlossaryCategoryCreate):
    """Create a glossary category"""
    try:
        result = await glossary_manager.create_category(
            name=category.name,
            glossary_guid=category.glossary_guid,
            short_description=category.short_description,
            long_description=category.long_description,
            parent_category_guid=category.parent_category_guid
        )
        
        return CategoryInfo(
            guid=result["guid"],
            name=result["name"],
            qualified_name=result["qualifiedName"],
            short_description=result["shortDescription"],
            long_description=result.get("longDescription"),
            parent_category_guid=result.get("parentCategoryGuid"),
            child_category_count=0,
            term_count=0
        )
        
    except Exception as e:
        logger.error(f"Failed to create category: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/terms/{term_guid}/approve")
async def approve_term(
    term_guid: str = Path(..., description="Term GUID"),
    approver_notes: Optional[str] = Body(None, embed=True)
):
    """Approve a glossary term"""
    try:
        await glossary_manager.approve_term(term_guid, approver_notes)
        
        return {
            "status": "success",
            "message": f"Term {term_guid} approved",
            "new_status": TermStatus.APPROVED.value
        }
        
    except Exception as e:
        logger.error(f"Failed to approve term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/terms/{term_guid}/deprecate")
async def deprecate_term(
    term_guid: str = Path(..., description="Term GUID"),
    reason: str = Body(..., embed=True),
    replacement_guid: Optional[str] = Body(None, embed=True)
):
    """Deprecate a glossary term"""
    try:
        await glossary_manager.deprecate_term(
            term_guid=term_guid,
            reason=reason,
            replacement_guid=replacement_guid
        )
        
        return {
            "status": "success",
            "message": f"Term {term_guid} deprecated",
            "new_status": TermStatus.DEPRECATED.value
        }
        
    except Exception as e:
        logger.error(f"Failed to deprecate term: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/terms/search")
async def search_terms(
    query: str = Query(..., description="Search query"),
    glossary_guid: Optional[str] = Query(None, description="Limit to glossary"),
    status: Optional[TermStatus] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search glossary terms"""
    try:
        results = await glossary_manager.search_terms(
            query=query,
            glossary_guid=glossary_guid,
            status=status,
            limit=limit
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to search terms: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import")
async def import_glossary(
    file_path: str = Body(..., embed=True, description="Path to import file"),
    format: str = Body("csv", embed=True, pattern="^(csv|json|excel)$"),
    glossary_guid: Optional[str] = Body(None, embed=True, description="Target glossary")
):
    """Import glossary terms from file"""
    try:
        result = await glossary_manager.import_glossary(
            file_path=file_path,
            format=format,
            glossary_guid=glossary_guid
        )
        
        return {
            "status": "success",
            "imported": result["imported"],
            "failed": result["failed"],
            "details": result["details"]
        }
        
    except Exception as e:
        logger.error(f"Failed to import glossary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export")
async def export_glossary(
    glossary_guid: str = Body(..., embed=True),
    format: str = Body("json", embed=True, pattern="^(csv|json|excel)$"),
    include_relationships: bool = Body(True, embed=True)
):
    """Export glossary to file"""
    try:
        result = await glossary_manager.export_glossary(
            glossary_guid=glossary_guid,
            format=format,
            include_relationships=include_relationships
        )
        
        return {
            "status": "success",
            "file_path": result["file_path"],
            "term_count": result["term_count"],
            "category_count": result["category_count"]
        }
        
    except Exception as e:
        logger.error(f"Failed to export glossary: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 


# Enhanced Business Glossary Endpoints

@router.post("/enhanced/suggest-terms")
async def suggest_business_terms(
    technical_name: str = Body(..., embed=True),
    context: Optional[Dict[str, Any]] = Body(None, embed=True)
):
    """
    Suggest business terms for a technical name using AI
    
    - **technical_name**: Technical name (column, table, etc.) to analyze
    - **context**: Optional context like schema, database, data type
    """
    if not business_glossary_enhanced:
        raise HTTPException(status_code=503, detail="Enhanced glossary not initialized")
    
    try:
        suggestions = await business_glossary_enhanced.suggest_business_terms(
            technical_name=technical_name,
            context=context
        )
        
        return {
            "technical_name": technical_name,
            "suggestions": suggestions,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to suggest terms: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enhanced/auto-map/{dataset_guid}")
async def create_automatic_mappings(
    dataset_guid: str,
    approval_required: bool = Query(True)
):
    """
    Create automatic business term mappings for a dataset
    
    - **dataset_guid**: GUID of the dataset to map
    - **approval_required**: Whether mappings require approval
    """
    if not business_glossary_enhanced:
        raise HTTPException(status_code=503, detail="Enhanced glossary not initialized")
    
    try:
        mappings = await business_glossary_enhanced.create_automatic_mappings(
            dataset_guid=dataset_guid,
            approval_required=approval_required
        )
        
        # Emit event
        if event_stream:
            await event_stream.publish(
                topic="catalog-glossary",
                event_type="auto_mappings_created",
                data={
                    "dataset_guid": dataset_guid,
                    "mappings_count": len(mappings),
                    "approval_required": approval_required
                }
            )
        
        return {
            "dataset_guid": dataset_guid,
            "mappings_created": len(mappings),
            "mappings": [
                {
                    "term_id": m.term_id,
                    "asset_id": m.asset_id,
                    "confidence": m.confidence,
                    "mapping_type": m.mapping_type,
                    "approved": m.approved
                }
                for m in mappings
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to create mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enhanced/term-usage/{term_guid}")
async def analyze_term_usage(
    term_guid: str,
    time_range_days: int = Query(30, ge=1, le=365)
):
    """
    Analyze how a business term is being used
    
    - **term_guid**: GUID of the business term
    - **time_range_days**: Days of history to analyze
    """
    if not business_glossary_enhanced:
        raise HTTPException(status_code=503, detail="Enhanced glossary not initialized")
    
    try:
        usage_stats = await business_glossary_enhanced.analyze_term_usage(
            term_guid=term_guid,
            time_range_days=time_range_days
        )
        
        return usage_stats
        
    except Exception as e:
        logger.error(f"Failed to analyze term usage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/enhanced/recommend-terms")
async def recommend_new_terms(
    limit: int = Query(20, ge=1, le=100)
):
    """
    Recommend new business terms based on unmapped technical assets
    
    - **limit**: Maximum number of recommendations
    """
    if not business_glossary_enhanced:
        raise HTTPException(status_code=503, detail="Enhanced glossary not initialized")
    
    try:
        recommendations = await business_glossary_enhanced.recommend_new_terms(limit=limit)
        
        return {
            "recommendations": recommendations,
            "total": len(recommendations),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enhanced/sync-external")
async def sync_with_external_glossary(
    external_glossary: Dict[str, Any] = Body(...)
):
    """
    Sync business terms with external business systems
    
    - **external_glossary**: External glossary data with terms
    """
    if not business_glossary_enhanced:
        raise HTTPException(status_code=503, detail="Enhanced glossary not initialized")
    
    try:
        sync_results = await business_glossary_enhanced.sync_with_business_systems(
            external_glossary=external_glossary
        )
        
        # Emit event
        if event_stream:
            await event_stream.publish(
                topic="catalog-glossary",
                event_type="external_sync_completed",
                data=sync_results
            )
        
        return sync_results
        
    except Exception as e:
        logger.error(f"Failed to sync glossary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/enhanced/validate-mappings")
async def validate_term_mappings(
    term_guid: Optional[str] = Body(None),
    dataset_guid: Optional[str] = Body(None)
):
    """
    Validate existing term mappings for accuracy
    
    - **term_guid**: Optional specific term to validate
    - **dataset_guid**: Optional specific dataset to validate
    """
    if not business_glossary_enhanced:
        raise HTTPException(status_code=503, detail="Enhanced glossary not initialized")
    
    try:
        # This would validate mappings and return results
        validation_results = {
            "total_mappings_checked": 0,
            "valid_mappings": 0,
            "invalid_mappings": 0,
            "suggestions": [],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Implementation would go here
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Failed to validate mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 