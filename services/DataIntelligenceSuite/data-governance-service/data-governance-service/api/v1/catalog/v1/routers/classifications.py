"""
Classifications API Router

RESTful API endpoints for classification operations.
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body

from app.api.v1.dependencies import get_classification_service, get_current_user
from app.services.catalog import ClassificationService

router = APIRouter()


@router.post("")
async def create_classification(
    name: str = Body(..., description="Classification name"),
    display_name: Optional[str] = Body(None, description="Display name"),
    description: Optional[str] = Body(None, description="Description"),
    parent: Optional[str] = Body(None, description="Parent classification name"),
    entity_types: Optional[List[str]] = Body(None, description="Applicable entity types"),
    attribute_defs: Optional[List[Dict[str, Any]]] = Body(None, description="Attribute definitions"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new classification definition.
    
    - **name**: Unique classification name
    - **display_name**: Human-readable display name
    - **description**: Classification description
    - **parent**: Parent classification for hierarchical structures
    - **entity_types**: List of entity types this classification applies to
    - **attribute_defs**: Custom attributes for this classification
    """
    result = await classification_service.create_classification(
        name=name,
        display_name=display_name,
        description=description,
        parent=parent,
        entity_types=entity_types,
        attribute_defs=attribute_defs
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("")
async def list_classifications(
    include_sub: bool = Query(True, description="Include sub-classifications"),
    entity_type: Optional[str] = Query(None, description="Filter by applicable entity type"),
    classification_service: ClassificationService = Depends(get_classification_service)
):
    """
    List available classifications.
    
    - **include_sub**: Whether to include sub-classifications
    - **entity_type**: Filter by entity type applicability
    """
    result = await classification_service.list_classifications(
        include_sub=include_sub,
        entity_type=entity_type
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/entities/{entity_guid}/assign")
async def assign_classification(
    entity_guid: str = Path(..., description="Entity GUID"),
    classification_name: str = Body(..., description="Classification name"),
    attributes: Optional[Dict[str, Any]] = Body(None, description="Classification attributes"),
    propagate: bool = Body(True, description="Propagate to related entities"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Assign classification to an entity.
    
    - **entity_guid**: Entity to classify
    - **classification_name**: Classification to apply
    - **attributes**: Classification-specific attributes
    - **propagate**: Whether to propagate to related entities
    """
    result = await classification_service.assign_classification(
        entity_guid=entity_guid,
        classification_name=classification_name,
        attributes=attributes,
        propagate=propagate
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.delete("/entities/{entity_guid}/classifications/{classification_name}")
async def remove_classification(
    entity_guid: str = Path(..., description="Entity GUID"),
    classification_name: str = Path(..., description="Classification name"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """Remove classification from entity."""
    result = await classification_service.remove_classification(
        entity_guid=entity_guid,
        classification_name=classification_name
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return {"success": result.data}


@router.post("/classify/auto")
async def auto_classify(
    entity_guid: Optional[str] = Body(None, description="Specific entity to classify"),
    entity_type: Optional[str] = Body(None, description="Type of entities to classify"),
    sample_size: int = Body(1000, ge=100, le=10000, description="Sample size"),
    classifiers: Optional[List[str]] = Body(None, description="Classifiers to run"),
    confidence_threshold: float = Body(0.8, ge=0.0, le=1.0, description="Minimum confidence"),
    dry_run: bool = Body(False, description="Preview without applying"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Automatically classify entities based on content.
    
    - **entity_guid**: Specific entity to classify (if not provided, uses entity_type)
    - **entity_type**: Type of entities to classify in bulk
    - **sample_size**: Number of entities to process
    - **classifiers**: Specific classifiers to run (default: pii, financial, healthcare)
    - **confidence_threshold**: Minimum confidence score to apply classification
    - **dry_run**: Preview results without applying classifications
    """
    result = await classification_service.auto_classify(
        entity_guid=entity_guid,
        entity_type=entity_type,
        sample_size=sample_size,
        classifiers=classifiers,
        confidence_threshold=confidence_threshold,
        dry_run=dry_run
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/rules")
async def create_classification_rule(
    name: str = Body(..., description="Rule name"),
    description: Optional[str] = Body(None, description="Rule description"),
    rule_type: str = Body(..., description="Rule type", regex="^(regex|contains|datatype|custom)$"),
    pattern: Optional[str] = Body(None, description="Pattern for matching"),
    classification: str = Body(..., description="Classification to apply"),
    confidence: float = Body(0.9, ge=0.0, le=1.0, description="Confidence score"),
    entity_types: Optional[List[str]] = Body(None, description="Applicable entity types"),
    enabled: bool = Body(True, description="Whether rule is enabled"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Create a classification rule.
    
    - **name**: Unique rule name
    - **description**: Rule description
    - **rule_type**: Type of rule (regex, contains, datatype, custom)
    - **pattern**: Pattern for matching (required for regex/contains)
    - **classification**: Classification to apply when rule matches
    - **confidence**: Confidence score for matches
    - **entity_types**: Limit rule to specific entity types
    - **enabled**: Whether rule is active
    """
    result = await classification_service.create_classification_rule(
        name=name,
        description=description,
        rule_type=rule_type,
        pattern=pattern,
        classification=classification,
        confidence=confidence,
        entity_types=entity_types,
        enabled=enabled
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/scan")
async def scan_for_classifications(
    entity_type: Optional[str] = Query(None, description="Limit scan to entity type"),
    limit: int = Query(100, ge=1, le=1000, description="Max entities to scan"),
    async_mode: bool = Query(True, description="Run scan asynchronously"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Scan entities for classifications.
    
    - **entity_type**: Limit scan to specific entity type
    - **limit**: Maximum number of entities to scan
    - **async_mode**: Run scan in background (returns scan ID)
    """
    result = await classification_service.scan_for_classifications(
        entity_type=entity_type,
        limit=limit,
        async_mode=async_mode
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/scan/{scan_id}")
async def get_scan_status(
    scan_id: str = Path(..., description="Scan ID"),
    classification_service: ClassificationService = Depends(get_classification_service)
):
    """Get classification scan status."""
    result = await classification_service.get_scan_status(scan_id)
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return result.data


@router.get("/stats")
async def get_classification_stats(
    classification_service: ClassificationService = Depends(get_classification_service)
):
    """Get classification statistics."""
    result = await classification_service.get_classification_stats()
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/bulk/assign")
async def bulk_assign_classifications(
    assignments: List[Dict[str, Any]] = Body(..., description="List of assignments"),
    classification_service: ClassificationService = Depends(get_classification_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Bulk assign classifications to entities.
    
    Each assignment should contain:
    - **entity_guid**: Entity GUID
    - **classification_name**: Classification to apply
    - **attributes**: Optional classification attributes
    """
    result = await classification_service.bulk_assign_classifications(assignments)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data 