"""
Entity API Router

Thin router for entity operations.
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path

from app.api.v1.dependencies import get_entity_service, get_current_user
from app.api.v1.models.requests.entity_requests import (
    CreateEntityRequest,
    UpdateEntityRequest,
    BulkCreateEntitiesRequest
)
from app.api.v1.models.responses.entity_responses import (
    EntityResponse,
    EntityListResponse,
    BulkOperationResponse
)
from app.services.catalog import EntityService

router = APIRouter(prefix="/entities", tags=["entities"])


@router.post("", response_model=EntityResponse)
async def create_entity(
    request: CreateEntityRequest,
    entity_service: EntityService = Depends(get_entity_service),
    current_user: dict = Depends(get_current_user)
):
    """Create a new entity"""
    # Add user context to request
    request_dict = request.dict()
    request_dict['owner'] = request_dict.get('owner') or current_user['id']
    
    result = await entity_service.create(request_dict)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return EntityResponse.from_domain(result.data)


@router.get("/{guid}", response_model=EntityResponse)
async def get_entity(
    guid: str = Path(..., description="Entity GUID"),
    entity_service: EntityService = Depends(get_entity_service)
):
    """Get an entity by GUID"""
    result = await entity_service.get_by_id(guid)
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return EntityResponse.from_domain(result.data)


@router.put("/{guid}", response_model=EntityResponse)
async def update_entity(
    guid: str = Path(..., description="Entity GUID"),
    request: UpdateEntityRequest,
    entity_service: EntityService = Depends(get_entity_service)
):
    """Update an entity"""
    result = await entity_service.update(guid, request.dict(exclude_unset=True))
    
    if not result.success:
        status_code = 404 if result.error == "Entity not found" else 400
        raise HTTPException(status_code=status_code, detail=result.error)
        
    return EntityResponse.from_domain(result.data)


@router.delete("/{guid}")
async def delete_entity(
    guid: str = Path(..., description="Entity GUID"),
    hard_delete: bool = Query(False, description="Perform hard delete"),
    entity_service: EntityService = Depends(get_entity_service)
):
    """Delete an entity"""
    result = await entity_service.delete(guid, hard_delete=hard_delete)
    
    if not result.success:
        status_code = 404 if result.error == "Entity not found" else 400
        raise HTTPException(status_code=status_code, detail=result.error)
        
    return {"success": True, "message": "Entity deleted"}


@router.get("", response_model=EntityListResponse)
async def list_entities(
    type_name: Optional[str] = Query(None, description="Filter by type"),
    owner: Optional[str] = Query(None, description="Filter by owner"),
    classification: Optional[str] = Query(None, description="Filter by classification"),
    limit: int = Query(100, ge=1, le=1000, description="Result limit"),
    offset: int = Query(0, ge=0, description="Result offset"),
    entity_service: EntityService = Depends(get_entity_service)
):
    """List entities with filters"""
    result = await entity_service.list_entities(
        type_name=type_name,
        owner=owner,
        classification=classification,
        limit=limit,
        offset=offset
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    entities, total = result.data
    
    return EntityListResponse(
        entities=[EntityResponse.from_domain(e) for e in entities],
        total=total,
        limit=limit,
        offset=offset
    )


@router.get("/search", response_model=EntityListResponse)
async def search_entities(
    query: str = Query(..., description="Search query"),
    type_name: Optional[str] = Query(None, description="Filter by type"),
    limit: int = Query(20, ge=1, le=100, description="Result limit"),
    offset: int = Query(0, ge=0, description="Result offset"),
    entity_service: EntityService = Depends(get_entity_service)
):
    """Search for entities"""
    result = await entity_service.search(
        query=query,
        type_name=type_name,
        limit=limit,
        offset=offset
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    entities, total = result.data
    
    return EntityListResponse(
        entities=[EntityResponse.from_domain(e) for e in entities],
        total=total,
        limit=limit,
        offset=offset
    )


@router.post("/bulk", response_model=BulkOperationResponse)
async def bulk_create_entities(
    request: BulkCreateEntitiesRequest,
    entity_service: EntityService = Depends(get_entity_service),
    current_user: dict = Depends(get_current_user)
):
    """Create multiple entities"""
    # Add owner to each entity
    requests = []
    for entity_request in request.entities:
        entity_dict = entity_request.dict()
        entity_dict['owner'] = entity_dict.get('owner') or current_user['id']
        requests.append(entity_dict)
    
    result = await entity_service.bulk_create(requests)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    # Build response
    results = []
    for i, entity in enumerate(result.data):
        results.append({
            "index": i,
            "success": True,
            "guid": entity.guid
        })
    
    return BulkOperationResponse(
        success_count=len(result.data),
        failure_count=0,
        results=results
    ) 