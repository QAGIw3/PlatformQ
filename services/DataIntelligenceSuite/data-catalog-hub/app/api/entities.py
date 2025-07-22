"""
Entity management API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body, Path
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from platformq_events import Event
from ..core import AtlasClient, SchemaRegistry, SchemaType

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/entities", tags=["entities"])

# Dependency injection
atlas_client: Optional[AtlasClient] = None
schema_registry: Optional[SchemaRegistry] = None
event_stream = None

def set_dependencies(atlas: AtlasClient, schemas: SchemaRegistry, events):
    """Set API dependencies"""
    global atlas_client, schema_registry, event_stream
    atlas_client = atlas
    schema_registry = schemas
    event_stream = events


# Request/Response models
class EntityAttributes(BaseModel):
    name: str
    qualifiedName: str
    description: Optional[str] = None
    owner: Optional[str] = None
    location: Optional[str] = None
    format: Optional[str] = None
    additional: Optional[Dict[str, Any]] = None
    
    class Config:
        extra = "allow"


class CreateEntityRequest(BaseModel):
    typeName: str
    attributes: EntityAttributes
    classifications: Optional[List[str]] = None
    schema: Optional[Dict[str, Any]] = None


class UpdateEntityRequest(BaseModel):
    attributes: Dict[str, Any]
    classifications: Optional[List[str]] = None


class BulkCreateRequest(BaseModel):
    entities: List[CreateEntityRequest]


class EntityResponse(BaseModel):
    guid: str
    typeName: str
    attributes: Dict[str, Any]
    classifications: Optional[List[Dict[str, Any]]] = None
    status: str
    createTime: Optional[str] = None
    updateTime: Optional[str] = None


# API Endpoints
@router.post("", response_model=EntityResponse)
async def create_entity(request: CreateEntityRequest = Body(...)):
    """Create a new entity"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        # Build entity object
        entity = {
            "typeName": request.typeName,
            "attributes": request.attributes.dict(exclude_none=True)
        }
        
        # Merge additional attributes
        if request.attributes.additional:
            entity["attributes"].update(request.attributes.additional)
            
        # Create entity
        created = await atlas_client.create_entity(entity)
        
        # Add classifications if specified
        if request.classifications:
            for classification in request.classifications:
                await atlas_client.add_classification(
                    created['guid'],
                    classification
                )
                
        # Register schema if provided
        if request.schema and schema_registry:
            schema_type = SchemaType(request.schema.get('type', 'json_schema'))
            await schema_registry.register_schema(
                name=request.attributes.qualifiedName,
                schema_type=schema_type,
                schema_definition=request.schema.get('schema', {})
            )
            
        # Emit event
        if event_stream:
            await event_stream.emit(Event(
                type="EntityCreated",
                data={
                    "guid": created['guid'],
                    "typeName": created['typeName'],
                    "name": created['attributes'].get('name'),
                    "owner": created['attributes'].get('owner')
                }
            ))
            
        return EntityResponse(**created)
        
    except Exception as e:
        logger.error(f"Failed to create entity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{guid}", response_model=EntityResponse)
async def get_entity(guid: str = Path(...)):
    """Get entity by GUID"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        entity = await atlas_client.get_entity_by_guid(guid)
        if not entity:
            raise HTTPException(status_code=404, detail=f"Entity {guid} not found")
            
        return EntityResponse(**entity)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get entity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{guid}", response_model=EntityResponse)
async def update_entity(
    guid: str = Path(...),
    request: UpdateEntityRequest = Body(...)
):
    """Update entity attributes"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        # Update attributes
        updated = await atlas_client.update_entity(guid, request.attributes)
        
        # Update classifications if specified
        if request.classifications is not None:
            # Get current classifications
            entity = await atlas_client.get_entity_by_guid(guid)
            current_classifications = {
                c['typeName'] for c in entity.get('classifications', [])
            }
            requested_classifications = set(request.classifications)
            
            # Add new classifications
            for classification in requested_classifications - current_classifications:
                await atlas_client.add_classification(guid, classification)
                
            # Remove old classifications
            for classification in current_classifications - requested_classifications:
                await atlas_client.remove_classification(guid, classification)
                
        # Emit event
        if event_stream:
            await event_stream.emit(Event(
                type="EntityUpdated",
                data={
                    "guid": guid,
                    "attributes": request.attributes
                }
            ))
            
        return EntityResponse(**updated)
        
    except Exception as e:
        logger.error(f"Failed to update entity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{guid}")
async def delete_entity(guid: str = Path(...)):
    """Delete entity by GUID"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        success = await atlas_client.delete_entity(guid)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to delete entity")
            
        # Emit event
        if event_stream:
            await event_stream.emit(Event(
                type="EntityDeleted",
                data={"guid": guid}
            ))
            
        return {"message": f"Entity {guid} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete entity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/bulk", response_model=List[EntityResponse])
async def bulk_create_entities(request: BulkCreateRequest = Body(...)):
    """Create multiple entities in bulk"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        # Build entity objects
        entities = []
        for entity_request in request.entities:
            entity = {
                "typeName": entity_request.typeName,
                "attributes": entity_request.attributes.dict(exclude_none=True)
            }
            
            if entity_request.attributes.additional:
                entity["attributes"].update(entity_request.attributes.additional)
                
            entities.append(entity)
            
        # Bulk create
        guid_assignments = await atlas_client.bulk_create_entities(entities)
        
        # Get created entities
        created_entities = []
        for temp_guid, actual_guid in guid_assignments.items():
            entity = await atlas_client.get_entity_by_guid(actual_guid)
            if entity:
                created_entities.append(EntityResponse(**entity))
                
        # Emit event
        if event_stream:
            await event_stream.emit(Event(
                type="EntitiesBulkCreated",
                data={
                    "count": len(created_entities),
                    "guids": [e.guid for e in created_entities]
                }
            ))
            
        return created_entities
        
    except Exception as e:
        logger.error(f"Failed to bulk create entities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/uniqueAttribute/{typeName}")
async def get_entity_by_attribute(
    typeName: str = Path(...),
    attrName: str = Query(...),
    attrValue: str = Query(...)
):
    """Get entity by unique attribute"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        entity = await atlas_client.get_entity_by_attribute(
            typeName, attrName, attrValue
        )
        
        if not entity:
            raise HTTPException(
                status_code=404,
                detail=f"Entity not found with {attrName}={attrValue}"
            )
            
        return EntityResponse(**entity)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get entity by attribute: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{guid}/classifications")
async def add_classifications(
    guid: str = Path(...),
    classifications: List[str] = Body(...)
):
    """Add classifications to entity"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        added = []
        for classification in classifications:
            success = await atlas_client.add_classification(guid, classification)
            if success:
                added.append(classification)
                
        return {
            "guid": guid,
            "classificationsAdded": added
        }
        
    except Exception as e:
        logger.error(f"Failed to add classifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{guid}/classifications/{classification}")
async def remove_classification(
    guid: str = Path(...),
    classification: str = Path(...)
):
    """Remove classification from entity"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        success = await atlas_client.remove_classification(guid, classification)
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to remove classification {classification}"
            )
            
        return {"message": f"Classification {classification} removed successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to remove classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{guid}/tags")
async def add_tags(
    guid: str = Path(...),
    tags: List[str] = Body(...)
):
    """Add tags to entity"""
    if not atlas_client:
        raise HTTPException(status_code=503, detail="Atlas client not initialized")
        
    try:
        # Get current entity
        entity = await atlas_client.get_entity_by_guid(guid)
        if not entity:
            raise HTTPException(status_code=404, detail=f"Entity {guid} not found")
            
        # Add tags to attributes
        current_tags = entity['attributes'].get('tags', [])
        new_tags = list(set(current_tags + tags))
        
        # Update entity
        updated = await atlas_client.update_entity(guid, {"tags": new_tags})
        
        return {
            "guid": guid,
            "tags": new_tags
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add tags: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 