"""
Schema API Router

RESTful API endpoints for schema registry operations.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body

from app.api.v1.dependencies import get_schema_service, get_current_user
from app.services.catalog import SchemaService
from app.core.schema_registry import SchemaType, CompatibilityMode

router = APIRouter()


@router.post("")
async def register_schema(
    name: str = Body(..., description="Schema name"),
    schema_type: SchemaType = Body(..., description="Schema type"),
    schema: dict = Body(..., description="Schema definition"),
    compatibility: Optional[CompatibilityMode] = Body(None, description="Compatibility mode"),
    schema_service: SchemaService = Depends(get_schema_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Register a new schema or version.
    
    - **name**: Unique schema name
    - **schema_type**: Type of schema (AVRO, JSON_SCHEMA, etc.)
    - **schema**: Schema definition
    - **compatibility**: Compatibility mode for versioning
    """
    result = await schema_service.register(
        name=name,
        schema_type=schema_type,
        schema_definition=schema,
        compatibility=compatibility
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/{schema_id}")
async def get_schema(
    schema_id: str = Path(..., description="Schema ID"),
    version: Optional[int] = Query(None, description="Schema version"),
    schema_service: SchemaService = Depends(get_schema_service)
):
    """Get schema by ID and optional version."""
    result = await schema_service.get_schema(schema_id, version)
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return result.data


@router.get("")
async def list_schemas(
    schema_type: Optional[SchemaType] = Query(None, description="Filter by schema type"),
    limit: int = Query(100, ge=1, le=1000, description="Result limit"),
    offset: int = Query(0, ge=0, description="Result offset"),
    schema_service: SchemaService = Depends(get_schema_service)
):
    """List schemas with optional filtering."""
    result = await schema_service.list_schemas(
        schema_type=schema_type,
        limit=limit,
        offset=offset
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    schemas, total = result.data
    return {
        "schemas": schemas,
        "total": total,
        "limit": limit,
        "offset": offset
    }


@router.get("/{schema_id}/versions")
async def get_schema_versions(
    schema_id: str = Path(..., description="Schema ID"),
    schema_service: SchemaService = Depends(get_schema_service)
):
    """Get all versions of a schema."""
    result = await schema_service.get_versions(schema_id)
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return result.data


@router.post("/{schema_id}/validate")
async def validate_compatibility(
    schema_id: str = Path(..., description="Schema ID"),
    schema: dict = Body(..., description="New schema to validate"),
    compatibility_mode: Optional[CompatibilityMode] = Body(None, description="Compatibility mode"),
    schema_service: SchemaService = Depends(get_schema_service)
):
    """Validate if new schema is compatible with existing schema."""
    result = await schema_service.validate_compatibility(
        schema_id=schema_id,
        new_schema=schema,
        compatibility_mode=compatibility_mode
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return {"compatible": result.data}


@router.post("/infer")
async def infer_schema(
    data_sample: List[dict] = Body(..., description="Sample data"),
    schema_type: SchemaType = Body(SchemaType.JSON_SCHEMA, description="Schema type to infer"),
    schema_service: SchemaService = Depends(get_schema_service),
    current_user: dict = Depends(get_current_user)
):
    """Infer schema from data sample."""
    result = await schema_service.infer_schema(
        data_sample=data_sample,
        schema_type=schema_type
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.delete("/{schema_id}")
async def delete_schema(
    schema_id: str = Path(..., description="Schema ID"),
    version: Optional[int] = Query(None, description="Specific version to delete"),
    schema_service: SchemaService = Depends(get_schema_service),
    current_user: dict = Depends(get_current_user)
):
    """Delete schema or specific version."""
    result = await schema_service.delete_schema(schema_id, version)
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return {"success": result.data}


@router.post("/entities/{entity_guid}/schema")
async def register_entity_schema(
    entity_guid: str = Path(..., description="Entity GUID"),
    schema: dict = Body(..., description="Schema definition"),
    schema_service: SchemaService = Depends(get_schema_service),
    current_user: dict = Depends(get_current_user)
):
    """Register schema for a specific entity."""
    result = await schema_service.register_for_entity(
        entity_guid=entity_guid,
        schema_definition=schema
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data 