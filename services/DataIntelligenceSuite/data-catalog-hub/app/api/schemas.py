"""
Schema Registry API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body, Path
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from ..core import SchemaRegistry, SchemaType, CompatibilityMode, CacheManager

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/schemas", tags=["schemas"])

# Dependency injection
schema_registry: Optional[SchemaRegistry] = None
cache_manager: Optional[CacheManager] = None

def set_dependencies(registry: SchemaRegistry, cache: CacheManager):
    """Set API dependencies"""
    global schema_registry, cache_manager
    schema_registry = registry
    cache_manager = cache


# Request/Response models
class RegisterSchemaRequest(BaseModel):
    name: str
    type: SchemaType
    schema: Dict[str, Any]
    compatibility: Optional[CompatibilityMode] = None


class ValidateSchemaRequest(BaseModel):
    schema: Dict[str, Any]
    compatibility: Optional[CompatibilityMode] = None


class InferSchemaRequest(BaseModel):
    sample_data: List[Dict[str, Any]]
    schema_type: SchemaType = SchemaType.JSON_SCHEMA


class SchemaResponse(BaseModel):
    id: str
    name: str
    version: int
    type: str
    schema: Dict[str, Any]
    compatibility: str
    checksum: str
    created_time: str


# API Endpoints
@router.post("", response_model=SchemaResponse)
async def register_schema(request: RegisterSchemaRequest = Body(...)):
    """Register a new schema"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        schema = await schema_registry.register_schema(
            name=request.name,
            schema_type=request.type,
            schema_definition=request.schema,
            compatibility=request.compatibility
        )
        
        return SchemaResponse(
            id=schema['attributes']['qualifiedName'],
            name=request.name,
            version=schema['attributes']['version'],
            type=schema['attributes']['schemaType'],
            schema=request.schema,
            compatibility=schema['attributes']['compatibility'],
            checksum=schema['attributes']['checksum'],
            created_time=schema['attributes']['createdTime']
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to register schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{schema_id}", response_model=SchemaResponse)
async def get_schema(
    schema_id: str = Path(...),
    version: Optional[int] = Query(None)
):
    """Get schema by ID and optional version"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        schema = await schema_registry.get_schema(schema_id, version)
        
        if not schema:
            raise HTTPException(
                status_code=404,
                detail=f"Schema {schema_id} not found"
            )
            
        attrs = schema['attributes']
        schema_def = json.loads(attrs['schemaDefinition'])
        
        return SchemaResponse(
            id=attrs['qualifiedName'],
            name=schema_id,
            version=attrs['version'],
            type=attrs['schemaType'],
            schema=schema_def,
            compatibility=attrs['compatibility'],
            checksum=attrs['checksum'],
            created_time=attrs.get('createdTime', '')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{schema_id}/versions", response_model=List[SchemaResponse])
async def get_schema_versions(schema_id: str = Path(...)):
    """Get all versions of a schema"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        versions = await schema_registry.get_schema_versions(schema_id)
        
        responses = []
        for version in versions:
            attrs = version['attributes']
            schema_def = json.loads(attrs['schemaDefinition'])
            
            responses.append(SchemaResponse(
                id=attrs['qualifiedName'],
                name=schema_id,
                version=attrs['version'],
                type=attrs['schemaType'],
                schema=schema_def,
                compatibility=attrs['compatibility'],
                checksum=attrs['checksum'],
                created_time=attrs.get('createdTime', '')
            ))
            
        return responses
        
    except Exception as e:
        logger.error(f"Failed to get schema versions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate")
async def validate_schema_compatibility(
    schema_id: str = Query(...),
    request: ValidateSchemaRequest = Body(...)
):
    """Validate schema compatibility"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        is_compatible = await schema_registry.validate_compatibility(
            schema_id=schema_id,
            new_schema=request.schema,
            compatibility_mode=request.compatibility
        )
        
        return {
            "compatible": is_compatible,
            "schema_id": schema_id,
            "compatibility_mode": request.compatibility.value if request.compatibility else "DEFAULT"
        }
        
    except Exception as e:
        logger.error(f"Failed to validate schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/infer", response_model=Dict[str, Any])
async def infer_schema(request: InferSchemaRequest = Body(...)):
    """Infer schema from data sample"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        if not request.sample_data:
            raise HTTPException(
                status_code=400,
                detail="Sample data is required"
            )
            
        inferred_schema = await schema_registry.infer_schema(
            data_sample=request.sample_data,
            schema_type=request.schema_type
        )
        
        return {
            "schema_type": request.schema_type.value,
            "inferred_schema": inferred_schema,
            "sample_size": len(request.sample_data)
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to infer schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[SchemaResponse])
async def list_schemas(
    schema_type: Optional[SchemaType] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List all schemas with optional filtering"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        schemas = await schema_registry.list_schemas(
            schema_type=schema_type,
            limit=limit,
            offset=offset
        )
        
        responses = []
        for schema in schemas:
            attrs = schema['attributes']
            schema_def = json.loads(attrs['schemaDefinition'])
            
            # Extract schema name from qualified name
            schema_name = attrs['qualifiedName'].split('.')[1]
            
            responses.append(SchemaResponse(
                id=attrs['qualifiedName'],
                name=schema_name,
                version=attrs['version'],
                type=attrs['schemaType'],
                schema=schema_def,
                compatibility=attrs['compatibility'],
                checksum=attrs['checksum'],
                created_time=attrs.get('createdTime', '')
            ))
            
        return responses
        
    except Exception as e:
        logger.error(f"Failed to list schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{schema_id}")
async def delete_schema(
    schema_id: str = Path(...),
    version: Optional[int] = Query(None)
):
    """Delete a schema or specific version"""
    if not schema_registry:
        raise HTTPException(status_code=503, detail="Schema registry not initialized")
        
    try:
        success = await schema_registry.delete_schema(schema_id, version)
        
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Schema {schema_id} not found"
            )
            
        if version:
            message = f"Schema {schema_id} version {version} deleted successfully"
        else:
            message = f"All versions of schema {schema_id} deleted successfully"
            
        return {"message": message}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cache/stats")
async def get_cache_stats():
    """Get schema cache statistics"""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not initialized")
        
    try:
        stats = await cache_manager.get_stats()
        schema_cache_stats = stats.get('catalog_schemas', {})
        
        return {
            "cache_size": schema_cache_stats.get('size', 0),
            "cache_ttl": settings.schema_cache_ttl,
            "all_caches": stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get cache stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Import json for schema parsing
import json
from ..core.config import settings 