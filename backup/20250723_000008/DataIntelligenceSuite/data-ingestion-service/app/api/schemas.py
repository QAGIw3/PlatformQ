"""
Schema Registry API endpoints
"""

import logging
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field

from app.core.schema_registry import SchemaRegistry, SchemaType, CompatibilityMode

logger = logging.getLogger(__name__)

router = APIRouter()

# Global schema registry (will be injected)
schema_registry: Optional[SchemaRegistry] = None


def set_schema_registry(registry: SchemaRegistry):
    """Set the global schema registry"""
    global schema_registry
    schema_registry = registry


# Request/Response Models
class SchemaRegistrationRequest(BaseModel):
    """Schema registration request"""
    schema_id: str = Field(..., description="Unique schema identifier")
    schema: Dict[str, Any] = Field(..., description="Schema definition")
    schema_type: SchemaType = Field(SchemaType.AVRO, description="Schema type")
    compatibility: Optional[CompatibilityMode] = Field(None, description="Compatibility mode")
    
    class Config:
        schema_extra = {
            "example": {
                "schema_id": "user_events",
                "schema": {
                    "type": "record",
                    "name": "UserEvent",
                    "fields": [
                        {"name": "user_id", "type": "string"},
                        {"name": "event_type", "type": "string"},
                        {"name": "timestamp", "type": "long"}
                    ]
                },
                "schema_type": "avro",
                "compatibility": "BACKWARD"
            }
        }


class SchemaValidationRequest(BaseModel):
    """Data validation request"""
    data: Dict[str, Any] = Field(..., description="Data to validate")
    schema_id: str = Field(..., description="Schema ID to validate against")
    version: Optional[int] = Field(None, description="Schema version")


class SchemaInfo(BaseModel):
    """Schema information"""
    id: str
    latest_version: int
    type: str
    created_at: str
    versions: List[int]


class SchemaVersion(BaseModel):
    """Schema version details"""
    id: str
    version: int
    type: str
    schema: Dict[str, Any]
    compatibility: str
    created_at: str
    checksum: str


# API Endpoints
@router.post("/", response_model=Dict[str, Any])
async def register_schema(request: SchemaRegistrationRequest):
    """Register a new schema or version"""
    try:
        result = await schema_registry.register_schema(
            schema_id=request.schema_id,
            schema=request.schema,
            schema_type=request.schema_type,
            compatibility=request.compatibility
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to register schema: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/", response_model=List[SchemaInfo])
async def list_schemas(schema_type: Optional[SchemaType] = None):
    """List all registered schemas"""
    try:
        schemas = await schema_registry.list_schemas(schema_type)
        
        return [
            SchemaInfo(
                id=schema["id"],
                latest_version=schema["latest_version"],
                type=schema["type"],
                created_at=schema["created_at"],
                versions=schema["versions"]
            )
            for schema in schemas
        ]
        
    except Exception as e:
        logger.error(f"Failed to list schemas: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{schema_id}", response_model=SchemaVersion)
async def get_schema(schema_id: str, version: Optional[int] = None):
    """Get a schema by ID and optional version"""
    try:
        schema = await schema_registry.get_schema(schema_id, version)
        
        if not schema:
            raise HTTPException(status_code=404, detail=f"Schema {schema_id} not found")
            
        return SchemaVersion(**schema)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get schema: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{schema_id}/versions", response_model=List[int])
async def list_schema_versions(schema_id: str):
    """List all versions of a schema"""
    try:
        versions = await schema_registry.list_versions(schema_id)
        
        if not versions:
            raise HTTPException(status_code=404, detail=f"Schema {schema_id} not found")
            
        return versions
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list schema versions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/validate", response_model=Dict[str, Any])
async def validate_data(request: SchemaValidationRequest):
    """Validate data against a schema"""
    try:
        is_valid, error = await schema_registry.validate_data(
            data=request.data,
            schema_id=request.schema_id,
            version=request.version
        )
        
        return {
            "valid": is_valid,
            "error": error,
            "schema_id": request.schema_id,
            "version": request.version
        }
        
    except Exception as e:
        logger.error(f"Failed to validate data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{schema_id}")
async def delete_schema(schema_id: str, version: Optional[int] = None):
    """Delete a schema or specific version"""
    try:
        result = await schema_registry.delete_schema(schema_id, version)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete schema: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/{schema_id}/compatibility/check")
async def check_compatibility(
    schema_id: str,
    schema: Dict[str, Any] = Body(...),
    mode: Optional[CompatibilityMode] = None
):
    """Check if a new schema version is compatible"""
    try:
        # Get latest version
        existing_schema = await schema_registry.get_schema(schema_id)
        if not existing_schema:
            return {
                "compatible": True,
                "reason": "No existing schema found"
            }
            
        # Check compatibility (this is simplified - full implementation in schema_registry)
        # For now, just try to register with dryRun
        try:
            await schema_registry.register_schema(
                schema_id=f"{schema_id}_test",
                schema=schema,
                schema_type=SchemaType(existing_schema["type"]),
                compatibility=mode
            )
            
            # Clean up test schema
            await schema_registry.delete_schema(f"{schema_id}_test")
            
            return {
                "compatible": True,
                "existing_version": existing_schema["version"]
            }
            
        except ValueError as e:
            return {
                "compatible": False,
                "reason": str(e),
                "existing_version": existing_schema["version"]
            }
            
    except Exception as e:
        logger.error(f"Failed to check compatibility: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 