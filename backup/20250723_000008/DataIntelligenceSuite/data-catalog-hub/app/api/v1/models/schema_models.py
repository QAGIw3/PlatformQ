"""
Schema API Models

Request and response models for schema operations.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class SchemaType(str, Enum):
    """Supported schema types."""
    AVRO = "avro"
    JSON_SCHEMA = "json_schema"
    PROTOBUF = "protobuf"
    PARQUET = "parquet"
    ORC = "orc"
    XML_SCHEMA = "xml_schema"
    OPENAPI = "openapi"
    GRAPHQL = "graphql"
    CUSTOM = "custom"


class CompatibilityMode(str, Enum):
    """Schema compatibility modes."""
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"
    NONE = "none"


class SchemaField(BaseModel):
    """Schema field definition."""
    name: str = Field(..., description="Field name")
    type: str = Field(..., description="Field type")
    nullable: bool = Field(True, description="Whether field is nullable")
    default_value: Optional[Any] = Field(None, description="Default value")
    description: Optional[str] = Field(None, description="Field description")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Field metadata")
    constraints: Optional[Dict[str, Any]] = Field(None, description="Field constraints")
    
    # For nested types
    fields: Optional[List['SchemaField']] = Field(None, description="Nested fields")
    items: Optional['SchemaField'] = Field(None, description="Array item type")
    values: Optional['SchemaField'] = Field(None, description="Map value type")


SchemaField.update_forward_refs()


class SchemaRegisterRequest(BaseModel):
    """Request model for registering a schema."""
    name: str = Field(..., min_length=1, max_length=255, description="Schema name")
    schema_type: SchemaType = Field(..., description="Schema type")
    schema: Dict[str, Any] = Field(..., description="Schema definition")
    compatibility: Optional[CompatibilityMode] = Field(None, description="Compatibility mode")
    description: Optional[str] = Field(None, description="Schema description")
    namespace: Optional[str] = Field(None, description="Schema namespace")
    tags: Optional[List[str]] = Field(None, description="Schema tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('schema')
    def validate_schema_not_empty(cls, v):
        if not v:
            raise ValueError('schema cannot be empty')
        return v


class SchemaUpdateRequest(BaseModel):
    """Request model for updating a schema."""
    description: Optional[str] = Field(None, description="Schema description")
    tags: Optional[List[str]] = Field(None, description="Schema tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    compatibility: Optional[CompatibilityMode] = Field(None, description="Compatibility mode")
    deprecated: Optional[bool] = Field(None, description="Mark schema as deprecated")
    deprecation_message: Optional[str] = Field(None, description="Deprecation message")


class SchemaResponse(BaseModel):
    """Response model for schema data."""
    id: str = Field(..., description="Schema ID")
    name: str = Field(..., description="Schema name")
    schema_type: SchemaType = Field(..., description="Schema type")
    version: int = Field(..., description="Schema version")
    schema: Dict[str, Any] = Field(..., description="Schema definition")
    
    # Metadata
    description: Optional[str] = Field(None, description="Schema description")
    namespace: Optional[str] = Field(None, description="Schema namespace")
    compatibility: Optional[CompatibilityMode] = Field(None, description="Compatibility mode")
    tags: List[str] = Field(default_factory=list, description="Schema tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    # Status
    status: str = Field(..., description="Schema status (active, deprecated)")
    deprecated: bool = Field(False, description="Whether schema is deprecated")
    deprecation_message: Optional[str] = Field(None, description="Deprecation message")
    
    # Fields
    fields: Optional[List[SchemaField]] = Field(None, description="Parsed schema fields")
    
    # References
    references: List[str] = Field(default_factory=list, description="Referenced schemas")
    referenced_by: List[str] = Field(default_factory=list, description="Schemas referencing this")
    entities_using: List[str] = Field(default_factory=list, description="Entities using this schema")
    
    # Audit
    created_by: str = Field(..., description="Creator")
    created_at: datetime = Field(..., description="Creation timestamp")
    modified_by: Optional[str] = Field(None, description="Last modifier")
    modified_at: Optional[datetime] = Field(None, description="Last modification timestamp")
    
    # Statistics
    usage_count: int = Field(0, description="Number of entities using this schema")
    validation_count: int = Field(0, description="Number of validations performed")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SchemaListResponse(BaseModel):
    """Response model for schema list."""
    schemas: List[SchemaResponse] = Field(..., description="List of schemas")
    total_count: int = Field(..., description="Total number of schemas")
    has_more: bool = Field(False, description="Whether more schemas exist")


class SchemaValidationRequest(BaseModel):
    """Request model for schema validation."""
    schema_id: str = Field(..., description="Schema ID to validate against")
    data: Dict[str, Any] = Field(..., description="Data to validate")
    strict: bool = Field(True, description="Whether to use strict validation")
    return_details: bool = Field(True, description="Return detailed validation errors")


class SchemaValidationResponse(BaseModel):
    """Response model for schema validation."""
    valid: bool = Field(..., description="Whether data is valid")
    errors: Optional[List[Dict[str, Any]]] = Field(None, description="Validation errors")
    warnings: Optional[List[Dict[str, Any]]] = Field(None, description="Validation warnings")
    validated_at: datetime = Field(default_factory=datetime.utcnow, description="Validation timestamp")


class SchemaInferenceRequest(BaseModel):
    """Request model for schema inference."""
    sample_data: List[Dict[str, Any]] = Field(..., min_items=1, description="Sample data")
    schema_type: SchemaType = Field(SchemaType.JSON_SCHEMA, description="Target schema type")
    infer_nullable: bool = Field(True, description="Infer nullable fields")
    infer_constraints: bool = Field(False, description="Infer field constraints")
    confidence_threshold: float = Field(0.9, ge=0.0, le=1.0, description="Confidence threshold")
    
    @validator('sample_data')
    def validate_sample_size(cls, v):
        if len(v) > 10000:
            raise ValueError('sample_data cannot exceed 10000 records')
        return v


class SchemaInferenceResponse(BaseModel):
    """Response model for schema inference."""
    inferred_schema: Dict[str, Any] = Field(..., description="Inferred schema")
    schema_type: SchemaType = Field(..., description="Schema type")
    confidence_scores: Dict[str, float] = Field(..., description="Field confidence scores")
    fields: List[SchemaField] = Field(..., description="Inferred fields")
    statistics: Dict[str, Any] = Field(..., description="Field statistics")
    warnings: Optional[List[str]] = Field(None, description="Inference warnings")


class SchemaCompareRequest(BaseModel):
    """Request model for schema comparison."""
    schema_id_1: str = Field(..., description="First schema ID")
    version_1: Optional[int] = Field(None, description="First schema version")
    schema_id_2: str = Field(..., description="Second schema ID")
    version_2: Optional[int] = Field(None, description="Second schema version")
    deep_compare: bool = Field(True, description="Perform deep comparison")


class SchemaCompareResponse(BaseModel):
    """Response model for schema comparison."""
    compatible: bool = Field(..., description="Whether schemas are compatible")
    compatibility_mode: Optional[CompatibilityMode] = Field(None, description="Compatibility type")
    differences: List[Dict[str, Any]] = Field(..., description="Schema differences")
    added_fields: List[str] = Field(default_factory=list, description="Added fields")
    removed_fields: List[str] = Field(default_factory=list, description="Removed fields")
    modified_fields: List[str] = Field(default_factory=list, description="Modified fields")
    breaking_changes: List[Dict[str, Any]] = Field(default_factory=list, description="Breaking changes")


class SchemaEvolutionRequest(BaseModel):
    """Request model for schema evolution."""
    schema_id: str = Field(..., description="Schema ID")
    new_schema: Dict[str, Any] = Field(..., description="New schema version")
    check_compatibility: bool = Field(True, description="Check compatibility")
    migration_strategy: Optional[str] = Field(None, description="Migration strategy")


class SchemaEvolutionResponse(BaseModel):
    """Response model for schema evolution."""
    new_version: int = Field(..., description="New schema version")
    compatible: bool = Field(..., description="Whether evolution is compatible")
    migration_required: bool = Field(..., description="Whether migration is required")
    migration_steps: Optional[List[Dict[str, Any]]] = Field(None, description="Migration steps")
    affected_entities: List[str] = Field(default_factory=list, description="Affected entities")
    warnings: Optional[List[str]] = Field(None, description="Evolution warnings")


class SchemaUsageStats(BaseModel):
    """Schema usage statistics."""
    schema_id: str = Field(..., description="Schema ID")
    total_entities: int = Field(..., description="Total entities using schema")
    entities_by_type: Dict[str, int] = Field(..., description="Entities grouped by type")
    validation_success_rate: float = Field(..., ge=0.0, le=1.0, description="Validation success rate")
    last_used: Optional[datetime] = Field(None, description="Last usage timestamp")
    usage_trend: Optional[List[Dict[str, Any]]] = Field(None, description="Usage trend data") 