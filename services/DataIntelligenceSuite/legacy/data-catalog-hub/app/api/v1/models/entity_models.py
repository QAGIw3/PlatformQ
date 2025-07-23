"""
Entity API Models

Request and response models for entity operations.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, validator

from .common import (
    AuditInfo,
    BusinessContext,
    ClassificationAttribute,
    Location,
    MetadataField,
    QualityMetrics,
    RelationshipInfo,
    SecurityInfo,
    Tag
)


class EntityCreateRequest(BaseModel):
    """Request model for creating an entity."""
    name: str = Field(..., min_length=1, max_length=255, description="Entity name")
    type: str = Field(..., description="Entity type (e.g., hive_table, s3_bucket)")
    qualified_name: str = Field(..., description="Fully qualified unique name")
    display_name: Optional[str] = Field(None, description="Display name")
    description: Optional[str] = Field(None, description="Entity description")
    owner: Optional[str] = Field(None, description="Entity owner")
    location: Optional[Location] = Field(None, description="Physical location")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Custom attributes")
    classifications: Optional[List[str]] = Field(None, description="Initial classifications")
    business_metadata: Optional[Dict[str, Any]] = Field(None, description="Business metadata")
    technical_metadata: Optional[Dict[str, Any]] = Field(None, description="Technical metadata")
    tags: Optional[List[Tag]] = Field(None, description="Entity tags")
    schema_ref: Optional[str] = Field(None, description="Reference to schema")
    parent_guid: Optional[str] = Field(None, description="Parent entity GUID")

    @validator('qualified_name')
    def validate_qualified_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError('qualified_name cannot be empty')
        return v


class EntityUpdateRequest(BaseModel):
    """Request model for updating an entity."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Entity name")
    display_name: Optional[str] = Field(None, description="Display name")
    description: Optional[str] = Field(None, description="Entity description")
    owner: Optional[str] = Field(None, description="Entity owner")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Custom attributes to update")
    business_metadata: Optional[Dict[str, Any]] = Field(None, description="Business metadata to update")
    technical_metadata: Optional[Dict[str, Any]] = Field(None, description="Technical metadata to update")
    tags: Optional[List[Tag]] = Field(None, description="Tags to update")
    status: Optional[str] = Field(None, regex="^(ACTIVE|DELETED|ARCHIVED)$", description="Entity status")

    class Config:
        validate_assignment = True


class EntityResponse(BaseModel):
    """Response model for entity data."""
    guid: str = Field(..., description="Entity GUID")
    name: str = Field(..., description="Entity name")
    type: str = Field(..., description="Entity type")
    qualified_name: str = Field(..., description="Fully qualified name")
    display_name: Optional[str] = Field(None, description="Display name")
    description: Optional[str] = Field(None, description="Entity description")
    owner: Optional[str] = Field(None, description="Entity owner")
    status: str = Field(..., description="Entity status")
    
    # Location and attributes
    location: Optional[Location] = Field(None, description="Physical location")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Entity attributes")
    
    # Metadata
    business_metadata: Optional[BusinessContext] = Field(None, description="Business metadata")
    technical_metadata: Dict[str, Any] = Field(default_factory=dict, description="Technical metadata")
    custom_metadata: Optional[List[MetadataField]] = Field(None, description="Custom metadata fields")
    
    # Classifications and tags
    classifications: List[Dict[str, Any]] = Field(default_factory=list, description="Entity classifications")
    tags: List[Tag] = Field(default_factory=list, description="Entity tags")
    
    # Quality and security
    quality_metrics: Optional[QualityMetrics] = Field(None, description="Data quality metrics")
    security_info: Optional[SecurityInfo] = Field(None, description="Security information")
    
    # Relationships
    relationships: List[RelationshipInfo] = Field(default_factory=list, description="Entity relationships")
    schema_ref: Optional[str] = Field(None, description="Associated schema reference")
    parent_guid: Optional[str] = Field(None, description="Parent entity GUID")
    
    # Audit
    audit_info: AuditInfo = Field(..., description="Audit information")
    
    # Computed fields
    is_incomplete: bool = Field(False, description="Whether entity data is incomplete")
    access_level: Optional[str] = Field(None, description="User's access level to this entity")
    usage_score: Optional[float] = Field(None, ge=0.0, description="Entity usage/popularity score")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EntityListResponse(BaseModel):
    """Response model for entity list."""
    entities: List[EntityResponse] = Field(..., description="List of entities")
    total_count: int = Field(..., description="Total number of entities")
    facets: Optional[Dict[str, List[Dict[str, Any]]]] = Field(None, description="Search facets")
    query_time_ms: Optional[int] = Field(None, description="Query execution time")
    next_page_token: Optional[str] = Field(None, description="Token for next page")


class EntitySearchRequest(BaseModel):
    """Request model for entity search."""
    query: Optional[str] = Field(None, description="Search query")
    filters: Optional[Dict[str, Any]] = Field(None, description="Search filters")
    entity_types: Optional[List[str]] = Field(None, description="Filter by entity types")
    classifications: Optional[List[str]] = Field(None, description="Filter by classifications")
    owners: Optional[List[str]] = Field(None, description="Filter by owners")
    tags: Optional[List[str]] = Field(None, description="Filter by tags")
    include_deleted: bool = Field(False, description="Include deleted entities")
    include_subtypes: bool = Field(True, description="Include entity subtypes")
    limit: int = Field(20, ge=1, le=1000, description="Result limit")
    offset: int = Field(0, ge=0, description="Result offset")
    sort_by: Optional[str] = Field(None, description="Sort field")
    sort_order: Optional[str] = Field("asc", regex="^(asc|desc)$", description="Sort order")


class EntityBulkRequest(BaseModel):
    """Request model for bulk entity operations."""
    entity_guids: List[str] = Field(..., min_items=1, max_items=100, description="Entity GUIDs")
    operation: str = Field(..., regex="^(DELETE|ARCHIVE|CLASSIFY|TAG)$", description="Operation to perform")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Operation parameters")


class EntityBulkResponse(BaseModel):
    """Response model for bulk entity operations."""
    total: int = Field(..., description="Total entities processed")
    succeeded: int = Field(..., description="Successful operations")
    failed: int = Field(..., description="Failed operations")
    results: List[Dict[str, Any]] = Field(..., description="Individual operation results")
    errors: Optional[List[Dict[str, Any]]] = Field(None, description="Error details for failed operations")


class EntityLineageRequest(BaseModel):
    """Request model for entity lineage."""
    direction: str = Field("BOTH", regex="^(INPUT|OUTPUT|BOTH)$", description="Lineage direction")
    depth: int = Field(3, ge=1, le=10, description="Lineage depth")
    include_processes: bool = Field(True, description="Include process entities")
    include_deleted: bool = Field(False, description="Include deleted entities")


class EntityAuditRequest(BaseModel):
    """Request model for entity audit history."""
    start_date: Optional[datetime] = Field(None, description="Start date for audit history")
    end_date: Optional[datetime] = Field(None, description="End date for audit history")
    event_types: Optional[List[str]] = Field(None, description="Filter by event types")
    users: Optional[List[str]] = Field(None, description="Filter by users")
    limit: int = Field(50, ge=1, le=500, description="Result limit")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if v and 'start_date' in values and values['start_date'] and v < values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class EntityAuditEvent(BaseModel):
    """Audit event for an entity."""
    event_id: str = Field(..., description="Event ID")
    event_type: str = Field(..., description="Event type")
    timestamp: datetime = Field(..., description="Event timestamp")
    user: str = Field(..., description="User who performed the action")
    details: Dict[str, Any] = Field(..., description="Event details")
    before_state: Optional[Dict[str, Any]] = Field(None, description="Entity state before change")
    after_state: Optional[Dict[str, Any]] = Field(None, description="Entity state after change") 