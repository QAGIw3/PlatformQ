"""
Common API Models

Shared request/response models used across the API.
"""

from typing import Dict, Any, Optional, List, Generic, TypeVar
from datetime import datetime
from pydantic import BaseModel, Field, validator


T = TypeVar('T')


class PaginationParams(BaseModel):
    """Common pagination parameters."""
    limit: int = Field(20, ge=1, le=1000, description="Number of results per page")
    offset: int = Field(0, ge=0, description="Number of results to skip")
    sort_by: Optional[str] = Field(None, description="Field to sort by")
    sort_order: Optional[str] = Field("asc", regex="^(asc|desc)$", description="Sort order")


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response."""
    items: List[T]
    total: int = Field(..., description="Total number of items")
    limit: int = Field(..., description="Items per page")
    offset: int = Field(..., description="Number of items skipped")
    has_more: bool = Field(..., description="Whether more items exist")
    
    @validator('has_more', always=True)
    def calculate_has_more(cls, v, values):
        total = values.get('total', 0)
        limit = values.get('limit', 0)
        offset = values.get('offset', 0)
        return (offset + limit) < total


class ErrorResponse(BaseModel):
    """Standard error response."""
    error: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code for programmatic handling")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request tracking ID")


class SuccessResponse(BaseModel):
    """Standard success response."""
    success: bool = Field(True, description="Operation success indicator")
    message: Optional[str] = Field(None, description="Success message")
    data: Optional[Dict[str, Any]] = Field(None, description="Operation result data")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")


class MetadataField(BaseModel):
    """Metadata field definition."""
    key: str = Field(..., description="Field key")
    value: Any = Field(..., description="Field value")
    type: Optional[str] = Field(None, description="Value type")
    description: Optional[str] = Field(None, description="Field description")
    is_required: bool = Field(False, description="Whether field is required")
    is_indexed: bool = Field(False, description="Whether field is indexed for search")


class ClassificationAttribute(BaseModel):
    """Classification attribute."""
    name: str = Field(..., description="Attribute name")
    value: Any = Field(..., description="Attribute value")
    type: str = Field("string", description="Attribute type")
    is_mandatory: bool = Field(False, description="Whether attribute is mandatory")
    valid_values: Optional[List[Any]] = Field(None, description="Valid values for enum types")


class RelationshipInfo(BaseModel):
    """Entity relationship information."""
    guid: str = Field(..., description="Related entity GUID")
    type: str = Field(..., description="Relationship type")
    entity_type: str = Field(..., description="Related entity type")
    display_name: str = Field(..., description="Related entity display name")
    direction: str = Field(..., regex="^(IN|OUT|BOTH)$", description="Relationship direction")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Relationship attributes")


class AuditInfo(BaseModel):
    """Audit information for entities."""
    created_by: str = Field(..., description="Creator username")
    created_at: datetime = Field(..., description="Creation timestamp")
    modified_by: Optional[str] = Field(None, description="Last modifier username")
    modified_at: Optional[datetime] = Field(None, description="Last modification timestamp")
    version: int = Field(1, ge=1, description="Entity version")


class QualityMetrics(BaseModel):
    """Data quality metrics."""
    completeness: float = Field(..., ge=0.0, le=1.0, description="Data completeness score")
    accuracy: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data accuracy score")
    consistency: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data consistency score")
    timeliness: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data timeliness score")
    validity: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data validity score")
    uniqueness: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data uniqueness score")
    overall_score: float = Field(..., ge=0.0, le=1.0, description="Overall quality score")
    issues: Optional[List[Dict[str, Any]]] = Field(None, description="Quality issues found")
    last_assessed: datetime = Field(..., description="Last assessment timestamp")


class BusinessContext(BaseModel):
    """Business context information."""
    business_domain: Optional[str] = Field(None, description="Business domain")
    business_owner: Optional[str] = Field(None, description="Business owner")
    data_steward: Optional[str] = Field(None, description="Data steward")
    criticality: Optional[str] = Field(None, regex="^(LOW|MEDIUM|HIGH|CRITICAL)$", description="Business criticality")
    retention_policy: Optional[str] = Field(None, description="Data retention policy")
    compliance_tags: Optional[List[str]] = Field(None, description="Compliance requirements")
    business_glossary_terms: Optional[List[str]] = Field(None, description="Associated business terms")


class TimeRange(BaseModel):
    """Time range for queries."""
    start_date: datetime = Field(..., description="Start date/time")
    end_date: datetime = Field(..., description="End date/time")
    
    @validator('end_date')
    def end_after_start(cls, v, values):
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class Location(BaseModel):
    """Resource location information."""
    type: str = Field(..., description="Location type (e.g., S3, HDFS, database)")
    path: str = Field(..., description="Resource path or URI")
    region: Optional[str] = Field(None, description="Geographic region")
    cluster: Optional[str] = Field(None, description="Cluster name")
    database: Optional[str] = Field(None, description="Database name")
    schema: Optional[str] = Field(None, description="Schema name")
    table: Optional[str] = Field(None, description="Table name")
    partition: Optional[str] = Field(None, description="Partition information")


class Tag(BaseModel):
    """Tag for categorization."""
    name: str = Field(..., description="Tag name")
    value: Optional[str] = Field(None, description="Tag value")
    source: Optional[str] = Field("manual", description="Tag source (manual, auto, system)")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Confidence for auto tags")


class SecurityInfo(BaseModel):
    """Security-related information."""
    access_level: str = Field(..., regex="^(PUBLIC|INTERNAL|CONFIDENTIAL|SECRET)$", description="Access level")
    encryption_enabled: bool = Field(False, description="Whether data is encrypted")
    encryption_type: Optional[str] = Field(None, description="Encryption type")
    data_masking_enabled: bool = Field(False, description="Whether data masking is enabled")
    masking_rules: Optional[List[Dict[str, Any]]] = Field(None, description="Data masking rules")
    access_groups: Optional[List[str]] = Field(None, description="Groups with access")
    compliance_certifications: Optional[List[str]] = Field(None, description="Compliance certifications") 