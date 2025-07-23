"""
Glossary API Models

Request and response models for business glossary operations.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class TermStatus(str, Enum):
    """Status of glossary terms."""
    DRAFT = "draft"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


class TermRelationType(str, Enum):
    """Types of term relationships."""
    SYNONYM = "synonym"
    RELATED = "related"
    SEE_ALSO = "see_also"
    REPLACED_BY = "replaced_by"
    PREFERRED_TERM = "preferred_term"
    BROADER = "broader"
    NARROWER = "narrower"


class MappingConfidence(str, Enum):
    """Confidence levels for term mappings."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    MANUAL = "manual"


class GlossaryCreateRequest(BaseModel):
    """Request model for creating glossary."""
    name: str = Field(..., min_length=1, max_length=255, description="Glossary name")
    short_description: str = Field(..., description="Short description")
    long_description: Optional[str] = Field(None, description="Detailed description")
    language: str = Field("en", description="Language code")
    usage: Optional[str] = Field(None, description="Usage guidelines")
    
    # Organization
    owner: Optional[str] = Field(None, description="Glossary owner")
    domain: Optional[str] = Field(None, description="Business domain")
    
    # Metadata
    tags: Optional[List[str]] = Field(None, description="Glossary tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class GlossaryResponse(BaseModel):
    """Response model for glossary data."""
    guid: str = Field(..., description="Glossary GUID")
    name: str = Field(..., description="Glossary name")
    short_description: str = Field(..., description="Short description")
    long_description: Optional[str] = Field(None, description="Detailed description")
    language: str = Field(..., description="Language code")
    usage: Optional[str] = Field(None, description="Usage guidelines")
    
    # Organization
    owner: Optional[str] = Field(None, description="Glossary owner")
    domain: Optional[str] = Field(None, description="Business domain")
    
    # Statistics
    term_count: int = Field(0, description="Number of terms")
    category_count: int = Field(0, description="Number of categories")
    approved_term_count: int = Field(0, description="Number of approved terms")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Glossary tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    # Audit
    created_by: str = Field(..., description="Creator")
    created_at: datetime = Field(..., description="Creation timestamp")
    modified_by: Optional[str] = Field(None, description="Last modifier")
    modified_at: Optional[datetime] = Field(None, description="Last modification timestamp")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class TermCreateRequest(BaseModel):
    """Request model for creating term."""
    name: str = Field(..., min_length=1, max_length=255, description="Term name")
    definition: str = Field(..., description="Term definition")
    glossary_guid: Optional[str] = Field(None, description="Parent glossary GUID")
    
    # Term details
    abbreviation: Optional[str] = Field(None, description="Abbreviation")
    usage: Optional[str] = Field(None, description="Usage guidelines")
    examples: Optional[List[str]] = Field(None, description="Usage examples")
    
    # Relationships
    related_terms: Optional[List[str]] = Field(None, description="Related term names")
    synonyms: Optional[List[str]] = Field(None, description="Synonym terms")
    see_also: Optional[List[str]] = Field(None, description="See also terms")
    
    # Organization
    categories: Optional[List[str]] = Field(None, description="Category GUIDs")
    domain: Optional[str] = Field(None, description="Business domain")
    
    # Status
    status: TermStatus = Field(TermStatus.DRAFT, description="Initial status")
    
    # Metadata
    tags: Optional[List[str]] = Field(None, description="Term tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class TermUpdateRequest(BaseModel):
    """Request model for updating term."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Term name")
    definition: Optional[str] = Field(None, description="Term definition")
    
    # Term details
    abbreviation: Optional[str] = Field(None, description="Abbreviation")
    usage: Optional[str] = Field(None, description="Usage guidelines")
    examples: Optional[List[str]] = Field(None, description="Usage examples")
    
    # Status
    status: Optional[TermStatus] = Field(None, description="Term status")
    
    # Metadata
    tags: Optional[List[str]] = Field(None, description="Term tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class TermResponse(BaseModel):
    """Response model for term data."""
    guid: str = Field(..., description="Term GUID")
    name: str = Field(..., description="Term name")
    definition: str = Field(..., description="Term definition")
    glossary_guid: str = Field(..., description="Parent glossary GUID")
    
    # Term details
    abbreviation: Optional[str] = Field(None, description="Abbreviation")
    usage: Optional[str] = Field(None, description="Usage guidelines")
    examples: List[str] = Field(default_factory=list, description="Usage examples")
    
    # Status
    status: TermStatus = Field(..., description="Term status")
    approval_date: Optional[datetime] = Field(None, description="Approval date")
    approved_by: Optional[str] = Field(None, description="Approver")
    
    # Relationships
    related_terms: List[Dict[str, Any]] = Field(default_factory=list, description="Related terms")
    synonyms: List[Dict[str, Any]] = Field(default_factory=list, description="Synonyms")
    see_also: List[Dict[str, Any]] = Field(default_factory=list, description="See also references")
    
    # Organization
    categories: List[Dict[str, Any]] = Field(default_factory=list, description="Categories")
    domain: Optional[str] = Field(None, description="Business domain")
    
    # Usage
    assigned_entities: List[str] = Field(default_factory=list, description="Assigned entity GUIDs")
    usage_count: int = Field(0, description="Number of entity assignments")
    last_used: Optional[datetime] = Field(None, description="Last usage timestamp")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Term tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    # Audit
    created_by: str = Field(..., description="Creator")
    created_at: datetime = Field(..., description="Creation timestamp")
    modified_by: Optional[str] = Field(None, description="Last modifier")
    modified_at: Optional[datetime] = Field(None, description="Last modification timestamp")


class TermListResponse(BaseModel):
    """Response model for term list."""
    terms: List[TermResponse] = Field(..., description="List of terms")
    total: int = Field(..., description="Total number of terms")
    filtered: int = Field(..., description="Number after filtering")


class TermAssignmentRequest(BaseModel):
    """Request model for term assignment."""
    term_guid: str = Field(..., description="Term GUID")
    entity_guids: List[str] = Field(..., min_items=1, description="Entity GUIDs")
    
    # Assignment options
    semantic_assignment: bool = Field(False, description="Use AI for semantic verification")
    confidence_threshold: float = Field(0.8, ge=0.0, le=1.0, description="Min confidence for AI")
    
    # Metadata
    assignment_reason: Optional[str] = Field(None, description="Assignment reason")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Assignment attributes")


class TermAssignmentResponse(BaseModel):
    """Response model for term assignment."""
    term_guid: str = Field(..., description="Term GUID")
    
    # Results
    total_entities: int = Field(..., description="Total entities processed")
    assigned: int = Field(..., description="Successfully assigned")
    failed: int = Field(..., description="Failed assignments")
    
    # Details
    assignments: List[Dict[str, Any]] = Field(..., description="Assignment details")
    errors: Optional[List[Dict[str, Any]]] = Field(None, description="Assignment errors")
    
    # AI results (if used)
    semantic_scores: Optional[Dict[str, float]] = Field(None, description="Semantic match scores")
    rejected_by_ai: Optional[List[str]] = Field(None, description="Entities rejected by AI")


class TermSuggestionRequest(BaseModel):
    """Request model for term suggestions."""
    technical_name: str = Field(..., description="Technical field/column name")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")
    
    # Context details
    entity_type: Optional[str] = Field(None, description="Entity type")
    schema_name: Optional[str] = Field(None, description="Schema name")
    table_name: Optional[str] = Field(None, description="Table name")
    data_type: Optional[str] = Field(None, description="Data type")
    sample_values: Optional[List[Any]] = Field(None, description="Sample data values")
    
    # Options
    max_suggestions: int = Field(5, ge=1, le=20, description="Maximum suggestions")
    include_similar: bool = Field(True, description="Include similar terms")
    glossary_guid: Optional[str] = Field(None, description="Limit to specific glossary")


class TermSuggestionResponse(BaseModel):
    """Response model for term suggestions."""
    technical_name: str = Field(..., description="Original technical name")
    
    # Suggestions
    suggestions: List[Dict[str, Any]] = Field(..., description="Suggested terms")
    
    # Each suggestion contains:
    # - term_guid: str
    # - term_name: str
    # - definition: str
    # - confidence: float
    # - reasoning: str
    # - match_type: str (exact, semantic, pattern, etc.)
    
    # Analysis
    analysis: Dict[str, Any] = Field(..., description="Name analysis results")
    context_used: Dict[str, Any] = Field(..., description="Context used for suggestions")


class AutoMappingRequest(BaseModel):
    """Request model for automatic term mapping."""
    dataset_guid: str = Field(..., description="Dataset GUID")
    
    # Options
    approval_required: bool = Field(True, description="Require approval")
    confidence_threshold: float = Field(0.7, ge=0.0, le=1.0, description="Min confidence")
    
    # Scope
    include_columns: Optional[List[str]] = Field(None, description="Specific columns")
    exclude_columns: Optional[List[str]] = Field(None, description="Columns to exclude")
    
    # Mapping strategy
    use_naming_patterns: bool = Field(True, description="Use naming patterns")
    use_data_profiling: bool = Field(True, description="Use data profiling")
    use_existing_mappings: bool = Field(True, description="Learn from existing")
    
    # Execution
    dry_run: bool = Field(False, description="Preview without applying")


class AutoMappingResponse(BaseModel):
    """Response model for automatic mapping."""
    dataset_guid: str = Field(..., description="Dataset GUID")
    status: str = Field(..., description="Mapping status")
    
    # Results
    total_columns: int = Field(..., description="Total columns analyzed")
    mappings_found: int = Field(..., description="Mappings found")
    mappings_applied: int = Field(..., description="Mappings applied")
    pending_approval: int = Field(..., description="Awaiting approval")
    
    # Mapping details
    mappings: List[Dict[str, Any]] = Field(..., description="Mapping details")
    
    # Each mapping contains:
    # - column_name: str
    # - column_guid: str
    # - suggested_term: str
    # - term_guid: str
    # - confidence: float
    # - reasoning: str
    # - status: str (applied, pending, rejected)
    
    # Statistics
    confidence_distribution: Dict[str, int] = Field(..., description="Mappings by confidence")
    match_type_distribution: Dict[str, int] = Field(..., description="Mappings by match type")
    
    # Timing
    processing_time_ms: int = Field(..., description="Processing time")


class CategoryCreateRequest(BaseModel):
    """Request model for creating category."""
    name: str = Field(..., min_length=1, max_length=255, description="Category name")
    glossary_guid: str = Field(..., description="Parent glossary GUID")
    short_description: str = Field(..., description="Short description")
    long_description: Optional[str] = Field(None, description="Detailed description")
    parent_category_guid: Optional[str] = Field(None, description="Parent category GUID")
    
    # Metadata
    tags: Optional[List[str]] = Field(None, description="Category tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class CategoryResponse(BaseModel):
    """Response model for category data."""
    guid: str = Field(..., description="Category GUID")
    name: str = Field(..., description="Category name")
    glossary_guid: str = Field(..., description="Parent glossary GUID")
    short_description: str = Field(..., description="Short description")
    long_description: Optional[str] = Field(None, description="Detailed description")
    
    # Hierarchy
    parent_category_guid: Optional[str] = Field(None, description="Parent category GUID")
    child_categories: List[str] = Field(default_factory=list, description="Child category GUIDs")
    
    # Content
    term_count: int = Field(0, description="Number of terms")
    terms: List[str] = Field(default_factory=list, description="Term GUIDs in category")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Category tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    # Audit
    created_by: str = Field(..., description="Creator")
    created_at: datetime = Field(..., description="Creation timestamp") 