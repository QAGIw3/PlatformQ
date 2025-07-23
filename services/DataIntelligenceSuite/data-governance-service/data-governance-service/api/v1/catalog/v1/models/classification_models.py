"""
Classification API Models

Request and response models for classification operations.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class ClassificationType(str, Enum):
    """Types of classifications."""
    SYSTEM = "system"
    BUSINESS = "business"
    SENSITIVITY = "sensitivity"
    QUALITY = "quality"
    COMPLIANCE = "compliance"
    CUSTOM = "custom"


class ClassificationSource(str, Enum):
    """Source of classification."""
    MANUAL = "manual"
    AUTO_DETECTED = "auto_detected"
    IMPORTED = "imported"
    INHERITED = "inherited"
    RULE_BASED = "rule_based"


class ConfidenceLevel(str, Enum):
    """Confidence levels for auto-classification."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ClassificationCreateRequest(BaseModel):
    """Request model for creating classification."""
    name: str = Field(..., min_length=1, max_length=255, description="Classification name")
    display_name: Optional[str] = Field(None, description="Display name")
    description: Optional[str] = Field(None, description="Description")
    type: ClassificationType = Field(ClassificationType.CUSTOM, description="Classification type")
    
    # Hierarchy
    parent: Optional[str] = Field(None, description="Parent classification name")
    
    # Applicability
    entity_types: Optional[List[str]] = Field(None, description="Applicable entity types")
    exclude_subtypes: bool = Field(False, description="Exclude subtypes")
    
    # Attributes
    attribute_defs: Optional[List[Dict[str, Any]]] = Field(None, description="Attribute definitions")
    
    # Behavior
    propagate_to_children: bool = Field(True, description="Propagate to child entities")
    propagate_to_lineage: bool = Field(False, description="Propagate through lineage")
    
    # Metadata
    tags: Optional[List[str]] = Field(None, description="Classification tags")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class ClassificationResponse(BaseModel):
    """Response model for classification data."""
    guid: str = Field(..., description="Classification GUID")
    name: str = Field(..., description="Classification name")
    display_name: Optional[str] = Field(None, description="Display name")
    description: Optional[str] = Field(None, description="Description")
    type: ClassificationType = Field(..., description="Classification type")
    
    # Hierarchy
    parent_guid: Optional[str] = Field(None, description="Parent classification GUID")
    children: List[str] = Field(default_factory=list, description="Child classification GUIDs")
    
    # Applicability
    entity_types: List[str] = Field(default_factory=list, description="Applicable entity types")
    exclude_subtypes: bool = Field(False, description="Whether subtypes are excluded")
    
    # Attributes
    attribute_defs: List[Dict[str, Any]] = Field(default_factory=list, description="Attribute definitions")
    
    # Behavior
    propagate_to_children: bool = Field(True, description="Propagate to children")
    propagate_to_lineage: bool = Field(False, description="Propagate through lineage")
    
    # Usage
    entity_count: int = Field(0, description="Number of entities with this classification")
    last_applied: Optional[datetime] = Field(None, description="Last time classification was applied")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Classification tags")
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


class ClassificationAssignRequest(BaseModel):
    """Request model for assigning classification."""
    entity_guid: str = Field(..., description="Entity GUID")
    classification_name: str = Field(..., description="Classification name")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Classification attributes")
    propagate: bool = Field(True, description="Propagate to related entities")
    
    # Assignment metadata
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Assignment confidence")
    source: ClassificationSource = Field(ClassificationSource.MANUAL, description="Assignment source")
    justification: Optional[str] = Field(None, description="Assignment justification")
    
    # Validity
    valid_from: Optional[datetime] = Field(None, description="Validity start date")
    valid_until: Optional[datetime] = Field(None, description="Validity end date")
    
    @validator('valid_until')
    def validate_validity_period(cls, v, values):
        if v and 'valid_from' in values and values['valid_from'] and v <= values['valid_from']:
            raise ValueError('valid_until must be after valid_from')
        return v


class ClassificationAssignResponse(BaseModel):
    """Response model for classification assignment."""
    entity_guid: str = Field(..., description="Entity GUID")
    classification_guid: str = Field(..., description="Classification GUID")
    assignment_guid: str = Field(..., description="Assignment GUID")
    
    # Assignment details
    status: str = Field(..., description="Assignment status")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Applied attributes")
    
    # Propagation results
    propagated_to: List[str] = Field(default_factory=list, description="Entities propagated to")
    propagation_errors: Optional[List[Dict[str, Any]]] = Field(None, description="Propagation errors")
    
    # Metadata
    assigned_at: datetime = Field(..., description="Assignment timestamp")
    assigned_by: str = Field(..., description="Assigner")


class AutoClassifyRequest(BaseModel):
    """Request model for auto-classification."""
    entity_guid: Optional[str] = Field(None, description="Specific entity to classify")
    entity_type: Optional[str] = Field(None, description="Type of entities to classify")
    
    # Scope
    sample_size: int = Field(1000, ge=100, le=10000, description="Sample size for bulk")
    include_existing: bool = Field(False, description="Re-classify already classified entities")
    
    # Classifiers
    classifiers: Optional[List[str]] = Field(None, description="Specific classifiers to run")
    classification_types: Optional[List[ClassificationType]] = Field(None, description="Types to detect")
    
    # Thresholds
    confidence_threshold: float = Field(0.8, ge=0.0, le=1.0, description="Minimum confidence")
    
    # Execution
    dry_run: bool = Field(False, description="Preview without applying")
    async_mode: bool = Field(True, description="Run asynchronously")
    
    @validator('entity_guid', 'entity_type')
    def validate_scope(cls, v, values, field):
        if field.name == 'entity_type' and not v and not values.get('entity_guid'):
            raise ValueError('Either entity_guid or entity_type must be provided')
        return v


class AutoClassifyResponse(BaseModel):
    """Response model for auto-classification."""
    job_id: Optional[str] = Field(None, description="Job ID for async execution")
    status: str = Field(..., description="Job status")
    
    # Results summary
    entities_scanned: int = Field(..., description="Number of entities scanned")
    classifications_found: int = Field(..., description="Number of classifications found")
    classifications_applied: int = Field(..., description="Number of classifications applied")
    
    # Detailed results
    results: List[Dict[str, Any]] = Field(..., description="Classification results")
    
    # Statistics
    confidence_distribution: Dict[str, int] = Field(..., description="Distribution by confidence")
    classification_distribution: Dict[str, int] = Field(..., description="Distribution by classification")
    
    # Errors
    errors: Optional[List[Dict[str, Any]]] = Field(None, description="Processing errors")
    
    # Timing
    start_time: datetime = Field(..., description="Start timestamp")
    end_time: Optional[datetime] = Field(None, description="End timestamp")
    processing_time_ms: Optional[int] = Field(None, description="Processing time")


class ClassificationRuleRequest(BaseModel):
    """Request model for classification rule."""
    name: str = Field(..., min_length=1, max_length=255, description="Rule name")
    description: Optional[str] = Field(None, description="Rule description")
    rule_type: str = Field(..., regex="^(regex|contains|datatype|custom)$", description="Rule type")
    
    # Rule definition
    pattern: Optional[str] = Field(None, description="Pattern for matching")
    conditions: Optional[List[Dict[str, Any]]] = Field(None, description="Rule conditions")
    
    # Target
    classification: str = Field(..., description="Classification to apply")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Attributes to set")
    
    # Confidence
    confidence: float = Field(0.9, ge=0.0, le=1.0, description="Confidence score")
    
    # Scope
    entity_types: Optional[List[str]] = Field(None, description="Applicable entity types")
    exclude_patterns: Optional[List[str]] = Field(None, description="Exclusion patterns")
    
    # Control
    enabled: bool = Field(True, description="Whether rule is enabled")
    priority: int = Field(100, ge=0, description="Rule priority")
    
    @validator('pattern')
    def validate_pattern_for_type(cls, v, values):
        rule_type = values.get('rule_type')
        if rule_type in ['regex', 'contains'] and not v:
            raise ValueError(f'pattern is required for rule_type: {rule_type}')
        return v


class ClassificationRuleResponse(BaseModel):
    """Response model for classification rule."""
    rule_id: str = Field(..., description="Rule ID")
    name: str = Field(..., description="Rule name")
    description: Optional[str] = Field(None, description="Rule description")
    rule_type: str = Field(..., description="Rule type")
    
    # Rule details
    pattern: Optional[str] = Field(None, description="Rule pattern")
    conditions: List[Dict[str, Any]] = Field(default_factory=list, description="Rule conditions")
    
    # Target
    classification: str = Field(..., description="Target classification")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Applied attributes")
    
    # Statistics
    match_count: int = Field(0, description="Number of matches")
    apply_count: int = Field(0, description="Number of applications")
    last_matched: Optional[datetime] = Field(None, description="Last match timestamp")
    
    # Control
    enabled: bool = Field(..., description="Whether rule is enabled")
    priority: int = Field(..., description="Rule priority")
    
    # Audit
    created_by: str = Field(..., description="Creator")
    created_at: datetime = Field(..., description="Creation timestamp")


class ClassificationScanRequest(BaseModel):
    """Request model for classification scan."""
    entity_type: Optional[str] = Field(None, description="Entity type to scan")
    limit: int = Field(100, ge=1, le=1000, description="Max entities to scan")
    
    # Scan options
    include_classified: bool = Field(False, description="Include already classified entities")
    check_inheritance: bool = Field(True, description="Check inherited classifications")
    check_propagation: bool = Field(True, description="Check propagation rules")
    
    # Execution
    async_mode: bool = Field(True, description="Run asynchronously")


class ClassificationScanResponse(BaseModel):
    """Response model for classification scan."""
    scan_id: str = Field(..., description="Scan ID")
    status: str = Field(..., description="Scan status")
    
    # Progress
    total_entities: int = Field(..., description="Total entities to scan")
    entities_scanned: int = Field(..., description="Entities scanned so far")
    progress_percent: float = Field(..., ge=0.0, le=100.0, description="Progress percentage")
    
    # Results
    classifications_needed: int = Field(..., description="Entities needing classification")
    inheritance_issues: int = Field(..., description="Inheritance issues found")
    propagation_issues: int = Field(..., description="Propagation issues found")
    
    # Recommendations
    recommendations: List[Dict[str, Any]] = Field(default_factory=list, description="Scan recommendations")
    
    # Timing
    start_time: datetime = Field(..., description="Scan start time")
    estimated_completion: Optional[datetime] = Field(None, description="Estimated completion")
    end_time: Optional[datetime] = Field(None, description="Scan end time") 