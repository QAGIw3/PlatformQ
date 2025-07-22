"""
Lineage API Models

Request and response models for lineage operations.
"""

from typing import Dict, Any, Optional, List, Set
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum


class LineageDirection(str, Enum):
    """Lineage traversal direction."""
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    BOTH = "both"


class ProcessType(str, Enum):
    """Process types for lineage."""
    ETL = "etl"
    STREAMING = "streaming"
    ML_TRAINING = "ml_training"
    ML_INFERENCE = "ml_inference"
    DATA_QUALITY = "data_quality"
    MANUAL = "manual"
    API = "api"
    QUERY = "query"
    REPLICATION = "replication"
    OTHER = "other"


class LineageNode(BaseModel):
    """Node in lineage graph."""
    guid: str = Field(..., description="Entity GUID")
    name: str = Field(..., description="Entity name")
    type: str = Field(..., description="Entity type")
    qualified_name: str = Field(..., description="Qualified name")
    display_name: Optional[str] = Field(None, description="Display name")
    status: str = Field("active", description="Entity status")
    
    # Node metadata
    level: int = Field(..., description="Distance from root node")
    is_process: bool = Field(False, description="Whether this is a process node")
    classifications: List[str] = Field(default_factory=list, description="Classifications")
    owner: Optional[str] = Field(None, description="Entity owner")
    
    # Quality and freshness
    quality_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Data quality score")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    
    # Impact metrics
    impact_score: Optional[float] = Field(None, ge=0.0, description="Impact score")
    downstream_count: Optional[int] = Field(None, description="Number of downstream dependencies")


class LineageEdge(BaseModel):
    """Edge in lineage graph."""
    from_guid: str = Field(..., description="Source entity GUID")
    to_guid: str = Field(..., description="Target entity GUID")
    process_guid: Optional[str] = Field(None, description="Process entity GUID")
    relationship_type: str = Field(..., description="Relationship type")
    
    # Edge metadata
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Edge attributes")
    created_at: Optional[datetime] = Field(None, description="Edge creation time")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Lineage confidence")
    
    # Data flow details
    columns_mapping: Optional[Dict[str, List[str]]] = Field(None, description="Column-level lineage")
    transformation: Optional[str] = Field(None, description="Transformation applied")


class LineageCreateRequest(BaseModel):
    """Request model for creating lineage."""
    process_name: str = Field(..., description="Process name")
    process_type: ProcessType = Field(..., description="Process type")
    inputs: List[str] = Field(..., min_items=1, description="Input entity GUIDs")
    outputs: List[str] = Field(..., min_items=1, description="Output entity GUIDs")
    
    # Process metadata
    process_qualified_name: Optional[str] = Field(None, description="Process qualified name")
    description: Optional[str] = Field(None, description="Process description")
    owner: Optional[str] = Field(None, description="Process owner")
    
    # Execution details
    execution_time: Optional[datetime] = Field(None, description="Execution timestamp")
    duration_ms: Optional[int] = Field(None, ge=0, description="Execution duration in ms")
    
    # Additional metadata
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    column_lineage: Optional[Dict[str, Dict[str, List[str]]]] = Field(None, description="Column-level lineage")
    
    @validator('inputs', 'outputs')
    def validate_guid_lists(cls, v):
        if len(set(v)) != len(v):
            raise ValueError('Duplicate GUIDs not allowed')
        return v


class LineageResponse(BaseModel):
    """Response model for lineage data."""
    guid: str = Field(..., description="Lineage/Process GUID")
    process_name: str = Field(..., description="Process name")
    process_type: ProcessType = Field(..., description="Process type")
    
    # Process details
    qualified_name: str = Field(..., description="Process qualified name")
    description: Optional[str] = Field(None, description="Process description")
    owner: Optional[str] = Field(None, description="Process owner")
    status: str = Field(..., description="Process status")
    
    # Lineage connections
    inputs: List[Dict[str, Any]] = Field(..., description="Input entities")
    outputs: List[Dict[str, Any]] = Field(..., description="Output entities")
    
    # Execution info
    execution_time: Optional[datetime] = Field(None, description="Execution timestamp")
    duration_ms: Optional[int] = Field(None, description="Execution duration")
    
    # Metadata
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    column_lineage: Optional[Dict[str, Dict[str, List[str]]]] = Field(None, description="Column-level lineage")
    
    # Audit
    created_at: datetime = Field(..., description="Creation timestamp")
    created_by: str = Field(..., description="Creator")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class LineageGraphResponse(BaseModel):
    """Response model for lineage graph."""
    root_guid: str = Field(..., description="Root entity GUID")
    direction: LineageDirection = Field(..., description="Lineage direction")
    depth: int = Field(..., description="Graph depth")
    
    # Graph data
    nodes: List[LineageNode] = Field(..., description="Graph nodes")
    edges: List[LineageEdge] = Field(..., description="Graph edges")
    
    # Statistics
    total_nodes: int = Field(..., description="Total number of nodes")
    total_edges: int = Field(..., description="Total number of edges")
    max_path_length: int = Field(..., description="Longest path in graph")
    
    # Metadata
    query_time_ms: int = Field(..., description="Query execution time")
    truncated: bool = Field(False, description="Whether results were truncated")
    
    @validator('nodes')
    def validate_root_exists(cls, v, values):
        if 'root_guid' in values:
            root_guid = values['root_guid']
            if not any(node.guid == root_guid for node in v):
                raise ValueError('Root node must be in nodes list')
        return v


class ImpactAnalysisRequest(BaseModel):
    """Request model for impact analysis."""
    entity_guid: str = Field(..., description="Entity to analyze")
    change_type: str = Field(..., description="Type of change")
    changes: Optional[List[Dict[str, Any]]] = Field(None, description="Specific changes")
    max_depth: int = Field(5, ge=1, le=10, description="Maximum analysis depth")
    include_indirect: bool = Field(True, description="Include indirect impacts")
    
    # Analysis options
    analyze_quality: bool = Field(True, description="Analyze quality impact")
    analyze_freshness: bool = Field(True, description="Analyze data freshness impact")
    analyze_schema: bool = Field(True, description="Analyze schema compatibility")


class ImpactAnalysisResponse(BaseModel):
    """Response model for impact analysis."""
    entity_guid: str = Field(..., description="Analyzed entity")
    change_type: str = Field(..., description="Type of change")
    
    # Impact summary
    total_impacted: int = Field(..., description="Total impacted entities")
    critical_impacts: int = Field(..., description="Critical impact count")
    direct_impacts: int = Field(..., description="Direct impact count")
    indirect_impacts: int = Field(..., description="Indirect impact count")
    
    # Impacted entities
    impacted_entities: List[Dict[str, Any]] = Field(..., description="Impacted entities with details")
    impact_paths: List[List[str]] = Field(..., description="Paths to impacted entities")
    
    # Impact details
    schema_impacts: Optional[List[Dict[str, Any]]] = Field(None, description="Schema compatibility issues")
    quality_impacts: Optional[List[Dict[str, Any]]] = Field(None, description="Quality impact details")
    freshness_impacts: Optional[List[Dict[str, Any]]] = Field(None, description="Data freshness impacts")
    
    # Recommendations
    recommendations: List[str] = Field(default_factory=list, description="Impact mitigation recommendations")
    estimated_effort: Optional[str] = Field(None, description="Estimated remediation effort")


class TransformationTrackingRequest(BaseModel):
    """Request model for transformation tracking."""
    source_entities: List[Dict[str, Any]] = Field(..., min_items=1, description="Source entities")
    target_entities: List[Dict[str, Any]] = Field(..., min_items=1, description="Target entities")
    transformation: Dict[str, Any] = Field(..., description="Transformation details")
    execution_context: Optional[Dict[str, Any]] = Field(None, description="Execution context")
    
    # Transformation details
    transformation_type: str = Field(..., description="Type of transformation")
    transformation_logic: Optional[str] = Field(None, description="Transformation logic/code")
    business_rules: Optional[List[str]] = Field(None, description="Applied business rules")
    
    # Quality metrics
    records_processed: Optional[int] = Field(None, ge=0, description="Records processed")
    records_failed: Optional[int] = Field(None, ge=0, description="Records failed")
    quality_checks: Optional[Dict[str, Any]] = Field(None, description="Quality check results")


class TransformationTrackingResponse(BaseModel):
    """Response model for transformation tracking."""
    tracking_id: str = Field(..., description="Transformation tracking ID")
    status: str = Field(..., description="Tracking status")
    
    # Transformation summary
    transformation_type: str = Field(..., description="Type of transformation")
    source_count: int = Field(..., description="Number of source entities")
    target_count: int = Field(..., description="Number of target entities")
    
    # Lineage created
    lineage_created: List[str] = Field(..., description="Created lineage GUIDs")
    processes_created: List[str] = Field(..., description="Created process GUIDs")
    
    # Compliance tracking
    compliance_verified: bool = Field(..., description="Compliance verification status")
    audit_trail_id: Optional[str] = Field(None, description="Audit trail ID")
    
    # Metrics
    execution_time_ms: int = Field(..., description="Execution time")
    timestamp: datetime = Field(..., description="Tracking timestamp")


class ComplianceAuditRequest(BaseModel):
    """Request model for compliance audit trail."""
    entity_guid: str = Field(..., description="Entity GUID")
    start_date: Optional[datetime] = Field(None, description="Start date")
    end_date: Optional[datetime] = Field(None, description="End date")
    include_lineage: bool = Field(True, description="Include lineage information")
    compliance_types: Optional[List[str]] = Field(None, description="Compliance types to check")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        if v and 'start_date' in values and values['start_date'] and v < values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v


class ComplianceAuditResponse(BaseModel):
    """Response model for compliance audit trail."""
    entity_guid: str = Field(..., description="Entity GUID")
    entity_name: str = Field(..., description="Entity name")
    
    # Audit trail
    audit_events: List[Dict[str, Any]] = Field(..., description="Audit events")
    total_events: int = Field(..., description="Total audit events")
    
    # Lineage audit
    lineage_events: Optional[List[Dict[str, Any]]] = Field(None, description="Lineage-related events")
    data_movements: Optional[List[Dict[str, Any]]] = Field(None, description="Data movement history")
    
    # Compliance status
    compliance_status: Dict[str, bool] = Field(..., description="Compliance check results")
    violations: List[Dict[str, Any]] = Field(default_factory=list, description="Compliance violations")
    
    # Data handling
    retention_compliance: Optional[Dict[str, Any]] = Field(None, description="Retention policy compliance")
    access_history: Optional[List[Dict[str, Any]]] = Field(None, description="Access history")
    
    # Report metadata
    report_generated_at: datetime = Field(default_factory=datetime.utcnow, description="Report timestamp")
    report_period: Optional[Dict[str, datetime]] = Field(None, description="Report period") 