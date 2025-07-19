"""
Feature Store Data Models
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
import json


class FeatureType(str, Enum):
    """Feature data types"""
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    MAP = "map"
    EMBEDDING = "embedding"


class FeatureTransformType(str, Enum):
    """Feature transformation types"""
    NONE = "none"
    NORMALIZE = "normalize"
    STANDARDIZE = "standardize"
    ONE_HOT = "one_hot"
    HASH = "hash"
    BUCKET = "bucket"
    EMBEDDING = "embedding"
    CUSTOM = "custom"


class FeatureSourceType(str, Enum):
    """Feature source types"""
    BATCH = "batch"
    STREAM = "stream"
    REQUEST = "request"
    FEDERATED = "federated"


class Feature(BaseModel):
    """Individual feature definition"""
    name: str = Field(..., description="Feature name")
    description: Optional[str] = Field(None, description="Feature description")
    type: FeatureType = Field(..., description="Feature data type")
    
    # Transformation
    transform: FeatureTransformType = Field(FeatureTransformType.NONE, description="Transformation type")
    transform_params: Optional[Dict[str, Any]] = Field(None, description="Transformation parameters")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Feature tags")
    owner: Optional[str] = Field(None, description="Feature owner")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Quality constraints
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Quality constraints")
    default_value: Optional[Any] = Field(None, description="Default value for missing data")
    
    # Lineage
    source_columns: List[str] = Field(default_factory=list, description="Source columns")
    derivation: Optional[str] = Field(None, description="Derivation expression")


class FeatureVersion(BaseModel):
    """Versioned feature data"""
    version: int = Field(..., description="Version number")
    feature_name: str = Field(..., description="Feature name")
    
    # Version metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = Field(..., description="Creator")
    description: Optional[str] = Field(None, description="Version description")
    
    # Data location
    offline_path: str = Field(..., description="Path to offline data")
    online_table: Optional[str] = Field(None, description="Online feature table")
    
    # Statistics
    statistics: Dict[str, Any] = Field(default_factory=dict, description="Feature statistics")
    row_count: int = Field(0, description="Number of rows")
    
    # Quality metrics
    quality_score: float = Field(1.0, description="Quality score")
    completeness: float = Field(1.0, description="Data completeness")
    
    # Schema evolution
    schema_changes: List[Dict[str, Any]] = Field(default_factory=list, description="Schema changes from previous version")
    is_compatible: bool = Field(True, description="Backward compatibility")


class FeatureGroup(BaseModel):
    """Group of related features"""
    name: str = Field(..., description="Feature group name")
    description: Optional[str] = Field(None, description="Description")
    
    # Features
    features: List[Feature] = Field(default_factory=list, description="Features in group")
    primary_keys: List[str] = Field(..., description="Primary key columns")
    event_time_column: Optional[str] = Field(None, description="Event time column for time-travel")
    
    # Source configuration
    source_type: FeatureSourceType = Field(..., description="Source type")
    source_query: Optional[str] = Field(None, description="Source query for batch features")
    source_stream: Optional[str] = Field(None, description="Source stream for streaming features")
    
    # Storage configuration
    offline_store: Dict[str, Any] = Field(default_factory=dict, description="Offline store config")
    online_store: Optional[Dict[str, Any]] = Field(None, description="Online store config")
    
    # Versioning
    versioning_enabled: bool = Field(True, description="Enable versioning")
    retention_days: int = Field(30, description="Version retention in days")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Tags")
    owner: str = Field(..., description="Owner")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Monitoring
    monitoring_enabled: bool = Field(True, description="Enable monitoring")
    alert_thresholds: Dict[str, Any] = Field(default_factory=dict, description="Alert thresholds")


class FeatureSet(BaseModel):
    """Set of features for training or serving"""
    name: str = Field(..., description="Feature set name")
    description: Optional[str] = Field(None, description="Description")
    
    # Feature selection
    feature_groups: List[str] = Field(..., description="Feature groups to include")
    features: List[str] = Field(..., description="Selected features")
    
    # Join configuration
    entity_keys: List[str] = Field(..., description="Entity keys for joining")
    join_type: str = Field("inner", description="Join type")
    
    # Time configuration
    timestamp_column: Optional[str] = Field(None, description="Timestamp column")
    max_age_seconds: Optional[int] = Field(None, description="Maximum feature age")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str = Field(..., description="Creator")
    use_case: Optional[str] = Field(None, description="Use case description")


class FeatureView(BaseModel):
    """Materialized feature view for serving"""
    name: str = Field(..., description="View name")
    description: Optional[str] = Field(None, description="Description")
    
    # Source configuration
    feature_set: str = Field(..., description="Source feature set")
    query: str = Field(..., description="Query to generate view")
    
    # Materialization
    materialization_enabled: bool = Field(True, description="Enable materialization")
    refresh_schedule: Optional[str] = Field(None, description="Refresh schedule (cron)")
    ttl_seconds: Optional[int] = Field(None, description="TTL for cached features")
    
    # Online serving
    online_enabled: bool = Field(False, description="Enable online serving")
    cache_config: Optional[Dict[str, Any]] = Field(None, description="Cache configuration")
    
    # Monitoring
    sla_seconds: Optional[float] = Field(None, description="SLA for serving latency")
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    last_refreshed: Optional[datetime] = Field(None, description="Last refresh time")
    
    
class FeatureRequest(BaseModel):
    """Request for feature serving"""
    entities: Dict[str, Any] = Field(..., description="Entity values")
    features: List[str] = Field(..., description="Requested features")
    timestamp: Optional[datetime] = Field(None, description="Point-in-time for features")
    include_metadata: bool = Field(False, description="Include feature metadata")


class FeatureResponse(BaseModel):
    """Response from feature serving"""
    features: Dict[str, Any] = Field(..., description="Feature values")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Feature metadata")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    latency_ms: float = Field(..., description="Serving latency") 