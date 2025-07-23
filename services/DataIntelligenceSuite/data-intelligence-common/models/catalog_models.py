"""
Data catalog models.

Provides models for data catalog entries, metadata, and lineage.
"""

import uuid
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field

from .base_models import TimestampedModel, VersionedModel, AuditedModel


class AssetType(str, Enum):
    """Types of catalog assets"""
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    DATABASE = "database"
    SCHEMA = "schema"
    DATASET = "dataset"
    MODEL = "model"
    DASHBOARD = "dashboard"
    REPORT = "report"
    PIPELINE = "pipeline"
    NOTEBOOK = "notebook"
    FEATURE = "feature"
    METRIC = "metric"
    UNKNOWN = "unknown"


class AssetStatus(str, Enum):
    """Asset lifecycle status"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    DELETED = "deleted"


class ClassificationType(str, Enum):
    """Data classification types"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"
    PHI = "phi"
    PCI = "pci"


class LineageType(str, Enum):
    """Types of lineage relationships"""
    DATA_FLOW = "data_flow"
    DERIVES_FROM = "derives_from"
    COPIES_FROM = "copies_from"
    TRANSFORMS_TO = "transforms_to"
    AGGREGATES_FROM = "aggregates_from"
    JOINS_WITH = "joins_with"
    FILTERS_FROM = "filters_from"
    SAMPLES_FROM = "samples_from"
    VERSION_OF = "version_of"
    REPLACES = "replaces"


class AccessLevel(str, Enum):
    """Access control levels"""
    NONE = "none"
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"


@dataclass
class Tag:
    """Asset tag"""
    key: str
    value: Optional[str] = None
    category: Optional[str] = None
    
    def __str__(self) -> str:
        if self.value:
            return f"{self.key}:{self.value}"
        return self.key
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "key": self.key,
            "value": self.value,
            "category": self.category
        }


@dataclass
class Classification:
    """Data classification"""
    type: ClassificationType
    level: Optional[int] = None
    reason: Optional[str] = None
    classified_by: Optional[str] = None
    classified_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    
    def is_expired(self) -> bool:
        """Check if classification is expired"""
        if self.expires_at:
            return datetime.utcnow() > self.expires_at
        return False
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "type": self.type.value,
            "level": self.level,
            "reason": self.reason,
            "classified_by": self.classified_by,
            "classified_at": self.classified_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None
        }


@dataclass
class SchemaField:
    """Schema field definition"""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    default_value: Optional[Any] = None
    is_primary_key: bool = False
    is_partition_key: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "description": self.description,
            "default_value": self.default_value,
            "is_primary_key": self.is_primary_key,
            "is_partition_key": self.is_partition_key,
            "metadata": self.metadata
        }


@dataclass
class AssetSchema:
    """Asset schema definition"""
    fields: List[SchemaField] = field(default_factory=list)
    version: str = "1.0"
    format: Optional[str] = None  # avro, parquet, json, etc.
    
    def get_field(self, name: str) -> Optional[SchemaField]:
        """Get field by name"""
        for field in self.fields:
            if field.name == name:
                return field
        return None
        
    def get_primary_keys(self) -> List[SchemaField]:
        """Get primary key fields"""
        return [f for f in self.fields if f.is_primary_key]
        
    def get_partition_keys(self) -> List[SchemaField]:
        """Get partition key fields"""
        return [f for f in self.fields if f.is_partition_key]
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "fields": [f.to_dict() for f in self.fields],
            "version": self.version,
            "format": self.format
        }


@dataclass
class AssetMetadata(TimestampedModel):
    """Extended metadata for catalog assets"""
    # Basic info
    description: Optional[str] = None
    documentation_url: Optional[str] = None
    
    # Ownership
    owner: Optional[str] = None
    team: Optional[str] = None
    steward: Optional[str] = None
    
    # Technical metadata
    location: Optional[str] = None
    connection_info: Dict[str, Any] = field(default_factory=dict)
    properties: Dict[str, Any] = field(default_factory=dict)
    
    # Business metadata
    business_glossary: List[str] = field(default_factory=list)
    use_cases: List[str] = field(default_factory=list)
    
    # Quality
    quality_score: Optional[float] = None
    completeness: Optional[float] = None
    
    # Usage
    popularity_score: Optional[float] = None
    last_accessed: Optional[datetime] = None
    access_count: int = 0
    
    # Cost
    storage_cost: Optional[float] = None
    compute_cost: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "description": self.description,
            "documentation_url": self.documentation_url,
            "owner": self.owner,
            "team": self.team,
            "steward": self.steward,
            "location": self.location,
            "connection_info": self.connection_info,
            "properties": self.properties,
            "business_glossary": self.business_glossary,
            "use_cases": self.use_cases,
            "quality_score": self.quality_score,
            "completeness": self.completeness,
            "popularity_score": self.popularity_score,
            "last_accessed": self.last_accessed.isoformat() if self.last_accessed else None,
            "access_count": self.access_count,
            "storage_cost": self.storage_cost,
            "compute_cost": self.compute_cost,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class Lineage:
    """Lineage relationship"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str = ""
    target_id: str = ""
    lineage_type: LineageType = LineageType.DATA_FLOW
    
    # Relationship metadata
    confidence: float = 1.0
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    # Additional info
    transformation: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "lineage_type": self.lineage_type.value,
            "confidence": self.confidence,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "transformation": self.transformation,
            "properties": self.properties
        }


@dataclass
class CatalogEntry(AuditedModel):
    """Main catalog entry"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    qualified_name: str = ""  # Fully qualified name
    asset_type: AssetType = AssetType.UNKNOWN
    status: AssetStatus = AssetStatus.DRAFT
    
    # Metadata
    metadata: AssetMetadata = field(default_factory=AssetMetadata)
    
    # Schema
    schema: Optional[AssetSchema] = None
    
    # Classification & Tags
    classifications: List[Classification] = field(default_factory=list)
    tags: List[Tag] = field(default_factory=list)
    
    # Lineage
    upstream_lineage: List[Lineage] = field(default_factory=list)
    downstream_lineage: List[Lineage] = field(default_factory=list)
    
    # Access control
    access_control: Dict[str, AccessLevel] = field(default_factory=dict)
    
    # Versioning
    version: str = "1.0"
    previous_versions: List[str] = field(default_factory=list)
    
    def add_tag(self, tag: Tag):
        """Add tag to entry"""
        if tag not in self.tags:
            self.tags.append(tag)
            
    def remove_tag(self, key: str, value: Optional[str] = None):
        """Remove tag from entry"""
        self.tags = [
            t for t in self.tags
            if not (t.key == key and (value is None or t.value == value))
        ]
        
    def add_classification(self, classification: Classification):
        """Add classification to entry"""
        # Remove existing classification of same type
        self.classifications = [
            c for c in self.classifications
            if c.type != classification.type
        ]
        self.classifications.append(classification)
        
    def get_classification(self, classification_type: ClassificationType) -> Optional[Classification]:
        """Get classification by type"""
        for c in self.classifications:
            if c.type == classification_type and not c.is_expired():
                return c
        return None
        
    def add_upstream_lineage(self, lineage: Lineage):
        """Add upstream lineage"""
        lineage.target_id = self.id
        self.upstream_lineage.append(lineage)
        
    def add_downstream_lineage(self, lineage: Lineage):
        """Add downstream lineage"""
        lineage.source_id = self.id
        self.downstream_lineage.append(lineage)
        
    def grant_access(self, principal: str, level: AccessLevel):
        """Grant access to principal"""
        self.access_control[principal] = level
        
    def revoke_access(self, principal: str):
        """Revoke access from principal"""
        self.access_control.pop(principal, None)
        
    def check_access(self, principal: str, required_level: AccessLevel) -> bool:
        """Check if principal has required access level"""
        granted_level = self.access_control.get(principal, AccessLevel.NONE)
        
        # Access levels are hierarchical
        level_hierarchy = {
            AccessLevel.NONE: 0,
            AccessLevel.READ: 1,
            AccessLevel.WRITE: 2,
            AccessLevel.DELETE: 3,
            AccessLevel.ADMIN: 4
        }
        
        return level_hierarchy.get(granted_level, 0) >= level_hierarchy.get(required_level, 0)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "qualified_name": self.qualified_name,
            "asset_type": self.asset_type.value,
            "status": self.status.value,
            "metadata": self.metadata.to_dict(),
            "schema": self.schema.to_dict() if self.schema else None,
            "classifications": [c.to_dict() for c in self.classifications],
            "tags": [t.to_dict() for t in self.tags],
            "upstream_lineage": [l.to_dict() for l in self.upstream_lineage],
            "downstream_lineage": [l.to_dict() for l in self.downstream_lineage],
            "access_control": {k: v.value for k, v in self.access_control.items()},
            "version": self.version,
            "previous_versions": self.previous_versions,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "is_deleted": self.is_deleted
        }


@dataclass
class DataProfile:
    """Statistical profile of data asset"""
    asset_id: str
    profiled_at: datetime = field(default_factory=datetime.utcnow)
    
    # Size metrics
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    size_bytes: Optional[int] = None
    
    # Column profiles
    column_profiles: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Data quality
    null_count: Dict[str, int] = field(default_factory=dict)
    unique_count: Dict[str, int] = field(default_factory=dict)
    
    # Statistics
    numeric_stats: Dict[str, Dict[str, float]] = field(default_factory=dict)
    string_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Patterns
    data_patterns: Dict[str, List[str]] = field(default_factory=dict)
    
    def add_column_profile(self, column: str, profile: Dict[str, Any]):
        """Add column profile"""
        self.column_profiles[column] = profile
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "asset_id": self.asset_id,
            "profiled_at": self.profiled_at.isoformat(),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "size_bytes": self.size_bytes,
            "column_profiles": self.column_profiles,
            "null_count": self.null_count,
            "unique_count": self.unique_count,
            "numeric_stats": self.numeric_stats,
            "string_stats": self.string_stats,
            "data_patterns": self.data_patterns
        }


@dataclass
class GlossaryTerm:
    """Business glossary term"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    term: str = ""
    definition: str = ""
    
    # Categorization
    category: Optional[str] = None
    domain: Optional[str] = None
    
    # Relationships
    synonyms: List[str] = field(default_factory=list)
    related_terms: List[str] = field(default_factory=list)
    parent_term: Optional[str] = None
    
    # Usage
    examples: List[str] = field(default_factory=list)
    
    # Metadata
    owner: Optional[str] = None
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    
    # Linked assets
    linked_assets: List[str] = field(default_factory=list)
    
    def add_synonym(self, synonym: str):
        """Add synonym"""
        if synonym not in self.synonyms:
            self.synonyms.append(synonym)
            
    def add_related_term(self, term: str):
        """Add related term"""
        if term not in self.related_terms:
            self.related_terms.append(term)
            
    def link_asset(self, asset_id: str):
        """Link asset to term"""
        if asset_id not in self.linked_assets:
            self.linked_assets.append(asset_id)
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "term": self.term,
            "definition": self.definition,
            "category": self.category,
            "domain": self.domain,
            "synonyms": self.synonyms,
            "related_terms": self.related_terms,
            "parent_term": self.parent_term,
            "examples": self.examples,
            "owner": self.owner,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "linked_assets": self.linked_assets
        } 