"""
Catalog Domain Events

Events emitted by catalog operations.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

from .event_bus import DomainEvent


# Entity Events
@dataclass
class EntityCreated(DomainEvent):
    """Emitted when a new entity is created"""
    entity_id: str = ""
    entity_type: str = ""
    name: str = ""
    qualified_name: str = ""
    owner: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.entity_id


@dataclass
class EntityUpdated(DomainEvent):
    """Emitted when an entity is updated"""
    entity_id: str = ""
    entity_type: str = ""
    changed_attributes: Dict[str, Any] = field(default_factory=dict)
    previous_values: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.entity_id


@dataclass
class EntityDeleted(DomainEvent):
    """Emitted when an entity is deleted"""
    entity_id: str = ""
    entity_type: str = ""
    soft_delete: bool = True
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.entity_id


@dataclass
class EntityClassified(DomainEvent):
    """Emitted when classifications are added to an entity"""
    entity_id: str = ""
    classifications: List[str] = field(default_factory=list)
    confidence_scores: Dict[str, float] = field(default_factory=dict)
    auto_classified: bool = False
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.entity_id


# Schema Events
@dataclass
class SchemaRegistered(DomainEvent):
    """Emitted when a new schema is registered"""
    schema_id: str = ""
    schema_name: str = ""
    schema_type: str = ""
    version: int = 1
    compatibility_mode: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.schema_id


@dataclass
class SchemaUpdated(DomainEvent):
    """Emitted when a schema is updated"""
    schema_id: str = ""
    previous_version: int = 0
    new_version: int = 1
    compatibility_check_passed: bool = True
    breaking_changes: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.schema_id


# Lineage Events
@dataclass
class LineageCreated(DomainEvent):
    """Emitted when lineage relationship is created"""
    lineage_id: str = ""
    process_id: str = ""
    process_type: str = ""
    input_entities: List[str] = field(default_factory=list)
    output_entities: List[str] = field(default_factory=list)
    execution_time: Optional[datetime] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.lineage_id


@dataclass 
class LineageUpdated(DomainEvent):
    """Emitted when lineage is updated"""
    lineage_id: str = ""
    process_id: str = ""
    added_inputs: List[str] = field(default_factory=list)
    removed_inputs: List[str] = field(default_factory=list)
    added_outputs: List[str] = field(default_factory=list)
    removed_outputs: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.lineage_id


# Glossary Events
@dataclass
class GlossaryTermCreated(DomainEvent):
    """Emitted when a glossary term is created"""
    term_id: str = ""
    term_name: str = ""
    definition: str = ""
    category: str = ""
    status: str = "draft"
    created_by: str = ""
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.term_id


@dataclass
class GlossaryTermMapped(DomainEvent):
    """Emitted when a term is mapped to technical assets"""
    term_id: str = ""
    asset_id: str = ""
    mapping_type: str = ""  # direct, inferred, suggested
    confidence: float = 0.0
    auto_mapped: bool = False
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.term_id


# Quality Events
@dataclass
class QualityAssessed(DomainEvent):
    """Emitted when data quality is assessed"""
    dataset_id: str = ""
    overall_score: float = 0.0
    trust_level: str = ""
    dimension_scores: Dict[str, float] = field(default_factory=dict)
    issues_found: int = 0
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.dataset_id


# Access Events
@dataclass
class AccessTracked(DomainEvent):
    """Emitted when data access is tracked"""
    user_id: str = ""
    asset_id: str = ""
    asset_type: str = ""
    access_type: str = ""  # view, query, download, etc.
    duration_ms: int = 0
    session_id: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        self.aggregate_id = self.asset_id 