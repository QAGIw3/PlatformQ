"""
Entity Domain Model

Core domain model for catalog entities.
"""

from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class EntityStatus(str, Enum):
    """Entity status values"""
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"
    PURGED = "PURGED"


@dataclass
class Entity:
    """
    Domain model representing a catalog entity.
    
    This is the core aggregate root for catalog operations.
    """
    
    # Identity
    guid: str
    type_name: str
    
    # Core attributes
    qualified_name: str
    name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    
    # Metadata
    status: EntityStatus = EntityStatus.ACTIVE
    created_time: Optional[datetime] = None
    created_by: Optional[str] = None
    modified_time: Optional[datetime] = None
    modified_by: Optional[str] = None
    version: int = 1
    
    # Relationships
    classifications: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    glossary_terms: List[str] = field(default_factory=list)
    
    # Custom attributes
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    # Computed properties
    _is_dirty: bool = field(default=False, init=False)
    _original_values: Dict[str, Any] = field(default_factory=dict, init=False)
    
    def __post_init__(self):
        """Initialize tracking of changes"""
        self._original_values = self.to_dict()
        
    @property
    def is_new(self) -> bool:
        """Check if this is a new entity"""
        return self.created_time is None
        
    @property
    def is_deleted(self) -> bool:
        """Check if entity is deleted"""
        return self.status == EntityStatus.DELETED
        
    def update_attribute(self, name: str, value: Any):
        """Update a single attribute"""
        if name in ['guid', 'type_name']:
            raise ValueError(f"Cannot update {name}")
            
        old_value = getattr(self, name, None)
        if old_value != value:
            setattr(self, name, value)
            self._is_dirty = True
            
    def update_attributes(self, updates: Dict[str, Any]):
        """Update multiple attributes"""
        for name, value in updates.items():
            self.update_attribute(name, value)
            
    def add_classification(self, classification: str):
        """Add a classification"""
        if classification not in self.classifications:
            self.classifications.append(classification)
            self._is_dirty = True
            
    def remove_classification(self, classification: str):
        """Remove a classification"""
        if classification in self.classifications:
            self.classifications.remove(classification)
            self._is_dirty = True
            
    def add_tag(self, tag: str):
        """Add a tag"""
        if tag not in self.tags:
            self.tags.append(tag)
            self._is_dirty = True
            
    def remove_tag(self, tag: str):
        """Remove a tag"""
        if tag in self.tags:
            self.tags.remove(tag)
            self._is_dirty = True
            
    def mark_deleted(self):
        """Soft delete the entity"""
        self.status = EntityStatus.DELETED
        self._is_dirty = True
        
    def get_changes(self) -> Dict[str, Any]:
        """Get changed attributes"""
        if not self._is_dirty:
            return {}
            
        changes = {}
        current = self.to_dict()
        
        for key, value in current.items():
            if key.startswith('_'):
                continue
            original_value = self._original_values.get(key)
            if original_value != value:
                changes[key] = {
                    'old': original_value,
                    'new': value
                }
                
        return changes
        
    def commit_changes(self):
        """Mark changes as committed"""
        self._original_values = self.to_dict()
        self._is_dirty = False
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'guid': self.guid,
            'type_name': self.type_name,
            'qualified_name': self.qualified_name,
            'name': self.name,
            'description': self.description,
            'owner': self.owner,
            'status': self.status.value,
            'created_time': self.created_time.isoformat() if self.created_time else None,
            'created_by': self.created_by,
            'modified_time': self.modified_time.isoformat() if self.modified_time else None,
            'modified_by': self.modified_by,
            'version': self.version,
            'classifications': self.classifications.copy(),
            'tags': self.tags.copy(),
            'glossary_terms': self.glossary_terms.copy(),
            'attributes': self.attributes.copy()
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """Create from dictionary"""
        # Parse datetime fields
        if data.get('created_time'):
            data['created_time'] = datetime.fromisoformat(data['created_time'])
        if data.get('modified_time'):
            data['modified_time'] = datetime.fromisoformat(data['modified_time'])
            
        # Parse status
        if data.get('status'):
            data['status'] = EntityStatus(data['status'])
            
        return cls(**data)
        
    def validate(self) -> List[str]:
        """Validate the entity"""
        errors = []
        
        if not self.guid:
            errors.append("GUID is required")
        if not self.type_name:
            errors.append("Type name is required")
        if not self.qualified_name:
            errors.append("Qualified name is required")
        if not self.name:
            errors.append("Name is required")
            
        return errors 