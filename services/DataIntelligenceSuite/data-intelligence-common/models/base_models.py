"""
Base Models for Data Intelligence

Provides base model classes with common functionality.
"""

from typing import Any, Dict, Optional, List
from datetime import datetime
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import uuid
from enum import Enum


class ModelMixin:
    """Base mixin for all models"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary"""
        result = {}
        for key, value in self.__dict__.items():
            if not key.startswith('_'):
                if hasattr(value, 'to_dict'):
                    result[key] = value.to_dict()
                elif isinstance(value, datetime):
                    result[key] = value.isoformat()
                elif isinstance(value, Enum):
                    result[key] = value.value
                elif isinstance(value, list):
                    result[key] = [
                        item.to_dict() if hasattr(item, 'to_dict') else item
                        for item in value
                    ]
                else:
                    result[key] = value
        return result
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelMixin':
        """Create model from dictionary"""
        # This is a simplified implementation
        # Actual implementation would handle type conversions
        return cls(**data)


@dataclass
class BaseModel(ModelMixin):
    """Base model with ID"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())


@dataclass
class TimestampedModel(BaseModel):
    """Model with timestamp fields"""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def update_timestamp(self):
        """Update the updated_at timestamp"""
        self.updated_at = datetime.utcnow()


@dataclass
class VersionedModel(TimestampedModel):
    """Model with version tracking"""
    version: int = 1
    version_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def increment_version(self):
        """Increment version and save to history"""
        # Save current state to history
        self.version_history.append({
            "version": self.version,
            "timestamp": self.updated_at,
            "data": self.to_dict()
        })
        
        # Increment version
        self.version += 1
        self.update_timestamp()


@dataclass
class AuditedModel(VersionedModel):
    """Model with audit fields"""
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    deleted_at: Optional[datetime] = None
    deleted_by: Optional[str] = None
    
    def soft_delete(self, user: str):
        """Soft delete the model"""
        self.deleted_at = datetime.utcnow()
        self.deleted_by = user
        self.update_timestamp()
        
    def restore(self):
        """Restore soft deleted model"""
        self.deleted_at = None
        self.deleted_by = None
        self.update_timestamp()
        
    @property
    def is_deleted(self) -> bool:
        """Check if model is soft deleted"""
        return self.deleted_at is not None 