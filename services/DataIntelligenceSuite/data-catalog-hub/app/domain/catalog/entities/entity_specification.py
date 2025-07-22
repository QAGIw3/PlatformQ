"""
Entity Specifications

Implements the specification pattern for entity queries.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from .entity import Entity, EntityStatus


class EntitySpecification(ABC):
    """Base specification interface"""
    
    @abstractmethod
    def is_satisfied_by(self, entity: Entity) -> bool:
        """Check if entity satisfies this specification"""
        pass
    
    def and_(self, other: 'EntitySpecification') -> 'CompositeSpecification':
        """Combine with AND logic"""
        return AndSpecification(self, other)
    
    def or_(self, other: 'EntitySpecification') -> 'CompositeSpecification':
        """Combine with OR logic"""
        return OrSpecification(self, other)
    
    def not_(self) -> 'NotSpecification':
        """Negate this specification"""
        return NotSpecification(self)


class CompositeSpecification(EntitySpecification):
    """Base class for composite specifications"""
    
    def __init__(self, left: EntitySpecification, right: EntitySpecification):
        self.left = left
        self.right = right


class AndSpecification(CompositeSpecification):
    """AND combination of specifications"""
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return self.left.is_satisfied_by(entity) and self.right.is_satisfied_by(entity)


class OrSpecification(CompositeSpecification):
    """OR combination of specifications"""
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return self.left.is_satisfied_by(entity) or self.right.is_satisfied_by(entity)


class NotSpecification(EntitySpecification):
    """NOT specification"""
    
    def __init__(self, specification: EntitySpecification):
        self.specification = specification
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return not self.specification.is_satisfied_by(entity)


# Concrete specifications

@dataclass
class TypeSpecification(EntitySpecification):
    """Filter by entity type"""
    type_name: str
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return entity.type_name == self.type_name


@dataclass
class StatusSpecification(EntitySpecification):
    """Filter by entity status"""
    status: EntityStatus
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return entity.status == self.status


@dataclass
class OwnerSpecification(EntitySpecification):
    """Filter by owner"""
    owner: str
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return entity.owner == self.owner


@dataclass
class ClassificationSpecification(EntitySpecification):
    """Filter by classification"""
    classification: str
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return self.classification in entity.classifications


@dataclass
class TagSpecification(EntitySpecification):
    """Filter by tag"""
    tag: str
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return self.tag in entity.tags


@dataclass
class UpdatedAfterSpecification(EntitySpecification):
    """Filter by update time"""
    since: datetime
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        if entity.modified_time:
            return entity.modified_time > self.since
        return False


@dataclass
class NameContainsSpecification(EntitySpecification):
    """Filter by name containing text"""
    text: str
    case_sensitive: bool = False
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        if self.case_sensitive:
            return self.text in entity.name
        return self.text.lower() in entity.name.lower()


@dataclass
class AttributeSpecification(EntitySpecification):
    """Filter by custom attribute value"""
    attribute_name: str
    expected_value: Any
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return entity.attributes.get(self.attribute_name) == self.expected_value


@dataclass
class ActiveEntitiesSpecification(EntitySpecification):
    """Filter for active entities only"""
    
    def is_satisfied_by(self, entity: Entity) -> bool:
        return entity.status == EntityStatus.ACTIVE


# Helper factory functions

def active_entities() -> EntitySpecification:
    """Get specification for active entities"""
    return ActiveEntitiesSpecification()


def by_type(type_name: str) -> EntitySpecification:
    """Get specification for entity type"""
    return TypeSpecification(type_name)


def by_owner(owner: str) -> EntitySpecification:
    """Get specification for owner"""
    return OwnerSpecification(owner)


def with_classification(classification: str) -> EntitySpecification:
    """Get specification for classification"""
    return ClassificationSpecification(classification)


def with_tag(tag: str) -> EntitySpecification:
    """Get specification for tag"""
    return TagSpecification(tag)


def updated_after(since: datetime) -> EntitySpecification:
    """Get specification for recently updated"""
    return UpdatedAfterSpecification(since) 