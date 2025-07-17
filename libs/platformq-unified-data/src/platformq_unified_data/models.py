"""Base model classes and field definitions for unified data access"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union
from uuid import uuid4
import json
from dataclasses import dataclass, field
from enum import Enum


class FieldType(Enum):
    """Supported field types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    UUID = "uuid"
    JSON = "json"
    BINARY = "binary"
    LIST = "list"
    DICT = "dict"


@dataclass
class Field:
    """Field definition for models"""
    name: str
    field_type: FieldType
    primary_key: bool = False
    required: bool = False
    indexed: bool = False
    unique: bool = False
    default: Any = None
    auto_now_add: bool = False
    auto_now: bool = False
    max_length: Optional[int] = None
    choices: Optional[List[Any]] = None
    
    def validate(self, value: Any) -> Any:
        """Validate and convert field value"""
        if value is None:
            if self.required:
                raise ValueError(f"Field '{self.name}' is required")
            return self.default() if callable(self.default) else self.default
        
        # Type validation and conversion
        if self.field_type == FieldType.STRING:
            value = str(value)
            if self.max_length and len(value) > self.max_length:
                raise ValueError(f"Field '{self.name}' exceeds max length {self.max_length}")
        elif self.field_type == FieldType.INTEGER:
            value = int(value)
        elif self.field_type == FieldType.FLOAT:
            value = float(value)
        elif self.field_type == FieldType.BOOLEAN:
            value = bool(value)
        elif self.field_type == FieldType.DATETIME:
            if isinstance(value, str):
                value = datetime.fromisoformat(value)
            elif not isinstance(value, datetime):
                raise ValueError(f"Field '{self.name}' must be a datetime")
        elif self.field_type == FieldType.UUID:
            value = str(value)
        elif self.field_type == FieldType.JSON:
            if isinstance(value, str):
                value = json.loads(value)
        
        # Choice validation
        if self.choices and value not in self.choices:
            raise ValueError(f"Field '{self.name}' must be one of {self.choices}")
        
        return value


class ModelMeta(type):
    """Metaclass for models to handle field definitions"""
    
    def __new__(mcs, name, bases, namespace):
        # Collect fields from class definition
        fields = {}
        for key, value in list(namespace.items()):
            if isinstance(value, Field):
                value.name = key
                fields[key] = value
                # Remove from namespace to avoid conflicts
                del namespace[key]
        
        # Add fields dictionary to class
        namespace['_fields'] = fields
        
        # Create the class
        cls = super().__new__(mcs, name, bases, namespace)
        
        return cls


class BaseModel(metaclass=ModelMeta):
    """Base model class for all data models"""
    
    __tablename__: str = None
    __keyspace__: str = "platformq"
    __stores__: List[str] = ["cassandra"]  # Primary stores for this model
    __cache_ttl__: int = 3600  # Cache TTL in seconds
    
    def __init__(self, **kwargs):
        # Initialize fields with values
        for field_name, field_def in self._fields.items():
            if field_name in kwargs:
                value = field_def.validate(kwargs[field_name])
            else:
                if field_def.auto_now_add:
                    value = datetime.utcnow()
                elif field_def.default is not None:
                    value = field_def.default() if callable(field_def.default) else field_def.default
                else:
                    value = None
            
            setattr(self, field_name, value)
        
        # Store any extra attributes
        for key, value in kwargs.items():
            if key not in self._fields:
                setattr(self, key, value)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model instance to dictionary"""
        data = {}
        for field_name, field_def in self._fields.items():
            value = getattr(self, field_name, None)
            if value is not None:
                if field_def.field_type == FieldType.DATETIME and isinstance(value, datetime):
                    value = value.isoformat()
                elif field_def.field_type == FieldType.JSON and not isinstance(value, str):
                    value = json.dumps(value)
                data[field_name] = value
        return data
    
    def validate(self):
        """Validate all fields"""
        for field_name, field_def in self._fields.items():
            value = getattr(self, field_name, None)
            field_def.validate(value)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """Create model instance from dictionary"""
        return cls(**data)
    
    @classmethod
    def get_primary_key_field(cls) -> Optional[Field]:
        """Get the primary key field"""
        for field_def in cls._fields.values():
            if field_def.primary_key:
                return field_def
        return None
    
    @classmethod
    def get_indexed_fields(cls) -> List[Field]:
        """Get all indexed fields"""
        return [f for f in cls._fields.values() if f.indexed]
    
    def __repr__(self):
        pk_field = self.get_primary_key_field()
        if pk_field:
            pk_value = getattr(self, pk_field.name, None)
            return f"<{self.__class__.__name__} {pk_field.name}={pk_value}>"
        return f"<{self.__class__.__name__}>"


# Convenience field creators
def StringField(primary_key=False, required=False, indexed=False, unique=False, 
                max_length=None, choices=None, default=None):
    """Create a string field"""
    return Field(
        name="",  # Will be set by metaclass
        field_type=FieldType.STRING,
        primary_key=primary_key,
        required=required,
        indexed=indexed,
        unique=unique,
        max_length=max_length,
        choices=choices,
        default=default
    )


def IntegerField(primary_key=False, required=False, indexed=False, unique=False, 
                 default=None):
    """Create an integer field"""
    return Field(
        name="",
        field_type=FieldType.INTEGER,
        primary_key=primary_key,
        required=required,
        indexed=indexed,
        unique=unique,
        default=default
    )


def FloatField(primary_key=False, required=False, indexed=False, default=None):
    """Create a float field"""
    return Field(
        name="",
        field_type=FieldType.FLOAT,
        primary_key=primary_key,
        required=required,
        indexed=indexed,
        default=default
    )


def BooleanField(required=False, default=None):
    """Create a boolean field"""
    return Field(
        name="",
        field_type=FieldType.BOOLEAN,
        required=required,
        default=default
    )


def DateTimeField(primary_key=False, required=False, indexed=False, 
                  auto_now_add=False, auto_now=False, default=None):
    """Create a datetime field"""
    return Field(
        name="",
        field_type=FieldType.DATETIME,
        primary_key=primary_key,
        required=required,
        indexed=indexed,
        auto_now_add=auto_now_add,
        auto_now=auto_now,
        default=default
    )


def UUIDField(primary_key=False, required=False, indexed=False, unique=False,
              default=None):
    """Create a UUID field"""
    if default is None and (primary_key or required):
        default = lambda: str(uuid4())
    
    return Field(
        name="",
        field_type=FieldType.UUID,
        primary_key=primary_key,
        required=required,
        indexed=indexed,
        unique=unique,
        default=default
    )


def JSONField(required=False, default=None):
    """Create a JSON field"""
    if default is None:
        default = dict
    
    return Field(
        name="",
        field_type=FieldType.JSON,
        required=required,
        default=default
    )


def ListField(required=False, default=None):
    """Create a list field"""
    if default is None:
        default = list
    
    return Field(
        name="",
        field_type=FieldType.LIST,
        required=required,
        default=default
    )


def DictField(required=False, default=None):
    """Create a dict field"""
    if default is None:
        default = dict
    
    return Field(
        name="",
        field_type=FieldType.DICT,
        required=required,
        default=default
    )


# Example model
class DigitalAsset(BaseModel):
    """Example digital asset model"""
    __tablename__ = "digital_assets"
    __stores__ = ["cassandra", "elasticsearch"]
    
    id = UUIDField(primary_key=True)
    name = StringField(required=True, indexed=True, max_length=255)
    type = StringField(required=True, indexed=True, choices=["model", "document", "image", "video"])
    owner_id = StringField(required=True, indexed=True)
    metadata = JSONField()
    tags = ListField()
    created_at = DateTimeField(auto_now_add=True, indexed=True)
    updated_at = DateTimeField(auto_now=True)
    is_public = BooleanField(default=False) 