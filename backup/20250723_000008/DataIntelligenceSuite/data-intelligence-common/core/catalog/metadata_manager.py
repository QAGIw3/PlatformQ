"""
Metadata management for catalog entities.

Provides comprehensive metadata operations and versioning.
"""

import json
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union, Type
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class MetadataType(str, Enum):
    """Types of metadata"""
    TECHNICAL = "technical"
    BUSINESS = "business"
    OPERATIONAL = "operational"
    QUALITY = "quality"
    SECURITY = "security"
    COMPLIANCE = "compliance"
    CUSTOM = "custom"


class FieldType(str, Enum):
    """Metadata field types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    DATE = "date"
    TIME = "time"
    JSON = "json"
    ARRAY = "array"
    OBJECT = "object"
    REFERENCE = "reference"


@dataclass
class MetadataField:
    """Metadata field definition"""
    name: str
    field_type: FieldType
    description: Optional[str] = None
    required: bool = False
    default_value: Optional[Any] = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def validate_value(self, value: Any) -> bool:
        """Validate field value"""
        if value is None:
            return not self.required
            
        # Type validation
        try:
            if self.field_type == FieldType.STRING:
                str(value)
            elif self.field_type == FieldType.INTEGER:
                int(value)
            elif self.field_type == FieldType.FLOAT:
                float(value)
            elif self.field_type == FieldType.BOOLEAN:
                bool(value)
            elif self.field_type == FieldType.DATETIME:
                if isinstance(value, str):
                    datetime.fromisoformat(value)
            elif self.field_type == FieldType.JSON:
                if isinstance(value, str):
                    json.loads(value)
            elif self.field_type == FieldType.ARRAY:
                if not isinstance(value, list):
                    return False
            elif self.field_type == FieldType.OBJECT:
                if not isinstance(value, dict):
                    return False
        except (ValueError, TypeError):
            return False
            
        # Constraint validation
        if "min_length" in self.constraints and len(str(value)) < self.constraints["min_length"]:
            return False
        if "max_length" in self.constraints and len(str(value)) > self.constraints["max_length"]:
            return False
        if "min_value" in self.constraints and value < self.constraints["min_value"]:
            return False
        if "max_value" in self.constraints and value > self.constraints["max_value"]:
            return False
        if "pattern" in self.constraints:
            import re
            if not re.match(self.constraints["pattern"], str(value)):
                return False
        if "enum" in self.constraints and value not in self.constraints["enum"]:
            return False
            
        return True


@dataclass
class MetadataSchema:
    """Metadata schema definition"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    version: str = "1.0.0"
    metadata_type: MetadataType = MetadataType.CUSTOM
    description: Optional[str] = None
    fields: List[MetadataField] = field(default_factory=list)
    parent_schema: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def add_field(self, field: MetadataField):
        """Add field to schema"""
        self.fields.append(field)
        self.updated_at = datetime.utcnow()
        
    def remove_field(self, field_name: str) -> bool:
        """Remove field from schema"""
        for i, field in enumerate(self.fields):
            if field.name == field_name:
                self.fields.pop(i)
                self.updated_at = datetime.utcnow()
                return True
        return False
        
    def get_field(self, field_name: str) -> Optional[MetadataField]:
        """Get field by name"""
        for field in self.fields:
            if field.name == field_name:
                return field
        return None
        
    def validate_metadata(self, metadata: Dict[str, Any]) -> tuple[bool, List[str]]:
        """Validate metadata against schema"""
        errors = []
        
        # Check required fields
        for field in self.fields:
            if field.required and field.name not in metadata:
                errors.append(f"Required field '{field.name}' is missing")
                
        # Validate field values
        for field_name, value in metadata.items():
            field = self.get_field(field_name)
            if field and not field.validate_value(value):
                errors.append(f"Invalid value for field '{field_name}'")
                
        return len(errors) == 0, errors


@dataclass
class MetadataVersion:
    """Metadata version entry"""
    version_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    entity_id: str = ""
    version_number: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    schema_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    change_summary: Optional[str] = None
    is_current: bool = True


class MetadataManager:
    """
    Manages metadata for catalog entities.
    
    Features:
    - Schema management
    - Metadata validation
    - Version control
    - Inheritance support
    - Event publishing
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._schemas: Dict[str, MetadataSchema] = {}
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._versions: Dict[str, List[MetadataVersion]] = {}
        self._schema_registry: Dict[str, str] = {}  # entity_id -> schema_id
        
        # Initialize default schemas
        self._initialize_default_schemas()
        
    def _initialize_default_schemas(self):
        """Initialize default metadata schemas"""
        # Technical metadata schema
        technical_schema = MetadataSchema(
            name="technical_metadata",
            metadata_type=MetadataType.TECHNICAL,
            description="Technical metadata for data assets"
        )
        technical_schema.add_field(MetadataField(
            name="format",
            field_type=FieldType.STRING,
            description="Data format",
            required=True
        ))
        technical_schema.add_field(MetadataField(
            name="size_bytes",
            field_type=FieldType.INTEGER,
            description="Size in bytes"
        ))
        technical_schema.add_field(MetadataField(
            name="row_count",
            field_type=FieldType.INTEGER,
            description="Number of rows"
        ))
        technical_schema.add_field(MetadataField(
            name="column_count",
            field_type=FieldType.INTEGER,
            description="Number of columns"
        ))
        technical_schema.add_field(MetadataField(
            name="compression",
            field_type=FieldType.STRING,
            description="Compression type"
        ))
        technical_schema.add_field(MetadataField(
            name="encoding",
            field_type=FieldType.STRING,
            description="Character encoding",
            default_value="utf-8"
        ))
        
        self.register_schema(technical_schema)
        
        # Business metadata schema
        business_schema = MetadataSchema(
            name="business_metadata",
            metadata_type=MetadataType.BUSINESS,
            description="Business metadata for data assets"
        )
        business_schema.add_field(MetadataField(
            name="business_owner",
            field_type=FieldType.STRING,
            description="Business owner",
            required=True
        ))
        business_schema.add_field(MetadataField(
            name="department",
            field_type=FieldType.STRING,
            description="Department"
        ))
        business_schema.add_field(MetadataField(
            name="cost_center",
            field_type=FieldType.STRING,
            description="Cost center"
        ))
        business_schema.add_field(MetadataField(
            name="business_criticality",
            field_type=FieldType.STRING,
            description="Business criticality level",
            constraints={"enum": ["low", "medium", "high", "critical"]}
        ))
        business_schema.add_field(MetadataField(
            name="data_classification",
            field_type=FieldType.STRING,
            description="Data classification",
            constraints={"enum": ["public", "internal", "confidential", "restricted"]}
        ))
        
        self.register_schema(business_schema)
        
        # Quality metadata schema
        quality_schema = MetadataSchema(
            name="quality_metadata",
            metadata_type=MetadataType.QUALITY,
            description="Data quality metadata"
        )
        quality_schema.add_field(MetadataField(
            name="quality_score",
            field_type=FieldType.FLOAT,
            description="Overall quality score",
            constraints={"min_value": 0.0, "max_value": 1.0}
        ))
        quality_schema.add_field(MetadataField(
            name="completeness",
            field_type=FieldType.FLOAT,
            description="Data completeness score",
            constraints={"min_value": 0.0, "max_value": 1.0}
        ))
        quality_schema.add_field(MetadataField(
            name="accuracy",
            field_type=FieldType.FLOAT,
            description="Data accuracy score",
            constraints={"min_value": 0.0, "max_value": 1.0}
        ))
        quality_schema.add_field(MetadataField(
            name="last_quality_check",
            field_type=FieldType.DATETIME,
            description="Last quality check timestamp"
        ))
        quality_schema.add_field(MetadataField(
            name="quality_issues",
            field_type=FieldType.ARRAY,
            description="List of quality issues"
        ))
        
        self.register_schema(quality_schema)
        
    def register_schema(self, schema: MetadataSchema) -> str:
        """Register metadata schema"""
        self._schemas[schema.id] = schema
        
        # Cache schema
        if self.cache:
            cache_key = f"metadata:schema:{schema.id}"
            self.cache.set(cache_key, schema.__dict__, ttl=3600)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="metadata.schema.registered",
                source="metadata_manager",
                data={"schema_id": schema.id, "schema_name": schema.name}
            ))
            
        logger.info(f"Registered metadata schema: {schema.name}")
        return schema.id
        
    def get_schema(self, schema_id: str) -> Optional[MetadataSchema]:
        """Get schema by ID"""
        # Check cache first
        if self.cache:
            cache_key = f"metadata:schema:{schema_id}"
            cached = self.cache.get(cache_key)
            if cached:
                return MetadataSchema(**cached)
                
        return self._schemas.get(schema_id)
        
    def list_schemas(
        self,
        metadata_type: Optional[MetadataType] = None
    ) -> List[MetadataSchema]:
        """List registered schemas"""
        schemas = list(self._schemas.values())
        
        if metadata_type:
            schemas = [s for s in schemas if s.metadata_type == metadata_type]
            
        return schemas
        
    def set_entity_schema(self, entity_id: str, schema_id: str):
        """Set schema for entity"""
        if schema_id not in self._schemas:
            raise ValueError(f"Schema not found: {schema_id}")
            
        self._schema_registry[entity_id] = schema_id
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="metadata.schema.assigned",
                source="metadata_manager",
                data={"entity_id": entity_id, "schema_id": schema_id}
            ))
            
    def get_entity_schema(self, entity_id: str) -> Optional[MetadataSchema]:
        """Get schema for entity"""
        schema_id = self._schema_registry.get(entity_id)
        if schema_id:
            return self.get_schema(schema_id)
        return None
        
    def set_metadata(
        self,
        entity_id: str,
        metadata: Dict[str, Any],
        schema_id: Optional[str] = None,
        validate: bool = True,
        user: Optional[str] = None
    ) -> MetadataVersion:
        """Set metadata for entity"""
        # Get or set schema
        if schema_id:
            self.set_entity_schema(entity_id, schema_id)
        
        schema = self.get_entity_schema(entity_id)
        
        # Validate metadata
        if validate and schema:
            is_valid, errors = schema.validate_metadata(metadata)
            if not is_valid:
                raise ValueError(f"Metadata validation failed: {', '.join(errors)}")
                
        # Create version
        current_version = self._get_current_version(entity_id)
        version_number = current_version.version_number + 1 if current_version else 1
        
        version = MetadataVersion(
            entity_id=entity_id,
            version_number=version_number,
            metadata=metadata,
            schema_id=schema_id or (schema.id if schema else None),
            created_by=user,
            is_current=True
        )
        
        # Mark previous version as not current
        if current_version:
            current_version.is_current = False
            
        # Store version
        if entity_id not in self._versions:
            self._versions[entity_id] = []
        self._versions[entity_id].append(version)
        
        # Update current metadata
        self._metadata[entity_id] = metadata
        
        # Cache metadata
        if self.cache:
            cache_key = f"metadata:entity:{entity_id}"
            self.cache.set(cache_key, metadata, ttl=300)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="metadata.updated",
                source="metadata_manager",
                data={
                    "entity_id": entity_id,
                    "version_id": version.version_id,
                    "version_number": version.version_number
                }
            ))
            
        logger.info(f"Set metadata for entity {entity_id}, version {version.version_number}")
        return version
        
    def get_metadata(
        self,
        entity_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get metadata for entity"""
        # Check cache for current version
        if not version and self.cache:
            cache_key = f"metadata:entity:{entity_id}"
            cached = self.cache.get(cache_key)
            if cached:
                return cached
                
        if version:
            # Get specific version
            versions = self._versions.get(entity_id, [])
            for v in versions:
                if v.version_number == version:
                    return v.metadata
        else:
            # Get current version
            return self._metadata.get(entity_id)
            
        return None
        
    def update_metadata(
        self,
        entity_id: str,
        updates: Dict[str, Any],
        merge: bool = True,
        validate: bool = True,
        user: Optional[str] = None
    ) -> MetadataVersion:
        """Update metadata for entity"""
        current_metadata = self.get_metadata(entity_id) or {}
        
        if merge:
            # Merge updates with current metadata
            new_metadata = {**current_metadata, **updates}
        else:
            new_metadata = updates
            
        return self.set_metadata(
            entity_id,
            new_metadata,
            validate=validate,
            user=user
        )
        
    def delete_metadata(self, entity_id: str) -> bool:
        """Delete all metadata for entity"""
        if entity_id in self._metadata:
            del self._metadata[entity_id]
            
        if entity_id in self._versions:
            del self._versions[entity_id]
            
        if entity_id in self._schema_registry:
            del self._schema_registry[entity_id]
            
        # Clear cache
        if self.cache:
            cache_key = f"metadata:entity:{entity_id}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="metadata.deleted",
                source="metadata_manager",
                data={"entity_id": entity_id}
            ))
            
        logger.info(f"Deleted metadata for entity {entity_id}")
        return True
        
    def get_metadata_history(
        self,
        entity_id: str,
        limit: int = 10
    ) -> List[MetadataVersion]:
        """Get metadata version history"""
        versions = self._versions.get(entity_id, [])
        # Sort by version number descending
        versions = sorted(versions, key=lambda v: v.version_number, reverse=True)
        return versions[:limit]
        
    def _get_current_version(self, entity_id: str) -> Optional[MetadataVersion]:
        """Get current metadata version"""
        versions = self._versions.get(entity_id, [])
        for version in versions:
            if version.is_current:
                return version
        return None
        
    def search_by_metadata(
        self,
        filters: Dict[str, Any],
        metadata_type: Optional[MetadataType] = None
    ) -> List[str]:
        """Search entities by metadata values"""
        matching_entities = []
        
        for entity_id, metadata in self._metadata.items():
            # Check metadata type if specified
            if metadata_type:
                schema = self.get_entity_schema(entity_id)
                if not schema or schema.metadata_type != metadata_type:
                    continue
                    
            # Check filters
            match = True
            for key, value in filters.items():
                if key not in metadata or metadata[key] != value:
                    match = False
                    break
                    
            if match:
                matching_entities.append(entity_id)
                
        return matching_entities
        
    def bulk_update_metadata(
        self,
        updates: List[Dict[str, Any]],
        validate: bool = True,
        user: Optional[str] = None
    ) -> Dict[str, Union[MetadataVersion, str]]:
        """Bulk update metadata for multiple entities"""
        results = {}
        
        for update in updates:
            entity_id = update.get("entity_id")
            if not entity_id:
                continue
                
            try:
                metadata = update.get("metadata", {})
                schema_id = update.get("schema_id")
                
                version = self.set_metadata(
                    entity_id,
                    metadata,
                    schema_id=schema_id,
                    validate=validate,
                    user=user
                )
                results[entity_id] = version
            except Exception as e:
                results[entity_id] = str(e)
                logger.error(f"Failed to update metadata for {entity_id}: {e}")
                
        return results
        
    def export_metadata(
        self,
        entity_ids: Optional[List[str]] = None,
        include_history: bool = False
    ) -> Dict[str, Any]:
        """Export metadata for backup or migration"""
        export_data = {
            "schemas": {},
            "metadata": {},
            "versions": {},
            "schema_registry": {}
        }
        
        # Export schemas
        for schema_id, schema in self._schemas.items():
            export_data["schemas"][schema_id] = schema.__dict__
            
        # Export metadata for specified entities or all
        entities_to_export = entity_ids or list(self._metadata.keys())
        
        for entity_id in entities_to_export:
            # Current metadata
            if entity_id in self._metadata:
                export_data["metadata"][entity_id] = self._metadata[entity_id]
                
            # Schema assignment
            if entity_id in self._schema_registry:
                export_data["schema_registry"][entity_id] = self._schema_registry[entity_id]
                
            # Version history
            if include_history and entity_id in self._versions:
                export_data["versions"][entity_id] = [
                    v.__dict__ for v in self._versions[entity_id]
                ]
                
        return export_data
        
    def import_metadata(
        self,
        import_data: Dict[str, Any],
        overwrite: bool = False
    ) -> Dict[str, int]:
        """Import metadata from export"""
        stats = {
            "schemas_imported": 0,
            "entities_imported": 0,
            "versions_imported": 0
        }
        
        # Import schemas
        for schema_id, schema_data in import_data.get("schemas", {}).items():
            if overwrite or schema_id not in self._schemas:
                schema = MetadataSchema(**schema_data)
                self._schemas[schema_id] = schema
                stats["schemas_imported"] += 1
                
        # Import metadata
        for entity_id, metadata in import_data.get("metadata", {}).items():
            if overwrite or entity_id not in self._metadata:
                self._metadata[entity_id] = metadata
                stats["entities_imported"] += 1
                
        # Import schema registry
        for entity_id, schema_id in import_data.get("schema_registry", {}).items():
            if overwrite or entity_id not in self._schema_registry:
                self._schema_registry[entity_id] = schema_id
                
        # Import versions
        for entity_id, versions in import_data.get("versions", {}).items():
            if overwrite or entity_id not in self._versions:
                self._versions[entity_id] = [
                    MetadataVersion(**v) for v in versions
                ]
                stats["versions_imported"] += len(versions)
                
        return stats 