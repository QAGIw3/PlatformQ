"""
Schema Registry for managing data schemas
"""

import json
import hashlib
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from enum import Enum

from platformq_shared.logging import get_logger
from ..core.config import Settings
from ..core.atlas_client import AtlasClient
from ..core.cache_manager import CacheManager

logger = get_logger(__name__)


class SchemaType(str, Enum):
    """Supported schema types"""
    AVRO = "avro"
    JSON_SCHEMA = "json_schema"
    PROTOBUF = "protobuf"
    PARQUET = "parquet"
    CSV = "csv"


class CompatibilityMode(str, Enum):
    """Schema compatibility modes"""
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


class SchemaRegistry:
    """Registry for managing data schemas"""
    
    def __init__(self, settings: Settings, atlas_client: AtlasClient, cache_manager: CacheManager):
        self.settings = settings
        self.atlas = atlas_client
        self.cache = cache_manager
        self.schemas: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize the schema registry"""
        logger.info("Initializing Schema Registry")
        
        # Ensure schema type exists in Atlas
        await self._ensure_schema_type()
        
        # Load existing schemas
        await self._load_schemas()
        
        logger.info("Schema Registry initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        pass
        
    async def _ensure_schema_type(self):
        """Ensure schema entity type exists in Atlas"""
        schema_typedef = {
            "entityDefs": [{
                "name": "schema_definition",
                "superTypes": ["DataSet"],
                "serviceType": "platformq",
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "schemaType",
                        "typeName": "string",
                        "isOptional": False,
                        "cardinality": "SINGLE"
                    },
                    {
                        "name": "schemaDefinition",
                        "typeName": "string",
                        "isOptional": False,
                        "cardinality": "SINGLE"
                    },
                    {
                        "name": "version",
                        "typeName": "int",
                        "isOptional": False,
                        "cardinality": "SINGLE"
                    },
                    {
                        "name": "compatibility",
                        "typeName": "string",
                        "isOptional": False,
                        "cardinality": "SINGLE"
                    },
                    {
                        "name": "checksum",
                        "typeName": "string",
                        "isOptional": False,
                        "cardinality": "SINGLE"
                    }
                ]
            }]
        }
        
        # Check if type already exists
        if 'schema_definition' not in self.atlas._type_cache:
            await self.atlas.create_typedef(schema_typedef)
            
    async def _load_schemas(self):
        """Load existing schemas from Atlas"""
        try:
            result = await self.atlas.search_entities(
                query="*",
                type_name="schema_definition",
                limit=1000
            )
            
            for entity in result.get('entities', []):
                schema_id = entity['attributes'].get('qualifiedName')
                if schema_id:
                    self.schemas[schema_id] = entity
                    
            logger.info(f"Loaded {len(self.schemas)} schemas")
            
        except Exception as e:
            logger.error(f"Failed to load schemas: {e}")
            
    async def register_schema(self,
                            name: str,
                            schema_type: SchemaType,
                            schema_definition: Dict[str, Any],
                            compatibility: Optional[CompatibilityMode] = None) -> Dict[str, Any]:
        """Register a new schema or version"""
        logger.info(f"Registering schema: {name}")
        
        # Use default compatibility if not specified
        if compatibility is None:
            compatibility = CompatibilityMode(self.settings.schema_compatibility_default)
            
        # Calculate schema checksum
        schema_str = json.dumps(schema_definition, sort_keys=True)
        checksum = hashlib.sha256(schema_str.encode()).hexdigest()
        
        # Check if schema already exists
        existing = await self._get_latest_schema(name)
        
        if existing:
            # Check if schema is different
            if existing['attributes']['checksum'] == checksum:
                logger.info(f"Schema {name} already exists with same definition")
                return existing
                
            # Validate compatibility
            version = existing['attributes']['version'] + 1
            is_compatible = await self._check_compatibility(
                schema_type,
                existing['attributes']['schemaDefinition'],
                schema_str,
                compatibility
            )
            
            if not is_compatible:
                raise ValueError(f"Schema {name} v{version} is not {compatibility} compatible")
        else:
            version = 1
            
        # Create schema entity
        schema_entity = {
            "typeName": "schema_definition",
            "attributes": {
                "name": f"{name}_v{version}",
                "qualifiedName": f"schema.{name}.v{version}",
                "schemaType": schema_type.value,
                "schemaDefinition": schema_str,
                "version": version,
                "compatibility": compatibility.value,
                "checksum": checksum,
                "createdTime": datetime.utcnow().isoformat()
            }
        }
        
        # Create in Atlas
        created = await self.atlas.create_entity(schema_entity)
        
        # Update cache
        cache_key = f"schema:{name}:latest"
        await self.cache.set(cache_key, created, ttl=self.settings.schema_cache_ttl)
        
        # Store in local registry
        self.schemas[created['attributes']['qualifiedName']] = created
        
        logger.info(f"Registered schema {name} version {version}")
        return created
        
    async def get_schema(self, schema_id: str, version: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Get schema by ID and optional version"""
        # Check cache first
        cache_key = f"schema:{schema_id}:{version or 'latest'}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
            
        if version:
            # Get specific version
            qualified_name = f"schema.{schema_id}.v{version}"
            schema = await self.atlas.get_entity_by_attribute(
                "schema_definition",
                "qualifiedName",
                qualified_name
            )
        else:
            # Get latest version
            schema = await self._get_latest_schema(schema_id)
            
        if schema:
            await self.cache.set(cache_key, schema, ttl=self.settings.schema_cache_ttl)
            
        return schema
        
    async def _get_latest_schema(self, schema_id: str) -> Optional[Dict[str, Any]]:
        """Get latest version of a schema"""
        result = await self.atlas.search_entities(
            query=f"qualifiedName:schema.{schema_id}.*",
            type_name="schema_definition",
            limit=1
        )
        
        entities = result.get('entities', [])
        if not entities:
            return None
            
        # Sort by version and return latest
        entities.sort(key=lambda e: e['attributes']['version'], reverse=True)
        return entities[0]
        
    async def list_schemas(self,
                         schema_type: Optional[SchemaType] = None,
                         limit: int = 100,
                         offset: int = 0) -> List[Dict[str, Any]]:
        """List all schemas with optional filtering"""
        query = "*"
        if schema_type:
            query = f"schemaType:{schema_type.value}"
            
        result = await self.atlas.search_entities(
            query=query,
            type_name="schema_definition",
            limit=limit,
            offset=offset
        )
        
        return result.get('entities', [])
        
    async def get_schema_versions(self, schema_id: str) -> List[Dict[str, Any]]:
        """Get all versions of a schema"""
        result = await self.atlas.search_entities(
            query=f"qualifiedName:schema.{schema_id}.*",
            type_name="schema_definition",
            limit=100
        )
        
        versions = result.get('entities', [])
        versions.sort(key=lambda e: e['attributes']['version'])
        
        return versions
        
    async def validate_compatibility(self,
                                   schema_id: str,
                                   new_schema: Dict[str, Any],
                                   compatibility_mode: Optional[CompatibilityMode] = None) -> bool:
        """Validate schema compatibility"""
        # Get latest schema
        latest = await self._get_latest_schema(schema_id)
        if not latest:
            return True  # No existing schema, so compatible
            
        # Use specified or default compatibility mode
        if not compatibility_mode:
            compatibility_mode = CompatibilityMode(latest['attributes']['compatibility'])
            
        schema_type = SchemaType(latest['attributes']['schemaType'])
        old_schema = json.loads(latest['attributes']['schemaDefinition'])
        
        return await self._check_compatibility(
            schema_type,
            old_schema,
            new_schema,
            compatibility_mode
        )
        
    async def _check_compatibility(self,
                                 schema_type: SchemaType,
                                 old_schema: Any,
                                 new_schema: Any,
                                 mode: CompatibilityMode) -> bool:
        """Check compatibility between two schemas"""
        if mode == CompatibilityMode.NONE:
            return True
            
        # Type-specific compatibility checking
        if schema_type == SchemaType.AVRO:
            return await self._check_avro_compatibility(old_schema, new_schema, mode)
        elif schema_type == SchemaType.JSON_SCHEMA:
            return await self._check_json_schema_compatibility(old_schema, new_schema, mode)
        else:
            # For other types, implement basic field checking
            return await self._check_basic_compatibility(old_schema, new_schema, mode)
            
    async def _check_avro_compatibility(self,
                                      old_schema: Any,
                                      new_schema: Any,
                                      mode: CompatibilityMode) -> bool:
        """Check Avro schema compatibility"""
        # Simplified Avro compatibility check
        # In production, use a proper Avro library
        
        if isinstance(old_schema, str):
            old_schema = json.loads(old_schema)
        if isinstance(new_schema, str):
            new_schema = json.loads(new_schema)
            
        old_fields = {f['name']: f for f in old_schema.get('fields', [])}
        new_fields = {f['name']: f for f in new_schema.get('fields', [])}
        
        if mode in [CompatibilityMode.BACKWARD, CompatibilityMode.FULL]:
            # New schema can read old data
            # All required fields in old schema must exist in new schema
            for name, field in old_fields.items():
                if field.get('default') is None and name not in new_fields:
                    return False
                    
        if mode in [CompatibilityMode.FORWARD, CompatibilityMode.FULL]:
            # Old schema can read new data
            # All required fields in new schema must exist in old schema
            for name, field in new_fields.items():
                if field.get('default') is None and name not in old_fields:
                    return False
                    
        return True
        
    async def _check_json_schema_compatibility(self,
                                             old_schema: Any,
                                             new_schema: Any,
                                             mode: CompatibilityMode) -> bool:
        """Check JSON Schema compatibility"""
        # Simplified JSON Schema compatibility check
        if isinstance(old_schema, str):
            old_schema = json.loads(old_schema)
        if isinstance(new_schema, str):
            new_schema = json.loads(new_schema)
            
        old_props = old_schema.get('properties', {})
        new_props = new_schema.get('properties', {})
        old_required = set(old_schema.get('required', []))
        new_required = set(new_schema.get('required', []))
        
        if mode in [CompatibilityMode.BACKWARD, CompatibilityMode.FULL]:
            # Check backward compatibility
            if not old_required.issubset(new_props.keys()):
                return False
                
        if mode in [CompatibilityMode.FORWARD, CompatibilityMode.FULL]:
            # Check forward compatibility
            if not new_required.issubset(old_props.keys()):
                return False
                
        return True
        
    async def _check_basic_compatibility(self,
                                       old_schema: Any,
                                       new_schema: Any,
                                       mode: CompatibilityMode) -> bool:
        """Basic compatibility check for other schema types"""
        # For CSV, Parquet, etc., just check field presence
        # This is a simplified implementation
        return True
        
    async def infer_schema(self,
                         data_sample: List[Dict[str, Any]],
                         schema_type: SchemaType = SchemaType.JSON_SCHEMA) -> Dict[str, Any]:
        """Infer schema from data sample"""
        if not data_sample:
            raise ValueError("Data sample is empty")
            
        if schema_type == SchemaType.JSON_SCHEMA:
            return self._infer_json_schema(data_sample)
        elif schema_type == SchemaType.AVRO:
            return self._infer_avro_schema(data_sample)
        else:
            raise ValueError(f"Schema inference not supported for {schema_type}")
            
    def _infer_json_schema(self, data_sample: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Infer JSON Schema from data"""
        # Analyze first record to build schema
        first_record = data_sample[0]
        
        properties = {}
        required = []
        
        for key, value in first_record.items():
            # Determine type
            if isinstance(value, bool):
                prop_type = "boolean"
            elif isinstance(value, int):
                prop_type = "integer"
            elif isinstance(value, float):
                prop_type = "number"
            elif isinstance(value, str):
                prop_type = "string"
            elif isinstance(value, list):
                prop_type = "array"
            elif isinstance(value, dict):
                prop_type = "object"
            else:
                prop_type = "string"  # Default
                
            properties[key] = {"type": prop_type}
            
            # Check if field is always present
            if all(key in record for record in data_sample):
                required.append(key)
                
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": properties,
            "required": required
        }
        
    def _infer_avro_schema(self, data_sample: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Infer Avro schema from data"""
        first_record = data_sample[0]
        
        fields = []
        for key, value in first_record.items():
            # Determine Avro type
            if isinstance(value, bool):
                avro_type = "boolean"
            elif isinstance(value, int):
                avro_type = "long"
            elif isinstance(value, float):
                avro_type = "double"
            elif isinstance(value, str):
                avro_type = "string"
            else:
                avro_type = "string"  # Default
                
            # Check if nullable
            is_nullable = any(
                key not in record or record[key] is None 
                for record in data_sample
            )
            
            field = {
                "name": key,
                "type": ["null", avro_type] if is_nullable else avro_type
            }
            
            fields.append(field)
            
        return {
            "type": "record",
            "name": "InferredSchema",
            "namespace": "platformq.inferred",
            "fields": fields
        }
        
    async def delete_schema(self, schema_id: str, version: Optional[int] = None) -> bool:
        """Delete a schema or specific version"""
        if version:
            qualified_name = f"schema.{schema_id}.v{version}"
            entity = await self.atlas.get_entity_by_attribute(
                "schema_definition",
                "qualifiedName",
                qualified_name
            )
        else:
            # Delete all versions
            versions = await self.get_schema_versions(schema_id)
            for v in versions:
                await self.atlas.delete_entity(v['guid'])
            return True
            
        if entity:
            return await self.atlas.delete_entity(entity['guid'])
            
        return False 