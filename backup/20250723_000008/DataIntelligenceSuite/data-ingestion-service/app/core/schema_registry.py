"""
Schema Registry

Centralized schema management with version control and compatibility checking
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import hashlib

import aioredis
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from jsonschema import validate, ValidationError, Draft7Validator
import fastavro

from .config import settings

logger = logging.getLogger(__name__)


class SchemaType(str, Enum):
    """Supported schema types"""
    AVRO = "avro"
    JSON = "json"
    PROTOBUF = "protobuf"
    PARQUET = "parquet"


class CompatibilityMode(str, Enum):
    """Schema compatibility modes"""
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


class SchemaRegistry:
    """Manages schemas for data ingestion"""
    
    def __init__(self, config: settings):
        self.config = config
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.redis_client: Optional[aioredis.Redis] = None
        self.schema_registry_client: Optional[SchemaRegistryClient] = None
        
    async def initialize(self):
        """Initialize the schema registry"""
        logger.info("Initializing Schema Registry")
        
        # Connect to Redis for caching
        try:
            self.redis_client = await aioredis.create_redis_pool(
                f"redis://redis:6379",
                maxsize=10
            )
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}")
            
        # Initialize Confluent Schema Registry client if available
        if hasattr(self.config, 'schema_registry_url'):
            try:
                conf = {
                    'url': self.config.schema_registry_url,
                    'basic.auth.credentials.source': 'USER_INFO',
                    'basic.auth.user.info': f"{self.config.schema_registry_username}:{self.config.schema_registry_password}"
                }
                self.schema_registry_client = SchemaRegistryClient(conf)
            except Exception as e:
                logger.warning(f"Failed to connect to Schema Registry: {e}")
                
    async def register_schema(
        self,
        schema_id: str,
        schema: Dict[str, Any],
        schema_type: SchemaType = SchemaType.AVRO,
        compatibility: Optional[CompatibilityMode] = None
    ) -> Dict[str, Any]:
        """Register a new schema or version"""
        
        # Validate schema format
        if not self._validate_schema_format(schema, schema_type):
            raise ValueError(f"Invalid {schema_type.value} schema format")
            
        # Check compatibility if this is a new version
        existing_schema = await self.get_schema(schema_id)
        if existing_schema:
            compatibility_mode = compatibility or CompatibilityMode(self.config.schema_compatibility)
            is_compatible, reason = await self._check_compatibility(
                existing_schema["schema"],
                schema,
                schema_type,
                compatibility_mode
            )
            
            if not is_compatible:
                raise ValueError(f"Schema incompatible: {reason}")
                
        # Generate version
        version = await self._get_next_version(schema_id)
        
        # Create schema entry
        schema_entry = {
            "id": schema_id,
            "version": version,
            "type": schema_type.value,
            "schema": schema,
            "compatibility": (compatibility or CompatibilityMode(self.config.schema_compatibility)).value,
            "created_at": datetime.utcnow().isoformat(),
            "checksum": self._calculate_checksum(schema)
        }
        
        # Store in memory
        if schema_id not in self.schemas:
            self.schemas[schema_id] = {}
        self.schemas[schema_id][str(version)] = schema_entry
        
        # Store in Redis cache
        if self.redis_client:
            cache_key = f"schema:{schema_id}:v{version}"
            await self.redis_client.setex(
                cache_key,
                self.config.schema_cache_size,
                json.dumps(schema_entry)
            )
            
        # Register with Confluent Schema Registry if available
        if self.schema_registry_client and schema_type == SchemaType.AVRO:
            try:
                schema_str = json.dumps(schema)
                registered_schema = self.schema_registry_client.register_schema(
                    subject_name=schema_id,
                    schema_str=schema_str
                )
                schema_entry["registry_id"] = registered_schema.schema_id
            except Exception as e:
                logger.error(f"Failed to register with Schema Registry: {e}")
                
        logger.info(f"Registered schema {schema_id} version {version}")
        
        return {
            "schema_id": schema_id,
            "version": version,
            "status": "registered"
        }
        
    async def get_schema(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get a schema by ID and optional version"""
        
        # Check cache first
        if self.redis_client:
            cache_key = f"schema:{schema_id}:v{version or 'latest'}"
            cached = await self.redis_client.get(cache_key)
            if cached:
                return json.loads(cached)
                
        # Check memory
        if schema_id in self.schemas:
            if version:
                return self.schemas[schema_id].get(str(version))
            else:
                # Return latest version
                versions = sorted(self.schemas[schema_id].keys(), key=int, reverse=True)
                if versions:
                    return self.schemas[schema_id][versions[0]]
                    
        # Try Confluent Schema Registry
        if self.schema_registry_client:
            try:
                if version:
                    schema = self.schema_registry_client.get_schema(
                        schema_id=f"{schema_id}-v{version}"
                    )
                else:
                    schema = self.schema_registry_client.get_latest_schema(schema_id)
                    
                return {
                    "id": schema_id,
                    "version": schema.version,
                    "type": "avro",
                    "schema": json.loads(schema.schema_str)
                }
            except Exception as e:
                logger.debug(f"Schema not found in registry: {e}")
                
        return None
        
    async def list_schemas(
        self,
        schema_type: Optional[SchemaType] = None
    ) -> List[Dict[str, Any]]:
        """List all registered schemas"""
        schemas = []
        
        for schema_id, versions in self.schemas.items():
            latest_version = sorted(versions.keys(), key=int, reverse=True)[0]
            schema = versions[latest_version]
            
            if schema_type and schema["type"] != schema_type.value:
                continue
                
            schemas.append({
                "id": schema_id,
                "latest_version": int(latest_version),
                "type": schema["type"],
                "created_at": schema["created_at"],
                "versions": list(map(int, versions.keys()))
            })
            
        return schemas
        
    async def list_versions(self, schema_id: str) -> List[int]:
        """List all versions of a schema"""
        if schema_id not in self.schemas:
            return []
            
        return sorted(map(int, self.schemas[schema_id].keys()))
        
    async def validate_data(
        self,
        data: Dict[str, Any],
        schema_id: str,
        version: Optional[int] = None
    ) -> Tuple[bool, Optional[str]]:
        """Validate data against a schema"""
        schema_entry = await self.get_schema(schema_id, version)
        if not schema_entry:
            return False, f"Schema {schema_id} not found"
            
        schema = schema_entry["schema"]
        schema_type = SchemaType(schema_entry["type"])
        
        try:
            if schema_type == SchemaType.JSON:
                validate(instance=data, schema=schema)
            elif schema_type == SchemaType.AVRO:
                # Validate using fastavro
                fastavro.schemaless_writer(None, schema, data)
            else:
                return False, f"Validation not implemented for {schema_type.value}"
                
            return True, None
            
        except ValidationError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Validation error: {str(e)}"
            
    async def delete_schema(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> Dict[str, Any]:
        """Delete a schema or specific version"""
        if schema_id not in self.schemas:
            raise ValueError(f"Schema {schema_id} not found")
            
        if version:
            # Delete specific version
            if str(version) in self.schemas[schema_id]:
                del self.schemas[schema_id][str(version)]
                
                # Clear from cache
                if self.redis_client:
                    cache_key = f"schema:{schema_id}:v{version}"
                    await self.redis_client.delete(cache_key)
                    
                logger.info(f"Deleted schema {schema_id} version {version}")
            else:
                raise ValueError(f"Version {version} not found for schema {schema_id}")
        else:
            # Delete all versions
            del self.schemas[schema_id]
            
            # Clear from cache
            if self.redis_client:
                pattern = f"schema:{schema_id}:*"
                cursor = b'0'
                while cursor:
                    cursor, keys = await self.redis_client.scan(
                        cursor, match=pattern
                    )
                    if keys:
                        await self.redis_client.delete(*keys)
                        
            logger.info(f"Deleted all versions of schema {schema_id}")
            
        return {
            "schema_id": schema_id,
            "version": version,
            "status": "deleted"
        }
        
    def _validate_schema_format(self, schema: Dict[str, Any], schema_type: SchemaType) -> bool:
        """Validate schema format based on type"""
        try:
            if schema_type == SchemaType.JSON:
                # Validate JSON Schema
                Draft7Validator.check_schema(schema)
                return True
            elif schema_type == SchemaType.AVRO:
                # Validate Avro schema
                fastavro.parse_schema(schema)
                return True
            elif schema_type == SchemaType.PARQUET:
                # Parquet schemas are typically derived from data
                return "fields" in schema
            else:
                # Add more validators as needed
                return True
                
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False
            
    async def _check_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        schema_type: SchemaType,
        mode: CompatibilityMode
    ) -> Tuple[bool, Optional[str]]:
        """Check if schemas are compatible"""
        
        if mode == CompatibilityMode.NONE:
            return True, None
            
        try:
            if schema_type == SchemaType.AVRO:
                return self._check_avro_compatibility(old_schema, new_schema, mode)
            elif schema_type == SchemaType.JSON:
                return self._check_json_compatibility(old_schema, new_schema, mode)
            else:
                # Default to allowing compatibility for unsupported types
                return True, None
                
        except Exception as e:
            return False, str(e)
            
    def _check_avro_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        mode: CompatibilityMode
    ) -> Tuple[bool, Optional[str]]:
        """Check Avro schema compatibility"""
        # Simplified compatibility check
        # In production, use confluent-kafka-python's compatibility checker
        
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}
        
        if mode == CompatibilityMode.BACKWARD:
            # New schema can read old data
            # All old fields must exist in new schema
            for field_name, old_field in old_fields.items():
                if field_name not in new_fields:
                    if "default" not in old_field:
                        return False, f"Field {field_name} removed without default"
                        
        elif mode == CompatibilityMode.FORWARD:
            # Old schema can read new data
            # All new fields must have defaults or exist in old schema
            for field_name, new_field in new_fields.items():
                if field_name not in old_fields:
                    if "default" not in new_field:
                        return False, f"New field {field_name} added without default"
                        
        elif mode == CompatibilityMode.FULL:
            # Both backward and forward compatible
            backward_ok, backward_reason = self._check_avro_compatibility(
                old_schema, new_schema, CompatibilityMode.BACKWARD
            )
            if not backward_ok:
                return False, backward_reason
                
            forward_ok, forward_reason = self._check_avro_compatibility(
                old_schema, new_schema, CompatibilityMode.FORWARD
            )
            if not forward_ok:
                return False, forward_reason
                
        return True, None
        
    def _check_json_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        mode: CompatibilityMode
    ) -> Tuple[bool, Optional[str]]:
        """Check JSON schema compatibility"""
        # Simplified check - in production, implement full JSON Schema compatibility
        
        old_props = old_schema.get("properties", {})
        new_props = new_schema.get("properties", {})
        old_required = set(old_schema.get("required", []))
        new_required = set(new_schema.get("required", []))
        
        if mode == CompatibilityMode.BACKWARD:
            # Check if required fields were removed
            if old_required - new_required:
                return False, "Required fields removed"
                
        elif mode == CompatibilityMode.FORWARD:
            # Check if new required fields were added
            if new_required - old_required:
                return False, "New required fields added"
                
        return True, None
        
    async def _get_next_version(self, schema_id: str) -> int:
        """Get next version number for a schema"""
        if schema_id not in self.schemas:
            return 1
            
        versions = list(map(int, self.schemas[schema_id].keys()))
        return max(versions) + 1 if versions else 1
        
    def _calculate_checksum(self, schema: Dict[str, Any]) -> str:
        """Calculate checksum for schema"""
        schema_str = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest() 