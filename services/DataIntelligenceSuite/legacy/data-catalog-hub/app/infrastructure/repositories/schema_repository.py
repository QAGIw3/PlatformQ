"""
Schema Repository Implementation

Handles schema persistence and management.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging
import hashlib
import json

from app.core.schema_registry import SchemaRegistry, SchemaType, CompatibilityMode
from app.services.storage import IgniteCacheAdapter

logger = logging.getLogger(__name__)


class SchemaRepository:
    """
    Repository for schema management.
    
    Handles schema storage, versioning, and compatibility checking.
    """
    
    def __init__(
        self,
        schema_registry: SchemaRegistry,
        cache_manager: IgniteCacheAdapter
    ):
        self.schema_registry = schema_registry
        self.cache_manager = cache_manager
        self.cache_prefix = "schema"
        
    async def register(
        self,
        name: str,
        schema_type: SchemaType,
        schema_definition: Dict[str, Any],
        compatibility: Optional[CompatibilityMode] = None
    ) -> Dict[str, Any]:
        """Register a new schema or version"""
        try:
            # Register with schema registry
            schema = await self.schema_registry.register_schema(
                name=name,
                schema_type=schema_type,
                schema_definition=schema_definition,
                compatibility=compatibility
            )
            
            # Cache the schema
            cache_key = f"{self.cache_prefix}:{schema['id']}"
            await self.cache_manager.set(cache_key, schema, ttl=3600)
            
            # Also cache by name for latest version
            name_key = f"{self.cache_prefix}:name:{name}:latest"
            await self.cache_manager.set(name_key, schema['id'], ttl=3600)
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to register schema: {e}")
            raise
            
    async def get_schema(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Get schema by ID and optional version"""
        try:
            # Check cache first
            cache_key = f"{self.cache_prefix}:{schema_id}"
            if version:
                cache_key += f":v{version}"
                
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached
            
            # Get from registry
            schema = await self.schema_registry.get_schema(schema_id, version)
            
            if schema:
                # Cache it
                await self.cache_manager.set(cache_key, schema, ttl=3600)
                
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get schema: {e}")
            raise
            
    async def find_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find schema by name (latest version)"""
        try:
            # Check cache for schema ID
            name_key = f"{self.cache_prefix}:name:{name}:latest"
            schema_id = await self.cache_manager.get(name_key)
            
            if schema_id:
                return await self.get_schema(schema_id)
            
            # Search in registry
            schemas = await self.schema_registry.list_schemas()
            for schema in schemas:
                if schema.get('name') == name:
                    # Cache the mapping
                    await self.cache_manager.set(
                        name_key,
                        schema['id'],
                        ttl=3600
                    )
                    return schema
                    
            return None
            
        except Exception as e:
            logger.error(f"Failed to find schema by name: {e}")
            raise
            
    async def list_schemas(
        self,
        schema_type: Optional[SchemaType] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List schemas with optional filtering"""
        try:
            # Get from registry
            all_schemas = await self.schema_registry.list_schemas(
                schema_type=schema_type,
                limit=limit,
                offset=offset
            )
            
            # Count total
            # TODO: Implement proper count query
            total = len(all_schemas)
            
            return all_schemas, total
            
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            raise
            
    async def get_versions(self, schema_id: str) -> List[Dict[str, Any]]:
        """Get all versions of a schema"""
        try:
            return await self.schema_registry.get_schema_versions(schema_id)
            
        except Exception as e:
            logger.error(f"Failed to get schema versions: {e}")
            raise
            
    async def check_compatibility(
        self,
        schema_id_or_name: str,
        new_schema: Dict[str, Any],
        compatibility_mode: Optional[CompatibilityMode] = None
    ) -> bool:
        """Check if new schema is compatible"""
        try:
            return await self.schema_registry.validate_compatibility(
                schema_id_or_name,
                new_schema,
                compatibility_mode
            )
            
        except Exception as e:
            logger.error(f"Failed to check compatibility: {e}")
            raise
            
    async def infer_schema(
        self,
        data_sample: List[Dict[str, Any]],
        schema_type: SchemaType
    ) -> Dict[str, Any]:
        """Infer schema from data sample"""
        try:
            return await self.schema_registry.infer_schema(
                data_sample,
                schema_type
            )
            
        except Exception as e:
            logger.error(f"Failed to infer schema: {e}")
            raise
            
    async def delete_schema(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> bool:
        """Delete schema or specific version"""
        try:
            success = await self.schema_registry.delete_schema(
                schema_id,
                version
            )
            
            if success:
                # Clear cache
                cache_key = f"{self.cache_prefix}:{schema_id}"
                if version:
                    cache_key += f":v{version}"
                await self.cache_manager.delete(cache_key)
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to delete schema: {e}")
            raise
            
    async def link_schema_to_entity(
        self,
        schema_id: str,
        entity_guid: str
    ) -> bool:
        """Link schema to an entity"""
        try:
            # Store mapping in cache
            # In a real implementation, this would be stored in Atlas
            mapping_key = f"{self.cache_prefix}:entity:{entity_guid}"
            await self.cache_manager.set(mapping_key, schema_id, ttl=None)
            
            # Also store reverse mapping
            reverse_key = f"{self.cache_prefix}:schema:{schema_id}:entities"
            entities = await self.cache_manager.get(reverse_key) or []
            if entity_guid not in entities:
                entities.append(entity_guid)
                await self.cache_manager.set(reverse_key, entities, ttl=None)
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to link schema to entity: {e}")
            raise
            
    async def get_schema_for_entity(
        self,
        entity_guid: str
    ) -> Optional[Dict[str, Any]]:
        """Get schema linked to an entity"""
        try:
            # Check mapping
            mapping_key = f"{self.cache_prefix}:entity:{entity_guid}"
            schema_id = await self.cache_manager.get(mapping_key)
            
            if schema_id:
                return await self.get_schema(schema_id)
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to get schema for entity: {e}")
            raise
            
    async def get_entities_using_schema(
        self,
        schema_id: str
    ) -> List[str]:
        """Get entities using a schema"""
        try:
            reverse_key = f"{self.cache_prefix}:schema:{schema_id}:entities"
            entities = await self.cache_manager.get(reverse_key) or []
            return entities
            
        except Exception as e:
            logger.error(f"Failed to get entities using schema: {e}")
            raise
            
    def _generate_schema_id(
        self,
        name: str,
        schema_definition: Dict[str, Any]
    ) -> str:
        """Generate unique schema ID"""
        content = f"{name}:{json.dumps(schema_definition, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()[:16] 