"""
Schema Service

Business logic for schema operations.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app.infrastructure.repositories import SchemaRepository
from app.core.schema_registry import SchemaType, CompatibilityMode
from app.events import EventBus
from app.services.interfaces import ServiceResult

logger = logging.getLogger(__name__)


class SchemaService:
    """
    Service layer for schema registry operations.
    
    Handles schema validation, compatibility checking, and version management.
    """
    
    def __init__(
        self,
        repository: SchemaRepository,
        event_bus: EventBus
    ):
        self.repository = repository
        self.event_bus = event_bus
        
    async def register(
        self,
        name: str,
        schema_type: SchemaType,
        schema_definition: Dict[str, Any],
        compatibility: Optional[CompatibilityMode] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Register a new schema or version.
        
        Args:
            name: Schema name
            schema_type: Type of schema (AVRO, JSON_SCHEMA, etc.)
            schema_definition: Schema definition
            compatibility: Compatibility mode
            
        Returns:
            ServiceResult with registered schema
        """
        try:
            # Validate schema
            validation_errors = self._validate_schema(
                schema_type,
                schema_definition
            )
            if validation_errors:
                return ServiceResult.failure(
                    error="Schema validation failed",
                    details={"errors": validation_errors}
                )
            
            # Check if schema exists
            existing = await self.repository.find_by_name(name)
            
            if existing:
                # Check compatibility
                is_compatible = await self.repository.check_compatibility(
                    name,
                    schema_definition,
                    compatibility or existing.get('compatibility', CompatibilityMode.BACKWARD)
                )
                if not is_compatible:
                    return ServiceResult.failure(
                        error="Schema is not compatible with existing versions",
                        details={"compatibility_mode": compatibility}
                    )
            
            # Register schema
            schema = await self.repository.register(
                name=name,
                schema_type=schema_type,
                schema_definition=schema_definition,
                compatibility=compatibility
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "SchemaRegistered",
                "schema_id": schema['id'],
                "name": name,
                "version": schema.get('version', 1),
                "schema_type": schema_type
            })
            
            return ServiceResult.success(schema)
            
        except Exception as e:
            logger.error(f"Failed to register schema: {e}")
            return ServiceResult.failure(
                error="Failed to register schema",
                details={"error": str(e)}
            )
    
    async def get_schema(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Get schema by ID and optional version"""
        try:
            schema = await self.repository.get_schema(schema_id, version)
            
            if not schema:
                return ServiceResult.failure(
                    error="Schema not found",
                    details={"schema_id": schema_id, "version": version}
                )
            
            return ServiceResult.success(schema)
            
        except Exception as e:
            logger.error(f"Failed to get schema: {e}")
            return ServiceResult.failure(
                error="Failed to get schema",
                details={"error": str(e)}
            )
    
    async def list_schemas(
        self,
        schema_type: Optional[SchemaType] = None,
        limit: int = 100,
        offset: int = 0
    ) -> ServiceResult[Tuple[List[Dict[str, Any]], int]]:
        """List schemas with optional filtering"""
        try:
            schemas, total = await self.repository.list_schemas(
                schema_type=schema_type,
                limit=limit,
                offset=offset
            )
            
            return ServiceResult.success((schemas, total))
            
        except Exception as e:
            logger.error(f"Failed to list schemas: {e}")
            return ServiceResult.failure(
                error="Failed to list schemas",
                details={"error": str(e)}
            )
    
    async def get_versions(
        self,
        schema_id: str
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Get all versions of a schema"""
        try:
            versions = await self.repository.get_versions(schema_id)
            
            return ServiceResult.success(versions)
            
        except Exception as e:
            logger.error(f"Failed to get schema versions: {e}")
            return ServiceResult.failure(
                error="Failed to get schema versions",
                details={"error": str(e)}
            )
    
    async def validate_compatibility(
        self,
        schema_id: str,
        new_schema: Dict[str, Any],
        compatibility_mode: Optional[CompatibilityMode] = None
    ) -> ServiceResult[bool]:
        """Validate if new schema is compatible"""
        try:
            is_compatible = await self.repository.check_compatibility(
                schema_id,
                new_schema,
                compatibility_mode
            )
            
            return ServiceResult.success(is_compatible)
            
        except Exception as e:
            logger.error(f"Failed to validate compatibility: {e}")
            return ServiceResult.failure(
                error="Failed to validate compatibility",
                details={"error": str(e)}
            )
    
    async def infer_schema(
        self,
        data_sample: List[Dict[str, Any]],
        schema_type: SchemaType = SchemaType.JSON_SCHEMA
    ) -> ServiceResult[Dict[str, Any]]:
        """Infer schema from data sample"""
        try:
            if not data_sample:
                return ServiceResult.failure(
                    error="Empty data sample",
                    details={"sample_size": 0}
                )
            
            inferred_schema = await self.repository.infer_schema(
                data_sample,
                schema_type
            )
            
            return ServiceResult.success(inferred_schema)
            
        except Exception as e:
            logger.error(f"Failed to infer schema: {e}")
            return ServiceResult.failure(
                error="Failed to infer schema",
                details={"error": str(e)}
            )
    
    async def delete_schema(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> ServiceResult[bool]:
        """Delete schema or specific version"""
        try:
            # Check if schema is in use
            if await self._is_schema_in_use(schema_id, version):
                return ServiceResult.failure(
                    error="Cannot delete schema in use",
                    details={"schema_id": schema_id}
                )
            
            success = await self.repository.delete_schema(schema_id, version)
            
            if success:
                # Publish event
                await self.event_bus.publish({
                    "event_type": "SchemaDeleted",
                    "schema_id": schema_id,
                    "version": version
                })
            
            return ServiceResult.success(success)
            
        except Exception as e:
            logger.error(f"Failed to delete schema: {e}")
            return ServiceResult.failure(
                error="Failed to delete schema",
                details={"error": str(e)}
            )
    
    async def register_for_entity(
        self,
        entity_guid: str,
        schema_definition: Dict[str, Any]
    ) -> ServiceResult[Dict[str, Any]]:
        """Register schema for a specific entity"""
        try:
            # Generate schema name from entity
            schema_name = f"entity_{entity_guid}_schema"
            
            # Infer schema type from definition
            schema_type = self._infer_schema_type(schema_definition)
            
            # Register schema
            result = await self.register(
                name=schema_name,
                schema_type=schema_type,
                schema_definition=schema_definition
            )
            
            if result.success:
                # Link schema to entity
                await self.repository.link_schema_to_entity(
                    result.data['id'],
                    entity_guid
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to register schema for entity: {e}")
            return ServiceResult.failure(
                error="Failed to register schema for entity",
                details={"error": str(e)}
            )
    
    def _validate_schema(
        self,
        schema_type: SchemaType,
        schema_definition: Dict[str, Any]
    ) -> List[str]:
        """Validate schema definition"""
        errors = []
        
        if not schema_definition:
            errors.append("Schema definition is empty")
            return errors
        
        if schema_type == SchemaType.JSON_SCHEMA:
            # Validate JSON Schema
            required_fields = ['type', 'properties']
            for field in required_fields:
                if field not in schema_definition:
                    errors.append(f"Missing required field: {field}")
                    
        elif schema_type == SchemaType.AVRO:
            # Validate Avro schema
            required_fields = ['type', 'name']
            for field in required_fields:
                if field not in schema_definition:
                    errors.append(f"Missing required field: {field}")
        
        return errors
    
    def _infer_schema_type(self, schema_definition: Dict[str, Any]) -> SchemaType:
        """Infer schema type from definition"""
        if 'properties' in schema_definition and '$schema' in schema_definition:
            return SchemaType.JSON_SCHEMA
        elif 'fields' in schema_definition and 'namespace' in schema_definition:
            return SchemaType.AVRO
        else:
            return SchemaType.JSON_SCHEMA  # Default
    
    async def _is_schema_in_use(
        self,
        schema_id: str,
        version: Optional[int] = None
    ) -> bool:
        """Check if schema is being used by any entity"""
        # TODO: Implement check across entities
        return False 