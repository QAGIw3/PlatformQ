"""
Entity Service

Business logic for entity operations.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app.domain.catalog.entities import (
    Entity,
    EntityRepository,
    EntitySpecification,
    active_entities,
    by_type,
    by_owner
)
from app.core import Classifier
from app.events import EventBus
from app.services.interfaces import ServiceResult

logger = logging.getLogger(__name__)


class EntityService:
    """
    Service layer for entity operations.
    
    Handles business logic, validation, and orchestration.
    """
    
    def __init__(
        self,
        repository: EntityRepository,
        schema_service: Any,  # Avoid circular import
        event_bus: EventBus,
        classifier: Classifier
    ):
        self.repository = repository
        self.schema_service = schema_service
        self.event_bus = event_bus
        self.classifier = classifier
        
    async def create(self, request: Dict[str, Any]) -> ServiceResult[Entity]:
        """
        Create a new entity.
        
        Args:
            request: Entity creation request
            
        Returns:
            ServiceResult containing the created entity or error
        """
        try:
            # Validate request
            validation_errors = self._validate_create_request(request)
            if validation_errors:
                return ServiceResult.failure(
                    error="Validation failed",
                    details=validation_errors
                )
            
            # Check for duplicates
            existing = await self.repository.find_by_qualified_name(
                request['qualified_name'],
                request['type_name']
            )
            if existing:
                return ServiceResult.failure(
                    error="Entity already exists",
                    details={"qualified_name": request['qualified_name']}
                )
            
            # Create entity
            entity = Entity(
                guid="",  # Will be assigned by Atlas
                type_name=request['type_name'],
                qualified_name=request['qualified_name'],
                name=request['name'],
                description=request.get('description'),
                owner=request.get('owner'),
                attributes=request.get('attributes', {})
            )
            
            # Add classifications if provided
            for classification in request.get('classifications', []):
                entity.add_classification(classification)
                
            # Add tags if provided
            for tag in request.get('tags', []):
                entity.add_tag(tag)
            
            # Save entity
            saved_entity = await self.repository.save(entity)
            
            # Register schema if provided
            if request.get('schema'):
                await self.schema_service.register_for_entity(
                    saved_entity.guid,
                    request['schema']
                )
            
            # Auto-classify if enabled
            if request.get('auto_classify', True):
                await self._auto_classify(saved_entity)
            
            return ServiceResult.success(saved_entity)
            
        except Exception as e:
            logger.error(f"Failed to create entity: {e}")
            return ServiceResult.failure(
                error="Failed to create entity",
                details={"error": str(e)}
            )
    
    async def update(
        self,
        guid: str,
        updates: Dict[str, Any]
    ) -> ServiceResult[Entity]:
        """Update an existing entity"""
        try:
            # Get existing entity
            entity = await self.repository.find_by_id(guid)
            if not entity:
                return ServiceResult.failure(
                    error="Entity not found",
                    details={"guid": guid}
                )
            
            # Validate updates
            validation_errors = self._validate_update_request(updates)
            if validation_errors:
                return ServiceResult.failure(
                    error="Validation failed",
                    details=validation_errors
                )
            
            # Apply updates
            if 'attributes' in updates:
                entity.update_attributes(updates['attributes'])
                
            if 'description' in updates:
                entity.update_attribute('description', updates['description'])
                
            if 'owner' in updates:
                entity.update_attribute('owner', updates['owner'])
                
            # Handle classifications
            if 'add_classifications' in updates:
                for classification in updates['add_classifications']:
                    entity.add_classification(classification)
                    
            if 'remove_classifications' in updates:
                for classification in updates['remove_classifications']:
                    entity.remove_classification(classification)
            
            # Handle tags
            if 'add_tags' in updates:
                for tag in updates['add_tags']:
                    entity.add_tag(tag)
                    
            if 'remove_tags' in updates:
                for tag in updates['remove_tags']:
                    entity.remove_tag(tag)
            
            # Save changes
            saved_entity = await self.repository.save(entity)
            
            return ServiceResult.success(saved_entity)
            
        except Exception as e:
            logger.error(f"Failed to update entity {guid}: {e}")
            return ServiceResult.failure(
                error="Failed to update entity",
                details={"error": str(e)}
            )
    
    async def get_by_id(self, guid: str) -> ServiceResult[Entity]:
        """Get entity by ID"""
        try:
            entity = await self.repository.find_by_id(guid)
            if not entity:
                return ServiceResult.failure(
                    error="Entity not found",
                    details={"guid": guid}
                )
                
            return ServiceResult.success(entity)
            
        except Exception as e:
            logger.error(f"Failed to get entity {guid}: {e}")
            return ServiceResult.failure(
                error="Failed to get entity",
                details={"error": str(e)}
            )
    
    async def delete(
        self,
        guid: str,
        hard_delete: bool = False
    ) -> ServiceResult[bool]:
        """Delete an entity"""
        try:
            # Check if entity exists
            entity = await self.repository.find_by_id(guid)
            if not entity:
                return ServiceResult.failure(
                    error="Entity not found",
                    details={"guid": guid}
                )
            
            # Check for dependencies
            if hard_delete:
                dependencies = await self._check_dependencies(guid)
                if dependencies:
                    return ServiceResult.failure(
                        error="Cannot delete entity with dependencies",
                        details={"dependencies": dependencies}
                    )
            
            # Delete entity
            success = await self.repository.delete(guid, soft=not hard_delete)
            
            return ServiceResult.success(success)
            
        except Exception as e:
            logger.error(f"Failed to delete entity {guid}: {e}")
            return ServiceResult.failure(
                error="Failed to delete entity",
                details={"error": str(e)}
            )
    
    async def list_entities(
        self,
        type_name: Optional[str] = None,
        owner: Optional[str] = None,
        classification: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> ServiceResult[Tuple[List[Entity], int]]:
        """List entities with filters"""
        try:
            # Build specification
            spec = active_entities()
            
            if type_name:
                spec = spec.and_(by_type(type_name))
                
            if owner:
                spec = spec.and_(by_owner(owner))
            
            # Get entities
            entities, total = await self.repository.find_by_specification(
                spec,
                limit=limit,
                offset=offset
            )
            
            return ServiceResult.success((entities, total))
            
        except Exception as e:
            logger.error(f"Failed to list entities: {e}")
            return ServiceResult.failure(
                error="Failed to list entities",
                details={"error": str(e)}
            )
    
    async def search(
        self,
        query: str,
        type_name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> ServiceResult[Tuple[List[Entity], int]]:
        """Search for entities"""
        try:
            entities, total = await self.repository.search(
                query=query,
                type_name=type_name,
                limit=limit,
                offset=offset
            )
            
            return ServiceResult.success((entities, total))
            
        except Exception as e:
            logger.error(f"Failed to search entities: {e}")
            return ServiceResult.failure(
                error="Failed to search entities",
                details={"error": str(e)}
            )
    
    async def bulk_create(
        self,
        requests: List[Dict[str, Any]]
    ) -> ServiceResult[List[Entity]]:
        """Create multiple entities"""
        try:
            # Validate all requests
            entities = []
            for i, request in enumerate(requests):
                validation_errors = self._validate_create_request(request)
                if validation_errors:
                    return ServiceResult.failure(
                        error=f"Validation failed for entity {i}",
                        details=validation_errors
                    )
                
                # Create entity
                entity = Entity(
                    guid="",
                    type_name=request['type_name'],
                    qualified_name=request['qualified_name'],
                    name=request['name'],
                    description=request.get('description'),
                    owner=request.get('owner'),
                    attributes=request.get('attributes', {})
                )
                entities.append(entity)
            
            # Save all entities
            saved_entities = await self.repository.bulk_save(entities)
            
            return ServiceResult.success(saved_entities)
            
        except Exception as e:
            logger.error(f"Failed to bulk create entities: {e}")
            return ServiceResult.failure(
                error="Failed to bulk create entities",
                details={"error": str(e)}
            )
    
    # Private helper methods
    
    def _validate_create_request(self, request: Dict[str, Any]) -> List[str]:
        """Validate entity creation request"""
        errors = []
        
        if not request.get('type_name'):
            errors.append("type_name is required")
        if not request.get('qualified_name'):
            errors.append("qualified_name is required")
        if not request.get('name'):
            errors.append("name is required")
            
        return errors
    
    def _validate_update_request(self, updates: Dict[str, Any]) -> List[str]:
        """Validate entity update request"""
        errors = []
        
        # Cannot update certain fields
        if 'guid' in updates:
            errors.append("Cannot update guid")
        if 'type_name' in updates:
            errors.append("Cannot update type_name")
        if 'qualified_name' in updates:
            errors.append("Cannot update qualified_name")
            
        return errors
    
    async def _auto_classify(self, entity: Entity):
        """Auto-classify entity"""
        try:
            result = await self.classifier.classify_entity(
                entity.guid,
                sample_data=entity.attributes.get('sample_data')
            )
            
            # Add detected classifications
            for classification, confidence in result.get('classifications', {}).items():
                if confidence > 0.8:  # High confidence threshold
                    entity.add_classification(classification)
                    
            # Save updated entity
            await self.repository.save(entity)
            
        except Exception as e:
            logger.error(f"Failed to auto-classify entity {entity.guid}: {e}")
    
    async def _check_dependencies(self, guid: str) -> List[str]:
        """Check for entity dependencies"""
        # TODO: Implement dependency checking
        # - Check lineage relationships
        # - Check if entity is referenced by others
        # - Check for child entities
        return [] 