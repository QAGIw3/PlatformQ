"""
Atlas Entity Repository Implementation

Concrete implementation of entity repository using Apache Atlas.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app.domain.catalog.entities import (
    Entity,
    EntityRepository,
    EntitySpecification,
    EntityStatus
)
from app.core import AtlasClient
from app.services.storage import IgniteCacheAdapter
from app.events import EventBus, EntityCreated, EntityUpdated, EntityDeleted

logger = logging.getLogger(__name__)


class AtlasEntityRepository(EntityRepository):
    """
    Atlas-based implementation of entity repository.
    
    Handles persistence to Apache Atlas with caching and events.
    """
    
    def __init__(
        self,
        atlas_client: AtlasClient,
        cache_manager: IgniteCacheAdapter,
        event_bus: EventBus
    ):
        self.atlas_client = atlas_client
        self.cache_manager = cache_manager
        self.event_bus = event_bus
        self.cache_prefix = "entity"
        
    async def save(self, entity: Entity) -> Entity:
        """Save an entity to Atlas"""
        try:
            # Validate entity
            errors = entity.validate()
            if errors:
                raise ValueError(f"Invalid entity: {', '.join(errors)}")
            
            # Convert to Atlas format
            atlas_entity = self._to_atlas_format(entity)
            
            # Save to Atlas
            if entity.is_new:
                result = await self.atlas_client.create_entity(atlas_entity)
                entity.guid = result['guid']
                entity.created_time = datetime.utcnow()
                entity.created_by = "system"  # TODO: Get from context
                
                # Emit created event
                await self.event_bus.publish(EntityCreated(
                    entity_id=entity.guid,
                    entity_type=entity.type_name,
                    name=entity.name,
                    qualified_name=entity.qualified_name,
                    owner=entity.owner or "",
                    attributes=entity.attributes
                ))
            else:
                # Update existing
                await self.atlas_client.update_entity(
                    entity.guid,
                    atlas_entity['attributes']
                )
                entity.modified_time = datetime.utcnow()
                entity.modified_by = "system"  # TODO: Get from context
                entity.version += 1
                
                # Emit updated event
                changes = entity.get_changes()
                if changes:
                    await self.event_bus.publish(EntityUpdated(
                        entity_id=entity.guid,
                        entity_type=entity.type_name,
                        changed_attributes=changes,
                        previous_values={k: v['old'] for k, v in changes.items()}
                    ))
            
            # Update cache
            await self._update_cache(entity)
            
            # Mark changes as committed
            entity.commit_changes()
            
            return entity
            
        except Exception as e:
            logger.error(f"Failed to save entity: {e}")
            raise
            
    async def find_by_id(self, guid: str) -> Optional[Entity]:
        """Find entity by GUID"""
        # Check cache first
        cached = await self._get_from_cache(guid)
        if cached:
            return cached
            
        # Get from Atlas
        atlas_entity = await self.atlas_client.get_entity_by_guid(guid)
        if not atlas_entity:
            return None
            
        # Convert to domain model
        entity = self._from_atlas_format(atlas_entity)
        
        # Update cache
        await self._update_cache(entity)
        
        return entity
        
    async def find_by_qualified_name(
        self,
        qualified_name: str,
        type_name: str
    ) -> Optional[Entity]:
        """Find entity by qualified name"""
        atlas_entity = await self.atlas_client.get_entity_by_attribute(
            type_name,
            "qualifiedName",
            qualified_name
        )
        
        if not atlas_entity:
            return None
            
        return self._from_atlas_format(atlas_entity)
        
    async def find_by_specification(
        self,
        specification: EntitySpecification,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Entity], int]:
        """Find entities matching specification"""
        # For now, get all and filter in memory
        # TODO: Translate specifications to Atlas DSL
        all_entities, total = await self.find_all(limit=1000, offset=0)
        
        # Filter by specification
        matching = [e for e in all_entities if specification.is_satisfied_by(e)]
        
        # Apply pagination
        paginated = matching[offset:offset + limit]
        
        return paginated, len(matching)
        
    async def find_all(
        self,
        type_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Entity], int]:
        """Find all entities"""
        result = await self.atlas_client.search_entities(
            query="*",
            type_name=type_name,
            limit=limit,
            offset=offset
        )
        
        entities = []
        for atlas_entity in result.get('entities', []):
            entity = self._from_atlas_format(atlas_entity)
            entities.append(entity)
            
        return entities, result.get('count', 0)
        
    async def delete(self, guid: str, soft: bool = True) -> bool:
        """Delete an entity"""
        try:
            if soft:
                # Soft delete by updating status
                entity = await self.find_by_id(guid)
                if not entity:
                    return False
                    
                entity.mark_deleted()
                await self.save(entity)
            else:
                # Hard delete from Atlas
                success = await self.atlas_client.delete_entity(guid)
                if success:
                    await self._remove_from_cache(guid)
                    
            # Emit deleted event
            await self.event_bus.publish(EntityDeleted(
                entity_id=guid,
                entity_type="",  # TODO: Get from entity
                soft_delete=soft
            ))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete entity {guid}: {e}")
            return False
            
    async def exists(self, guid: str) -> bool:
        """Check if entity exists"""
        entity = await self.find_by_id(guid)
        return entity is not None
        
    async def count(
        self,
        type_name: Optional[str] = None,
        include_deleted: bool = False
    ) -> int:
        """Count entities"""
        # TODO: Use Atlas metrics API
        _, total = await self.find_all(type_name=type_name, limit=1)
        return total
        
    async def bulk_save(self, entities: List[Entity]) -> List[Entity]:
        """Save multiple entities"""
        # Convert all to Atlas format
        atlas_entities = [self._to_atlas_format(e) for e in entities]
        
        # Bulk create in Atlas
        guid_map = await self.atlas_client.bulk_create_entities(atlas_entities)
        
        # Update GUIDs for new entities
        saved_entities = []
        for entity in entities:
            if entity.is_new and entity.qualified_name in guid_map:
                entity.guid = guid_map[entity.qualified_name]
                entity.created_time = datetime.utcnow()
                entity.created_by = "system"
                
            entity.commit_changes()
            saved_entities.append(entity)
            
            # Update cache
            await self._update_cache(entity)
            
        return saved_entities
        
    async def search(
        self,
        query: str,
        type_name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[Entity], int]:
        """Full-text search"""
        result = await self.atlas_client.search_entities(
            query=query,
            type_name=type_name,
            limit=limit,
            offset=offset
        )
        
        entities = []
        for atlas_entity in result.get('entities', []):
            entity = self._from_atlas_format(atlas_entity)
            entities.append(entity)
            
        return entities, result.get('count', 0)
        
    async def get_recently_updated(
        self,
        since: datetime,
        type_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Entity]:
        """Get recently updated entities"""
        # TODO: Use Atlas audit API
        all_entities, _ = await self.find_all(type_name=type_name, limit=limit)
        
        recent = []
        for entity in all_entities:
            if entity.modified_time and entity.modified_time > since:
                recent.append(entity)
                
        return recent
        
    # Private helper methods
    
    def _to_atlas_format(self, entity: Entity) -> Dict[str, Any]:
        """Convert domain entity to Atlas format"""
        return {
            "typeName": entity.type_name,
            "guid": entity.guid if not entity.is_new else None,
            "attributes": {
                "qualifiedName": entity.qualified_name,
                "name": entity.name,
                "description": entity.description,
                "owner": entity.owner,
                **entity.attributes
            },
            "classifications": [
                {"typeName": c} for c in entity.classifications
            ]
        }
        
    def _from_atlas_format(self, atlas_entity: Dict[str, Any]) -> Entity:
        """Convert Atlas entity to domain model"""
        attributes = atlas_entity.get('attributes', {})
        
        # Extract standard attributes
        entity_data = {
            'guid': atlas_entity['guid'],
            'type_name': atlas_entity['typeName'],
            'qualified_name': attributes.get('qualifiedName', ''),
            'name': attributes.get('name', ''),
            'description': attributes.get('description'),
            'owner': attributes.get('owner'),
            'status': EntityStatus(atlas_entity.get('status', 'ACTIVE')),
            'version': atlas_entity.get('version', 1),
            'classifications': [
                c.get('typeName', '') 
                for c in atlas_entity.get('classifications', [])
            ],
            'tags': atlas_entity.get('labels', [])
        }
        
        # Add timestamps if available
        if atlas_entity.get('createTime'):
            entity_data['created_time'] = datetime.fromtimestamp(
                atlas_entity['createTime'] / 1000
            )
        if atlas_entity.get('updateTime'):
            entity_data['modified_time'] = datetime.fromtimestamp(
                atlas_entity['updateTime'] / 1000
            )
            
        # Add custom attributes
        custom_attrs = {}
        for key, value in attributes.items():
            if key not in ['qualifiedName', 'name', 'description', 'owner']:
                custom_attrs[key] = value
        entity_data['attributes'] = custom_attrs
        
        return Entity(**entity_data)
        
    async def _update_cache(self, entity: Entity):
        """Update entity in cache"""
        cache_key = f"{self.cache_prefix}:{entity.guid}"
        await self.cache_manager.set(
            cache_key,
            entity.to_dict(),
            ttl=300  # 5 minutes
        )
        
    async def _get_from_cache(self, guid: str) -> Optional[Entity]:
        """Get entity from cache"""
        cache_key = f"{self.cache_prefix}:{guid}"
        cached_data = await self.cache_manager.get(cache_key)
        
        if cached_data:
            return Entity.from_dict(cached_data)
        return None
        
    async def _remove_from_cache(self, guid: str):
        """Remove entity from cache"""
        cache_key = f"{self.cache_prefix}:{guid}"
        await self.cache_manager.delete(cache_key) 