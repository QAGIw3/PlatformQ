"""
Base catalog interface and entities.

Provides common patterns for data catalog operations.
"""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class EntityType(str, Enum):
    """Catalog entity types"""
    DATASET = "dataset"
    TABLE = "table"
    DATABASE = "database"
    SCHEMA = "schema"
    COLUMN = "column"
    FILE = "file"
    STREAM = "stream"
    API = "api"
    MODEL = "model"
    PIPELINE = "pipeline"
    WORKFLOW = "workflow"
    REPORT = "report"
    DASHBOARD = "dashboard"
    METRIC = "metric"
    DIMENSION = "dimension"
    CUSTOM = "custom"


class EntityStatus(str, Enum):
    """Entity lifecycle status"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    DELETED = "deleted"
    PENDING = "pending"


@dataclass
class EntityMetadata:
    """Entity metadata"""
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    updated_at: datetime = field(default_factory=datetime.utcnow)
    updated_by: Optional[str] = None
    version: int = 1
    tags: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, Any] = field(default_factory=dict)
    
    def update(self, user: Optional[str] = None):
        """Update metadata timestamp and version"""
        self.updated_at = datetime.utcnow()
        self.updated_by = user
        self.version += 1


@dataclass
class CatalogEntity:
    """Base catalog entity"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    type: EntityType = EntityType.CUSTOM
    qualified_name: str = ""
    description: Optional[str] = None
    status: EntityStatus = EntityStatus.ACTIVE
    owner: Optional[str] = None
    metadata: EntityMetadata = field(default_factory=EntityMetadata)
    attributes: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[str, List[str]] = field(default_factory=dict)
    classifications: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.qualified_name:
            self.qualified_name = f"{self.type.value}://{self.name}"
            
    def add_relationship(self, relation_type: str, target_id: str):
        """Add relationship to another entity"""
        if relation_type not in self.relationships:
            self.relationships[relation_type] = []
        if target_id not in self.relationships[relation_type]:
            self.relationships[relation_type].append(target_id)
            
    def add_classification(self, classification: str):
        """Add classification to entity"""
        if classification not in self.classifications:
            self.classifications.append(classification)
            
    def set_attribute(self, key: str, value: Any):
        """Set custom attribute"""
        self.attributes[key] = value
        self.metadata.update()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.value,
            "qualified_name": self.qualified_name,
            "description": self.description,
            "status": self.status.value,
            "owner": self.owner,
            "metadata": {
                "created_at": self.metadata.created_at.isoformat(),
                "created_by": self.metadata.created_by,
                "updated_at": self.metadata.updated_at.isoformat(),
                "updated_by": self.metadata.updated_by,
                "version": self.metadata.version,
                "tags": self.metadata.tags,
                "labels": self.metadata.labels,
                "annotations": self.metadata.annotations
            },
            "attributes": self.attributes,
            "relationships": self.relationships,
            "classifications": self.classifications
        }


@dataclass
class CatalogConfig:
    """Catalog configuration"""
    name: str = "data-catalog"
    description: str = "Data catalog service"
    backend: str = "atlas"  # atlas, amundsen, datahub, custom
    cache_enabled: bool = True
    cache_ttl: int = 300  # seconds
    event_enabled: bool = True
    auto_discovery: bool = True
    discovery_interval: int = 3600  # seconds
    metadata_sync: bool = True
    lineage_tracking: bool = True
    quality_integration: bool = True
    search_enabled: bool = True
    glossary_enabled: bool = True
    access_control: bool = True
    custom_config: Dict[str, Any] = field(default_factory=dict)


class BaseCatalog(ABC):
    """
    Base catalog interface.
    
    Provides common functionality for data catalog operations:
    - Entity management
    - Metadata operations
    - Search capabilities
    - Lineage tracking
    - Event publishing
    """
    
    def __init__(
        self,
        config: CatalogConfig,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.config = config
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Entity storage
        self._entities: Dict[str, CatalogEntity] = {}
        self._type_index: Dict[EntityType, Set[str]] = {}
        self._name_index: Dict[str, str] = {}
        
        self._initialized = False
        
    async def initialize(self):
        """Initialize catalog"""
        if self._initialized:
            return
            
        logger.info(f"Initializing {self.config.name} catalog")
        
        try:
            # Initialize backend
            await self._initialize_backend()
            
            # Load existing entities
            await self._load_entities()
            
            # Start auto-discovery if enabled
            if self.config.auto_discovery:
                import asyncio
                asyncio.create_task(self._discovery_loop())
                
            self._initialized = True
            logger.info(f"{self.config.name} catalog initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize catalog: {e}")
            raise
            
    @abstractmethod
    async def _initialize_backend(self):
        """Initialize catalog backend"""
        pass
        
    @abstractmethod
    async def _load_entities(self):
        """Load entities from backend"""
        pass
        
    async def create_entity(
        self,
        entity: CatalogEntity,
        user: Optional[str] = None
    ) -> CatalogEntity:
        """Create new catalog entity"""
        # Validate entity
        self._validate_entity(entity)
        
        # Set metadata
        entity.metadata.created_by = user
        entity.metadata.updated_by = user
        
        # Store entity
        await self._store_entity(entity)
        
        # Update indices
        self._update_indices(entity)
        
        # Cache if enabled
        if self.cache and self.config.cache_enabled:
            await self._cache_entity(entity)
            
        # Publish event
        if self.event_bus and self.config.event_enabled:
            await self._publish_event("catalog.entity.created", entity)
            
        logger.info(f"Created entity: {entity.qualified_name}")
        return entity
        
    async def get_entity(
        self,
        entity_id: str,
        include_relationships: bool = True
    ) -> Optional[CatalogEntity]:
        """Get entity by ID"""
        # Check cache first
        if self.cache and self.config.cache_enabled:
            cached = await self._get_cached_entity(entity_id)
            if cached:
                return cached
                
        # Get from backend
        entity = await self._get_entity_from_backend(entity_id)
        
        if entity and include_relationships:
            await self._load_relationships(entity)
            
        # Cache result
        if entity and self.cache and self.config.cache_enabled:
            await self._cache_entity(entity)
            
        return entity
        
    async def update_entity(
        self,
        entity_id: str,
        updates: Dict[str, Any],
        user: Optional[str] = None
    ) -> CatalogEntity:
        """Update existing entity"""
        entity = await self.get_entity(entity_id)
        if not entity:
            raise ValueError(f"Entity not found: {entity_id}")
            
        # Apply updates
        for key, value in updates.items():
            if key == "metadata":
                # Don't overwrite metadata directly
                continue
            elif key == "attributes":
                entity.attributes.update(value)
            elif key == "relationships":
                entity.relationships.update(value)
            elif hasattr(entity, key):
                setattr(entity, key, value)
                
        # Update metadata
        entity.metadata.update(user)
        
        # Store updated entity
        await self._store_entity(entity)
        
        # Invalidate cache
        if self.cache and self.config.cache_enabled:
            await self._invalidate_cache(entity_id)
            
        # Publish event
        if self.event_bus and self.config.event_enabled:
            await self._publish_event("catalog.entity.updated", entity)
            
        logger.info(f"Updated entity: {entity.qualified_name}")
        return entity
        
    async def delete_entity(
        self,
        entity_id: str,
        soft_delete: bool = True,
        user: Optional[str] = None
    ) -> bool:
        """Delete entity"""
        entity = await self.get_entity(entity_id)
        if not entity:
            return False
            
        if soft_delete:
            # Soft delete - mark as deleted
            entity.status = EntityStatus.DELETED
            entity.metadata.update(user)
            await self._store_entity(entity)
        else:
            # Hard delete
            await self._delete_entity_from_backend(entity_id)
            self._remove_from_indices(entity)
            
        # Invalidate cache
        if self.cache and self.config.cache_enabled:
            await self._invalidate_cache(entity_id)
            
        # Publish event
        if self.event_bus and self.config.event_enabled:
            event_type = "catalog.entity.soft_deleted" if soft_delete else "catalog.entity.deleted"
            await self._publish_event(event_type, entity)
            
        logger.info(f"Deleted entity: {entity.qualified_name}")
        return True
        
    async def search_entities(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[CatalogEntity]:
        """Search for entities"""
        # Search in backend
        results = await self._search_backend(query, filters, limit, offset)
        
        # Apply post-processing
        results = self._apply_search_filters(results, filters)
        
        return results
        
    async def get_entities_by_type(
        self,
        entity_type: EntityType,
        limit: int = 100,
        offset: int = 0
    ) -> List[CatalogEntity]:
        """Get entities by type"""
        if entity_type in self._type_index:
            entity_ids = list(self._type_index[entity_type])[offset:offset + limit]
            entities = []
            for entity_id in entity_ids:
                entity = await self.get_entity(entity_id)
                if entity:
                    entities.append(entity)
            return entities
        return []
        
    def _validate_entity(self, entity: CatalogEntity):
        """Validate entity"""
        if not entity.name:
            raise ValueError("Entity name is required")
        if not entity.type:
            raise ValueError("Entity type is required")
            
    def _update_indices(self, entity: CatalogEntity):
        """Update internal indices"""
        # Type index
        if entity.type not in self._type_index:
            self._type_index[entity.type] = set()
        self._type_index[entity.type].add(entity.id)
        
        # Name index
        self._name_index[entity.name.lower()] = entity.id
        
    def _remove_from_indices(self, entity: CatalogEntity):
        """Remove from internal indices"""
        # Type index
        if entity.type in self._type_index:
            self._type_index[entity.type].discard(entity.id)
            
        # Name index
        self._name_index.pop(entity.name.lower(), None)
        
    async def _cache_entity(self, entity: CatalogEntity):
        """Cache entity"""
        if self.cache:
            cache_key = f"catalog:entity:{entity.id}"
            await self.cache.set(
                cache_key,
                entity.to_dict(),
                ttl=self.config.cache_ttl
            )
            
    async def _get_cached_entity(self, entity_id: str) -> Optional[CatalogEntity]:
        """Get entity from cache"""
        if self.cache:
            cache_key = f"catalog:entity:{entity_id}"
            cached = await self.cache.get(cache_key)
            if cached:
                return self._dict_to_entity(cached)
        return None
        
    async def _invalidate_cache(self, entity_id: str):
        """Invalidate cache for entity"""
        if self.cache:
            cache_key = f"catalog:entity:{entity_id}"
            await self.cache.delete(cache_key)
            
    async def _publish_event(self, event_type: str, entity: CatalogEntity):
        """Publish catalog event"""
        if self.event_bus:
            event = Event(
                type=event_type,
                source=f"catalog:{self.config.name}",
                data=entity.to_dict(),
                timestamp=datetime.utcnow()
            )
            await self.event_bus.publish(event)
            
    def _dict_to_entity(self, data: Dict[str, Any]) -> CatalogEntity:
        """Convert dictionary to entity"""
        # Create metadata
        metadata_data = data.get("metadata", {})
        metadata = EntityMetadata(
            created_at=datetime.fromisoformat(metadata_data.get("created_at", datetime.utcnow().isoformat())),
            created_by=metadata_data.get("created_by"),
            updated_at=datetime.fromisoformat(metadata_data.get("updated_at", datetime.utcnow().isoformat())),
            updated_by=metadata_data.get("updated_by"),
            version=metadata_data.get("version", 1),
            tags=metadata_data.get("tags", []),
            labels=metadata_data.get("labels", {}),
            annotations=metadata_data.get("annotations", {})
        )
        
        # Create entity
        return CatalogEntity(
            id=data["id"],
            name=data["name"],
            type=EntityType(data["type"]),
            qualified_name=data["qualified_name"],
            description=data.get("description"),
            status=EntityStatus(data.get("status", "active")),
            owner=data.get("owner"),
            metadata=metadata,
            attributes=data.get("attributes", {}),
            relationships=data.get("relationships", {}),
            classifications=data.get("classifications", [])
        )
        
    @abstractmethod
    async def _store_entity(self, entity: CatalogEntity):
        """Store entity in backend"""
        pass
        
    @abstractmethod
    async def _get_entity_from_backend(self, entity_id: str) -> Optional[CatalogEntity]:
        """Get entity from backend"""
        pass
        
    @abstractmethod
    async def _delete_entity_from_backend(self, entity_id: str):
        """Delete entity from backend"""
        pass
        
    @abstractmethod
    async def _search_backend(
        self,
        query: str,
        filters: Optional[Dict[str, Any]],
        limit: int,
        offset: int
    ) -> List[CatalogEntity]:
        """Search in backend"""
        pass
        
    @abstractmethod
    async def _load_relationships(self, entity: CatalogEntity):
        """Load entity relationships"""
        pass
        
    def _apply_search_filters(
        self,
        results: List[CatalogEntity],
        filters: Optional[Dict[str, Any]]
    ) -> List[CatalogEntity]:
        """Apply additional filters to search results"""
        if not filters:
            return results
            
        filtered = []
        for entity in results:
            include = True
            
            # Check status filter
            if "status" in filters and entity.status != filters["status"]:
                include = False
                
            # Check owner filter
            if "owner" in filters and entity.owner != filters["owner"]:
                include = False
                
            # Check tag filter
            if "tags" in filters:
                required_tags = set(filters["tags"])
                entity_tags = set(entity.metadata.tags)
                if not required_tags.issubset(entity_tags):
                    include = False
                    
            if include:
                filtered.append(entity)
                
        return filtered
        
    async def _discovery_loop(self):
        """Auto-discovery loop"""
        while True:
            try:
                await self._run_discovery()
                await asyncio.sleep(self.config.discovery_interval)
            except Exception as e:
                logger.error(f"Discovery error: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute
                
    @abstractmethod
    async def _run_discovery(self):
        """Run discovery process"""
        pass
        
    async def shutdown(self):
        """Shutdown catalog"""
        logger.info(f"Shutting down {self.config.name} catalog")
        # Implement cleanup
        pass 