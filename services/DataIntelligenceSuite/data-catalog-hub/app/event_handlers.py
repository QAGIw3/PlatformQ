"""
Event Handlers

Domain event handlers for catalog operations.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from dependency_injector.wiring import inject, Provide

from app.core.container import Container
from app.events import (
    EntityCreated,
    EntityUpdated,
    EntityDeleted,
    EntityClassified,
    SchemaRegistered,
    SchemaUpdated,
    LineageCreated,
    GlossaryTermCreated,
    GlossaryTermAssigned
)
from app.services.search import UnifiedSearchService
from app.services.analytics import AccessAnalyticsService
from app.services.catalog import EntityService

logger = logging.getLogger(__name__)


# Entity Event Handlers

@inject
async def index_entity_on_create(
    event: EntityCreated,
    search_service: UnifiedSearchService = Provide[Container.unified_search_service]
):
    """Index newly created entity for search"""
    try:
        logger.info(f"Indexing new entity {event.entity_id} for search")
        
        # Prepare entity data for indexing
        entity_data = {
            "id": event.entity_id,
            "type": event.entity_type,
            "name": event.name,
            "qualified_name": event.qualified_name,
            "owner": event.owner,
            "attributes": event.attributes,
            "created_at": event.occurred_at.isoformat()
        }
        
        # Index in search service
        await search_service.index_entity(entity_data)
        
    except Exception as e:
        logger.error(f"Failed to index entity {event.entity_id}: {e}")


@inject
async def update_index_on_entity_update(
    event: EntityUpdated,
    search_service: UnifiedSearchService = Provide[Container.unified_search_service]
):
    """Update search index when entity is updated"""
    try:
        logger.info(f"Updating search index for entity {event.entity_id}")
        
        # Update only changed fields in search index
        await search_service.update_entity_index(
            entity_id=event.entity_id,
            updates=event.changed_attributes
        )
        
    except Exception as e:
        logger.error(f"Failed to update index for entity {event.entity_id}: {e}")


@inject
async def remove_from_index_on_delete(
    event: EntityDeleted,
    search_service: UnifiedSearchService = Provide[Container.unified_search_service]
):
    """Remove entity from search index when deleted"""
    try:
        logger.info(f"Removing entity {event.entity_id} from search index")
        
        await search_service.remove_from_index(event.entity_id)
        
    except Exception as e:
        logger.error(f"Failed to remove entity {event.entity_id} from index: {e}")


# Classification Event Handlers

@inject
async def track_classification_event(
    event: EntityClassified,
    analytics_service: AccessAnalyticsService = Provide[Container.access_analytics_service]
):
    """Track classification events for analytics"""
    try:
        logger.info(f"Tracking classification event for entity {event.entity_id}")
        
        # Track the classification event
        await analytics_service.track_event(
            event_type="entity_classified",
            entity_id=event.entity_id,
            details={
                "classifications": event.classifications,
                "auto_classified": event.auto_classified,
                "confidence_scores": event.confidence_scores
            }
        )
        
    except Exception as e:
        logger.error(f"Failed to track classification event: {e}")


# Schema Event Handlers

@inject
async def notify_schema_changes(
    event: SchemaUpdated,
    entity_service: EntityService = Provide[Container.entity_service]
):
    """Notify about schema changes that might affect entities"""
    try:
        logger.info(f"Processing schema update for {event.schema_id}")
        
        if event.breaking_changes:
            # Find entities using this schema
            entities = await entity_service.find_by_schema(event.schema_id)
            
            # Log warning for affected entities
            for entity in entities:
                logger.warning(
                    f"Entity {entity.guid} may be affected by breaking schema changes: "
                    f"{event.breaking_changes}"
                )
                
    except Exception as e:
        logger.error(f"Failed to process schema update: {e}")


# Lineage Event Handlers

@inject
async def update_lineage_cache(
    event: LineageCreated
):
    """Update lineage cache when new lineage is created"""
    try:
        logger.info(f"New lineage created: {event.process_name}")
        
        # Lineage cache is handled by the repository
        # This handler could be used for additional processing
        
    except Exception as e:
        logger.error(f"Failed to process lineage creation: {e}")


# Glossary Event Handlers

@inject
async def index_glossary_term(
    event: GlossaryTermCreated,
    search_service: UnifiedSearchService = Provide[Container.unified_search_service]
):
    """Index new glossary term for search"""
    try:
        logger.info(f"Indexing glossary term {event.term_id}")
        
        # Prepare term data for indexing
        term_data = {
            "id": event.term_id,
            "type": "glossary_term",
            "name": event.term_name,
            "definition": event.definition,
            "status": event.status,
            "created_at": event.occurred_at.isoformat()
        }
        
        await search_service.index_entity(term_data)
        
    except Exception as e:
        logger.error(f"Failed to index glossary term: {e}")


# Analytics Event Handlers

@inject
async def track_entity_access(
    event: Dict[str, Any],
    analytics_service: AccessAnalyticsService = Provide[Container.access_analytics_service]
):
    """Track entity access for analytics"""
    try:
        entity_id = event.get("entity_id")
        user_id = event.get("user_id")
        access_type = event.get("access_type", "view")
        
        logger.info(f"Tracking access to entity {entity_id} by user {user_id}")
        
        await analytics_service.track_access(
            user_id=user_id,
            entity_id=entity_id,
            access_type=access_type,
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Failed to track entity access: {e}")


@inject
async def update_quality_metrics(
    event: Dict[str, Any]
):
    """Update quality metrics when quality is assessed"""
    try:
        entity_id = event.get("entity_id")
        quality_score = event.get("quality_score")
        
        logger.info(f"Updating quality metrics for entity {entity_id}: {quality_score}")
        
        # Quality metrics are handled by the quality service
        # This handler could trigger additional actions
        
    except Exception as e:
        logger.error(f"Failed to update quality metrics: {e}")


# Utility function to register all handlers

def register_all_handlers(event_bus):
    """Register all event handlers with the event bus"""
    
    # Entity handlers
    event_bus.register_handler("EntityCreated", index_entity_on_create)
    event_bus.register_handler("EntityUpdated", update_index_on_entity_update)
    event_bus.register_handler("EntityDeleted", remove_from_index_on_delete)
    event_bus.register_handler("EntityClassified", track_classification_event)
    
    # Schema handlers
    event_bus.register_handler("SchemaUpdated", notify_schema_changes)
    
    # Lineage handlers
    event_bus.register_handler("LineageCreated", update_lineage_cache)
    
    # Glossary handlers
    event_bus.register_handler("GlossaryTermCreated", index_glossary_term)
    
    # Analytics handlers
    event_bus.register_handler("EntityAccessed", track_entity_access)
    event_bus.register_handler("QualityAssessed", update_quality_metrics)
    
    logger.info("All event handlers registered") 