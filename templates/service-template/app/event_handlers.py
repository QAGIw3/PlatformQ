"""
Event Handlers

Demonstrates how to use the new event processing framework with
standardized patterns for retry, error handling, and monitoring.
"""

import logging
from typing import Any, Dict, Optional

from platformq_event_framework import (
    BaseEventProcessor,
    EventProcessingResult,
    EventProcessingStatus,
    EventContext,
    retry_on_failure,
    batch_processor,
    rate_limited
)

from .database import get_db_client

logger = logging.getLogger(__name__)


class ServiceEventHandler(BaseEventProcessor):
    """
    Event handler for {service_name} service using the new framework
    """
    
    async def handle_event(self, event_data: Dict[str, Any], context: EventContext) -> EventProcessingResult:
        """
        Main event handler that routes to specific handlers based on event type
        """
        event_type = event_data.get("event_type", context.event_type)
        
        logger.info(f"Processing event {context.event_id} of type {event_type}")
        
        # Route to specific handlers based on event type
        handlers = {
            "entity_created": self.handle_entity_created,
            "entity_updated": self.handle_entity_updated,
            "entity_deleted": self.handle_entity_deleted,
            "batch_process": self.handle_batch_process,
        }
        
        handler = handlers.get(event_type)
        if not handler:
            logger.warning(f"No handler for event type: {event_type}")
            return EventProcessingResult(
                status=EventProcessingStatus.SKIPPED,
                message=f"No handler for event type: {event_type}"
            )
            
        try:
            return await handler(event_data, context)
        except Exception as e:
            logger.error(f"Error handling event {context.event_id}: {e}")
            raise
            
    @retry_on_failure(max_attempts=3, backoff_factor=2)
    async def handle_entity_created(self, event_data: Dict[str, Any], context: EventContext) -> EventProcessingResult:
        """
        Handle entity creation events with automatic retry
        """
        try:
            # Extract entity data
            entity_id = event_data.get("entity_id")
            entity_data = event_data.get("data", {})
            
            # Use the unified data gateway for database operations
            db_client = await get_db_client()
            
            # Example: Store entity in database
            query = """
                INSERT INTO entities (id, tenant_id, data, created_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (id) DO NOTHING
            """
            
            result = await db_client.query(
                query=query,
                context={
                    "tenant_id": context.tenant_id,
                    "parameters": [entity_id, context.tenant_id, entity_data]
                },
                target_database={"type": "postgresql", "name": "platformq"}
            )
            
            logger.info(f"Created entity {entity_id} for tenant {context.tenant_id}")
            
            return EventProcessingResult(
                status=EventProcessingStatus.SUCCESS,
                data={"entity_id": entity_id}
            )
            
        except Exception as e:
            logger.error(f"Failed to create entity: {e}")
            # The retry decorator will handle retries automatically
            raise
            
    async def handle_entity_updated(self, event_data: Dict[str, Any], context: EventContext) -> EventProcessingResult:
        """
        Handle entity update events
        """
        try:
            entity_id = event_data.get("entity_id")
            updates = event_data.get("updates", {})
            
            db_client = await get_db_client()
            
            # Update entity
            query = """
                UPDATE entities 
                SET data = data || $1, updated_at = NOW()
                WHERE id = $2 AND tenant_id = $3
            """
            
            result = await db_client.query(
                query=query,
                context={
                    "tenant_id": context.tenant_id,
                    "parameters": [updates, entity_id, context.tenant_id]
                },
                target_database={"type": "postgresql", "name": "platformq"}
            )
            
            return EventProcessingResult(
                status=EventProcessingStatus.SUCCESS,
                data={"entity_id": entity_id, "updated": True}
            )
            
        except Exception as e:
            logger.error(f"Failed to update entity: {e}")
            return EventProcessingResult(
                status=EventProcessingStatus.FAILED,
                error=e,
                message=str(e)
            )
            
    async def handle_entity_deleted(self, event_data: Dict[str, Any], context: EventContext) -> EventProcessingResult:
        """
        Handle entity deletion events
        """
        try:
            entity_id = event_data.get("entity_id")
            
            db_client = await get_db_client()
            
            # Soft delete entity
            query = """
                UPDATE entities 
                SET deleted_at = NOW()
                WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
            """
            
            result = await db_client.query(
                query=query,
                context={
                    "tenant_id": context.tenant_id,
                    "parameters": [entity_id, context.tenant_id]
                },
                target_database={"type": "postgresql", "name": "platformq"}
            )
            
            return EventProcessingResult(
                status=EventProcessingStatus.SUCCESS,
                data={"entity_id": entity_id, "deleted": True}
            )
            
        except Exception as e:
            logger.error(f"Failed to delete entity: {e}")
            return EventProcessingResult(
                status=EventProcessingStatus.FAILED,
                error=e,
                message=str(e)
            )
            
    @batch_processor(batch_size=100, timeout=10)
    @rate_limited(max_calls=1000, period=60)  # 1000 calls per minute
    async def handle_batch_process(self, event_data: Dict[str, Any], context: EventContext) -> EventProcessingResult:
        """
        Handle batch processing events with rate limiting
        """
        try:
            batch_id = event_data.get("batch_id")
            items = event_data.get("items", [])
            
            logger.info(f"Processing batch {batch_id} with {len(items)} items")
            
            db_client = await get_db_client()
            
            # Process items in batch using multi-query
            queries = []
            for item in items:
                queries.append({
                    "query": "INSERT INTO batch_items (batch_id, item_id, data) VALUES ($1, $2, $3)",
                    "context": {
                        "parameters": [batch_id, item["id"], item["data"]]
                    }
                })
                
            results = await db_client.multi_query(
                queries=queries,
                context={"tenant_id": context.tenant_id}
            )
            
            successful = sum(1 for r in results if not r.get("error"))
            
            logger.info(f"Batch {batch_id} processed: {successful}/{len(items)} successful")
            
            return EventProcessingResult(
                status=EventProcessingStatus.SUCCESS,
                data={
                    "batch_id": batch_id,
                    "processed": successful,
                    "total": len(items)
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to process batch: {e}")
            return EventProcessingResult(
                status=EventProcessingStatus.FAILED,
                error=e,
                message=str(e)
            )
            
    # Override the base process_event method if needed
    async def process_event(self, event_data: Any, context: EventContext) -> EventProcessingResult:
        """
        Main event processing method called by the framework
        """
        return await self.handle_event(event_data, context) 