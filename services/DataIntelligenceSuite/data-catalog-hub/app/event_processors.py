"""
Event Processors for Search Service

Handles index updates based on platform events.
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus
)
from platformq_events import (
    AssetCreatedEvent,
    AssetUpdatedEvent,
    AssetDeletedEvent,
    SimulationCompletedEvent,
    ProjectCreatedEvent,
    ProjectUpdatedEvent,
    DocumentUpdatedEvent,
    DocumentDeletedEvent,
    UserCreatedEvent,
    UserUpdatedEvent,
    SearchIndexRequestEvent,
    BulkIndexRequestEvent
)

from .repository import SearchIndexRepository
from .indexer import AssetIndexer, SimulationIndexer, ProjectIndexer, DocumentIndexer, UserIndexer

logger = logging.getLogger(__name__)


class SearchIndexEventProcessor(EventProcessor):
    """Process events that require search index updates"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 search_index_repo: SearchIndexRepository,
                 indexers: Dict[str, Any]):
        super().__init__(service_name, pulsar_url)
        self.search_index_repo = search_index_repo
        self.indexers = indexers
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting search index event processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping search index event processor")
        
    @event_handler("persistent://platformq/*/asset-created-events", AssetCreatedEvent)
    async def handle_asset_created(self, event: AssetCreatedEvent, msg):
        """Index newly created assets"""
        try:
            asset_indexer = self.indexers.get("assets")
            if not asset_indexer:
                logger.warning("Asset indexer not available")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Prepare document for indexing
            document = {
                "id": event.asset_id,
                "name": event.name,
                "description": event.description,
                "file_type": event.file_type,
                "tags": event.tags,
                "creator_id": event.creator_id,
                "tenant_id": event.tenant_id,
                "created_at": event.created_at,
                "storage_path": event.storage_path,
                "metadata": event.metadata,
                "status": "active"
            }
            
            # Index in Elasticsearch
            await asset_indexer.index_asset(event.asset_id, document)
            
            # Index vectors if content available
            if event.content_preview:
                await asset_indexer.index_asset_vectors(
                    event.asset_id,
                    event.content_preview,
                    event.metadata
                )
                
            # Record indexing operation
            await self.search_index_repo.record_indexing(
                entity_id=event.asset_id,
                entity_type="asset",
                operation="create",
                tenant_id=event.tenant_id
            )
            
            logger.info(f"Indexed new asset: {event.asset_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error indexing asset: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/asset-updated-events", AssetUpdatedEvent)
    async def handle_asset_updated(self, event: AssetUpdatedEvent, msg):
        """Update asset in search index"""
        try:
            asset_indexer = self.indexers.get("assets")
            if not asset_indexer:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Prepare update
            update_doc = {
                "name": event.name,
                "description": event.description,
                "tags": event.tags,
                "updated_at": event.updated_at,
                "metadata": event.metadata,
                "version": event.version
            }
            
            # Update in Elasticsearch
            await asset_indexer.update_asset(event.asset_id, update_doc)
            
            # Update vectors if content changed
            if event.content_changed and event.content_preview:
                await asset_indexer.update_asset_vectors(
                    event.asset_id,
                    event.content_preview
                )
                
            # Record update
            await self.search_index_repo.record_indexing(
                entity_id=event.asset_id,
                entity_type="asset",
                operation="update",
                tenant_id=event.tenant_id
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error updating asset index: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/asset-deleted-events", AssetDeletedEvent)
    async def handle_asset_deleted(self, event: AssetDeletedEvent, msg):
        """Remove asset from search index"""
        try:
            asset_indexer = self.indexers.get("assets")
            if not asset_indexer:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Delete from Elasticsearch
            await asset_indexer.delete_asset(event.asset_id)
            
            # Delete vectors
            await asset_indexer.delete_asset_vectors(event.asset_id)
            
            # Record deletion
            await self.search_index_repo.record_indexing(
                entity_id=event.asset_id,
                entity_type="asset",
                operation="delete",
                tenant_id=event.tenant_id
            )
            
            logger.info(f"Deleted asset from index: {event.asset_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error deleting asset from index: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/simulation-completed-events", SimulationCompletedEvent)
    async def handle_simulation_completed(self, event: SimulationCompletedEvent, msg):
        """Index completed simulation results"""
        try:
            sim_indexer = self.indexers.get("simulations")
            if not sim_indexer:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Prepare simulation document
            document = {
                "id": event.simulation_id,
                "name": event.simulation_name,
                "type": event.simulation_type,
                "status": event.status,
                "owner_id": event.owner_id,
                "tenant_id": event.tenant_id,
                "created_at": event.created_at,
                "completed_at": event.completed_at,
                "duration_seconds": event.duration_seconds,
                "result_path": event.result_path,
                "metrics": event.metrics,
                "parameters": event.parameters,
                "tags": event.tags
            }
            
            # Index simulation
            await sim_indexer.index_simulation(event.simulation_id, document)
            
            # Index result summary if available
            if event.result_summary:
                await sim_indexer.index_simulation_vectors(
                    event.simulation_id,
                    event.result_summary,
                    event.metrics
                )
                
            # Record indexing
            await self.search_index_repo.record_indexing(
                entity_id=event.simulation_id,
                entity_type="simulation",
                operation="create",
                tenant_id=event.tenant_id
            )
            
            logger.info(f"Indexed completed simulation: {event.simulation_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error indexing simulation: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/project-created-events", ProjectCreatedEvent)
    async def handle_project_created(self, event: ProjectCreatedEvent, msg):
        """Index new projects"""
        try:
            project_indexer = self.indexers.get("projects")
            if not project_indexer:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Prepare project document
            document = {
                "id": event.project_id,
                "name": event.project_name,
                "description": event.description,
                "type": event.project_type,
                "creator_id": event.creator_id,
                "tenant_id": event.tenant_id,
                "created_at": event.created_at,
                "collaborators": event.collaborators,
                "tags": event.tags,
                "status": "active",
                "visibility": event.visibility
            }
            
            # Index project
            await project_indexer.index_project(event.project_id, document)
            
            # Record indexing
            await self.search_index_repo.record_indexing(
                entity_id=event.project_id,
                entity_type="project",
                operation="create",
                tenant_id=event.tenant_id
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error indexing project: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/document-updated-events",
        DocumentUpdatedEvent,
        max_batch_size=50,
        max_wait_time_ms=5000
    )
    async def handle_document_updates(self, events: List[DocumentUpdatedEvent], msgs):
        """Batch index document updates"""
        try:
            doc_indexer = self.indexers.get("documents")
            if not doc_indexer:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Prepare batch updates
            bulk_operations = []
            for event in events:
                update_doc = {
                    "_id": event.document_id,
                    "_index": f"platformq_documents_{event.tenant_id}",
                    "_op_type": "update",
                    "doc": {
                        "name": event.document_name,
                        "path": event.document_path,
                        "updated_by": event.updated_by,
                        "updated_at": event.updated_at,
                        "version": event.version,
                        "size_bytes": event.size_bytes
                    },
                    "doc_as_upsert": True
                }
                bulk_operations.append(update_doc)
                
            # Bulk index
            if bulk_operations:
                await doc_indexer.bulk_index(bulk_operations)
                
            # Record operations
            for event in events:
                await self.search_index_repo.record_indexing(
                    entity_id=event.document_id,
                    entity_type="document",
                    operation="update",
                    tenant_id=event.tenant_id
                )
                
            logger.info(f"Batch indexed {len(events)} document updates")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error batch indexing documents: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/search-index-request-events", SearchIndexRequestEvent)
    async def handle_index_request(self, event: SearchIndexRequestEvent, msg):
        """Handle explicit index requests"""
        try:
            indexer = self.indexers.get(event.entity_type)
            if not indexer:
                logger.warning(f"No indexer for entity type: {event.entity_type}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Perform requested operation
            if event.operation == "index":
                await indexer.index_entity(event.entity_id, event.document)
            elif event.operation == "update":
                await indexer.update_entity(event.entity_id, event.document)
            elif event.operation == "delete":
                await indexer.delete_entity(event.entity_id)
            elif event.operation == "reindex":
                # Fetch fresh data and reindex
                await self._reindex_entity(event.entity_id, event.entity_type, event.tenant_id)
                
            # Record operation
            await self.search_index_repo.record_indexing(
                entity_id=event.entity_id,
                entity_type=event.entity_type,
                operation=event.operation,
                tenant_id=event.tenant_id
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing index request: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/user-created-events", UserCreatedEvent)
    async def handle_user_created(self, event: UserCreatedEvent, msg):
        """Index new users for user search"""
        try:
            user_indexer = self.indexers.get("users")
            if not user_indexer:
                # Create basic user indexer if not available
                from .indexer import UserIndexer
                user_indexer = UserIndexer(
                    self.indexers.get("assets").es_client if "assets" in self.indexers else None
                )
                
            if not user_indexer:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Prepare user document
            document = {
                "id": event.user_id,
                "username": event.username,
                "full_name": event.full_name,
                "email": event.email,
                "roles": event.roles,
                "tenant_id": event.tenant_id,
                "created_at": event.created_at,
                "status": "active",
                "searchable_name": f"{event.full_name} {event.username}"
            }
            
            # Index user
            await user_indexer.index_user(event.user_id, document)
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error indexing user: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _reindex_entity(self, entity_id: str, entity_type: str, tenant_id: str):
        """Reindex entity by fetching fresh data"""
        # This would typically fetch data from the appropriate service
        # For now, we'll just log the request
        logger.info(f"Reindex requested for {entity_type}:{entity_id}")
        
        # In a real implementation:
        # 1. Fetch entity data from source service
        # 2. Process and enrich data
        # 3. Index in Elasticsearch
        # 4. Update vectors if applicable 