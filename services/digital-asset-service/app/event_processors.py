"""
Event Processors for Digital Asset Service

Handles all asset-related events using the new event processing framework.
"""

import logging
import uuid
from typing import Optional, Dict, Any
from datetime import datetime

from platformq_shared import (
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    batch_event_handler
)
from platformq_events import (
    FunctionExecutionCompletedEvent,
    AssetCreatedEvent,
    AssetUpdatedEvent,
    AssetProcessingCompletedEvent,
    DataLakeAssetCreatedEvent,
    AssetVCIssuedEvent,
    AssetUploadedEvent,
    AssetMetadataUpdatedEvent
)

from .repository import AssetRepository
from .schemas import AssetCreate, AssetUpdate
from .marketplace.blockchain_client import BlockchainClient

logger = logging.getLogger(__name__)


class AssetEventProcessor(EventProcessor):
    """Main event processor for asset lifecycle events"""
    
    def __init__(self, service_name: str, pulsar_url: str, 
                 asset_repository: AssetRepository, event_publisher):
        super().__init__(service_name, pulsar_url)
        self.asset_repository = asset_repository
        self.event_publisher = event_publisher
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting asset event processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping asset event processor")
        
    @event_handler("persistent://platformq/*/asset-uploaded-events", AssetUploadedEvent)
    async def handle_asset_uploaded(self, event: AssetUploadedEvent, msg):
        """Handle asset upload completion"""
        try:
            # Create asset record
            asset = AssetCreate(
                name=event.name,
                description=event.description,
                file_type=event.file_type,
                file_size=event.file_size,
                storage_path=event.storage_path,
                creator_id=event.creator_id,
                tenant_id=event.tenant_id,
                metadata=event.metadata or {}
            )
            
            created_asset = self.asset_repository.create(asset)
            
            # Publish asset created event
            created_event = AssetCreatedEvent(
                asset_id=str(created_asset.id),
                name=created_asset.name,
                file_type=created_asset.file_type,
                creator_id=created_asset.creator_id,
                tenant_id=created_asset.tenant_id,
                created_at=created_asset.created_at.isoformat()
            )
            
            await self.event_publisher.publish_event(
                topic=f"persistent://platformq/{event.tenant_id}/asset-created-events",
                event=created_event
            )
            
            logger.info(f"Created asset {created_asset.id} from upload event")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing asset upload: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/asset-metadata-update-events", 
        AssetMetadataUpdatedEvent,
        max_batch_size=50,
        max_wait_time_ms=1000
    )
    async def handle_metadata_updates(self, events: list[AssetMetadataUpdatedEvent], msgs):
        """Handle batch metadata updates"""
        try:
            # Process updates in batch
            for event in events:
                self.asset_repository.update_metadata(
                    asset_id=uuid.UUID(event.asset_id),
                    metadata=event.metadata,
                    version=event.version
                )
                
            logger.info(f"Processed {len(events)} metadata updates in batch")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing metadata batch: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            )


class FunctionResultProcessor(EventProcessor):
    """Process function execution results for assets"""
    
    def __init__(self, service_name: str, pulsar_url: str, asset_repository: AssetRepository):
        super().__init__(service_name, pulsar_url)
        self.asset_repository = asset_repository
        
    @event_handler(
        "persistent://platformq/*/function-execution-completed-events",
        FunctionExecutionCompletedEvent
    )
    async def handle_function_result(self, event: FunctionExecutionCompletedEvent, msg):
        """Process function execution results"""
        try:
            if event.status == "SUCCESS":
                # Update asset with function results
                if event.payload and event.payload_schema_version:
                    self.asset_repository.update_payload(
                        asset_id=uuid.UUID(event.asset_id),
                        payload=event.payload,
                        payload_schema_version=event.payload_schema_version
                    )
                elif event.results:
                    self.asset_repository.update_metadata(
                        asset_id=uuid.UUID(event.asset_id),
                        metadata=event.results
                    )
                    
                logger.info(f"Updated asset {event.asset_id} with function results")
                
            else:
                logger.error(f"Function execution failed for asset {event.asset_id}: {event.error_message}")
                
                # Could update asset status to indicate processing failure
                self.asset_repository.update_processing_status(
                    asset_id=uuid.UUID(event.asset_id),
                    status="processing_failed",
                    error_message=event.error_message
                )
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing function result: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class DataLakeAssetProcessor(EventProcessor):
    """Process data lake asset events"""
    
    def __init__(self, service_name: str, pulsar_url: str, 
                 asset_repository: AssetRepository, event_publisher):
        super().__init__(service_name, pulsar_url)
        self.asset_repository = asset_repository
        self.event_publisher = event_publisher
        
    @event_handler(
        "persistent://platformq/*/data-lake-asset-created-events",
        DataLakeAssetCreatedEvent
    )
    async def handle_data_lake_asset(self, event: DataLakeAssetCreatedEvent, msg):
        """Handle assets created in data lake"""
        try:
            # Check if asset already exists
            existing = self.asset_repository.get_by_storage_path(event.storage_path)
            if existing:
                logger.info(f"Asset already exists for path {event.storage_path}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Create asset from data lake event
            asset = AssetCreate(
                name=event.name,
                description=f"Data lake asset: {event.description}",
                file_type=event.file_type,
                file_size=event.file_size,
                storage_path=event.storage_path,
                creator_id=event.creator_id,
                tenant_id=event.tenant_id,
                metadata={
                    "source": "data_lake",
                    "quality_score": event.quality_score,
                    "schema_version": event.schema_version,
                    "tags": event.tags or []
                }
            )
            
            created_asset = self.asset_repository.create(asset)
            
            # Trigger asset processing if needed
            if event.trigger_processing:
                processing_event = AssetProcessingCompletedEvent(
                    asset_id=str(created_asset.id),
                    processing_type="data_quality",
                    status="pending"
                )
                
                await self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{event.tenant_id}/asset-processing-events",
                    event=processing_event
                )
                
            logger.info(f"Created asset {created_asset.id} from data lake")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing data lake asset: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class VCAssetProcessor(EventProcessor):
    """Process verifiable credential events for assets"""
    
    def __init__(self, service_name: str, pulsar_url: str, asset_repository: AssetRepository):
        super().__init__(service_name, pulsar_url)
        self.asset_repository = asset_repository
        self.blockchain_client = BlockchainClient()
        
    @event_handler(
        "persistent://platformq/*/asset-vc-issued-events",
        AssetVCIssuedEvent
    )
    async def handle_vc_issued(self, event: AssetVCIssuedEvent, msg):
        """Handle verifiable credential issuance for assets"""
        try:
            # Update asset with VC information
            asset = self.asset_repository.get(uuid.UUID(event.asset_id))
            if not asset:
                logger.error(f"Asset {event.asset_id} not found")
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    message="Asset not found"
                )
                
            # Add VC to asset metadata
            vc_metadata = {
                "verifiable_credentials": {
                    event.credential_type: {
                        "id": event.credential_id,
                        "issuer": event.issuer,
                        "issued_at": event.issued_at,
                        "expires_at": event.expires_at,
                        "proof": event.proof
                    }
                }
            }
            
            # Merge with existing metadata
            updated_metadata = {**asset.metadata, **vc_metadata}
            
            self.asset_repository.update_metadata(
                asset_id=asset.id,
                metadata=updated_metadata
            )
            
            # If it's an ownership credential, update blockchain
            if event.credential_type == "ownership":
                try:
                    tx_hash = await self.blockchain_client.record_ownership(
                        asset_id=event.asset_id,
                        owner_did=event.subject_did,
                        credential_id=event.credential_id
                    )
                    
                    # Update with blockchain transaction
                    self.asset_repository.update_metadata(
                        asset_id=asset.id,
                        metadata={
                            "blockchain_tx": tx_hash,
                            "owner_did": event.subject_did
                        }
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to record ownership on blockchain: {e}")
                    # Don't fail the whole process
                    
            logger.info(f"Updated asset {event.asset_id} with VC {event.credential_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing VC issuance: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            ) 