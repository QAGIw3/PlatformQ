"""
Enhanced Repository for Digital Asset Service

Uses the new repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from platformq_shared import PostgresRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    AssetCreatedEvent, 
    AssetUpdatedEvent,
    AssetDeletedEvent,
    IndexableEntityEvent
)

from .models import DigitalAsset, AssetReview, ProcessingRule
from .schemas import AssetCreate, AssetUpdate, AssetFilter
from .core.config import settings

logger = logging.getLogger(__name__)


class AssetRepository(PostgresRepository[DigitalAsset]):
    """Enhanced repository for digital assets"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory, DigitalAsset)
        self.event_publisher = event_publisher
        
    def create(self, asset_data: AssetCreate, owner_id: str, tenant_id: str) -> DigitalAsset:
        """Create a new digital asset"""
        with self.get_session() as session:
            # Create asset
            asset = DigitalAsset(
                id=uuid.uuid4(),
                name=asset_data.name,
                description=asset_data.description,
                file_type=asset_data.file_type,
                file_size=asset_data.file_size,
                storage_path=asset_data.storage_path,
                creator_id=owner_id,
                tenant_id=tenant_id,
                metadata=asset_data.metadata or {},
                tags=asset_data.tags or [],
                status="active",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(asset)
            session.commit()
            session.refresh(asset)
            
            # Publish event if publisher available
            if self.event_publisher:
                event = AssetCreatedEvent(
                    asset_id=str(asset.id),
                    name=asset.name,
                    file_type=asset.file_type,
                    creator_id=asset.creator_id,
                    tenant_id=asset.tenant_id,
                    created_at=asset.created_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/asset-created-events",
                    event=event
                )
                
                # Also publish for search indexing
                indexable_event = IndexableEntityEvent(
                    entity_type="asset",
                    entity_id=str(asset.id),
                    operation="create",
                    tenant_id=tenant_id,
                    data={
                        "name": asset.name,
                        "description": asset.description,
                        "file_type": asset.file_type,
                        "tags": asset.tags,
                        "creator_id": asset.creator_id
                    }
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/indexable-entity-events",
                    event=indexable_event
                )
                
            logger.info(f"Created asset {asset.id} for tenant {tenant_id}")
            return asset
            
    def get_by_storage_path(self, storage_path: str) -> Optional[DigitalAsset]:
        """Get asset by storage path"""
        with self.get_session() as session:
            return session.query(DigitalAsset).filter(
                DigitalAsset.storage_path == storage_path
            ).first()
            
    def update_metadata(self, asset_id: uuid.UUID, metadata: Dict[str, Any], 
                       merge: bool = True) -> Optional[DigitalAsset]:
        """Update asset metadata"""
        with self.get_session() as session:
            asset = session.query(DigitalAsset).filter(
                DigitalAsset.id == asset_id
            ).first()
            
            if not asset:
                return None
                
            if merge:
                # Merge with existing metadata
                asset.metadata = {**asset.metadata, **metadata}
            else:
                # Replace metadata
                asset.metadata = metadata
                
            asset.updated_at = datetime.utcnow()
            session.commit()
            session.refresh(asset)
            
            # Publish update event
            if self.event_publisher:
                event = AssetUpdatedEvent(
                    asset_id=str(asset.id),
                    updated_fields=["metadata"],
                    updated_at=asset.updated_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{asset.tenant_id}/asset-updated-events",
                    event=event
                )
                
            return asset
            
    def update_payload(self, asset_id: uuid.UUID, payload: bytes, 
                      payload_schema_version: str) -> Optional[DigitalAsset]:
        """Update asset payload data"""
        with self.get_session() as session:
            asset = session.query(DigitalAsset).filter(
                DigitalAsset.id == asset_id
            ).first()
            
            if not asset:
                return None
                
            asset.payload = payload
            asset.payload_schema_version = payload_schema_version
            asset.updated_at = datetime.utcnow()
            
            session.commit()
            session.refresh(asset)
            
            return asset
            
    def update_processing_status(self, asset_id: uuid.UUID, status: str, 
                                error_message: Optional[str] = None) -> Optional[DigitalAsset]:
        """Update asset processing status"""
        with self.get_session() as session:
            asset = session.query(DigitalAsset).filter(
                DigitalAsset.id == asset_id
            ).first()
            
            if not asset:
                return None
                
            asset.processing_status = status
            if error_message:
                asset.metadata["processing_error"] = error_message
                
            asset.updated_at = datetime.utcnow()
            session.commit()
            session.refresh(asset)
            
            return asset
            
    def search(self, filters: AssetFilter, tenant_id: str, 
               skip: int = 0, limit: int = 100) -> List[DigitalAsset]:
        """Search assets with filters"""
        with self.get_session() as session:
            query = session.query(DigitalAsset).filter(
                DigitalAsset.tenant_id == tenant_id
            )
            
            # Apply filters
            if filters.name_contains:
                query = query.filter(
                    DigitalAsset.name.contains(filters.name_contains)
                )
                
            if filters.file_types:
                query = query.filter(
                    DigitalAsset.file_type.in_(filters.file_types)
                )
                
            if filters.tags:
                # Asset must have all specified tags
                for tag in filters.tags:
                    query = query.filter(
                        DigitalAsset.tags.contains([tag])
                    )
                    
            if filters.creator_id:
                query = query.filter(
                    DigitalAsset.creator_id == filters.creator_id
                )
                
            if filters.created_after:
                query = query.filter(
                    DigitalAsset.created_at >= filters.created_after
                )
                
            if filters.created_before:
                query = query.filter(
                    DigitalAsset.created_at <= filters.created_before
                )
                
            if filters.min_size:
                query = query.filter(
                    DigitalAsset.file_size >= filters.min_size
                )
                
            if filters.max_size:
                query = query.filter(
                    DigitalAsset.file_size <= filters.max_size
                )
                
            # Apply ordering
            if filters.order_by == "created_at":
                query = query.order_by(
                    DigitalAsset.created_at.desc() 
                    if filters.order_desc else DigitalAsset.created_at
                )
            elif filters.order_by == "name":
                query = query.order_by(
                    DigitalAsset.name.desc() 
                    if filters.order_desc else DigitalAsset.name
                )
            elif filters.order_by == "file_size":
                query = query.order_by(
                    DigitalAsset.file_size.desc() 
                    if filters.order_desc else DigitalAsset.file_size
                )
            else:
                query = query.order_by(DigitalAsset.created_at.desc())
                
            # Apply pagination
            return query.offset(skip).limit(limit).all()
            
    def get_by_tags(self, tags: List[str], tenant_id: str, 
                    match_all: bool = True) -> List[DigitalAsset]:
        """Get assets by tags"""
        with self.get_session() as session:
            query = session.query(DigitalAsset).filter(
                DigitalAsset.tenant_id == tenant_id
            )
            
            if match_all:
                # Must have all tags
                for tag in tags:
                    query = query.filter(
                        DigitalAsset.tags.contains([tag])
                    )
            else:
                # Must have at least one tag
                tag_filters = [
                    DigitalAsset.tags.contains([tag]) for tag in tags
                ]
                query = query.filter(or_(*tag_filters))
                
            return query.all()
            
    def get_assets_for_review(self, tenant_id: str, 
                             limit: int = 10) -> List[DigitalAsset]:
        """Get assets that need review"""
        with self.get_session() as session:
            # Get assets with pending reviews
            subquery = session.query(AssetReview.asset_id).filter(
                AssetReview.status == "pending"
            ).subquery()
            
            return session.query(DigitalAsset).filter(
                and_(
                    DigitalAsset.tenant_id == tenant_id,
                    DigitalAsset.id.in_(subquery),
                    DigitalAsset.status == "pending_review"
                )
            ).limit(limit).all()
            
    def get_marketplace_assets(self, tenant_id: str, 
                              skip: int = 0, limit: int = 100) -> List[DigitalAsset]:
        """Get assets listed in marketplace"""
        with self.get_session() as session:
            return session.query(DigitalAsset).filter(
                and_(
                    DigitalAsset.tenant_id == tenant_id,
                    DigitalAsset.marketplace_status == "listed",
                    DigitalAsset.status == "active"
                )
            ).order_by(DigitalAsset.created_at.desc()).offset(skip).limit(limit).all()
            
    def bulk_update_status(self, asset_ids: List[uuid.UUID], 
                          status: str, tenant_id: str) -> int:
        """Bulk update asset status"""
        with self.get_session() as session:
            count = session.query(DigitalAsset).filter(
                and_(
                    DigitalAsset.id.in_(asset_ids),
                    DigitalAsset.tenant_id == tenant_id
                )
            ).update({
                DigitalAsset.status: status,
                DigitalAsset.updated_at: datetime.utcnow()
            }, synchronize_session=False)
            
            session.commit()
            
            # Publish bulk update event
            if self.event_publisher and count > 0:
                for asset_id in asset_ids:
                    event = AssetUpdatedEvent(
                        asset_id=str(asset_id),
                        updated_fields=["status"],
                        updated_at=datetime.utcnow().isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{tenant_id}/asset-updated-events",
                        event=event
                    )
                    
            return count
            
    def get_statistics(self, tenant_id: str) -> Dict[str, Any]:
        """Get asset statistics for a tenant"""
        with self.get_session() as session:
            total_count = session.query(func.count(DigitalAsset.id)).filter(
                DigitalAsset.tenant_id == tenant_id
            ).scalar()
            
            total_size = session.query(func.sum(DigitalAsset.file_size)).filter(
                DigitalAsset.tenant_id == tenant_id
            ).scalar() or 0
            
            # Count by file type
            type_counts = session.query(
                DigitalAsset.file_type,
                func.count(DigitalAsset.id)
            ).filter(
                DigitalAsset.tenant_id == tenant_id
            ).group_by(DigitalAsset.file_type).all()
            
            # Count by status
            status_counts = session.query(
                DigitalAsset.status,
                func.count(DigitalAsset.id)
            ).filter(
                DigitalAsset.tenant_id == tenant_id
            ).group_by(DigitalAsset.status).all()
            
            return {
                "total_assets": total_count,
                "total_size_bytes": total_size,
                "by_file_type": dict(type_counts),
                "by_status": dict(status_counts),
                "average_size_bytes": total_size / total_count if total_count > 0 else 0
            }


class AssetReviewRepository(PostgresRepository[AssetReview]):
    """Repository for asset reviews"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory, AssetReview)
        
    def create_review(self, asset_id: uuid.UUID, reviewer_id: str, 
                     review_type: str, tenant_id: str) -> AssetReview:
        """Create a new review request"""
        with self.get_session() as session:
            review = AssetReview(
                id=uuid.uuid4(),
                asset_id=asset_id,
                reviewer_id=reviewer_id,
                review_type=review_type,
                tenant_id=tenant_id,
                status="pending",
                created_at=datetime.utcnow()
            )
            
            session.add(review)
            session.commit()
            session.refresh(review)
            
            return review
            
    def get_pending_reviews(self, reviewer_id: str, 
                           tenant_id: str) -> List[AssetReview]:
        """Get pending reviews for a reviewer"""
        with self.get_session() as session:
            return session.query(AssetReview).filter(
                and_(
                    AssetReview.reviewer_id == reviewer_id,
                    AssetReview.tenant_id == tenant_id,
                    AssetReview.status == "pending"
                )
            ).order_by(AssetReview.created_at).all()
            
    def complete_review(self, review_id: uuid.UUID, approved: bool, 
                       comments: Optional[str] = None) -> Optional[AssetReview]:
        """Complete a review"""
        with self.get_session() as session:
            review = session.query(AssetReview).filter(
                AssetReview.id == review_id
            ).first()
            
            if not review:
                return None
                
            review.status = "approved" if approved else "rejected"
            review.comments = comments
            review.completed_at = datetime.utcnow()
            
            session.commit()
            session.refresh(review)
            
            return review 