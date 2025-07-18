"""
Repository for Notification Service

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func, desc

from platformq_shared import PostgresRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    NotificationCreatedEvent,
    NotificationDeliveredEvent,
    NotificationFailedEvent
)

from .database import Notification, NotificationFeedback, TrustedSource
from .schemas import (
    NotificationCreate,
    NotificationUpdate,
    NotificationFilter,
    TrustedSourceCreate
)

logger = logging.getLogger(__name__)


class NotificationRepository(PostgresRepository[Notification]):
    """Repository for notifications"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory, Notification)
        self.event_publisher = event_publisher
        
    def create(self, notification_data: Dict[str, Any]) -> Notification:
        """Create a new notification"""
        with self.get_session() as session:
            notification = Notification(
                id=uuid.uuid4(),
                recipient_id=notification_data["recipient_id"],
                tenant_id=notification_data["tenant_id"],
                type=notification_data["type"],
                title=notification_data["title"],
                content=notification_data["content"],
                priority=notification_data.get("priority", "medium"),
                channels=notification_data.get("channels", ["zulip"]),
                status="pending",
                metadata=notification_data.get("metadata", {}),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(notification)
            session.commit()
            session.refresh(notification)
            
            # Publish event
            if self.event_publisher:
                event = NotificationCreatedEvent(
                    notification_id=str(notification.id),
                    recipient_id=notification.recipient_id,
                    tenant_id=notification.tenant_id,
                    type=notification.type,
                    priority=notification.priority,
                    created_at=notification.created_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{notification.tenant_id}/notification-created-events",
                    event=event
                )
                
            logger.info(f"Created notification {notification.id} for recipient {notification.recipient_id}")
            return notification
            
    def update_status(self, notification_id: uuid.UUID, status: str,
                     error_message: Optional[str] = None) -> Optional[Notification]:
        """Update notification delivery status"""
        with self.get_session() as session:
            notification = session.query(Notification).filter(
                Notification.id == notification_id
            ).first()
            
            if not notification:
                return None
                
            notification.status = status
            notification.updated_at = datetime.utcnow()
            
            if status == "delivered":
                notification.delivered_at = datetime.utcnow()
            elif status == "failed" and error_message:
                notification.metadata["error"] = error_message
                
            session.commit()
            session.refresh(notification)
            
            # Publish appropriate event
            if self.event_publisher:
                if status == "delivered":
                    event = NotificationDeliveredEvent(
                        notification_id=str(notification.id),
                        recipient_id=notification.recipient_id,
                        delivered_at=notification.delivered_at.isoformat(),
                        channel=notification.metadata.get("delivered_channel", "unknown")
                    )
                    topic = f"persistent://platformq/{notification.tenant_id}/notification-delivered-events"
                elif status == "failed":
                    event = NotificationFailedEvent(
                        notification_id=str(notification.id),
                        recipient_id=notification.recipient_id,
                        error_message=error_message,
                        failed_at=datetime.utcnow().isoformat()
                    )
                    topic = f"persistent://platformq/{notification.tenant_id}/notification-failed-events"
                else:
                    return notification
                    
                self.event_publisher.publish_event(topic=topic, event=event)
                
            return notification
            
    def get_undelivered(self, tenant_id: str, older_than_minutes: int = 5) -> List[Notification]:
        """Get notifications that haven't been delivered"""
        with self.get_session() as session:
            cutoff_time = datetime.utcnow() - timedelta(minutes=older_than_minutes)
            
            return session.query(Notification).filter(
                and_(
                    Notification.tenant_id == tenant_id,
                    Notification.status == "pending",
                    Notification.created_at <= cutoff_time
                )
            ).order_by(Notification.priority.desc(), Notification.created_at).all()
            
    def get_by_recipient(self, recipient_id: str, tenant_id: str,
                        include_read: bool = False, limit: int = 50) -> List[Notification]:
        """Get notifications for a specific recipient"""
        with self.get_session() as session:
            query = session.query(Notification).filter(
                and_(
                    Notification.recipient_id == recipient_id,
                    Notification.tenant_id == tenant_id
                )
            )
            
            if not include_read:
                query = query.filter(
                    or_(
                        Notification.read_at.is_(None),
                        Notification.status != "read"
                    )
                )
                
            return query.order_by(
                Notification.created_at.desc()
            ).limit(limit).all()
            
    def mark_as_read(self, notification_id: uuid.UUID, recipient_id: str) -> Optional[Notification]:
        """Mark notification as read"""
        with self.get_session() as session:
            notification = session.query(Notification).filter(
                and_(
                    Notification.id == notification_id,
                    Notification.recipient_id == recipient_id
                )
            ).first()
            
            if not notification:
                return None
                
            notification.read_at = datetime.utcnow()
            notification.status = "read"
            notification.updated_at = datetime.utcnow()
            
            session.commit()
            session.refresh(notification)
            
            return notification
            
    def bulk_mark_as_read(self, notification_ids: List[uuid.UUID],
                         recipient_id: str) -> int:
        """Mark multiple notifications as read"""
        with self.get_session() as session:
            count = session.query(Notification).filter(
                and_(
                    Notification.id.in_(notification_ids),
                    Notification.recipient_id == recipient_id,
                    Notification.read_at.is_(None)
                )
            ).update({
                Notification.read_at: datetime.utcnow(),
                Notification.status: "read",
                Notification.updated_at: datetime.utcnow()
            }, synchronize_session=False)
            
            session.commit()
            return count
            
    def search(self, filters: NotificationFilter, tenant_id: str,
               skip: int = 0, limit: int = 100) -> List[Notification]:
        """Search notifications with filters"""
        with self.get_session() as session:
            query = session.query(Notification).filter(
                Notification.tenant_id == tenant_id
            )
            
            if filters.recipient_id:
                query = query.filter(Notification.recipient_id == filters.recipient_id)
                
            if filters.type:
                query = query.filter(Notification.type == filters.type)
                
            if filters.priority:
                query = query.filter(Notification.priority == filters.priority)
                
            if filters.status:
                query = query.filter(Notification.status == filters.status)
                
            if filters.created_after:
                query = query.filter(Notification.created_at >= filters.created_after)
                
            if filters.created_before:
                query = query.filter(Notification.created_at <= filters.created_before)
                
            if filters.unread_only:
                query = query.filter(Notification.read_at.is_(None))
                
            # Apply ordering
            query = query.order_by(
                Notification.created_at.desc()
            )
            
            return query.offset(skip).limit(limit).all()
            
    def get_statistics(self, tenant_id: str, 
                      time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """Get notification statistics"""
        with self.get_session() as session:
            base_query = session.query(Notification).filter(
                Notification.tenant_id == tenant_id
            )
            
            if time_window:
                cutoff = datetime.utcnow() - time_window
                base_query = base_query.filter(Notification.created_at >= cutoff)
                
            # Total notifications
            total = base_query.count()
            
            # By status
            status_counts = session.query(
                Notification.status,
                func.count(Notification.id)
            ).filter(
                Notification.tenant_id == tenant_id
            ).group_by(Notification.status).all()
            
            # By type
            type_counts = session.query(
                Notification.type,
                func.count(Notification.id)
            ).filter(
                Notification.tenant_id == tenant_id
            ).group_by(Notification.type).all()
            
            # Delivery rate
            delivered = base_query.filter(Notification.status == "delivered").count()
            delivery_rate = (delivered / total * 100) if total > 0 else 0
            
            # Average delivery time
            delivery_times = session.query(
                func.avg(
                    func.extract('epoch', Notification.delivered_at - Notification.created_at)
                )
            ).filter(
                and_(
                    Notification.tenant_id == tenant_id,
                    Notification.delivered_at.isnot(None)
                )
            ).scalar()
            
            return {
                "total_notifications": total,
                "by_status": dict(status_counts),
                "by_type": dict(type_counts),
                "delivery_rate": round(delivery_rate, 2),
                "avg_delivery_time_seconds": round(delivery_times or 0, 2)
            }


class TrustedSourceRepository(PostgresRepository[TrustedSource]):
    """Repository for trusted notification sources"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory, TrustedSource)
        
    def create(self, source_data: TrustedSourceCreate, tenant_id: str) -> TrustedSource:
        """Create a new trusted source"""
        with self.get_session() as session:
            source = TrustedSource(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                source_id=source_data.source_id,
                source_type=source_data.source_type,
                trust_level=source_data.trust_level,
                metadata=source_data.metadata or {},
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(source)
            session.commit()
            session.refresh(source)
            
            return source
            
    def get_by_source(self, source_id: str, source_type: str,
                     tenant_id: str) -> Optional[TrustedSource]:
        """Get trusted source by ID and type"""
        with self.get_session() as session:
            return session.query(TrustedSource).filter(
                and_(
                    TrustedSource.source_id == source_id,
                    TrustedSource.source_type == source_type,
                    TrustedSource.tenant_id == tenant_id
                )
            ).first()
            
    def update_trust_level(self, source_id: str, source_type: str,
                          tenant_id: str, new_level: float) -> Optional[TrustedSource]:
        """Update trust level for a source"""
        with self.get_session() as session:
            source = session.query(TrustedSource).filter(
                and_(
                    TrustedSource.source_id == source_id,
                    TrustedSource.source_type == source_type,
                    TrustedSource.tenant_id == tenant_id
                )
            ).first()
            
            if not source:
                return None
                
            source.trust_level = new_level
            source.updated_at = datetime.utcnow()
            
            session.commit()
            session.refresh(source)
            
            return source
            
    def get_high_trust_sources(self, tenant_id: str,
                              min_trust_level: float = 0.8) -> List[TrustedSource]:
        """Get sources with high trust levels"""
        with self.get_session() as session:
            return session.query(TrustedSource).filter(
                and_(
                    TrustedSource.tenant_id == tenant_id,
                    TrustedSource.trust_level >= min_trust_level
                )
            ).order_by(TrustedSource.trust_level.desc()).all() 