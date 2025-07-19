"""
Core notification classes and manager.
"""

import logging
from enum import Enum
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field
import asyncio
from abc import ABC, abstractmethod

from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class NotificationChannel(Enum):
    """Supported notification channels"""
    EMAIL = "email"
    ZULIP = "zulip"
    WEBHOOK = "webhook"
    SMS = "sms"
    IN_APP = "in_app"


class NotificationPriority(Enum):
    """Notification priority levels"""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5


class NotificationStatus(Enum):
    """Notification delivery status"""
    PENDING = "pending"
    SENDING = "sending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class Notification:
    """Notification data model"""
    notification_id: str
    tenant_id: str
    user_id: Optional[str]
    channels: List[NotificationChannel]
    priority: NotificationPriority
    subject: str
    content: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: NotificationStatus = NotificationStatus.PENDING
    attempts: int = 0
    last_attempt_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    error_message: Optional[str] = None


class NotificationChannelBase(ABC):
    """Base class for notification channels"""
    
    @abstractmethod
    async def send(self, notification: Notification) -> bool:
        """Send notification through this channel"""
        pass
        
    @abstractmethod
    async def health_check(self) -> bool:
        """Check if channel is healthy"""
        pass


class NotificationManager:
    """
    Central notification manager that handles multi-channel delivery.
    
    This replaces the notification service with a library that can be
    used by any service needing notification capabilities.
    """
    
    def __init__(self,
                 event_publisher: Optional[EventPublisher] = None,
                 max_retries: int = 3,
                 retry_delay: int = 30):
        self.event_publisher = event_publisher
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.channels: Dict[NotificationChannel, NotificationChannelBase] = {}
        self._retry_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        
    def register_channel(self, 
                        channel_type: NotificationChannel, 
                        channel_impl: NotificationChannelBase):
        """Register a notification channel implementation"""
        self.channels[channel_type] = channel_impl
        logger.info(f"Registered notification channel: {channel_type.value}")
        
    async def send_notification(self,
                              tenant_id: str,
                              user_id: Optional[str],
                              channels: List[Union[str, NotificationChannel]],
                              subject: str,
                              content: str,
                              priority: Union[str, NotificationPriority] = NotificationPriority.NORMAL,
                              metadata: Dict[str, Any] = None) -> Notification:
        """
        Send a notification through specified channels.
        
        Args:
            tenant_id: Tenant identifier
            user_id: User identifier (optional for broadcast)
            channels: List of channels to send through
            subject: Notification subject
            content: Notification content
            priority: Notification priority
            metadata: Additional metadata
            
        Returns:
            Notification object with delivery status
        """
        # Convert string channels to enum
        channel_enums = []
        for ch in channels:
            if isinstance(ch, str):
                try:
                    channel_enums.append(NotificationChannel(ch))
                except ValueError:
                    logger.warning(f"Unknown channel: {ch}")
            else:
                channel_enums.append(ch)
                
        # Convert priority if string
        if isinstance(priority, str):
            try:
                priority = NotificationPriority[priority.upper()]
            except KeyError:
                priority = NotificationPriority.NORMAL
                
        # Create notification
        notification = Notification(
            notification_id=f"notif_{datetime.utcnow().timestamp()}",
            tenant_id=tenant_id,
            user_id=user_id,
            channels=channel_enums,
            priority=priority,
            subject=subject,
            content=content,
            metadata=metadata or {}
        )
        
        # Send through each channel
        await self._deliver_notification(notification)
        
        return notification
        
    async def _deliver_notification(self, notification: Notification):
        """Deliver notification through all specified channels"""
        notification.status = NotificationStatus.SENDING
        notification.attempts += 1
        notification.last_attempt_at = datetime.utcnow()
        
        success_count = 0
        failed_channels = []
        
        for channel_type in notification.channels:
            if channel_type not in self.channels:
                logger.warning(f"Channel {channel_type} not registered")
                failed_channels.append(channel_type)
                continue
                
            try:
                channel = self.channels[channel_type]
                success = await channel.send(notification)
                
                if success:
                    success_count += 1
                    logger.info(f"Notification {notification.notification_id} sent via {channel_type.value}")
                else:
                    failed_channels.append(channel_type)
                    
            except Exception as e:
                logger.error(f"Error sending notification via {channel_type}: {e}")
                failed_channels.append(channel_type)
                
        # Update status
        if success_count == len(notification.channels):
            notification.status = NotificationStatus.DELIVERED
            notification.delivered_at = datetime.utcnow()
        elif success_count > 0:
            notification.status = NotificationStatus.DELIVERED  # Partial success
            notification.error_message = f"Failed channels: {failed_channels}"
        else:
            notification.status = NotificationStatus.FAILED
            notification.error_message = "All channels failed"
            
            # Retry if under limit
            if notification.attempts < self.max_retries:
                notification.status = NotificationStatus.RETRYING
                await self._retry_queue.put((notification, failed_channels))
                
        # Publish event if publisher available
        if self.event_publisher:
            await self._publish_notification_event(notification)
            
    async def _publish_notification_event(self, notification: Notification):
        """Publish notification event"""
        try:
            await self.event_publisher.publish(
                f"persistent://platformq/{notification.tenant_id}/notification-events",
                {
                    "notification_id": notification.notification_id,
                    "status": notification.status.value,
                    "channels": [ch.value for ch in notification.channels],
                    "priority": notification.priority.value,
                    "attempts": notification.attempts,
                    "delivered_at": notification.delivered_at.isoformat() if notification.delivered_at else None,
                    "error_message": notification.error_message
                }
            )
        except Exception as e:
            logger.error(f"Failed to publish notification event: {e}")
            
    async def start_retry_processor(self):
        """Start background task to process retries"""
        self._running = True
        while self._running:
            try:
                # Get notification from retry queue with timeout
                notification, failed_channels = await asyncio.wait_for(
                    self._retry_queue.get(),
                    timeout=10.0
                )
                
                # Wait before retry
                await asyncio.sleep(self.retry_delay)
                
                # Retry only failed channels
                notification.channels = failed_channels
                await self._deliver_notification(notification)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in retry processor: {e}")
                
    async def stop(self):
        """Stop the notification manager"""
        self._running = False
        
    async def get_channel_health(self) -> Dict[str, bool]:
        """Get health status of all registered channels"""
        health_status = {}
        
        for channel_type, channel in self.channels.items():
            try:
                is_healthy = await channel.health_check()
                health_status[channel_type.value] = is_healthy
            except Exception as e:
                logger.error(f"Health check failed for {channel_type}: {e}")
                health_status[channel_type.value] = False
                
        return health_status 