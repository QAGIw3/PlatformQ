"""
Event Processors for Notification Service

Handles notification-related events using the event processing framework.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
import asyncio

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients
)
from platformq_events import (
    UserCreatedEvent,
    DocumentUpdatedEvent,
    ProactiveAlertEvent,
    AssetCreatedEvent,
    SimulationCompletedEvent,
    WorkflowCompletedEvent,
    NotificationRequestEvent,
    TenantCreatedEvent
)

from .repository import NotificationRepository
from .trust.trust_enhanced_notifications import (
    TrustEnhancedNotificationSystem,
    NotificationDeliveryManager,
    NotificationPriority,
    NotificationTrustLevel
)

logger = logging.getLogger(__name__)


class NotificationEventProcessor(EventProcessor):
    """Process events that trigger notifications"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 notification_repo: NotificationRepository,
                 trust_system: TrustEnhancedNotificationSystem,
                 delivery_manager: NotificationDeliveryManager,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.notification_repo = notification_repo
        self.trust_system = trust_system
        self.delivery_manager = delivery_manager
        self.service_clients = service_clients
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting notification event processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping notification event processor")
        
    @event_handler("persistent://platformq/*/user-created-events", UserCreatedEvent)
    async def handle_user_created(self, event: UserCreatedEvent, msg):
        """Send welcome notification to new users"""
        try:
            # Create welcome notification
            notification = await self.notification_repo.create({
                "recipient_id": event.user_id,
                "tenant_id": event.tenant_id,
                "type": "welcome",
                "title": "Welcome to PlatformQ!",
                "content": f"Hello {event.full_name}, welcome to PlatformQ! Let's get you started.",
                "priority": NotificationPriority.MEDIUM.value,
                "channels": ["zulip", "email"]
            })
            
            # Determine trust level for new user (usually low initially)
            trust_level = await self.trust_system.calculate_trust_level(
                user_id=event.user_id,
                notification_type="welcome"
            )
            
            # Send notification
            result = await self.delivery_manager.send_notification(
                notification=notification,
                trust_level=trust_level
            )
            
            if result.delivered:
                logger.info(f"Welcome notification sent to user {event.user_id}")
            else:
                logger.warning(f"Failed to send welcome notification: {result.error}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing user created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/document-updated-events", DocumentUpdatedEvent)
    async def handle_document_updated(self, event: DocumentUpdatedEvent, msg):
        """Notify collaborators about document updates"""
        try:
            # Get document collaborators
            collaborators = await self._get_document_collaborators(
                event.document_id,
                event.tenant_id
            )
            
            # Filter out the updater
            recipients = [c for c in collaborators if c != event.updated_by]
            
            if not recipients:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Create notification for each recipient
            for recipient_id in recipients:
                notification = await self.notification_repo.create({
                    "recipient_id": recipient_id,
                    "tenant_id": event.tenant_id,
                    "type": "document_update",
                    "title": f"Document '{event.document_name}' Updated",
                    "content": f"{event.updater_name} made changes to '{event.document_name}'",
                    "priority": NotificationPriority.LOW.value,
                    "channels": ["zulip"],
                    "metadata": {
                        "document_id": event.document_id,
                        "version": event.version
                    }
                })
                
                # Calculate trust level
                trust_level = await self.trust_system.calculate_trust_level(
                    user_id=recipient_id,
                    sender_id=event.updated_by,
                    notification_type="document_update"
                )
                
                # Send notification
                await self.delivery_manager.send_notification(
                    notification=notification,
                    trust_level=trust_level
                )
                
            logger.info(f"Sent document update notifications to {len(recipients)} users")
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing document updated event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/proactive-alert-events", ProactiveAlertEvent)
    async def handle_proactive_alert(self, event: ProactiveAlertEvent, msg):
        """Handle AI-generated proactive alerts"""
        try:
            # Determine priority based on alert severity
            priority_map = {
                "critical": NotificationPriority.CRITICAL,
                "high": NotificationPriority.HIGH,
                "medium": NotificationPriority.MEDIUM,
                "low": NotificationPriority.LOW
            }
            priority = priority_map.get(event.severity, NotificationPriority.MEDIUM)
            
            # Create notification
            notification = await self.notification_repo.create({
                "recipient_id": event.user_id,
                "tenant_id": event.tenant_id,
                "type": "proactive_alert",
                "title": event.alert_title,
                "content": event.alert_message,
                "priority": priority.value,
                "channels": self._determine_channels_by_priority(priority),
                "metadata": {
                    "alert_type": event.alert_type,
                    "source_system": event.source_system,
                    "confidence_score": event.confidence_score
                }
            })
            
            # High trust for system-generated alerts
            trust_level = NotificationTrustLevel.HIGH
            
            # Send notification with appropriate urgency
            result = await self.delivery_manager.send_notification(
                notification=notification,
                trust_level=trust_level,
                immediate=priority in [NotificationPriority.CRITICAL, NotificationPriority.HIGH]
            )
            
            logger.info(f"Proactive alert sent: {event.alert_type} to user {event.user_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing proactive alert: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/notification-request-events",
        NotificationRequestEvent,
        max_batch_size=50,
        max_wait_time_ms=5000
    )
    async def handle_notification_requests(self, events: List[NotificationRequestEvent], msgs):
        """Handle batch notification requests efficiently"""
        try:
            successful = 0
            
            # Group by tenant for efficiency
            by_tenant = {}
            for event in events:
                if event.tenant_id not in by_tenant:
                    by_tenant[event.tenant_id] = []
                by_tenant[event.tenant_id].append(event)
                
            # Process each tenant's notifications
            for tenant_id, tenant_events in by_tenant.items():
                # Bulk create notifications
                notifications = []
                for event in tenant_events:
                    notification = await self.notification_repo.create({
                        "recipient_id": event.recipient_id,
                        "tenant_id": tenant_id,
                        "type": event.notification_type,
                        "title": event.title,
                        "content": event.content,
                        "priority": event.priority,
                        "channels": event.channels,
                        "metadata": event.metadata
                    })
                    notifications.append((notification, event))
                    
                # Send notifications in parallel
                tasks = []
                for notification, event in notifications:
                    # Calculate trust level
                    trust_level = await self.trust_system.calculate_trust_level(
                        user_id=event.recipient_id,
                        sender_id=event.sender_id,
                        notification_type=event.notification_type
                    )
                    
                    task = self.delivery_manager.send_notification(
                        notification=notification,
                        trust_level=trust_level
                    )
                    tasks.append(task)
                    
                results = await asyncio.gather(*tasks, return_exceptions=True)
                successful += sum(1 for r in results if not isinstance(r, Exception) and r.delivered)
                
            logger.info(f"Processed {successful}/{len(events)} notification requests successfully")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing notification batch: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/simulation-completed-events", SimulationCompletedEvent)
    async def handle_simulation_completed(self, event: SimulationCompletedEvent, msg):
        """Notify users when simulations complete"""
        try:
            # Get simulation owner and collaborators
            simulation_info = await self.service_clients.get(
                "simulation-service",
                f"/api/v1/simulations/{event.simulation_id}"
            )
            
            recipients = [simulation_info["owner_id"]] + simulation_info.get("collaborators", [])
            
            # Create notifications
            for recipient_id in recipients:
                notification = await self.notification_repo.create({
                    "recipient_id": recipient_id,
                    "tenant_id": event.tenant_id,
                    "type": "simulation_complete",
                    "title": f"Simulation '{event.simulation_name}' Completed",
                    "content": f"Your simulation has finished with status: {event.status}",
                    "priority": NotificationPriority.MEDIUM.value,
                    "channels": ["zulip", "email"],
                    "metadata": {
                        "simulation_id": event.simulation_id,
                        "duration_seconds": event.duration_seconds,
                        "result_url": event.result_url
                    }
                })
                
                # Send notification
                await self.delivery_manager.send_notification(
                    notification=notification,
                    trust_level=NotificationTrustLevel.HIGH
                )
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing simulation completed event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _get_document_collaborators(self, document_id: str, tenant_id: str) -> List[str]:
        """Get list of document collaborators"""
        try:
            response = await self.service_clients.get(
                "digital-asset-service",
                f"/api/v1/digital-assets/{document_id}/collaborators"
            )
            return response.get("collaborators", [])
        except Exception as e:
            logger.error(f"Failed to get document collaborators: {e}")
            return []
            
    def _determine_channels_by_priority(self, priority: NotificationPriority) -> List[str]:
        """Determine notification channels based on priority"""
        if priority == NotificationPriority.CRITICAL:
            return ["zulip", "email", "sms", "push"]
        elif priority == NotificationPriority.HIGH:
            return ["zulip", "email", "push"]
        elif priority == NotificationPriority.MEDIUM:
            return ["zulip", "email"]
        else:
            return ["zulip"] 