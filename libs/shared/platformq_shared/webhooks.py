"""
PlatformQ Webhook Handlers

This module provides webhook receivers and handlers for third-party integrations:
- Nextcloud webhooks (file events, user events, shares)
- Zulip webhooks (messages, reactions, user events)
- OpenProject webhooks (work packages, projects, time entries)
"""

import hmac
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional, Callable, List
from fastapi import Request, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field, validator
import asyncio

from .event_publisher import EventPublisher
from .resilience import get_metrics_collector

logger = logging.getLogger(__name__)


# --- Webhook Event Models ---

class WebhookEvent(BaseModel):
    """Base webhook event model"""
    event_id: str
    event_type: str
    timestamp: datetime
    source: str
    data: Dict[str, Any]


class NextcloudWebhookEvent(WebhookEvent):
    """Nextcloud webhook event"""
    source: str = "nextcloud"
    user: Optional[str] = None
    object_type: Optional[str] = None
    object_id: Optional[str] = None
    
    @validator('event_type')
    def validate_event_type(cls, v):
        valid_types = [
            'file_created', 'file_updated', 'file_deleted', 'file_moved',
            'folder_created', 'folder_deleted', 'share_created', 'share_deleted',
            'user_created', 'user_deleted', 'group_added', 'group_removed'
        ]
        if v not in valid_types:
            logger.warning(f"Unknown Nextcloud event type: {v}")
        return v


class ZulipWebhookEvent(WebhookEvent):
    """Zulip webhook event"""
    source: str = "zulip"
    user_id: Optional[int] = None
    user_email: Optional[str] = None
    
    @validator('event_type')
    def validate_event_type(cls, v):
        valid_types = [
            'message', 'message_edited', 'message_deleted',
            'reaction_added', 'reaction_removed',
            'subscription_added', 'subscription_removed',
            'stream_created', 'stream_deleted',
            'user_created', 'user_deactivated',
            'typing_started', 'typing_stopped',
            'presence_update'
        ]
        if v not in valid_types:
            logger.warning(f"Unknown Zulip event type: {v}")
        return v


class OpenProjectWebhookEvent(WebhookEvent):
    """OpenProject webhook event"""
    source: str = "openproject"
    user: Optional[Dict[str, Any]] = None
    project: Optional[Dict[str, Any]] = None
    
    @validator('event_type')
    def validate_event_type(cls, v):
        valid_types = [
            'work_package:created', 'work_package:updated', 'work_package:deleted',
            'project:created', 'project:updated', 'project:deleted',
            'membership:created', 'membership:deleted',
            'time_entry:created', 'time_entry:updated',
            'attachment:created', 'attachment:deleted',
            'wiki_page:created', 'wiki_page:updated'
        ]
        if v not in valid_types:
            logger.warning(f"Unknown OpenProject event type: {v}")
        return v


# --- Webhook Security ---

class WebhookSecurity:
    """Handle webhook security verification"""
    
    @staticmethod
    def verify_signature(payload: bytes, signature: str, secret: str, 
                        algorithm: str = 'sha256') -> bool:
        """Verify webhook signature"""
        if algorithm == 'sha256':
            expected = hmac.new(
                secret.encode(),
                payload,
                hashlib.sha256
            ).hexdigest()
        elif algorithm == 'sha1':
            expected = hmac.new(
                secret.encode(),
                payload,
                hashlib.sha1
            ).hexdigest()
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
            
        return hmac.compare_digest(expected, signature)
    
    @staticmethod
    def verify_nextcloud_signature(request: Request, payload: bytes, secret: str) -> bool:
        """Verify Nextcloud webhook signature"""
        # Nextcloud uses a custom header format
        signature = request.headers.get('X-Nextcloud-Webhook-Signature', '')
        if not signature:
            return False
            
        # Remove 'sha256=' prefix if present
        if signature.startswith('sha256='):
            signature = signature[7:]
            
        return WebhookSecurity.verify_signature(payload, signature, secret)
    
    @staticmethod
    def verify_zulip_signature(request: Request, payload: bytes, secret: str) -> bool:
        """Verify Zulip webhook signature"""
        signature = request.headers.get('X-Zulip-Webhook-Signature', '')
        if not signature:
            return False
            
        return WebhookSecurity.verify_signature(payload, signature, secret)
    
    @staticmethod
    def verify_openproject_signature(request: Request, payload: bytes, secret: str) -> bool:
        """Verify OpenProject webhook signature"""
        signature = request.headers.get('X-OP-Signature', '')
        if not signature:
            return False
            
        return WebhookSecurity.verify_signature(payload, signature, secret, 'sha1')


# --- Webhook Handlers ---

class WebhookHandler(ABC):
    """Abstract base class for webhook handlers"""
    
    def __init__(self, event_publisher: Optional[EventPublisher] = None,
                 metrics_collector=None):
        self.event_publisher = event_publisher
        self.metrics = metrics_collector or get_metrics_collector()
        self.handlers: Dict[str, List[Callable]] = {}
        
    @abstractmethod
    async def process_webhook(self, event: WebhookEvent) -> Dict[str, Any]:
        """Process the webhook event"""
        pass
    
    def register_handler(self, event_type: str, handler: Callable):
        """Register a handler for a specific event type"""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
        
    async def _execute_handlers(self, event_type: str, event: WebhookEvent):
        """Execute all registered handlers for an event type"""
        handlers = self.handlers.get(event_type, [])
        
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Handler error for {event_type}: {e}", exc_info=True)


class NextcloudWebhookHandler(WebhookHandler):
    """Handle Nextcloud webhooks"""
    
    async def process_webhook(self, event: NextcloudWebhookEvent) -> Dict[str, Any]:
        """Process Nextcloud webhook event"""
        start_time = datetime.now()
        
        try:
            logger.info(f"Processing Nextcloud webhook: {event.event_type}")
            
            # Execute registered handlers
            await self._execute_handlers(event.event_type, event)
            
            # Publish to event stream
            if self.event_publisher and event.event_type in ['file_created', 'file_updated']:
                await self._publish_file_event(event)
            
            # Record metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_request(
                service="webhook-handler",
                endpoint=f"nextcloud_{event.event_type}",
                duration=duration,
                status_code=200
            )
            
            return {"status": "processed", "event_id": event.event_id}
            
        except Exception as e:
            logger.error(f"Failed to process Nextcloud webhook: {e}", exc_info=True)
            
            # Record error metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_request(
                service="webhook-handler",
                endpoint=f"nextcloud_{event.event_type}",
                duration=duration,
                status_code=500,
                error=str(e)
            )
            
            raise
    
    async def _publish_file_event(self, event: NextcloudWebhookEvent):
        """Publish file event to event stream"""
        file_data = event.data.get('file', {})
        
        # Map to internal event schema
        internal_event = {
            'event_type': 'digital_asset_updated' if event.event_type == 'file_updated' else 'digital_asset_created',
            'asset_id': file_data.get('id'),
            'path': file_data.get('path'),
            'name': file_data.get('name'),
            'size': file_data.get('size'),
            'mime_type': file_data.get('mime_type'),
            'user': event.user,
            'timestamp': event.timestamp.isoformat()
        }
        
        # Publish to appropriate topic
        self.event_publisher.publish(
            topic_base='digital-asset-events',
            tenant_id=event.data.get('tenant_id', 'default'),
            data=internal_event
        )


class ZulipWebhookHandler(WebhookHandler):
    """Handle Zulip webhooks"""
    
    async def process_webhook(self, event: ZulipWebhookEvent) -> Dict[str, Any]:
        """Process Zulip webhook event"""
        start_time = datetime.now()
        
        try:
            logger.info(f"Processing Zulip webhook: {event.event_type}")
            
            # Execute registered handlers
            await self._execute_handlers(event.event_type, event)
            
            # Handle specific event types
            if event.event_type == 'message':
                await self._process_message_event(event)
            elif event.event_type == 'reaction_added':
                await self._process_reaction_event(event)
                
            # Record metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_request(
                service="webhook-handler",
                endpoint=f"zulip_{event.event_type}",
                duration=duration,
                status_code=200
            )
            
            return {"status": "processed", "event_id": event.event_id}
            
        except Exception as e:
            logger.error(f"Failed to process Zulip webhook: {e}", exc_info=True)
            
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_request(
                service="webhook-handler",
                endpoint=f"zulip_{event.event_type}",
                duration=duration,
                status_code=500,
                error=str(e)
            )
            
            raise
    
    async def _process_message_event(self, event: ZulipWebhookEvent):
        """Process message events for commands or mentions"""
        message = event.data.get('message', {})
        content = message.get('content', '')
        
        # Check for bot commands
        if content.startswith('/platformq'):
            await self._handle_bot_command(event, content)
        
        # Check for mentions of integrated services
        if 'nextcloud' in content.lower():
            logger.info("Nextcloud mentioned in message")
        if 'openproject' in content.lower():
            logger.info("OpenProject mentioned in message")
    
    async def _handle_bot_command(self, event: ZulipWebhookEvent, content: str):
        """Handle bot commands from Zulip"""
        parts = content.split()
        if len(parts) < 2:
            return
            
        command = parts[1].lower()
        
        if command == 'create-project':
            # Trigger project creation workflow
            logger.info("Project creation requested via Zulip")
        elif command == 'status':
            # Return platform status
            logger.info("Status request via Zulip")
    
    async def _process_reaction_event(self, event: ZulipWebhookEvent):
        """Process reaction events for workflow triggers"""
        reaction = event.data.get('emoji_name', '')
        
        # Special reactions that trigger workflows
        if reaction == 'checkmark':
            logger.info("Approval reaction detected")
        elif reaction == 'x':
            logger.info("Rejection reaction detected")


class OpenProjectWebhookHandler(WebhookHandler):
    """Handle OpenProject webhooks"""
    
    async def process_webhook(self, event: OpenProjectWebhookEvent) -> Dict[str, Any]:
        """Process OpenProject webhook event"""
        start_time = datetime.now()
        
        try:
            logger.info(f"Processing OpenProject webhook: {event.event_type}")
            
            # Execute registered handlers
            await self._execute_handlers(event.event_type, event)
            
            # Handle specific event types
            if event.event_type.startswith('work_package:'):
                await self._process_work_package_event(event)
            elif event.event_type.startswith('time_entry:'):
                await self._process_time_entry_event(event)
                
            # Record metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_request(
                service="webhook-handler",
                endpoint=f"openproject_{event.event_type.replace(':', '_')}",
                duration=duration,
                status_code=200
            )
            
            return {"status": "processed", "event_id": event.event_id}
            
        except Exception as e:
            logger.error(f"Failed to process OpenProject webhook: {e}", exc_info=True)
            
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.record_request(
                service="webhook-handler",
                endpoint=f"openproject_{event.event_type.replace(':', '_')}",
                duration=duration,
                status_code=500,
                error=str(e)
            )
            
            raise
    
    async def _process_work_package_event(self, event: OpenProjectWebhookEvent):
        """Process work package events"""
        work_package = event.data.get('work_package', {})
        
        # Publish to event stream
        if self.event_publisher:
            internal_event = {
                'event_type': event.event_type,
                'work_package_id': work_package.get('id'),
                'subject': work_package.get('subject'),
                'project_id': work_package.get('project', {}).get('id'),
                'status': work_package.get('status', {}).get('name'),
                'assignee': work_package.get('assignee', {}).get('name'),
                'updated_by': event.user.get('name') if event.user else None,
                'timestamp': event.timestamp.isoformat()
            }
            
            self.event_publisher.publish(
                topic_base='project-events',
                tenant_id=event.data.get('tenant_id', 'default'),
                data=internal_event
            )
    
    async def _process_time_entry_event(self, event: OpenProjectWebhookEvent):
        """Process time entry events"""
        time_entry = event.data.get('time_entry', {})
        
        logger.info(f"Time entry {event.event_type.split(':')[1]}: "
                   f"{time_entry.get('hours')} hours on work package "
                   f"{time_entry.get('work_package', {}).get('id')}")


# --- Webhook Router Factory ---

class WebhookRouterFactory:
    """Factory for creating webhook routers"""
    
    @staticmethod
    def create_nextcloud_routes(handler: NextcloudWebhookHandler, 
                              webhook_secret: str):
        """Create Nextcloud webhook routes"""
        from fastapi import APIRouter
        
        router = APIRouter(prefix="/webhooks/nextcloud", tags=["webhooks"])
        
        @router.post("/")
        async def handle_nextcloud_webhook(
            request: Request,
            background_tasks: BackgroundTasks
        ):
            """Handle Nextcloud webhook"""
            # Read body
            body = await request.body()
            
            # Verify signature
            if webhook_secret and not WebhookSecurity.verify_nextcloud_signature(
                request, body, webhook_secret
            ):
                raise HTTPException(status_code=401, detail="Invalid signature")
            
            # Parse event
            try:
                data = json.loads(body)
                event = NextcloudWebhookEvent(
                    event_id=data.get('eventId', ''),
                    event_type=data.get('event', ''),
                    timestamp=datetime.fromisoformat(data.get('time', datetime.now().isoformat())),
                    user=data.get('user'),
                    object_type=data.get('objectType'),
                    object_id=data.get('objectId'),
                    data=data
                )
            except Exception as e:
                logger.error(f"Failed to parse Nextcloud webhook: {e}")
                raise HTTPException(status_code=400, detail="Invalid webhook data")
            
            # Process in background
            background_tasks.add_task(handler.process_webhook, event)
            
            return {"status": "accepted"}
        
        return router
    
    @staticmethod
    def create_zulip_routes(handler: ZulipWebhookHandler,
                           webhook_secret: str):
        """Create Zulip webhook routes"""
        from fastapi import APIRouter
        
        router = APIRouter(prefix="/webhooks/zulip", tags=["webhooks"])
        
        @router.post("/")
        async def handle_zulip_webhook(
            request: Request,
            background_tasks: BackgroundTasks
        ):
            """Handle Zulip webhook"""
            # Read body
            body = await request.body()
            
            # Verify signature
            if webhook_secret and not WebhookSecurity.verify_zulip_signature(
                request, body, webhook_secret
            ):
                raise HTTPException(status_code=401, detail="Invalid signature")
            
            # Parse event
            try:
                data = json.loads(body)
                event = ZulipWebhookEvent(
                    event_id=data.get('id', ''),
                    event_type=data.get('trigger', ''),
                    timestamp=datetime.fromtimestamp(data.get('timestamp', datetime.now().timestamp())),
                    user_id=data.get('user_id'),
                    user_email=data.get('user_email'),
                    data=data.get('data', data)
                )
            except Exception as e:
                logger.error(f"Failed to parse Zulip webhook: {e}")
                raise HTTPException(status_code=400, detail="Invalid webhook data")
            
            # Process in background
            background_tasks.add_task(handler.process_webhook, event)
            
            return {"status": "accepted"}
        
        return router
    
    @staticmethod
    def create_openproject_routes(handler: OpenProjectWebhookHandler,
                                 webhook_secret: str):
        """Create OpenProject webhook routes"""
        from fastapi import APIRouter
        
        router = APIRouter(prefix="/webhooks/openproject", tags=["webhooks"])
        
        @router.post("/")
        async def handle_openproject_webhook(
            request: Request,
            background_tasks: BackgroundTasks
        ):
            """Handle OpenProject webhook"""
            # Read body
            body = await request.body()
            
            # Verify signature
            if webhook_secret and not WebhookSecurity.verify_openproject_signature(
                request, body, webhook_secret
            ):
                raise HTTPException(status_code=401, detail="Invalid signature")
            
            # Parse event
            try:
                data = json.loads(body)
                event = OpenProjectWebhookEvent(
                    event_id=data.get('id', ''),
                    event_type=data.get('action', ''),
                    timestamp=datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat())),
                    user=data.get('user'),
                    project=data.get('project'),
                    data=data
                )
            except Exception as e:
                logger.error(f"Failed to parse OpenProject webhook: {e}")
                raise HTTPException(status_code=400, detail="Invalid webhook data")
            
            # Process in background
            background_tasks.add_task(handler.process_webhook, event)
            
            return {"status": "accepted"}
        
        return router


# --- Webhook Manager ---

class WebhookManager:
    """Central webhook management"""
    
    def __init__(self, event_publisher: Optional[EventPublisher] = None):
        self.event_publisher = event_publisher
        self.metrics = get_metrics_collector()
        
        # Initialize handlers
        self.nextcloud_handler = NextcloudWebhookHandler(event_publisher, self.metrics)
        self.zulip_handler = ZulipWebhookHandler(event_publisher, self.metrics)
        self.openproject_handler = OpenProjectWebhookHandler(event_publisher, self.metrics)
        
        # Track webhook statistics
        self.stats = {
            "nextcloud": {"received": 0, "processed": 0, "failed": 0},
            "zulip": {"received": 0, "processed": 0, "failed": 0},
            "openproject": {"received": 0, "processed": 0, "failed": 0}
        }
    
    def register_handler(self, platform: str, event_type: str, handler: Callable):
        """Register a custom handler for a specific platform and event type"""
        if platform == "nextcloud":
            self.nextcloud_handler.register_handler(event_type, handler)
        elif platform == "zulip":
            self.zulip_handler.register_handler(event_type, handler)
        elif platform == "openproject":
            self.openproject_handler.register_handler(event_type, handler)
        else:
            raise ValueError(f"Unknown platform: {platform}")
    
    def get_routers(self, webhook_secrets: Dict[str, str]):
        """Get FastAPI routers for all webhooks"""
        routers = []
        
        if "nextcloud" in webhook_secrets:
            routers.append(WebhookRouterFactory.create_nextcloud_routes(
                self.nextcloud_handler,
                webhook_secrets["nextcloud"]
            ))
        
        if "zulip" in webhook_secrets:
            routers.append(WebhookRouterFactory.create_zulip_routes(
                self.zulip_handler,
                webhook_secrets["zulip"]
            ))
        
        if "openproject" in webhook_secrets:
            routers.append(WebhookRouterFactory.create_openproject_routes(
                self.openproject_handler,
                webhook_secrets["openproject"]
            ))
        
        return routers
    
    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get webhook processing statistics"""
        return self.stats.copy()


# --- Example Usage ---

def setup_webhooks(app, config_loader, event_publisher):
    """
    Set up webhook handling for a FastAPI app
    
    Example:
        from platformq_shared.webhooks import setup_webhooks
        
        # In your app startup
        setup_webhooks(app, config_loader, event_publisher)
    """
    # Get webhook secrets from config
    webhook_secrets = {
        "nextcloud": config_loader.get_secret("platformq/nextcloud", "webhook_secret", default=""),
        "zulip": config_loader.get_secret("platformq/zulip", "webhook_secret", default=""),
        "openproject": config_loader.get_secret("platformq/openproject", "webhook_secret", default="")
    }
    
    # Create webhook manager
    webhook_manager = WebhookManager(event_publisher)
    
    # Register custom handlers if needed
    # webhook_manager.register_handler("nextcloud", "file_created", custom_file_handler)
    
    # Add routers to app
    for router in webhook_manager.get_routers(webhook_secrets):
        app.include_router(router)
    
    # Store manager in app state
    app.state.webhook_manager = webhook_manager
    
    logger.info("Webhook handlers configured") 