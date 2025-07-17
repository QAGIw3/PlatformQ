from platformq_shared.base_service import create_base_app
from platformq_shared.config import ConfigLoader
from platformq_shared.zulip_client import ZulipClient, ZulipError
from platformq_shared.resilience import CircuitBreakerError, RateLimitExceeded, get_metrics_collector
from platformq_shared.events import UserCreatedEvent, DocumentUpdatedEvent, ProactiveAlertEvent
import pulsar
from pulsar.schema import AvroSchema
import logging
import threading
import asyncio
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import Request, Response, status, HTTPException
from pydantic import BaseModel, Field
import grpc
import zulip
import os
import json

# Assuming the generate_grpc.sh script has been run
from .grpc_generated import graph_intelligence_pb2, graph_intelligence_pb2_grpc

# --- Setup ---
logger = logging.getLogger(__name__)

# Global Zulip client instance
_zulip_client = None
_metrics_collector = None


def get_zulip_client() -> ZulipClient:
    """Get or create Zulip client instance"""
    global _zulip_client
    if _zulip_client is None:
        config_loader = ConfigLoader()
        settings = config_loader.load_settings()
        _zulip_client = ZulipClient(
            zulip_site=config_loader.get_config("platformq/zulip/site"),
            zulip_email=config_loader.get_secret("platformq/zulip", "email"),
            zulip_api_key=config_loader.get_secret("platformq/zulip", "api_key"),
            use_connection_pool=True,
            rate_limit=20.0
        )
    return _zulip_client


class NotificationRequest(BaseModel):
    """Request model for sending notifications"""
    stream: str = Field(..., description="Target stream name")
    topic: str = Field(..., description="Message topic")
    content: str = Field(..., description="Message content (markdown supported)")
    mentions: Optional[List[str]] = Field(None, description="Users to mention (@user)")
    priority: str = Field("normal", description="Priority: low, normal, high, urgent")


class NotificationPreferences(BaseModel):
    """User notification preferences"""
    streams: List[str] = Field(default_factory=list)
    muted_topics: List[str] = Field(default_factory=list)
    notification_types: Dict[str, bool] = Field(default_factory=dict)
    quiet_hours: Optional[Dict[str, str]] = None  # {"start": "22:00", "end": "08:00"}


class NotificationFormatter:
    """Format notifications with rich content"""
    
    @staticmethod
    def format_user_created(event: UserCreatedEvent) -> str:
        """Format new user notification"""
        return f"""🎉 **New User Joined!**

Welcome **{event.full_name}** to our platform!

📧 Email: {event.email}
🏢 Tenant: {getattr(event, 'tenant_id', 'default')}
📅 Joined: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}

Let's give them a warm welcome! 👋"""
    
    @staticmethod
    def format_document_updated(event: DocumentUpdatedEvent) -> str:
        """Format document update notification"""
        return f"""📄 **Document Updated**

**{event.saved_by_user_id}** has updated a document:

📁 Path: `{event.document_path}`
🕐 Updated: {datetime.now().strftime('%I:%M %p')}

[View Document](#) | [See Changes](#) | [Comment](#)"""
    
    @staticmethod
    def format_community_insight(community) -> str:
        """Format community detection notification"""
        member_list = ", ".join(community.user_ids[:5])
        if len(community.user_ids) > 5:
            member_list += f" and {len(community.user_ids) - 5} others"
            
        return f"""🤝 **New Community Detected!**

A collaborative community has formed around shared interests:

👥 **Members**: {member_list}
🏷️ **Community ID**: {community.community_id}
📊 **Size**: {len(community.user_ids)} members

**Suggested Actions:**
• Create a dedicated Zulip stream for this community
• Set up a shared Nextcloud folder
• Create an OpenProject workspace

Would you like to set up collaboration tools for this community?"""
    
    @staticmethod
    def format_error_notification(service: str, error: str, details: Dict = None) -> str:
        """Format error notification"""
        details_text = ""
        if details:
            details_text = "\n**Details:**\n"
            for key, value in details.items():
                details_text += f"• {key}: {value}\n"
                
        return f"""⚠️ **Service Alert: {service}**

An error has occurred that requires attention:

❌ **Error**: {error}
🕐 **Time**: {datetime.now().strftime('%I:%M %p')}
{details_text}
**Actions:**
• Check service logs
• Verify service health
• Contact on-call if critical"""

    @staticmethod
    def format_proactive_alert(event: ProactiveAlertEvent) -> str:
        """Format proactive alert notification"""
        return f"""🔮 **Proactive Alert for Simulation {event.simulation_id}**

Our predictive monitoring system has detected a potential issue:

**Reason**: {event.reason}
**Severity**: {event.severity}

**Details**:
{json.dumps(event.details, indent=2)}

**Recommendation**:
Review the simulation parameters and consider adjusting them to avoid potential problems.
"""


# --- Enhanced Notification Service ---
class NotificationService:
    """Enhanced notification service with advanced features"""
    
    def __init__(self):
        self.zulip = get_zulip_client()
        self.metrics = get_metrics_collector()
        self.formatter = NotificationFormatter()
        
        # Notification statistics
        self.stats = {
            "sent": 0,
            "failed": 0,
            "rate_limited": 0
        }
        
        # User preferences cache (would be from database)
        self.user_preferences = {}
        
    def send_notification(self, stream: str, topic: str, content: str,
                         priority: str = "normal", mentions: List[str] = None,
                         thread_id: Optional[int] = None) -> Dict:
        """Send a notification with enhanced features"""
        try:
            # Add mentions if specified
            if mentions:
                mention_text = " ".join([f"@**{user}**" for user in mentions])
                content = f"{mention_text}\n\n{content}"
            
            # Add priority indicator
            if priority == "urgent":
                content = f"🚨 **URGENT** 🚨\n\n{content}"
            elif priority == "high":
                content = f"❗ **High Priority**\n\n{content}"
                
            # Send message
            result = self.zulip.send_message(
                content=content,
                message_type="stream",
                to=stream,
                topic=topic
            )
            
            self.stats["sent"] += 1
            
            # Record metrics
            self.metrics.record_request(
                service="notification-service",
                endpoint="send_notification",
                duration=0.1,  # Would measure actual duration
                status_code=200
            )
            
            return result
            
        except (CircuitBreakerError, RateLimitExceeded) as e:
            logger.error(f"Service temporarily unavailable: {e}")
            self.stats["rate_limited"] += 1
            raise
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
            self.stats["failed"] += 1
            
            self.metrics.record_request(
                service="notification-service",
                endpoint="send_notification",
                duration=0.1,
                status_code=500,
                error=str(e)
            )
            raise
    
    def should_notify_user(self, user_email: str, notification_type: str) -> bool:
        """Check if user should receive this notification type"""
        prefs = self.user_preferences.get(user_email, NotificationPreferences())
        
        # Check if notification type is enabled
        if not prefs.notification_types.get(notification_type, True):
            return False
            
        # Check quiet hours
        if prefs.quiet_hours:
            now = datetime.now()
            start_time = datetime.strptime(prefs.quiet_hours["start"], "%H:%M").time()
            end_time = datetime.strptime(prefs.quiet_hours["end"], "%H:%M").time()
            
            if start_time <= now.time() <= end_time:
                logger.debug(f"User {user_email} is in quiet hours")
                return False
                
        return True
    
    def create_notification_stream(self, name: str, description: str,
                                 invite_only: bool = False) -> Dict:
        """Create a dedicated notification stream"""
        try:
            return self.zulip.create_stream(
                name=name,
                description=description,
                invite_only=invite_only,
                stream_post_policy=1  # Anyone can post
            )
        except Exception as e:
            logger.error(f"Failed to create notification stream: {e}")
            raise


# --- Global notification service instance ---
notification_service = NotificationService()


# --- Pulsar Consumer Thread ---
def alert_consumer_loop(app):
    """Consumes proactive alerts and sends notifications"""
    logger.info("Starting proactive alert consumer thread...")
    client = pulsar.Client('pulsar://pulsar:6650')
    consumer = client.subscribe(
        'proactive-alerts',
        'notification-service-alerts-sub',
        schema=AvroSchema(ProactiveAlertEvent)
    )

    while not app.state.stop_event.is_set():
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None: continue
            
            event = msg.value()
            content = notification_service.formatter.format_proactive_alert(event)
            notification_service.send_notification(
                stream=f"simulations-{event.simulation_id}",
                topic="Predictive Alerts",
                content=content,
                priority="high"
            )
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in alert consumer: {e}")
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)
    
    consumer.close()
    client.close()

def notification_consumer_loop(app):
    """Enhanced notification consumer with better error handling"""
    logger.info("Starting enhanced notification consumer thread...")
    
    config_loader = app.state.config_loader
    settings = config_loader.load_settings()
    
    client = zulip.Client(
        api_key=settings.get("ZULIP_API_KEY"),
        email=settings.get("ZULIP_EMAIL"),
        site=settings.get("ZULIP_SITE")
    )
    
    # Subscribe to multiple topics
    consumers = {
        "user": client.subscribe(
            topic_pattern="persistent://platformq/.*/user-events",
            subscription_name="notification-service-user-sub",
            schema=AvroSchema(UserCreatedEvent)
        ),
        "document": client.subscribe(
            topic_pattern="persistent://platformq/.*/document-events",
            subscription_name="notification-service-doc-sub",
            schema=AvroSchema(DocumentUpdatedEvent)
        )
    }
    
    while not app.state.stop_event.is_set():
        for consumer_type, consumer in consumers.items():
            try:
                msg = consumer.receive(timeout_millis=1000)
                if msg is None:
                    continue
                    
                data = msg.value()
                topic = msg.topic_name()
                
                # Extract tenant from topic
                tenant_match = re.search(r'platformq/([a-f0-9-]+)/', topic)
                tenant_id = tenant_match.group(1) if tenant_match else "default"
                
                # Format message based on event type
                if isinstance(data, UserCreatedEvent):
                    content = notification_service.formatter.format_user_created(data)
                    stream = f"tenant-{tenant_id}" if tenant_id != "default" else "general"
                    notification_service.send_notification(
                        stream=stream,
                        topic="New Users",
                        content=content,
                        priority="normal"
                    )
                    
                elif isinstance(data, DocumentUpdatedEvent):
                    content = notification_service.formatter.format_document_updated(data)
                    stream = f"tenant-{tenant_id}" if tenant_id != "default" else "general"
                    notification_service.send_notification(
                        stream=stream,
                        topic="Document Updates",
                        content=content,
                        priority="low"
                    )
                
                consumer.acknowledge(msg)
                logger.debug(f"Processed {consumer_type} event from {topic}")
                
            except Exception as e:
                logger.error(f"Error processing {consumer_type} message: {e}")
                if msg:
                    consumer.negative_acknowledge(msg)
    
    # Cleanup
    for consumer in consumers.values():
        consumer.close()
    client.close()
    logger.info("Notification consumer stopped")


# --- Background Task for Graph Intelligence ---
async def check_for_graph_insights_async(app):
    """Enhanced graph insights checker with better formatting"""
    logger.info("Checking for new graph insights via gRPC...")
    
    config_loader = app.state.config_loader
    settings = config_loader.load_settings()
    
    grpc_target = settings.get(
        "GRAPH_INTELLIGENCE_SERVICE_GRPC_TARGET",
        "graph-intelligence-service:50052"
    )
    
    try:
        # For now, we assume a single 'default' tenant for notifications
        tenant_id = "default"
        
        async with grpc.aio.insecure_channel(grpc_target) as channel:
            stub = graph_intelligence_pb2_grpc.GraphIntelligenceServiceStub(channel)
            request = graph_intelligence_pb2.GetCommunityInsightsRequest(tenant_id=tenant_id)
            response = await stub.GetCommunityInsights(request)

        if not response.communities:
            logger.info("No new communities found")
            return

        for community in response.communities:
            content = notification_service.formatter.format_community_insight(community)
            notification_service.send_notification(
                stream="platform-insights",
                topic="Community Detection",
                content=content,
                priority="high",
                mentions=["admin"]  # Notify admins
            )
            logger.info(f"Sent community insight notification for {community.community_id}")

    except grpc.aio.AioRpcError as e:
        logger.error(f"gRPC error checking graph insights: {e.details()}")
        
        # Send error notification
        error_content = notification_service.formatter.format_error_notification(
            service="Graph Intelligence",
            error=f"gRPC Error: {e.code().name}",
            details={"details": e.details(), "code": str(e.code())}
        )
        
        try:
            notification_service.send_notification(
                stream="platform-alerts",
                topic="Service Errors",
                content=error_content,
                priority="urgent"
            )
        except Exception as notify_error:
            logger.error(f"Failed to send error notification: {notify_error}")
            
    except Exception as e:
        logger.error(f"Unexpected error checking graph insights: {e}")


# --- FastAPI App ---
app = create_base_app(
    service_name="notification-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None,
)


@app.on_event("startup")
def startup_event():
    """Initialize notification service on startup"""
    app.state.stop_event = threading.Event()
    
    # Start consumer thread
    consumer_thread = threading.Thread(
        target=notification_consumer_loop,
        args=(app,),
        name="NotificationConsumer"
    )
    consumer_thread.daemon = True
    consumer_thread.start()
    app.state.consumer_thread = consumer_thread
    
    alert_thread = threading.Thread(
        target=alert_consumer_loop,
        args=(app,),
        name="AlertConsumer"
    )
    alert_thread.daemon = True
    alert_thread.start()
    app.state.alert_thread = alert_thread
    
    # Schedule periodic graph insights check
    async def periodic_insights_check():
        while not app.state.stop_event.is_set():
            try:
                await check_for_graph_insights_async(app)
            except Exception as e:
                logger.error(f"Error in periodic insights check: {e}")
            await asyncio.sleep(300)  # Check every 5 minutes
    
    # Start background task
    asyncio.create_task(periodic_insights_check())
    
    logger.info("Notification service started successfully")


@app.on_event("shutdown")
def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down notification service...")
    app.state.stop_event.set()
    
    if hasattr(app.state, "consumer_thread"):
        app.state.consumer_thread.join(timeout=5)
    if hasattr(app.state, "alert_thread"):
        app.state.alert_thread.join(timeout=5)
        
    logger.info(f"Notification stats: {notification_service.stats}")


@app.get("/")
def read_root():
    """Health check endpoint"""
    return {
        "service": "notification-service",
        "status": "running",
        "stats": notification_service.stats
    }


@app.post("/api/v1/notify")
async def send_notification(request: NotificationRequest):
    """
    Send a notification manually
    
    This endpoint allows other services to send notifications directly.
    """
    try:
        result = notification_service.send_notification(
            stream=request.stream,
            topic=request.topic,
            content=request.content,
            priority=request.priority,
            mentions=request.mentions
        )
        
        return {
            "success": True,
            "message_id": result.get("id"),
            "result": "Notification sent successfully"
        }
        
    except (CircuitBreakerError, RateLimitExceeded):
        raise HTTPException(
            status_code=503,
            detail="Notification service temporarily unavailable"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send notification: {str(e)}"
        )


@app.post("/api/v1/streams")
async def create_notification_stream(
    name: str,
    description: str,
    invite_only: bool = False
):
    """Create a new notification stream"""
    try:
        result = notification_service.create_notification_stream(
            name=name,
            description=description,
            invite_only=invite_only
        )
        
        return {
            "success": True,
            "stream": name,
            "result": "Stream created successfully"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create stream: {str(e)}"
        )


@app.put("/api/v1/preferences/{user_email}")
async def update_notification_preferences(
    user_email: str,
    preferences: NotificationPreferences
):
    """Update user notification preferences"""
    notification_service.user_preferences[user_email] = preferences
    
    return {
        "success": True,
        "message": "Preferences updated successfully"
    }


@app.post("/webhooks/nextcloud")
async def handle_nextcloud_webhook(request: Request):
    """
    Handle incoming webhooks from Nextcloud
    
    This would process file events, user activities, etc.
    """
    try:
        payload = await request.json()
        
        # Parse Nextcloud event
        event_type = payload.get("event", "unknown")
        
        if event_type == "file_created":
            content = f"📁 New file created: **{payload.get('path')}**"
        elif event_type == "file_shared":
            content = f"🔗 File shared: **{payload.get('path')}** with {payload.get('share_with')}"
        else:
            content = f"Nextcloud event: {event_type}"
            
        # Send notification
        notification_service.send_notification(
            stream="nextcloud-activity",
            topic="File Events",
            content=content,
            priority="low"
        )
        
        return {"status": "processed"}
        
    except Exception as e:
        logger.error(f"Failed to process Nextcloud webhook: {e}")
        raise HTTPException(status_code=500, detail="Webhook processing failed") 