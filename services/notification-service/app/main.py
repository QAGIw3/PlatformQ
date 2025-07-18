"""
Notification Service

Multi-channel notification delivery system with trust-based routing.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status, HTTPException, Depends, Query
import logging
import asyncio
from typing import Dict, List, Optional, Any

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader
from platformq_shared.zulip_client import ZulipClient, ZulipError
from platformq_shared.resilience import CircuitBreakerError, RateLimitExceeded, get_metrics_collector
from platformq_events import (
    UserCreatedEvent,
    DocumentUpdatedEvent,
    ProactiveAlertEvent,
    NotificationRequestEvent,
    NotificationDeliveredEvent
)

from .api import endpoints
from .api.deps import get_db_session, get_api_key_crud, get_user_crud, get_password_verifier
from .repository import NotificationRepository, TrustedSourceRepository
from .event_processors import NotificationEventProcessor
from .trust.trust_enhanced_notifications import (
    TrustEnhancedNotificationSystem,
    NotificationDeliveryManager,
    NotificationTrustLevel,
    NotificationPriority
)

logger = logging.getLogger(__name__)

# Service components
notification_event_processor = None
zulip_client = None
trust_system = None
delivery_manager = None
service_clients = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global notification_event_processor, zulip_client, trust_system, delivery_manager, service_clients
    
    # Startup
    logger.info("Starting Notification Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize Zulip client
    zulip_client = ZulipClient(
        api_url=settings.get("zulip_api_url", "https://zulip.example.com/api/v1"),
        email=settings.get("zulip_bot_email", "bot@example.com"),
        api_key=settings.get("zulip_api_key", ""),
        site=settings.get("zulip_site", "https://zulip.example.com")
    )
    app.state.zulip_client = zulip_client
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize repositories
    app.state.notification_repo = NotificationRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.trusted_source_repo = TrustedSourceRepository(get_db_session)
    
    # Initialize trust system
    trust_system = TrustEnhancedNotificationSystem(
        graph_service_url=settings.get("graph_intelligence_service_url", "http://graph-intelligence-service:8000"),
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        min_trust_score=float(settings.get("min_trust_score", "0.5"))
    )
    await trust_system.initialize()
    app.state.trust_system = trust_system
    
    # Initialize delivery manager
    delivery_manager = NotificationDeliveryManager(
        zulip_client=zulip_client,
        email_service=None,  # Add email service when ready
        sms_service=None,    # Add SMS service when ready
        event_publisher=app.state.event_publisher
    )
    app.state.delivery_manager = delivery_manager
    
    # Initialize event processor
    notification_event_processor = NotificationEventProcessor(
        service_name="notification-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        notification_repo=app.state.notification_repo,
        trust_system=trust_system,
        delivery_manager=delivery_manager,
        service_clients=service_clients
    )
    
    # Start event processor
    await notification_event_processor.start()
    
    # Initialize metrics collector
    app.state.metrics_collector = get_metrics_collector()
    
    logger.info("Notification Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Notification Service...")
    
    # Stop event processor
    if notification_event_processor:
        await notification_event_processor.stop()
        
    # Cleanup trust system
    if trust_system:
        await trust_system.cleanup()
        
    logger.info("Notification Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="notification-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[notification_event_processor] if notification_event_processor else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["notifications"])

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "notification-service",
        "version": "2.0",
        "features": [
            "multi-channel-delivery",
            "trust-based-routing",
            "event-driven",
            "zulip-integration",
            "proactive-alerts"
        ]
    }


# Backward compatibility endpoints (to be deprecated)
@app.post("/send")
async def send_notification_legacy(
    request: Request,
    db: Session = Depends(get_db_session),
    current_user: dict = Depends(get_current_user)
):
    """Legacy endpoint - use /api/v1/notifications instead"""
    return HTTPException(
        status_code=410,
        detail="This endpoint is deprecated. Please use POST /api/v1/notifications"
    ) 