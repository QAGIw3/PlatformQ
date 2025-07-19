"""
Auth Service

OIDC-compliant authentication service with SSO, user management, and API key handling.
Enhanced with event processing and standardized patterns.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    add_error_handlers
)
from platformq_events import UserCreatedEvent, UserUpdatedEvent, TenantCreatedEvent

from .api import endpoints, siwe_endpoints, s2s, policy_endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud, 
    get_user_crud, 
    get_password_verifier
)
from .event_processors import AuthEventProcessor

logger = logging.getLogger(__name__)

# Event processor for auth-related events
auth_event_processor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global auth_event_processor
    
    # Startup
    logger.info("Starting Auth Service with enhanced patterns...")
    
    # Initialize event processor
    auth_event_processor = AuthEventProcessor(
        service_name="auth-service",
        pulsar_url="pulsar://pulsar:6650",
        db_session=app.state.cassandra_manager
    )
    
    # Start event processor
    await auth_event_processor.start()
    
    logger.info("Auth Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Auth Service...")
    
    # Stop event processor
    if auth_event_processor:
        await auth_event_processor.stop()
        
    logger.info("Auth Service shutdown complete")

# Create app with event processors
app = create_base_app(
    service_name="auth-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[auth_event_processor] if auth_event_processor else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["Users"])
app.include_router(siwe_endpoints.router, prefix="/api/v1", tags=["SIWE"])
app.include_router(s2s.router, prefix="/api/v1", tags=["S2S"])
app.include_router(policy_endpoints.router, prefix="/api/v1", tags=["Policy"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {
        "service": "auth-service",
        "version": "2.0",
        "features": ["oidc", "sso", "api-keys", "event-driven", "policy-evaluation", "unified-authorization"]
    }
