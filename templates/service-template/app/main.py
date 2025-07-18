"""
{service_name} Service

This is a template service showcasing best practices with the new consolidated infrastructure:
- Uses Unified Data Gateway for all database operations
- Uses Event Router Service for event routing
- Uses Event Framework for standardized event processing
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from platformq_shared import create_base_app, ConfigLoader

# Import from the new event framework
from platformq_event_framework import (
    BaseEventProcessor,
    EventProcessingResult,
    EventProcessingStatus,
    EventContext,
    event_handler,
    EventMetrics
)

from .api import endpoints
from .core.config import settings
from .event_handlers import ServiceEventHandler
from .database import get_db_client

logger = logging.getLogger(__name__)

# Global instances
event_handler: Optional[ServiceEventHandler] = None
db_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_handler, db_client
    
    # Startup
    logger.info("Starting {service_name} Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize database client (uses Unified Data Gateway)
    db_client = await get_db_client()
    app.state.db_client = db_client
    
    # Initialize event handler with the new framework
    event_handler = ServiceEventHandler(
        service_name="{service_name}-service",
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        metrics=EventMetrics("{service_name}-service")
    )
    await event_handler.initialize()
    
    # Register event handlers using the new patterns
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/{service_name}-events",
        handler=event_handler.handle_event,
        subscription_name="{service_name}-service-sub"
    )
    
    # Start event processing
    await event_handler.start()
    
    logger.info("{service_name} Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down {service_name} Service...")
    
    # Stop event handler
    if event_handler:
        await event_handler.stop()
        
    # Close database client
    if db_client:
        await db_client.close()
    
    logger.info("{service_name} Service shutdown complete")


# Create app
app = create_base_app(
    service_name="{service_name}-service",
    lifespan=lifespan
)

# Include routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["{service_name}"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "{service_name}-service",
        "version": "1.0.0",
        "status": "operational",
        "infrastructure": {
            "database": "unified-data-gateway",
            "events": "event-router-service",
            "framework": "platformq-event-framework"
        }
    }
