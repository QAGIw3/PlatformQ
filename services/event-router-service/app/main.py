"""
Event Router Service

Centralized event routing, transformation, and processing hub for the platform.
Provides intelligent routing, event enrichment, and dead letter queue management.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
import asyncio

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from platformq_shared import (
    create_base_app,
    ConfigLoader,
    ErrorCode,
    AppException
)

from .core.config import settings
from .core.event_router import EventRouter
from .core.schema_registry import SchemaRegistry
from .core.transformation_engine import TransformationEngine
from .core.dead_letter_handler import DeadLetterHandler
from .core.event_store import EventStore
from .api import routes, admin, health, blockchain
from .core.blockchain_event_handler import BlockchainEventHandler
from .monitoring import EventMetrics

logger = logging.getLogger(__name__)

# Global instances
event_router: Optional[EventRouter] = None
schema_registry: Optional[SchemaRegistry] = None
transformation_engine: Optional[TransformationEngine] = None
dead_letter_handler: Optional[DeadLetterHandler] = None
event_store: Optional[EventStore] = None
event_metrics: Optional[EventMetrics] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_router, schema_registry, transformation_engine
    global dead_letter_handler, event_store, event_metrics
    
    # Startup
    logger.info("Starting Event Router Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize event metrics
    event_metrics = EventMetrics()
    await event_metrics.initialize()
    app.state.metrics = event_metrics
    
    # Initialize schema registry
    schema_registry = SchemaRegistry(
        registry_url=config.get("schema_registry_url", "http://schema-registry:8081"),
        cache_ttl=int(config.get("schema_cache_ttl", 3600))
    )
    await schema_registry.initialize()
    app.state.schema_registry = schema_registry
    
    # Initialize event store
    event_store = EventStore(
        mongodb_uri=config.get("mongodb_uri", "mongodb://mongodb:27017"),
        database=config.get("event_store_db", "event_store"),
        retention_days=int(config.get("event_retention_days", 30))
    )
    await event_store.connect()
    app.state.event_store = event_store
    
    # Initialize transformation engine
    transformation_engine = TransformationEngine(
        schema_registry=schema_registry,
        metrics=event_metrics
    )
    app.state.transformation_engine = transformation_engine
    
    # Initialize dead letter handler
    dead_letter_handler = DeadLetterHandler(
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        event_store=event_store,
        metrics=event_metrics
    )
    await dead_letter_handler.initialize()
    app.state.dead_letter_handler = dead_letter_handler
    
    # Initialize event router
    event_router = EventRouter(
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        schema_registry=schema_registry,
        transformation_engine=transformation_engine,
        dead_letter_handler=dead_letter_handler,
        event_store=event_store,
        metrics=event_metrics,
        config=config
    )
    await event_router.initialize()
    app.state.event_router = event_router
    
    # Initialize blockchain event handler
    blockchain_event_handler = BlockchainEventHandler(
        event_router=event_router,
        schema_registry=schema_registry,
        event_store=event_store
    )
    app.state.blockchain_event_handler = blockchain_event_handler
    
    # Start background tasks
    asyncio.create_task(event_router.start_routing())
    asyncio.create_task(dead_letter_handler.process_dead_letters())
    asyncio.create_task(event_store.cleanup_old_events())
    asyncio.create_task(event_metrics.export_metrics_loop())
    
    logger.info("Event Router Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Event Router Service...")
    
    # Stop components
    if event_router:
        await event_router.shutdown()
        
    if dead_letter_handler:
        await dead_letter_handler.shutdown()
        
    if event_store:
        await event_store.disconnect()
        
    if schema_registry:
        await schema_registry.shutdown()
        
    if event_metrics:
        await event_metrics.shutdown()
    
    logger.info("Event Router Service shutdown complete")


# Create app
app = create_base_app(
    service_name="event-router-service",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(routes.router, prefix="/api/v1/routes", tags=["routes"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])
app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
app.include_router(blockchain.router, prefix="/api/v1/blockchain", tags=["blockchain"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "event-router-service",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "intelligent-routing",
            "event-transformation",
            "schema-evolution",
            "dead-letter-handling",
            "event-replay",
            "content-based-routing",
            "event-enrichment",
            "batch-processing"
        ]
    } 