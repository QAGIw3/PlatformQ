"""
Application Factory

Creates and configures the FastAPI application using dependency injection.
"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_client import make_asgi_app
from dependency_injector import containers

from app.core.config import settings
from app.core.container import Container
from app.api.v1 import create_api_router
import logging

logger = logging.getLogger(__name__)


def create_application(
    container: Optional[Container] = None,
    testing: bool = False
) -> FastAPI:
    """
    Application factory that creates and configures the FastAPI app.
    
    Args:
        container: Optional DI container (creates default if not provided)
        testing: Whether running in test mode
        
    Returns:
        Configured FastAPI application
    """
    
    # Create container if not provided
    if container is None:
        container = Container()
        container.config.from_pydantic(settings)
    
    # Create lifespan context manager
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application lifespan management"""
        # Startup
        logger.info("Starting Data Catalog Hub...")
        
        # Initialize infrastructure
        await container.atlas_client().initialize()
        await container.ignite_cache_adapter().initialize()
        
        # Initialize services
        await container.unified_search_service().initialize()
        
        # Register event handlers
        setup_event_handlers(container)
        
        logger.info("Data Catalog Hub started successfully")
        
        yield
        
        # Shutdown
        logger.info("Shutting down Data Catalog Hub...")
        
        # Cleanup resources
        await container.atlas_client().cleanup()
        await container.ignite_cache_adapter().cleanup()
        
        logger.info("Data Catalog Hub shutdown complete")
    
    # Create FastAPI app
    app = FastAPI(
        title="Data Catalog Hub",
        description="Unified metadata management and intelligent search platform",
        version="3.0.0",
        lifespan=lifespan if not testing else None,
        docs_url="/api/docs",
        redoc_url="/api/redoc"
    )
    
    # Store container in app state
    app.container = container
    
    # Add middleware
    setup_middleware(app)
    
    # Add routes
    setup_routes(app)
    
    # Mount metrics endpoint
    if not testing:
        metrics_app = make_asgi_app()
        app.mount("/metrics", metrics_app)
    
    return app


def setup_middleware(app: FastAPI):
    """Configure application middleware"""
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure based on environment
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # GZip compression
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    
    # Add custom middleware here
    # app.add_middleware(AuthenticationMiddleware)
    # app.add_middleware(RequestLoggingMiddleware)


def setup_routes(app: FastAPI):
    """Configure application routes"""
    
    # Create and include API v1 router
    api_v1_router = create_api_router()
    app.include_router(api_v1_router, prefix="/api/v1")
    
    # Health check endpoint
    @app.get("/health")
    async def health_check():
        """Simple health check"""
        return {
            "status": "healthy",
            "service": "data-catalog-hub",
            "version": "3.0.0"
        }
    
    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint with service info"""
        return {
            "service": "Data Catalog Hub",
            "version": "3.0.0",
            "description": "Unified metadata management and intelligent search",
            "api_docs": "/api/docs",
            "health": "/health",
            "metrics": "/metrics"
        }


def setup_event_handlers(container: Container):
    """Setup domain event handlers"""
    
    event_bus = container.event_bus()
    
    # Register event handlers for search indexing
    from app.event_handlers import (
        index_entity_on_create,
        update_index_on_entity_update,
        remove_from_index_on_delete
    )
    
    event_bus.register_handler("EntityCreated", index_entity_on_create)
    event_bus.register_handler("EntityUpdated", update_index_on_entity_update)
    event_bus.register_handler("EntityDeleted", remove_from_index_on_delete)
    
    # Register handlers for analytics
    from app.event_handlers import (
        track_entity_access,
        update_quality_metrics
    )
    
    event_bus.register_handler("EntityAccessed", track_entity_access)
    event_bus.register_handler("QualityAssessed", update_quality_metrics)
    
    logger.info("Event handlers registered")


# Export for wire decorator
container = Container() 