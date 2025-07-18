"""
{{ cookiecutter.service_name }}

{{ cookiecutter.service_description }}
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging
import asyncio
from typing import Optional

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    add_error_handlers
)
{% if cookiecutter.use_event_processing %}
from platformq_events import (
    # Import your service-specific events here
    GenericEvent
)
{% endif %}

from .api import endpoints
from .api.deps import (
    get_db_session,
    get_api_key_crud,
    get_user_crud,
    get_password_verifier
)
{% if cookiecutter.database_type != "none" %}
from .repository import {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository
{% endif %}
{% if cookiecutter.use_event_processing %}
from .event_processors import {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}EventProcessor
{% endif %}
from .core.config import settings

logger = logging.getLogger(__name__)

{% if cookiecutter.use_event_processing %}
# Event processors
event_processor = None
{% endif %}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    {% if cookiecutter.use_event_processing %}global event_processor{% endif %}
    
    # Startup
    logger.info("Starting {{ cookiecutter.service_name }}...")
    
    {% if cookiecutter.database_type != "none" %}
    # Initialize repository
    app.state.repository = {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository(
        get_db_session,
        event_publisher=app.state.event_publisher if hasattr(app.state, 'event_publisher') else None
    )
    {% endif %}
    
    {% if cookiecutter.use_event_processing %}
    # Initialize event processor
    event_processor = {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}EventProcessor(
        service_name="{{ cookiecutter.service_slug }}",
        pulsar_url=settings.pulsar_url,
        {% if cookiecutter.database_type != "none" %}repository=app.state.repository{% endif %}
    )
    
    # Start event processor
    await event_processor.start()
    {% endif %}
    
    {% if cookiecutter.use_ignite_cache %}
    # Initialize Ignite cache connection
    from .cache import IgniteCacheManager
    app.state.cache_manager = IgniteCacheManager()
    await app.state.cache_manager.connect()
    {% endif %}
    
    logger.info("{{ cookiecutter.service_name }} initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down {{ cookiecutter.service_name }}...")
    
    {% if cookiecutter.use_event_processing %}
    # Stop event processor
    if event_processor:
        await event_processor.stop()
    {% endif %}
    
    {% if cookiecutter.use_ignite_cache %}
    # Close cache connections
    if hasattr(app.state, 'cache_manager'):
        await app.state.cache_manager.close()
    {% endif %}
    
    logger.info("{{ cookiecutter.service_name }} shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="{{ cookiecutter.service_slug }}",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    {% if cookiecutter.use_event_processing %}
    event_processors=[event_processor] if event_processor else []
    {% else %}
    event_processors=[]
    {% endif %}
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["{{ cookiecutter.service_slug }}"])

{% if cookiecutter.include_websocket %}
# Include WebSocket endpoints
from .api import websocket
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])
{% endif %}

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "{{ cookiecutter.service_slug }}",
        "version": "1.0.0",
        "description": "{{ cookiecutter.service_description }}",
        "features": [
            {% if cookiecutter.use_event_processing %}"event-driven",{% endif %}
            {% if cookiecutter.database_type != "none" %}"database",{% endif %}
            {% if cookiecutter.use_ignite_cache %}"caching",{% endif %}
            {% if cookiecutter.include_websocket %}"websocket",{% endif %}
            {% if cookiecutter.include_grpc %}"grpc",{% endif %}
            "api"
        ]
    } 