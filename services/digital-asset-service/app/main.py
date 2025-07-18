"""
Digital Asset Service

Core engine for managing all digital assets within PlatformQ.
Enhanced with event-driven architecture and standardized patterns.
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
from platformq_events import (
    FunctionExecutionCompletedEvent,
    AssetCreatedEvent,
    AssetUpdatedEvent,
    AssetProcessingCompletedEvent,
    DataLakeAssetCreatedEvent,
    AssetVCIssuedEvent
)

from .api.endpoints import digital_assets, processing_rules, marketplace
from .postgres_db import get_db_session, Base, engine
from .repository import AssetRepository
from .event_processors import (
    AssetEventProcessor,
    FunctionResultProcessor,
    DataLakeAssetProcessor,
    VCAssetProcessor
)
from .core.config import settings

logger = logging.getLogger(__name__)

# Create all tables in the database if they don't exist
# In production, use Alembic migrations instead
Base.metadata.create_all(bind=engine)

# Event processors
asset_event_processor = None
function_result_processor = None
data_lake_processor = None
vc_processor = None


# Placeholder dependencies - replace with real implementations
def get_api_key_crud_placeholder():
    return None

def get_user_crud_placeholder():
    return None

def get_password_verifier_placeholder():
    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global asset_event_processor, function_result_processor, data_lake_processor, vc_processor
    
    # Startup
    logger.info("Starting Digital Asset Service with enhanced patterns...")
    
    # Initialize repository
    app.state.asset_repository = AssetRepository(get_db_session)
    
    # Initialize event processors
    pulsar_url = settings.pulsar_url
    
    # Main asset event processor
    asset_event_processor = AssetEventProcessor(
        service_name="digital-asset-service",
        pulsar_url=pulsar_url,
        asset_repository=app.state.asset_repository,
        event_publisher=app.state.event_publisher
    )
    
    # Function execution result processor
    function_result_processor = FunctionResultProcessor(
        service_name="digital-asset-service-functions",
        pulsar_url=pulsar_url,
        asset_repository=app.state.asset_repository
    )
    
    # Data lake asset processor
    data_lake_processor = DataLakeAssetProcessor(
        service_name="digital-asset-service-datalake",
        pulsar_url=pulsar_url,
        asset_repository=app.state.asset_repository,
        event_publisher=app.state.event_publisher
    )
    
    # Verifiable credential processor
    vc_processor = VCAssetProcessor(
        service_name="digital-asset-service-vc",
        pulsar_url=pulsar_url,
        asset_repository=app.state.asset_repository
    )
    
    # Start all processors
    await asyncio.gather(
        asset_event_processor.start(),
        function_result_processor.start(),
        data_lake_processor.start(),
        vc_processor.start()
    )
    
    logger.info("Digital Asset Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Digital Asset Service...")
    
    # Stop all processors
    await asyncio.gather(
        asset_event_processor.stop() if asset_event_processor else None,
        function_result_processor.stop() if function_result_processor else None,
        data_lake_processor.stop() if data_lake_processor else None,
        vc_processor.stop() if vc_processor else None
    )
    
    logger.info("Digital Asset Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="digital-asset-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
    event_processors=[
        asset_event_processor,
        function_result_processor,
        data_lake_processor,
        vc_processor
    ] if all([asset_event_processor, function_result_processor, data_lake_processor, vc_processor]) else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(digital_assets.router, prefix="/api/v1", tags=["digital-assets"])
app.include_router(processing_rules.router, prefix="/api/v1", tags=["processing-rules"])
app.include_router(marketplace.router, prefix="/api/v1", tags=["marketplace"])

# Import and include the CAD collaboration router
try:
    from .api.endpoints import cad_collaboration
    app.include_router(cad_collaboration.router, prefix="/api/v1", tags=["cad-collaboration"])
    logger.info("CAD collaboration endpoints loaded")
except ImportError:
    logger.warning("CAD collaboration endpoints not available")

# Import and include unified data endpoints
try:
    from .api.endpoints import digital_assets_unified
    app.include_router(digital_assets_unified.router, prefix="/api/v1", tags=["unified-data"])
    logger.info("Unified data endpoints loaded")
except ImportError:
    logger.warning("Unified data endpoints not available")

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "digital-asset-service",
        "version": "2.0",
        "features": [
            "asset-management",
            "marketplace",
            "processing-rules",
            "event-driven",
            "cad-collaboration",
            "unified-data"
        ]
    }
