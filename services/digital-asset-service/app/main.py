from platformq_shared.base_service import create_base_app
from .api.endpoints import digital_assets, processing_rules, marketplace
from .postgres_db import get_db_session
from . import crud
from . import postgres_db
from platformq_shared.event_publisher import EventPublisher
from .core.config import settings
import threading
import pulsar
from pulsar.schema import AvroSchema
from platformq_shared.events import FunctionExecutionCompleted
import uuid
import logging
from .messaging.vc_consumer import asset_vc_consumer
from .core.config import settings
from .db.session import SessionLocal, engine
from .repository import AssetRepository
from .messaging.data_lake_consumer import DataLakeAssetConsumer
import asyncio
from typing import Optional, Dict
from fastapi import FastAPI
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# Create all tables in the database if they don't exist
# In a real production app, this would be handled by Alembic migrations.
postgres_db.Base.metadata.create_all(bind=postgres_db.engine)

# The database tables are now managed by Alembic migrations.
# No need for Base.metadata.create_all(bind=engine) here anymore.

# TODO: Create real CRUD functions and a real password verifier.
def get_api_key_crud_placeholder():
    return None

def get_user_crud_placeholder():
    return None

def get_password_verifier_placeholder():
    return None

def result_consumer_loop(app):
    logger.info("Starting consumer for function execution results...")
    
    pulsar_url = settings.pulsar_url

    client = pulsar.Client(pulsar_url)
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/function-execution-completed-events",
        subscription_name="digital-asset-service-results-sub",
        schema=AvroSchema(FunctionExecutionCompleted)
    )
    
    db_session = next(get_db_session())

    while True:
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None: continue
            
            event = msg.value()
            if event.status == "SUCCESS" and (event.payload is not None or event.results):
                logger.info(f"Received successful execution result for asset {event.asset_id}. Updating metadata.")
                # If payload and payload_schema_version are present, use them for update
                if event.payload is not None and event.payload_schema_version is not None:
                    crud.crud_digital_asset.update_asset_metadata(
                        db=db_session,
                        asset_id=uuid.UUID(event.asset_id),
                        new_metadata=None, # No generic metadata update
                        payload_schema_version=event.payload_schema_version,
                        payload=event.payload
                    )
                elif event.results is not None: # Fallback for old events or generic metadata
                    crud.crud_digital_asset.update_asset_metadata(
                        db=db_session,
                        asset_id=uuid.UUID(event.asset_id),
                        new_metadata=event.results
                    )
            elif event.status == "FAILURE":
                logger.error(f"Received failure event for asset {event.asset_id}: {event.error_message}")

            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in result consumer loop: {e}")
            if 'consumer' in locals() and msg:
                consumer.negative_acknowledge(msg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global asset_repository, data_lake_consumer, consumer_task
    
    # Startup
    logger.info("Initializing Digital Asset Service...")
    
    # In a real app, you'd use a dependency injection system
    # to provide the db session to the repository.
    db_session_factory = SessionLocal
    asset_repository = AssetRepository(db_session_factory)
    
    data_lake_consumer = DataLakeAssetConsumer(asset_repository)
    await data_lake_consumer.start()
    
    consumer_task = asyncio.create_task(data_lake_consumer.consume_messages())
    
    logger.info("Digital Asset Service initialized successfully")
    yield
    # Shutdown
    logger.info("Shutting down Digital Asset Service...")
    if consumer_task:
        consumer_task.cancel()
    if data_lake_consumer:
        await data_lake_consumer.close()
    logger.info("Digital Asset Service shutdown complete")


app = FastAPI(
    title="PlatformQ Digital Asset Service",
    description="Service for managing digital assets in the PlatformQ ecosystem.",
    version="1.0.0",
    lifespan=lifespan
)

@app.on_event("startup")
def startup_event():
    pulsar_url = settings.pulsar_url
    publisher = EventPublisher(pulsar_url=pulsar_url)
    publisher.connect()
    app.state.event_publisher = publisher
    
    # Start the background consumer
    thread = threading.Thread(target=result_consumer_loop, args=(app,), daemon=True)
    thread.start()
    
    # Start the VC consumer
    asset_vc_consumer.start()

@app.on_event("shutdown")
def shutdown_event():
    if app.state.event_publisher:
        app.state.event_publisher.close()
    
    # Stop the VC consumer
    asset_vc_consumer.stop()

app.include_router(digital_assets.router, prefix="/api/v1", tags=["digital-assets"])
app.include_router(processing_rules.router, prefix="/api/v1", tags=["processing-rules"])
app.include_router(marketplace.router, prefix="/api/v1", tags=["marketplace"])

# Import and include the CAD collaboration router
try:
    from .api.endpoints import cad_collaboration
    app.include_router(cad_collaboration.router, prefix="/api/v1", tags=["cad-collaboration"])
except ImportError:
    logger.warning("CAD collaboration endpoints not available")

@app.get("/")
def read_root():
    return {"message": "digital-asset-service is running"}

event_publisher = EventPublisher('pulsar://localhost:6650')

def publish_event(topic: str, data: Dict):
    event_publisher.publish(topic, data)

async def automate_lifecycle(asset_id: str):
    publish_event('asset_lifecycle', {'asset_id': asset_id})
