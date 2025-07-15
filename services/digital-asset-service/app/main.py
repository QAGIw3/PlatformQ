from platformq_shared.base_service import create_base_app
from .api.endpoints import digital_assets, processing_rules
from .postgres_db import get_db_session
from . import crud
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
import threading
import pulsar
from pulsar.schema import AvroSchema
from platformq_shared.events import FunctionExecutionCompleted
import uuid
import logging

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
    
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    pulsar_url = settings.get("PULSAR_URL", "pulsar://pulsar:6650")

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
            if event.status == "SUCCESS" and event.results:
                logger.info(f"Received successful execution result for asset {event.asset_id}. Updating metadata.")
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


app = create_base_app(
    service_name="digital-asset-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

@app.on_event("startup")
def startup_event():
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    pulsar_url = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
    publisher = EventPublisher(pulsar_url=pulsar_url)
    publisher.connect()
    app.state.event_publisher = publisher
    
    # Start the background consumer
    thread = threading.Thread(target=result_consumer_loop, args=(app,), daemon=True)
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    if app.state.event_publisher:
        app.state.event_publisher.close()

app.include_router(digital_assets.router, prefix="/api/v1", tags=["digital-assets"])
app.include_router(processing_rules.router, prefix="/api/v1", tags=["processing-rules"])

@app.get("/")
def read_root():
    return {"message": "digital-asset-service is running"}
