from shared_lib.base_service import create_base_app
from shared_lib.config import ConfigLoader
import pulsar
import avro.schema
import avro.io
import io
import logging
import threading
import time
from fastapi import Request, Response, status
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
import json
import re
import httpx
from platformq_shared.events import IssueVerifiableCredential, DigitalAssetCreated, ExecuteWasmFunction
from pulsar.schema import AvroSchema

# --- Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Configuration & Globals ---
config_loader = ConfigLoader()
settings = config_loader.load_settings()
# We would add API keys for Zulip, Nextcloud, etc. to Vault
# ZULIP_API_KEY = config_loader.get_secret(...)

def get_tenant_from_topic(topic: str) -> str:
    """Extracts tenant ID from a topic name using a regex."""
    # This regex looks for the tenant ID in topics like:
    # persistent://platformq/0a1b2c3d-..../proposal-approved-events
    match = re.search(r'platformq/([a-f0-9-]+)/', topic)
    if match:
        return match.group(1)
    logger.warning(f"Could not extract tenant ID from topic: {topic}")
    return None

# --- Pulsar Consumer Thread for Workflows ---
def workflow_consumer_loop(app):
    logger.info("Starting workflow consumer thread...")
    publisher = app.state.event_publisher
    client = pulsar.Client(settings["PULSAR_URL"])
    
    # This consumer handles the dynamic WASM processing workflow
    asset_consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/digital-asset-created-events",
        subscription_name="workflow-service-asset-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(DigitalAssetCreated)
    )

    # We would have other consumers for other workflows, e.g.:
    # proposal_consumer = client.subscribe(...)

    while True: # This loop would need to be more sophisticated to handle multiple consumers
        try:
            msg = asset_consumer.receive(timeout_millis=1000)
            if msg is None: continue
            
            asset_event = msg.value()
            logger.info(f"  - (Workflow) Received DigitalAssetCreated event for asset: {asset_event.asset_id}")
            
            # Check for a processing rule
            rule = None
            try:
                # The service name 'digital-asset-service' would come from a service discovery mechanism
                api_url = f"http://digital-asset-service:8000/api/v1/processing-rules/{asset_event.asset_type}"
                with httpx.Client() as http_client:
                    response = http_client.get(api_url, timeout=5.0)
                    if response.status_code == 200:
                        rule = response.json()
                        logger.info(f"  - (Workflow) Found processing rule for asset_type '{asset_event.asset_type}': route to WASM module '{rule['wasm_module_id']}'")
                    elif response.status_code != 404:
                            logger.warning(f"  - (Workflow) Non-404 error when checking for processing rule: {response.status_code}")
            except httpx.RequestError as e:
                logger.error(f"  - (Workflow) Could not connect to digital-asset-service to check for rules: {e}")

            if rule:
                exec_event = ExecuteWasmFunction(
                    tenant_id=asset_event.tenant_id,
                    asset_id=asset_event.asset_id,
                    asset_uri=asset_event.raw_data_uri,
                    wasm_module_id=rule['wasm_module_id']
                )
                publisher.publish(
                    topic_base='wasm-function-execution-requests',
                    tenant_id=asset_event.tenant_id,
                    schema_class=ExecuteWasmFunction,
                    data=exec_event
                )
                logger.info(f"  - (Workflow) Published ExecuteWasmFunction event for asset {asset_event.asset_id}")

            asset_consumer.acknowledge(msg)
            
        except Exception as e:
            logger.error(f"Error in workflow consumer loop: {e}")
            if 'asset_consumer' in locals() and msg:
                asset_consumer.negative_acknowledge(msg)

# --- FastAPI App ---
app = create_base_app(
    service_name="workflow-service",
    # Pass placeholders as this service doesn't use these directly
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None,
)

@app.on_event("startup")
def startup_event():
    # Make the event publisher available to the app
    pulsar_url = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
    app.state.event_publisher = EventPublisher(pulsar_url=pulsar_url)
    app.state.event_publisher.connect()
    # Start the Pulsar consumer in a background thread
    thread = threading.Thread(target=workflow_consumer_loop, args=(app,), daemon=True)
    thread.start()

@app.on_event("shutdown")
def shutdown_event():
    if app.state.event_publisher:
        app.state.event_publisher.close()

@app.post("/webhooks/openproject")
async def handle_openproject_webhook(request: Request):
    """
    Receives webhooks from OpenProject, transforms them into a standard
    Avro event, and publishes them to Pulsar.
    """
    # This function now correctly uses the publisher from the app state
    publisher = request.app.state.event_publisher
    
    # In a real app, you would parse the OpenProject webhook payload here
    # to extract the relevant data (project_id, project_name, etc.)
    
    # Example placeholder data:
    event_data = {
        "project_id": "12345",
        "project_name": "New Website Launch",
        "creator_id": "some-user-id",
        "event_timestamp": int(time.time() * 1000)
    }
    
    publisher.publish(
        topic='project-events',
        schema_path='schemas/project_created.avsc',
        data=event_data
    )
    
    logger.info("Received and processed webhook from OpenProject, published to Pulsar.")
    return Response(status_code=status.HTTP_204_NO_CONTENT) 

@app.post("/webhooks/onlyoffice")
async def handle_onlyoffice_webhook(request: Request):
    """
    Receives callbacks from OnlyOffice/Nextcloud when a document is saved,
    and publishes a standardized 'DocumentUpdated' event to Pulsar.
    """
    # This function now correctly uses the publisher from the app state
    publisher = request.app.state.event_publisher
    payload = await request.json()
    
    # This is a simplified mapping. A real implementation would have more
    # robust parsing and error handling. We'd also get the tenant_id from
    # the request, perhaps via a pre-configured header in Nextcloud's webhook.
    # For now, we'll use a placeholder.
    tenant_id = "00000000-0000-0000-0000-000000000000" # Placeholder

    # As with the openproject webhook, this needs a proper 'DocumentUpdatedEvent'
    # Record class to be fully compliant. Leaving as-is for now.
    
    event_data = {
        "tenant_id": tenant_id,
        "document_id": str(payload.get("fileId", "")),
        "document_path": payload.get("key", ""), # The 'key' is often the document path
        "saved_by_user_id": str(payload.get("users", [""])[0]),
        "event_timestamp": int(time.time() * 1000)
    }
    
    publisher.publish(
        topic_base='document-events',
        tenant_id=tenant_id,
        schema_path='schemas/document_updated.avsc',
        data=event_data
    )
    
    logger.info("Received OnlyOffice save callback, published to Pulsar.")
    # OnlyOffice expects a specific JSON response to confirm success
    return {"error": 0} 