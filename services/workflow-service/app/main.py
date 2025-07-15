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
    consumer = client.subscribe(
        "persistent://public/default/project-events",
        subscription_name="workflow-service-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )

    while True:
        try:
            msg = consumer.receive()
            logger.info(f"Received message from topic: {msg.topic_name()}")
            # Here, you would deserialize the 'ProjectCreated' event
            # and then call the APIs for Zulip, Nextcloud, and OpenProject
            # to perform the cross-application workflow.
            
            # --- Project Approved Workflow (Example) ---
            # This is a placeholder for deserializing an approval event.
            # In a real system, you would use an Avro schema.
            # For now, we assume 'data' is a dictionary from a JSON payload.
            if "proposal-approved-events" in msg.topic_name():
                tenant_id = get_tenant_from_topic(msg.topic_name())
                data = json.loads(msg.data().decode('utf-8')) # Placeholder deserialization
                logger.info(f"  - (Workflow) Proposal {data['proposal_id']} approved for tenant {tenant_id}.")
                
                logger.info("  - (Workflow) Publishing event to request Verifiable Credential for approval...")
                
                event_data = {
                    "tenant_id": tenant_id,
                    "proposal_id": data['proposal_id'],
                    "approver_id": data['approver_id'],
                    "customer_name": data['customer_name'],
                    "event_timestamp": int(time.time() * 1000)
                }

                publisher.publish(
                    topic_base='verifiable-credential-issuance-requests',
                    tenant_id=tenant_id,
                    schema_path='libs/event-schemas/issue_verifiable_credential.avsc',
                    data=event_data
                )
                
                logger.info("  - (Workflow) Event published. The VC service will handle the issuance.")
            
            consumer.acknowledge(msg)
        except Exception as e:
            logger.error(f"Error in workflow consumer loop: {e}")
            consumer.negative_acknowledge(msg)

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
    app.state.event_publisher = EventPublisher()
    # Start the Pulsar consumer in a background thread
    thread = threading.Thread(target=workflow_consumer_loop, args=(app,), daemon=True)
    thread.start()

@app.post("/webhooks/openproject")
async def handle_openproject_webhook(request: Request):
    """
    Receives webhooks from OpenProject, transforms them into a standard
    Avro event, and publishes them to Pulsar.
    """
    publisher = app.state.event_publisher
    
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
    publisher = app.state.event_publisher
    payload = await request.json()
    
    # This is a simplified mapping. A real implementation would have more
    # robust parsing and error handling. We'd also get the tenant_id from
    # the request, perhaps via a pre-configured header in Nextcloud's webhook.
    # For now, we'll use a placeholder.
    tenant_id = "00000000-0000-0000-0000-000000000000" # Placeholder
    
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