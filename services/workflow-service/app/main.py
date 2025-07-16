from shared_lib.base_service import create_base_app
from shared_lib.config import ConfigLoader
import pulsar
import avro.schema
import avro.io
import io
import logging
import threading
import time
from datetime import datetime
from fastapi import Request, Response, status, HTTPException
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
import json
import re
import httpx
from platformq_shared.events import IssueVerifiableCredential, DigitalAssetCreated, ExecuteWasmFunction, DocumentUpdatedEvent, ProjectCreatedEvent
from pulsar.schema import AvroSchema
from .airflow_bridge import AirflowBridge, EventToDAGProcessor
from fastapi import Query
from typing import List, Optional

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
    
    # Initialize Airflow bridge if enabled
    airflow_enabled = settings.get("AIRFLOW_ENABLED", "false").lower() == "true"
    if airflow_enabled:
        app.state.airflow_bridge = AirflowBridge()
        
        # Start event to DAG processor
        pulsar_client = pulsar.Client(pulsar_url)
        app.state.event_processor = EventToDAGProcessor(
            app.state.airflow_bridge, 
            pulsar_client
        )
        
        # Subscribe to events that should trigger DAGs
        event_topics = [
            "persistent://platformq/.*/digital-asset-created-events",
            "persistent://platformq/.*/project-creation-requests",
            "persistent://platformq/.*/proposal-approved-events",
            "persistent://platformq/.*/document-updated-events"
        ]
        app.state.event_processor.subscribe_to_events(
            event_topics, 
            "workflow-airflow-bridge"
        )
        
        # Start processing in background thread
        thread = threading.Thread(
            target=app.state.event_processor.process_events, 
            daemon=True
        )
        thread.start()
        logger.info("Airflow bridge initialized and event processing started")
    else:
        # Start the legacy Pulsar consumer in a background thread
        thread = threading.Thread(target=workflow_consumer_loop, args=(app,), daemon=True)
        thread.start()
        logger.info("Running in legacy mode without Airflow integration")

@app.on_event("shutdown")
def shutdown_event():
    if app.state.event_publisher:
        app.state.event_publisher.close()
    
    # Stop Airflow event processor if running
    if hasattr(app.state, 'event_processor'):
        app.state.event_processor.stop()

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
    payload = await request.json()

    # Instantiate the ProjectCreatedEvent using the parsed payload
    project_created_event = ProjectCreatedEvent(
        project_id=str(payload.get("project", {}).get("id", "")),
        project_name=payload.get("project", {}).get("name", ""),
        creator_id=str(payload.get("created_by", {}).get("id", "")), # Assuming webhook includes creator info
        event_timestamp=int(time.time() * 1000)
    )
    
    publisher.publish(
        topic_base='project-events',
        tenant_id="00000000-0000-0000-0000-000000000000", # Placeholder tenant ID
        schema_class=ProjectCreatedEvent,
        data=project_created_event
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

    # Instantiate the DocumentUpdatedEvent using the parsed payload
    document_updated_event = DocumentUpdatedEvent(
        tenant_id=tenant_id,
        document_id=str(payload.get("fileid", "")),
        document_path=payload.get("path", ""), # OnlyOffice uses 'path' for the file path
        saved_by_user_id=str(payload.get("userid", "")), # OnlyOffice uses 'userid'
        event_timestamp=int(time.time() * 1000) # Current timestamp
    )
    
    publisher.publish(
        topic_base='document-events',
        tenant_id=tenant_id,
        schema_class=DocumentUpdatedEvent,
        data=document_updated_event
    )
    
    logger.info("Received OnlyOffice save callback, published to Pulsar.")
    # OnlyOffice expects a specific JSON response to confirm success
    return {"error": 0}

# --- Airflow Management API Endpoints ---

@app.get("/api/v1/workflows")
async def list_workflows(
    request: Request,
    only_active: bool = Query(False, description="Only show active workflows")
):
    """
    List all available workflows (Airflow DAGs)
    """
    if not hasattr(request.app.state, 'airflow_bridge'):
        return {"error": "Airflow integration not enabled"}
    
    try:
        dags = request.app.state.airflow_bridge.list_dags()
        
        if only_active:
            dags = [dag for dag in dags if not dag.get('is_paused', True)]
        
        return {
            "workflows": dags,
            "total": len(dags)
        }
    except Exception as e:
        logger.error(f"Failed to list workflows: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/workflows/{workflow_id}/trigger")
async def trigger_workflow(
    request: Request,
    workflow_id: str,
    conf: Optional[Dict[str, Any]] = None
):
    """
    Manually trigger a workflow (DAG run)
    """
    if not hasattr(request.app.state, 'airflow_bridge'):
        return {"error": "Airflow integration not enabled"}
    
    try:
        # Add tenant context from request
        conf = conf or {}
        conf['manual_trigger'] = True
        conf['triggered_at'] = datetime.utcnow().isoformat()
        
        result = request.app.state.airflow_bridge.trigger_dag(workflow_id, conf)
        
        return {
            "workflow_id": workflow_id,
            "run_id": result.get('dag_run_id'),
            "state": result.get('state'),
            "execution_date": result.get('execution_date')
        }
    except Exception as e:
        logger.error(f"Failed to trigger workflow {workflow_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/workflows/{workflow_id}/runs/{run_id}")
async def get_workflow_run_status(
    request: Request,
    workflow_id: str,
    run_id: str
):
    """
    Get the status of a workflow run
    """
    if not hasattr(request.app.state, 'airflow_bridge'):
        return {"error": "Airflow integration not enabled"}
    
    try:
        status = request.app.state.airflow_bridge.get_dag_run_status(workflow_id, run_id)
        return status
    except Exception as e:
        logger.error(f"Failed to get workflow run status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/api/v1/workflows/{workflow_id}")
async def update_workflow_state(
    request: Request,
    workflow_id: str,
    is_paused: bool = Query(..., description="Whether to pause or unpause the workflow")
):
    """
    Pause or unpause a workflow
    """
    if not hasattr(request.app.state, 'airflow_bridge'):
        return {"error": "Airflow integration not enabled"}
    
    try:
        result = request.app.state.airflow_bridge.pause_dag(workflow_id, is_paused)
        return {
            "workflow_id": workflow_id,
            "is_paused": result.get('is_paused'),
            "message": f"Workflow {'paused' if is_paused else 'unpaused'} successfully"
        }
    except Exception as e:
        logger.error(f"Failed to update workflow state: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/event-mappings")
async def get_event_mappings(request: Request):
    """
    Get all event to workflow mappings
    """
    if not hasattr(request.app.state, 'airflow_bridge'):
        return {"error": "Airflow integration not enabled"}
    
    mappings = request.app.state.airflow_bridge.get_event_mappings()
    return {
        "mappings": mappings,
        "total": len(mappings)
    }

@app.post("/api/v1/event-mappings")
async def register_event_mapping(
    request: Request,
    event_type: str = Query(..., description="Event type to map"),
    workflow_id: str = Query(..., description="Workflow ID to trigger")
):
    """
    Register a new event to workflow mapping
    """
    if not hasattr(request.app.state, 'airflow_bridge'):
        return {"error": "Airflow integration not enabled"}
    
    request.app.state.airflow_bridge.register_event_mapping(event_type, workflow_id)
    
    return {
        "event_type": event_type,
        "workflow_id": workflow_id,
        "message": "Mapping registered successfully"
    } 