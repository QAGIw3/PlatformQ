from platformq_shared.base_service import create_base_app
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.webhooks import setup_webhooks, WebhookManager
import pulsar
import logging
import threading
import time
from datetime import datetime
from fastapi import Request, Response, status, HTTPException, Query
from platformq_shared.events import (
    IssueVerifiableCredential, DigitalAssetCreated, ExecuteWasmFunction, 
    DocumentUpdatedEvent, ProjectCreatedEvent
)
from pulsar.schema import AvroSchema
from .airflow_bridge import AirflowBridge, EventToDAGProcessor
from typing import List, Optional, Dict, Any
import json
import re
import httpx

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



# --- Project Workflow Consumer ---
def project_workflow_consumer_loop(app):
    """Enhanced project workflow consumer that processes project events"""
    logger.info("Starting project workflow consumer...")
    
    publisher = app.state.event_publisher
    client = pulsar.Client(settings["PULSAR_URL"])
    
    # Subscribe to project events
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/project-events",
        subscription_name="workflow-project-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(ProjectCreatedEvent)
    )
    
    while not app.state.stop_event.is_set():
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None:
                continue
                
            event = msg.value()
            tenant_id = get_tenant_from_topic(msg.topic_name())
            
            logger.info(f"Processing project event: {event.project_name}")
            
            # Trigger additional project setup workflows
            if hasattr(app.state, 'airflow_bridge'):
                # Trigger Airflow DAG for project setup
                app.state.airflow_bridge.trigger_dag(
                    'project_setup_workflow',
                    conf={
                        'project_id': event.project_id,
                        'project_name': event.project_name,
                        'tenant_id': tenant_id,
                        'creator_id': event.creator_id
                    }
                )
            
            consumer.acknowledge(msg)
            
        except Exception as e:
            logger.error(f"Error in project workflow consumer: {e}")
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)
                
    consumer.close()
    client.close()

# --- Document Update Consumer ---
def document_update_consumer_loop(app):
    """Enhanced document update consumer"""
    logger.info("Starting document update consumer...")
    
    publisher = app.state.event_publisher
    client = pulsar.Client(settings["PULSAR_URL"])
    
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/document-events",
        subscription_name="workflow-document-sub",
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(DocumentUpdatedEvent)
    )
    
    while not app.state.stop_event.is_set():
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None:
                continue
                
            event = msg.value()
            logger.info(f"Processing document update: {event.document_path}")
            
            # Trigger document processing workflows
            if event.document_path.endswith('.md'):
                # Trigger markdown processing
                logger.info("Triggering markdown processing workflow")
            elif event.document_path.endswith('.pdf'):
                # Trigger PDF processing  
                logger.info("Triggering PDF processing workflow")
                
            consumer.acknowledge(msg)
            
        except Exception as e:
            logger.error(f"Error in document update consumer: {e}")
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)
                
    consumer.close()
    client.close()

# --- Trust Activity Consumer for Automated Reputation ---
def trust_activity_consumer_loop(app):
    """
    Consumes trust activity events and updates reputation scores automatically.
    Tracks activities like:
    - Successful credential issuance/verification
    - Cross-chain bridge operations
    - High-value asset creation
    - Dispute resolutions
    """
    logger.info("Starting trust activity consumer...")
    publisher = app.state.event_publisher
    client = pulsar.Client(settings["PULSAR_URL"])
    
    consumer = client.subscribe(
        "persistent://platformq/trust/activity-events",
        subscription_name="workflow-trust-activity-sub",
        consumer_type=pulsar.ConsumerType.Shared
    )
    
    # Activity impact scores
    activity_impacts = {
        'credential_issued': 0.02,
        'credential_verified': 0.03,
        'bridge_transfer_completed': 0.05,
        'high_value_asset_created': 0.04,
        'dispute_resolved_positive': 0.10,
        'dispute_resolved_negative': -0.15,
        'event_published': 0.001,
        'verification_failed': -0.05,
        'suspicious_activity': -0.20,
    }
    
    vc_service_url = settings.get("VC_SERVICE_URL", "http://verifiable-credential-service:8000")
    graph_service_url = settings.get("GRAPH_SERVICE_URL", "http://graph-intelligence-service:8000")
    
    while not app.state.stop_event.is_set():
        try:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None:
                continue
                
            # Parse activity event
            activity_data = json.loads(msg.data().decode('utf-8'))
            entity_id = activity_data['entity_id']
            activity_type = activity_data['activity_type']
            metadata = activity_data.get('metadata', {})
            
            logger.info(f"Processing trust activity: {activity_type} for entity {entity_id}")
            
            # Calculate trust impact
            base_impact = activity_impacts.get(activity_type, 0.0)
            
            # Adjust impact based on metadata
            if activity_type == 'credential_issued':
                # Higher impact for more valuable credentials
                cred_type = metadata.get('credential_type', '')
                if 'HighValue' in cred_type or 'SoulBound' in cred_type:
                    base_impact *= 1.5
                    
            elif activity_type == 'bridge_transfer_completed':
                # Higher impact for larger transfers
                transfer_value = metadata.get('transfer_value', 0)
                if transfer_value > 10000:
                    base_impact *= 2.0
                elif transfer_value > 1000:
                    base_impact *= 1.5
                    
            # Send trust event to graph intelligence service
            try:
                with httpx.Client() as http_client:
                    response = http_client.post(
                        f"{graph_service_url}/api/v1/graph/ingest-trust-event",
                        json={
                            "event_type": activity_type,
                            "entity_id": entity_id,
                            "trust_delta": base_impact,
                            "metadata": metadata
                        }
                    )
                    response.raise_for_status()
                    logger.info(f"Trust event ingested for {entity_id}: delta={base_impact}")
            except Exception as e:
                logger.error(f"Failed to ingest trust event: {e}")
                
            # Check for reputation milestones
            if base_impact > 0:
                check_reputation_milestones(entity_id, activity_type, metadata, publisher)
                
            consumer.acknowledge(msg)
            
        except Exception as e:
            logger.error(f"Error in trust activity consumer: {e}", exc_info=True)
            if 'msg' in locals() and msg:
                consumer.negative_acknowledge(msg)
    
    consumer.close()
    client.close()


def check_reputation_milestones(entity_id: str, activity_type: str, metadata: dict, publisher):
    """
    Check if entity has reached reputation milestones and trigger rewards.
    
    Milestones:
    - First verified credential: Award "Trusted Member" badge
    - 10 successful verifications: Award "Verification Expert" credential
    - 100 trust score: Award "Community Leader" SBT
    - 5 successful bridges: Award "Cross-Chain Pioneer" credential
    """
    vc_service_url = settings.get("VC_SERVICE_URL", "http://verifiable-credential-service:8000")
    
    try:
        # Get current entity stats
        with httpx.Client() as client:
            response = client.get(f"{vc_service_url}/api/v1/trust/entities/{entity_id}")
            if response.status_code != 200:
                return
                
            entity_data = response.json()
            trust_score = entity_data.get('trust_score', 0)
            activity_counts = entity_data.get('activity_counts', {})
            
        # Check milestones
        milestones_triggered = []
        
        # First verification milestone
        if (activity_type == 'credential_verified' and 
            activity_counts.get('credential_verified', 0) == 1):
            milestones_triggered.append({
                'type': 'TrustedMemberBadge',
                'reason': 'First credential verification completed'
            })
            
        # Verification expert milestone
        if activity_counts.get('credential_verified', 0) >= 10:
            milestones_triggered.append({
                'type': 'VerificationExpertCredential',
                'reason': '10 successful verifications completed'
            })
            
        # Community leader milestone
        if trust_score >= 0.9:  # 90% trust score
            milestones_triggered.append({
                'type': 'CommunityLeaderSBT',
                'reason': 'Achieved exceptional trust score',
                'is_sbt': True
            })
            
        # Cross-chain pioneer milestone
        if activity_counts.get('bridge_transfer_completed', 0) >= 5:
            milestones_triggered.append({
                'type': 'CrossChainPioneerCredential',
                'reason': '5 successful cross-chain transfers'
            })
            
        # Issue milestone credentials
        for milestone in milestones_triggered:
            logger.info(f"Milestone reached for {entity_id}: {milestone['type']}")
            
            # Request credential issuance
            issue_event = IssueVerifiableCredential(
                tenant_id="platform",  # Platform-wide milestone
                subject={
                    "id": entity_id,
                    "achievement": milestone['type'],
                    "reason": milestone['reason'],
                    "achieved_at": datetime.utcnow().isoformat() + "Z"
                },
                credential_type=milestone['type'],
                options={
                    "store_on_ipfs": True,
                    "create_sbt": milestone.get('is_sbt', False)
                }
            )
            
            if publisher:
                publisher.publish(
                    topic_base='issue-verifiable-credential-events',
                    tenant_id="platform",
                    schema_class=IssueVerifiableCredential,
                    data=issue_event
                )
                
    except Exception as e:
        logger.error(f"Error checking reputation milestones: {e}")


# --- Custom Webhook Handlers ---

async def handle_nextcloud_file_created(event):
    """Handle Nextcloud file created events"""
    logger.info(f"Nextcloud file created: {event.data.get('file', {}).get('path')}")
    # Trigger file processing workflows based on file type

async def handle_zulip_command(event):
    """Handle Zulip bot commands"""
    message = event.data.get('message', {})
    content = message.get('content', '')
    
    if '/platformq create-project' in content:
        # Extract project name and trigger project creation
        logger.info("Project creation requested via Zulip command")

async def handle_openproject_work_package(event):
    """Handle OpenProject work package events"""
    wp = event.data.get('work_package', {})
    logger.info(f"Work package {event.event_type}: {wp.get('subject')}")
    
    # Trigger workflows based on work package type/status
    if wp.get('type', {}).get('name') == 'Epic' and wp.get('status', {}).get('name') == 'Closed':
        logger.info("Epic completed, triggering milestone workflow")


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
    app.state.stop_event = threading.Event()
    
    # Set up webhook handlers
    setup_webhooks(app, config_loader, app.state.event_publisher)
    
    # Register custom webhook handlers
    if hasattr(app.state, 'webhook_manager'):
        webhook_manager: WebhookManager = app.state.webhook_manager
        
        # Nextcloud handlers
        webhook_manager.register_handler('nextcloud', 'file_created', handle_nextcloud_file_created)
        
        # Zulip handlers
        webhook_manager.register_handler('zulip', 'message', handle_zulip_command)
        
        # OpenProject handlers
        webhook_manager.register_handler('openproject', 'work_package:created', handle_openproject_work_package)
        webhook_manager.register_handler('openproject', 'work_package:updated', handle_openproject_work_package)
        
        logger.info("Custom webhook handlers registered")
    
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
        app.state.workflow_thread = thread
        logger.info("Running in legacy mode without Airflow integration")
    
    # Start project workflow consumer
    proj_thread = threading.Thread(target=project_workflow_consumer_loop, args=(app,), daemon=True)
    proj_thread.start()
    app.state.project_thread = proj_thread
    
    # Start document update consumer
    doc_thread = threading.Thread(target=document_update_consumer_loop, args=(app,), daemon=True)
    doc_thread.start()
    app.state.document_thread = doc_thread
    
    # Start trust activity tracking consumer
    trust_thread = threading.Thread(target=trust_activity_consumer_loop, args=(app,), daemon=True)
    trust_thread.start()
    app.state.trust_thread = trust_thread
    
    logger.info("All consumer threads started")

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutdown signal received, stopping consumer threads.")
    
    # Signal all threads to stop
    if hasattr(app.state, 'stop_event'):
        app.state.stop_event.set()
    
    # Wait for threads to complete
    if hasattr(app.state, 'workflow_thread'):
        app.state.workflow_thread.join(timeout=5)
    if hasattr(app.state, 'project_thread'):
        app.state.project_thread.join(timeout=5)
    if hasattr(app.state, 'document_thread'):
        app.state.document_thread.join(timeout=5)
    if hasattr(app.state, 'trust_thread'):
        app.state.trust_thread.join(timeout=5)
    
    # Stop event publisher
    if app.state.event_publisher:
        app.state.event_publisher.close()
    
    # Stop Airflow event processor if running
    if hasattr(app.state, 'event_processor'):
        app.state.event_processor.stop()
    
    logger.info("All consumer threads stopped.")

# Legacy webhook endpoints (kept for backward compatibility)
@app.post("/webhooks/openproject/legacy")
async def handle_openproject_webhook_legacy(request: Request):
    """
    Legacy webhook handler - use the new webhook routes instead
    """
    logger.warning("Legacy OpenProject webhook endpoint called - please update to new webhook routes")
    
    publisher = request.app.state.event_publisher
    payload = await request.json()

    project_created_event = ProjectCreatedEvent(
        project_id=str(payload.get("project", {}).get("id", "")),
        project_name=payload.get("project", {}).get("name", ""),
        creator_id=str(payload.get("created_by", {}).get("id", "")),
        event_timestamp=int(time.time() * 1000)
    )
    
    publisher.publish(
        topic_base='project-events',
        tenant_id="00000000-0000-0000-0000-000000000000",
        schema_class=ProjectCreatedEvent,
        data=project_created_event
    )
    
    return Response(status_code=status.HTTP_204_NO_CONTENT) 

@app.post("/webhooks/onlyoffice/legacy")
async def handle_onlyoffice_webhook_legacy(request: Request):
    """
    Legacy OnlyOffice webhook handler - use Nextcloud webhooks instead
    """
    logger.warning("Legacy OnlyOffice webhook endpoint called - please update to Nextcloud webhook routes")
    
    publisher = request.app.state.event_publisher
    payload = await request.json()
    
    tenant_id = "00000000-0000-0000-0000-000000000000"

    document_updated_event = DocumentUpdatedEvent(
        tenant_id=tenant_id,
        document_id=str(payload.get("fileid", "")),
        document_path=payload.get("path", ""),
        saved_by_user_id=str(payload.get("userid", "")),
        event_timestamp=int(time.time() * 1000)
    )
    
    publisher.publish(
        topic_base='document-events',
        tenant_id=tenant_id,
        schema_class=DocumentUpdatedEvent,
        data=document_updated_event
    )
    
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

# --- Webhook Statistics Endpoint ---
@app.get("/api/v1/webhook-stats")
async def get_webhook_stats(request: Request):
    """Get webhook processing statistics"""
    if hasattr(request.app.state, 'webhook_manager'):
        stats = request.app.state.webhook_manager.get_stats()
        return {
            "stats": stats,
            "timestamp": datetime.utcnow().isoformat()
        }
    return {"error": "Webhook manager not initialized"} 