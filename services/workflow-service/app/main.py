"""
Workflow Service

Airflow-based workflow orchestration with verifiable credentials.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status, HTTPException, Query, Depends
import logging
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import uuid

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    WorkflowCreatedEvent,
    WorkflowStartedEvent,
    WorkflowCompletedEvent,
    WorkflowFailedEvent,
    TaskCompletedEvent,
    AssetCreatedEvent,
    DocumentUpdatedEvent,
    ProjectCreatedEvent,
    DAOEvent
)

from .api import endpoints
from .api import compute_endpoints
from .api import data_platform_endpoints
from .api.deps import get_db_session, get_api_key_crud, get_user_crud, get_password_verifier
from .repository import WorkflowRepository, TaskRepository, ResourceAuthorizationRepository
from .event_processors import WorkflowEventProcessor, AssetWorkflowProcessor
from .airflow_bridge import AirflowBridge, EventToDAGProcessor
from .dynamic_dags import FederatedSimulation, generate_federated_dag
from .verifiable_credentials.workflow_credentials import (
    WorkflowCredentialManager,
    WorkflowCredentialVerifier,
    CredentialType,
    VerifiablePresentation
)
from .compute_orchestration import (
    WorkflowComputeOrchestrator,
    WorkflowResourceEstimate,
    WorkflowResourceType,
    TaskResourceProfile
)

logger = logging.getLogger(__name__)

# Service components
workflow_event_processor = None
asset_workflow_processor = None
airflow_bridge = None
credential_manager = None
credential_verifier = None
service_clients = None
compute_orchestrator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global workflow_event_processor, asset_workflow_processor, airflow_bridge
    global credential_manager, credential_verifier, service_clients, compute_orchestrator
    
    # Startup
    logger.info("Starting Workflow Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize repositories
    app.state.workflow_repo = WorkflowRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.task_repo = TaskRepository(get_db_session)
    app.state.resource_auth_repo = ResourceAuthorizationRepository(get_db_session)
    
    # Initialize Airflow bridge
    airflow_bridge = AirflowBridge(
        airflow_url=settings.get("airflow_url", "http://airflow-webserver:8080"),
        username=settings.get("airflow_username", "admin"),
        password=settings.get("airflow_password", "admin")
    )
    app.state.airflow_bridge = airflow_bridge
    
    # Initialize credential system
    credential_manager = WorkflowCredentialManager(
        issuer_did=settings.get("workflow_issuer_did", "did:example:workflow-service"),
        signing_key=settings.get("workflow_signing_key", ""),
        vc_service_url=settings.get("vc_service_url", "http://verifiable-credential-service:8000")
    )
    await credential_manager.initialize()
    app.state.credential_manager = credential_manager
    
    credential_verifier = WorkflowCredentialVerifier(
        vc_service_url=settings.get("vc_service_url", "http://verifiable-credential-service:8000")
    )
    app.state.credential_verifier = credential_verifier
    
    # Initialize compute orchestrator
    compute_orchestrator = WorkflowComputeOrchestrator(
        derivatives_engine_url=settings.get("derivatives_engine_url", "http://derivatives-engine-service:8000"),
        airflow_api_url=settings.get("airflow_url", "http://airflow-webserver:8080"),
        event_publisher=app.state.event_publisher
    )
    app.state.compute_orchestrator = compute_orchestrator
    
    # Initialize event processors
    workflow_event_processor = WorkflowEventProcessor(
        service_name="workflow-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        workflow_repo=app.state.workflow_repo,
        task_repo=app.state.task_repo,
        airflow_bridge=airflow_bridge,
        credential_manager=credential_manager,
        service_clients=service_clients
    )
    
    asset_workflow_processor = AssetWorkflowProcessor(
        service_name="workflow-service-assets",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        workflow_repo=app.state.workflow_repo,
        airflow_bridge=airflow_bridge
    )
    
    # Start event processors
    await asyncio.gather(
        workflow_event_processor.start(),
        asset_workflow_processor.start()
    )
    
    # Initialize event to DAG processor
    app.state.event_to_dag_processor = EventToDAGProcessor(
        airflow_bridge=airflow_bridge,
        event_publisher=app.state.event_publisher
    )
    
    logger.info("Workflow Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Workflow Service...")
    
    # Stop event processors
    await asyncio.gather(
        workflow_event_processor.stop() if workflow_event_processor else asyncio.sleep(0),
        asset_workflow_processor.stop() if asset_workflow_processor else asyncio.sleep(0)
    )
    
    logger.info("Workflow Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="workflow-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[workflow_event_processor, asset_workflow_processor] 
    if all([workflow_event_processor, asset_workflow_processor]) else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["workflows"])
app.include_router(compute_endpoints.router, prefix="/api/v1", tags=["workflow-compute"])
app.include_router(data_platform_endpoints.router, prefix="/api/v1", tags=["data-platform-workflows"])

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "workflow-service",
        "version": "2.0",
        "features": [
            "airflow-orchestration",
            "verifiable-credentials",
            "event-driven-workflows",
            "dynamic-dag-generation",
            "federated-simulations",
            "data-platform-integration",
            "ml-training-workflows",
            "realtime-analytics-pipelines"
        ]
    }


# Legacy webhook endpoints (to be migrated to event-driven)
@app.post("/webhooks/document-updated")
async def handle_document_webhook(request: Request):
    """Legacy webhook - use event processing instead"""
    return {"status": "deprecated", "message": "Use event-driven processing"}


@app.post("/webhooks/project-created")
async def handle_project_webhook(request: Request):
    """Legacy webhook - use event processing instead"""
    return {"status": "deprecated", "message": "Use event-driven processing"} 