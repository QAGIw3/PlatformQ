"""
Workflow Service

Airflow-based workflow orchestration with verifiable credentials.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status, HTTPException, Query, Depends, Header
import logging
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import uuid
import consul

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

from .vault_consul_integration import VaultConsulIntegration
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
vault_consul = None
workflow_event_processor = None
asset_workflow_processor = None
airflow_bridge = None
credential_manager = None
credential_verifier = None
service_clients = None
compute_orchestrator = None


async def get_vault_consul() -> VaultConsulIntegration:
    """Dependency to get Vault/Consul integration"""
    if not vault_consul:
        raise HTTPException(status_code=500, detail="Vault/Consul integration not initialized")
    return vault_consul


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global vault_consul, workflow_event_processor, asset_workflow_processor, airflow_bridge
    global credential_manager, credential_verifier, service_clients, compute_orchestrator
    
    # Startup
    logger.info("Starting Workflow Service...")
    
    # Initialize Vault and Consul integration first
    vault_consul = VaultConsulIntegration()
    await vault_consul.initialize()
    
    # Get configurations from Vault and Consul
    airflow_config = await vault_consul.get_airflow_config()
    workflow_config = await vault_consul.get_workflow_config()
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize service clients with Vault tokens
    service_clients = ServiceClients(
        base_timeout=30.0,
        max_retries=3,
        token_provider=vault_consul.get_service_token
    )
    app.state.service_clients = service_clients
    
    # Initialize repositories
    app.state.workflow_repo = WorkflowRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.task_repo = TaskRepository(get_db_session)
    app.state.resource_auth_repo = ResourceAuthorizationRepository(get_db_session)
    
    # Initialize Airflow bridge with Vault credentials
    airflow_bridge = AirflowBridge(
        airflow_url=airflow_config['base_url'],
        username=airflow_config['username'],
        password=airflow_config['password'],
        api_key=airflow_config['api_key'],
        vault_integration=vault_consul
    )
    app.state.airflow_bridge = airflow_bridge
    
    # Initialize credential system with Vault signing keys
    credential_manager = WorkflowCredentialManager(
        issuer_did=await vault_consul._signing_keys.get('did', {}).get('id', 'did:example:workflow-service'),
        vault_integration=vault_consul,
        vc_service_url=settings.get("vc_service_url", "http://verifiable-credential-service:8000")
    )
    await credential_manager.initialize()
    app.state.credential_manager = credential_manager
    
    credential_verifier = WorkflowCredentialVerifier(
        vc_service_url=settings.get("vc_service_url", "http://verifiable-credential-service:8000"),
        vault_integration=vault_consul
    )
    app.state.credential_verifier = credential_verifier
    
    # Initialize compute orchestrator
    compute_orchestrator = WorkflowComputeOrchestrator(
        derivatives_engine_url=settings.get("derivatives_engine_url", "http://derivatives-engine-service:8000"),
        airflow_api_url=airflow_config['base_url'],
        event_publisher=app.state.event_publisher,
        vault_integration=vault_consul
    )
    app.state.compute_orchestrator = compute_orchestrator
    
    # Initialize event processors with Vault integration
    workflow_event_processor = WorkflowEventProcessor(
        service_name="workflow-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        workflow_repo=app.state.workflow_repo,
        task_repo=app.state.task_repo,
        airflow_bridge=airflow_bridge,
        credential_manager=credential_manager,
        service_clients=service_clients,
        vault_integration=vault_consul
    )
    
    asset_workflow_processor = AssetWorkflowProcessor(
        service_name="workflow-service-assets",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        workflow_repo=app.state.workflow_repo,
        airflow_bridge=airflow_bridge,
        vault_integration=vault_consul
    )
    
    # Start event processors
    await asyncio.gather(
        workflow_event_processor.start(),
        asset_workflow_processor.start()
    )
    
    # Initialize event to DAG processor
    app.state.event_to_dag_processor = EventToDAGProcessor(
        airflow_bridge=airflow_bridge,
        event_publisher=app.state.event_publisher,
        vault_integration=vault_consul
    )
    
    # Store Vault/Consul integration in app state
    app.state.vault_consul = vault_consul
    
    # Register workflow-specific health checks with Consul
    await vault_consul.consul_client.agent.check.register(
        name=f"{vault_consul.service_name}-airflow",
        check=consul.Check.http(
            f"http://localhost:8000/health/airflow",
            interval="30s",
            timeout="10s"
        )
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
    
    # Close Vault/Consul integration
    if vault_consul:
        await vault_consul.close()
    
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
            "realtime-analytics-pipelines",
            "vault-secured",
            "consul-coordinated",
            "distributed-locking"
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


# Health check with Vault/Consul status
@app.get("/health")
async def health_check():
    """Comprehensive health check"""
    health = {
        "status": "healthy",
        "checks": {}
    }
    
    # Check Vault/Consul integration
    if vault_consul:
        try:
            if vault_consul.vault_client.is_authenticated():
                health["checks"]["vault"] = {"status": "healthy"}
            else:
                health["checks"]["vault"] = {"status": "unhealthy", "error": "Not authenticated"}
                health["status"] = "degraded"
                
            consul_health = await vault_consul.consul_client.health.node("consul")
            if consul_health:
                health["checks"]["consul"] = {"status": "healthy"}
            else:
                health["checks"]["consul"] = {"status": "unhealthy"}
                health["status"] = "degraded"
        except Exception as e:
            health["checks"]["security"] = {"status": "down", "error": str(e)}
            health["status"] = "unhealthy"
    
    # Check Airflow
    if airflow_bridge:
        try:
            airflow_health = await airflow_bridge.health_check()
            health["checks"]["airflow"] = airflow_health
        except Exception as e:
            health["checks"]["airflow"] = {"status": "down", "error": str(e)}
            health["status"] = "degraded"
    
    # Check workflow locks
    if vault_consul:
        try:
            active_locks = len(vault_consul._active_locks)
            health["checks"]["distributed_locks"] = {
                "status": "healthy",
                "active_locks": active_locks
            }
        except Exception as e:
            health["checks"]["distributed_locks"] = {"status": "error", "error": str(e)}
    
    return health


# Airflow-specific health endpoint for Consul
@app.get("/health/airflow")
async def airflow_health():
    """Airflow-specific health check"""
    try:
        if airflow_bridge:
            return await airflow_bridge.health_check()
        return {"status": "unhealthy", "error": "Airflow bridge not initialized"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


# Workflow execution with distributed locking
@app.post("/api/v1/workflows/{workflow_id}/execute")
async def execute_workflow_with_lock(
    workflow_id: str,
    vc: VaultConsulIntegration = Depends(get_vault_consul)
):
    """Execute workflow with distributed lock"""
    # Acquire lock
    lock_acquired = await vc.acquire_workflow_lock(workflow_id)
    if not lock_acquired:
        raise HTTPException(
            status_code=409,
            detail=f"Workflow {workflow_id} is already being executed"
        )
    
    try:
        # Execute workflow
        result = await app.state.airflow_bridge.trigger_dag(
            dag_id=f"workflow_{workflow_id}",
            conf={"workflow_id": workflow_id}
        )
        
        return {
            "workflow_id": workflow_id,
            "execution_id": result.get("dag_run_id"),
            "status": "started"
        }
        
    finally:
        # Always release lock
        await vc.release_workflow_lock(workflow_id)


# Get workflow metrics
@app.get("/api/v1/workflows/metrics")
async def get_workflow_metrics(
    vc: VaultConsulIntegration = Depends(get_vault_consul)
):
    """Get workflow execution metrics"""
    return await vc.get_workflow_metrics()


# Update workflow template (admin only)
@app.put("/api/v1/admin/workflow-templates/{template_name}")
async def update_workflow_template(
    template_name: str,
    template_config: Dict[str, Any],
    x_admin_token: str = Header(None),
    vc: VaultConsulIntegration = Depends(get_vault_consul)
):
    """Update workflow template configuration"""
    # Verify admin token
    if not x_admin_token or x_admin_token != os.environ.get("ADMIN_TOKEN"):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        await vc.update_workflow_template(template_name, template_config)
        return {
            "status": "updated",
            "template": template_name,
            "config": template_config
        }
    except Exception as e:
        logger.error(f"Failed to update template: {e}")
        raise HTTPException(status_code=500, detail="Update failed")


# Get task secrets for execution
@app.get("/api/v1/tasks/{task_id}/secrets")
async def get_task_secrets(
    task_id: str,
    task_type: str = Query(..., description="Type of task"),
    vc: VaultConsulIntegration = Depends(get_vault_consul)
):
    """Get secrets for task execution"""
    try:
        # Verify task exists and is authorized
        # This would check the task repository
        
        secrets = await vc.get_task_secrets(task_type)
        
        # Mask sensitive values for API response
        masked_secrets = {
            "environment": secrets.get("environment", {}),
            "credentials_provided": bool(secrets.get("credentials"))
        }
        
        return masked_secrets
        
    except Exception as e:
        logger.error(f"Failed to get task secrets: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve secrets") 