"""
Simulation Service

Multi-agent simulation platform with federated ML and multi-physics support.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
import time
from uuid import UUID
import asyncio
import logging

from platformq_shared import (
    create_base_app,
    EventProcessor,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher
from platformq_events import SimulationStartedEvent, SimulationRunCompleted, SimulationStartRequestEvent

from .api import endpoints
from .api.endpoints import multi_physics_ws
from .api.deps import (
    get_current_tenant_and_user,
    get_db_session,
    get_event_publisher,
    get_api_key_crud,
    get_user_crud,
    get_password_verifier
)
from .repository import (
    SimulationRepository,
    AgentDefinitionRepository,
    AgentStateRepository,
    SimulationResultRepository
)
from .event_processors import (
    SimulationLifecycleProcessor,
    FederatedSimulationProcessor,
    MultiPhysicsSimulationProcessor,
    CollaborationEventProcessor
)
from .collaboration import SimulationCollaborationManager
from .ignite_manager import SimulationIgniteManager
from .federated_ml_integration import SimulationMLOrchestrator

logger = logging.getLogger(__name__)

# Service components
simulation_processor = None
federated_processor = None
physics_processor = None
collab_processor = None
ignite_manager = None
ml_orchestrator = None
collab_manager = None
service_clients = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global simulation_processor, federated_processor, physics_processor, collab_processor
    global ignite_manager, ml_orchestrator, collab_manager, service_clients
    
    # Startup
    logger.info("Starting Simulation Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize repositories
    app.state.simulation_repo = SimulationRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.agent_def_repo = AgentDefinitionRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.agent_state_repo = AgentStateRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.result_repo = SimulationResultRepository(get_db_session)
    
    # Initialize Ignite manager
    ignite_manager = SimulationIgniteManager(
        ignite_host=settings.get("ignite_host", "localhost"),
        ignite_port=int(settings.get("ignite_port", 10800))
    )
    await ignite_manager.connect()
    app.state.ignite_manager = ignite_manager
    
    # Initialize ML orchestrator
    ml_orchestrator = SimulationMLOrchestrator(
        mlflow_url=settings.get("mlflow_url", "http://mlflow-server:5000"),
        federated_server_url=settings.get("federated_server_url", "http://federated-learning-service:8000")
    )
    await ml_orchestrator.initialize()
    app.state.ml_orchestrator = ml_orchestrator
    
    # Initialize collaboration manager
    collab_manager = SimulationCollaborationManager()
    app.state.collab_manager = collab_manager
    
    # Initialize event processors
    simulation_processor = SimulationLifecycleProcessor(
        service_name="simulation-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        simulation_repo=app.state.simulation_repo,
        agent_def_repo=app.state.agent_def_repo,
        agent_state_repo=app.state.agent_state_repo,
        result_repo=app.state.result_repo,
        ignite_manager=ignite_manager,
        service_clients=service_clients
    )
    
    federated_processor = FederatedSimulationProcessor(
        service_name="simulation-service-federated",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        simulation_repo=app.state.simulation_repo,
        ml_orchestrator=ml_orchestrator,
        service_clients=service_clients
    )
    
    physics_processor = MultiPhysicsSimulationProcessor(
        service_name="simulation-service-physics",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        simulation_repo=app.state.simulation_repo,
        result_repo=app.state.result_repo,
        ignite_manager=ignite_manager
    )
    
    collab_processor = CollaborationEventProcessor(
        service_name="simulation-service-collab",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        collab_manager=collab_manager
    )
    
    # Start event processors
    await asyncio.gather(
        simulation_processor.start(),
        federated_processor.start(),
        physics_processor.start(),
        collab_processor.start()
    )
    
    logger.info("Simulation Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Simulation Service...")
    
    # Stop event processors
    await asyncio.gather(
        simulation_processor.stop() if simulation_processor else asyncio.sleep(0),
        federated_processor.stop() if federated_processor else asyncio.sleep(0),
        physics_processor.stop() if physics_processor else asyncio.sleep(0),
        collab_processor.stop() if collab_processor else asyncio.sleep(0)
    )
    
    # Cleanup resources
    if ml_orchestrator:
        await ml_orchestrator.stop()
        
    if ignite_manager:
        await ignite_manager.disconnect()
        
    logger.info("Simulation Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="simulation-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[
        simulation_processor, federated_processor, 
        physics_processor, collab_processor
    ] if all([simulation_processor, federated_processor, physics_processor, collab_processor]) else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["simulations"])
app.include_router(
    multi_physics_ws.router, 
    prefix="/api/v1/multi-physics", 
    tags=["multi-physics-ws"]
)

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "simulation-service",
        "version": "2.0",
        "features": [
            "multi-agent-simulation",
            "federated-ml",
            "multi-physics",
            "real-time-collaboration",
            "ignite-integration",
            "event-driven"
        ]
    }


# Refactored API endpoints using new patterns
class AgentDefinition(BaseModel):
    agent_type_name: str
    behavior_rules: str
    initial_state_distribution: str

class SimulationCreateRequest(BaseModel):
    simulation_name: str
    agent_definitions: List[AgentDefinition]
    description: Optional[str] = None
    simulation_type: str = "multi-agent"


@app.post("/api/v1/simulations", status_code=201)
async def create_simulation(
    sim_in: SimulationCreateRequest,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """Create a new simulation with agent definitions"""
    tenant_id = UUID(context["tenant_id"])
    user = context["user"]
    
    # Use repository to create simulation
    simulation_repo = app.state.simulation_repo
    agent_def_repo = app.state.agent_def_repo
    
    # Create simulation
    simulation = simulation_repo.create(
        simulation_data={
            "simulation_name": sim_in.simulation_name,
            "description": sim_in.description or "",
            "simulation_type": sim_in.simulation_type,
            "created_by": user["id"]
        },
        tenant_id=tenant_id
    )
    
    # Create agent definitions
    for agent_def in sim_in.agent_definitions:
        agent_def_repo.create(
            agent_data=agent_def,
            tenant_id=tenant_id,
            simulation_id=simulation["simulation_id"]
        )
        
    return {
        "simulation_id": str(simulation["simulation_id"]),
        "simulation_name": simulation["simulation_name"],
        "status": simulation["status"],
        "created_at": simulation["created_at"].isoformat()
    }


@app.post("/api/v1/simulations/{simulation_id}/start")
async def start_simulation(
    simulation_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Start a simulation - triggers event processing"""
    tenant_id = UUID(context["tenant_id"])
    
    # Verify simulation exists
    simulation_repo = app.state.simulation_repo
    sim = simulation_repo.get(tenant_id, simulation_id)
    
    if not sim or sim["status"] != 'defined':
        raise HTTPException(
            status_code=400,
            detail="Simulation not found or already running"
        )
    
    # Publish start event
    publisher.publish(
        topic_base='simulation-start-request-events',
        tenant_id=str(tenant_id),
        schema_class=SimulationStartRequestEvent,
        data=SimulationStartRequestEvent(
            tenant_id=str(tenant_id),
            simulation_id=str(simulation_id),
            requested_by=context["user"]["id"]
        )
    )
    
    return {"message": f"Simulation {simulation_id} start requested"}


@app.get("/api/v1/simulations/{simulation_id}")
async def get_simulation(
    simulation_id: UUID,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get simulation details"""
    tenant_id = UUID(context["tenant_id"])
    
    simulation_repo = app.state.simulation_repo
    simulation = simulation_repo.get(tenant_id, simulation_id)
    
    if not simulation:
        raise HTTPException(status_code=404, detail="Simulation not found")
        
    # Get agent definitions
    agent_def_repo = app.state.agent_def_repo
    agent_defs = agent_def_repo.get_by_simulation(tenant_id, simulation_id)
    
    return {
        "simulation": simulation,
        "agent_definitions": agent_defs
    }


@app.get("/api/v1/simulations")
async def list_simulations(
    status: Optional[str] = None,
    limit: int = 100,
    context: dict = Depends(get_current_tenant_and_user)
):
    """List simulations for tenant"""
    tenant_id = UUID(context["tenant_id"])
    
    simulation_repo = app.state.simulation_repo
    simulations = simulation_repo.list_by_tenant(tenant_id, status=status, limit=limit)
    
    return {
        "simulations": simulations,
        "total": len(simulations)
    }


@app.websocket("/ws/simulations/{simulation_id}")
async def simulation_websocket(
    websocket: WebSocket,
    simulation_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """WebSocket endpoint for real-time simulation updates"""
    await websocket.accept()
    
    # Add to collaboration manager
    collab_manager = app.state.collab_manager
    await collab_manager.add_participant(
        simulation_id,
        context["user"]["id"],
        context["user"].get("username", "Unknown"),
        "viewer"
    )
    
    try:
        while True:
            # Receive updates from client
            data = await websocket.receive_json()
            
            # Process collaboration update
            await collab_manager.process_update(
                simulation_id,
                context["user"]["id"],
                data.get("type"),
                data.get("data")
            )
            
    except WebSocketDisconnect:
        # Remove from collaboration
        await collab_manager.remove_participant(
            simulation_id,
            context["user"]["id"]
        )


# Legacy endpoint kept for backward compatibility
class CompletionRequest(BaseModel):
    run_id: str
    status: str
    log_uri: str

@app.post("/api/v1/simulations/{simulation_id}/complete")
async def complete_simulation_legacy(
    simulation_id: UUID,
    completion_data: CompletionRequest,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher),
):
    """Legacy completion endpoint - use events instead"""
    tenant_id = str(context["tenant_id"])
    
    publisher.publish(
        topic_base='simulation-lifecycle-events',
        tenant_id=tenant_id,
        schema_class=SimulationRunCompleted,
        data=SimulationRunCompleted(
            tenant_id=tenant_id,
            simulation_id=str(simulation_id),
            run_id=completion_data.run_id,
            status=completion_data.status,
            log_uri=completion_data.log_uri
        )
    )
    
    return {"status": "completion_event_published"}
