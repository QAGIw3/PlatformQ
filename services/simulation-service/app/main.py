from fastapi import Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from sqlalchemy.orm import Session
import time
from uuid import UUID

from .api.deps import get_current_tenant_and_user, get_db_session, get_event_publisher
from .crud import crud_simulation, crud_agent_definition, crud_agent_state
from platformq_shared.events import SimulationStartedEvent, SimulationRunCompleted
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.base_service import create_base_app

from fastapi import WebSocket, WebSocketDisconnect, BackgroundTasks
import asyncio
from .collaboration import SimulationCollaborationManager
from .ignite_manager import SimulationIgniteManager
from .api import endpoints
from .api.endpoints import multi_physics_ws

import logging
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

app = create_base_app(
    service_name="simulation-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["simulation-service"])

# Include multi-physics WebSocket routes
app.include_router(
    multi_physics_ws.router, 
    prefix="/api/v1/multi-physics", 
    tags=["multi-physics-ws"]
)

# Initialize services on startup
@app.on_event("startup")
async def startup_event():
    """Initialize Ignite and collaboration manager"""
    # Initialize Ignite connection
    app.state.ignite_manager = SimulationIgniteManager()
    await app.state.ignite_manager.connect()
    
    # Initialize collaboration manager
    app.state.collaboration_manager = SimulationCollaborationManager(
        ignite_manager=app.state.ignite_manager,
        event_publisher=app.state.event_publisher
    )
    await app.state.collaboration_manager.start()
    
    # Initialize multi-physics WebSocket manager
    multi_physics_ws.initialize_ws_manager(app.state.ignite_manager)
    
    logger.info("Simulation service started with collaboration features and multi-physics support")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if hasattr(app.state, "collaboration_manager"):
        await app.state.collaboration_manager.stop()
    
    if hasattr(app.state, "ignite_manager"):
        await app.state.ignite_manager.disconnect()
    
    logger.info("Simulation service shutdown complete")

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "simulation-service is running"}

class AgentDefinition(BaseModel):
    agent_type_name: str
    behavior_rules: str
    initial_state_distribution: str

class SimulationCreateRequest(BaseModel):
    simulation_name: str
    agent_definitions: List[AgentDefinition]

@app.post("/api/v1/simulations", status_code=201)
def create_simulation(
    sim_in: SimulationCreateRequest,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    tenant_id = context["tenant_id"]
    
    # 1. Create the main simulation record
    new_sim = crud_simulation.create_simulation(db, tenant_id=tenant_id, name=sim_in.simulation_name)
    sim_id = new_sim["simulation_id"]
    
    # 2. Create the agent definitions for this simulation
    for agent_def in sim_in.agent_definitions:
        crud_agent_definition.create_agent_definition(
            db,
            tenant_id=tenant_id,
            simulation_id=sim_id,
            name=agent_def.agent_type_name,
            rules=agent_def.behavior_rules,
            initial_state=agent_def.initial_state_distribution
        )
        
    return new_sim

@app.post("/api/v1/simulations/{simulation_id}/start")
def start_simulation(
    simulation_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher),
    db: Session = Depends(get_db_session)
):
    tenant_id = context["tenant_id"]
    
    sim = crud_simulation.get_simulation(db, tenant_id=tenant_id, simulation_id=simulation_id)
    if not sim or sim.status != 'defined':
        raise HTTPException(status_code=400, detail="Simulation not found or already running.")
    
    publisher.publish(
        topic_base='simulation-control-events',
        tenant_id=str(tenant_id),
        schema_class=SimulationStartedEvent,
        data=SimulationStartedEvent(tenant_id=str(tenant_id), simulation_id=str(simulation_id))
    )
    
    return {"message": f"Simulation {simulation_id} start signal sent."}

class CompletionRequest(BaseModel):
    run_id: str
    status: str
    log_uri: str

@app.post("/api/v1/simulations/{simulation_id}/complete")
def complete_simulation(
    simulation_id: UUID,
    completion_data: CompletionRequest,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher),
):
    """
    An internal endpoint for simulation workers to report completion.
    Publishes a SimulationRunCompleted event.
    """
    tenant_id = str(context["tenant_id"])
    
    # In a real app, we might update the simulation status in our DB here.
    
    publisher.publish(
        topic_base='simulation-lifecycle-events',
        tenant_id=tenant_id,
        schema_class=SimulationRunCompleted,
        data=SimulationRunCompleted(
            tenant_id=tenant_id,
            simulation_id=str(simulation_id),
            run_id=completion_data.run_id,
            status=completion_data.status,
            log_uri=completion_data.log_uri,
        )
    )
    
    return {"message": f"Completion of simulation {simulation_id} recorded."}


@app.get("/api/v1/simulations/{simulation_id}/state")
def get_simulation_state(
    simulation_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """
    Returns the real-time state of all agents in a running simulation.
    """
    tenant_id = context["tenant_id"]
    return crud_agent_state.get_agent_states_for_simulation(
        db, tenant_id=tenant_id, simulation_id=simulation_id
    )

@app.post("/api/v1/simulations/{simulation_id}/collaborate")
async def create_collaboration_session(
    simulation_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """Create a new collaboration session for a simulation"""
    tenant_id = context["tenant_id"]
    user_id = context["user"]["id"]
    
    # Verify simulation exists and user has access
    sim = crud_simulation.get_simulation(db, tenant_id=tenant_id, simulation_id=simulation_id)
    if not sim:
        raise HTTPException(status_code=404, detail="Simulation not found")
    
    # Create collaboration session
    session_id = await app.state.collaboration_manager.create_session(
        str(simulation_id), user_id
    )
    
    return {
        "session_id": session_id,
        "websocket_url": f"/api/v1/ws/collaborate/{session_id}"
    }

@app.get("/api/v1/simulations/{simulation_id}/sessions")
async def get_active_sessions(
    simulation_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Get active collaboration sessions for a simulation"""
    # Get sessions from Ignite
    sessions = await app.state.ignite_manager.get_active_sessions()
    
    # Filter by simulation_id
    simulation_sessions = [
        s for s in sessions 
        if s.get("simulation_id") == str(simulation_id)
    ]
    
    return {"sessions": simulation_sessions}

@app.websocket("/api/v1/ws/collaborate/{session_id}")
async def websocket_collaborate(
    websocket: WebSocket,
    session_id: str,
    user_id: str = None  # In production, extract from auth token
):
    """WebSocket endpoint for real-time simulation collaboration"""
    await websocket.accept()
    
    if not user_id:
        user_id = "anonymous"  # In production, require auth
    
    try:
        # Join collaboration session
        await app.state.collaboration_manager.join_session(
            session_id, user_id, websocket
        )
        
        # Handle messages
        while True:
            message = await websocket.receive_json()
            await app.state.collaboration_manager.handle_websocket_message(
                session_id, user_id, message
            )
            
    except WebSocketDisconnect:
        logger.info(f"User {user_id} disconnected from session {session_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Leave session
        await app.state.collaboration_manager.leave_session(session_id, user_id)

@app.post("/api/v1/simulations/{simulation_id}/checkpoints")
async def create_checkpoint(
    simulation_id: UUID,
    session_id: str,
    name: str = "Checkpoint",
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a checkpoint of current simulation state"""
    tenant_id = context["tenant_id"]
    user_id = context["user"]["id"]
    
    # Get current state from session
    state_data = await app.state.ignite_manager.get_simulation_state(session_id)
    if not state_data:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Create checkpoint
    checkpoint_id = str(uuid.uuid4())
    await app.state.ignite_manager.create_checkpoint(
        session_id,
        checkpoint_id,
        state_data,
        {
            "name": name,
            "created_by": user_id,
            "simulation_id": str(simulation_id)
        }
    )
    
    return {
        "checkpoint_id": checkpoint_id,
        "created_at": datetime.utcnow().isoformat()
    }

@app.get("/api/v1/simulations/{simulation_id}/checkpoints")
async def list_checkpoints(
    simulation_id: UUID,
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """List checkpoints for a simulation session"""
    checkpoints = await app.state.ignite_manager.list_checkpoints(session_id)
    return {"checkpoints": checkpoints}

@app.post("/api/v1/simulations/{simulation_id}/restore/{checkpoint_id}")
async def restore_checkpoint(
    simulation_id: UUID,
    checkpoint_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Restore simulation from a checkpoint"""
    # Get checkpoint
    checkpoint = await app.state.ignite_manager.get_checkpoint(checkpoint_id)
    if not checkpoint:
        raise HTTPException(status_code=404, detail="Checkpoint not found")
    
    # Create new session from checkpoint
    user_id = context["user"]["id"]
    session_id = await app.state.collaboration_manager.create_session(
        str(simulation_id), user_id
    )
    
    # Restore state
    await app.state.ignite_manager.save_simulation_state(
        session_id, checkpoint["state_data"]
    )
    
    return {
        "session_id": session_id,
        "restored_from": checkpoint_id
    }
