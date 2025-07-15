from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from sqlalchemy.orm import Session
import time
from uuid import UUID

from .api.deps import get_current_tenant_and_user, get_db_session, get_event_publisher
from .crud import crud_simulation, crud_agent_definition, crud_agent_state
from platformq_shared.events import SimulationStartedEvent

app = create_base_app(
    service_name="simulation-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["simulation-service"])

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
