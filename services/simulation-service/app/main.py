from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from sqlalchemy.orm import Session
import time

from .api.deps import get_current_tenant_and_user, get_db_session, get_event_publisher

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
    """
    Defines a new simulation and its agents.
    """
    tenant_id = context["tenant_id"]
    # In a real implementation, we would have a full CRUD module for this.
    # For now, we'll use conceptual direct queries.
    
    # 1. Create the main simulation record
    # db.execute("INSERT INTO simulations ...", [tenant_id, sim_id, ...])
    
    # 2. Create the agent definitions for this simulation
    # for agent_def in sim_in.agent_definitions:
    #     db.execute("INSERT INTO agent_definitions ...", [tenant_id, sim_id, agent_def.agent_type_name, ...])
        
    return {"message": f"Simulation '{sim_in.simulation_name}' defined successfully."}

@app.post("/api/v1/simulations/{simulation_id}/start")
def start_simulation(
    simulation_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher),
):
    """
    Starts a simulation by publishing a SimulationStarted event.
    """
    tenant_id = context["tenant_id"]
    
    # In a real app, you would verify the simulation exists and is in a 'defined' state.
    
    event_data = {
        "simulation_id": simulation_id,
        "tenant_id": str(tenant_id),
        "event_timestamp": int(time.time() * 1000)
    }
    
    publisher.publish(
        topic_base='simulation-control-events',
        tenant_id=str(tenant_id),
        schema_path='schemas/simulation_started.avsc', # We will create this schema
        data=event_data
    )
    
    return {"message": f"Simulation {simulation_id} start signal sent."}
