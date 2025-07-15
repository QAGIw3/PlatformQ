from cassandra.cluster import Session
from uuid import UUID

def get_agent_states_for_simulation(db: Session, *, tenant_id: UUID, simulation_id: UUID):
    """Fetches the current state of all agents in a simulation."""
    query = "SELECT agent_id, agent_type, position_x, position_y, attributes FROM agent_states WHERE tenant_id = %s AND simulation_id = %s"
    prepared = db.prepare(query)
    rows = db.execute(prepared, [tenant_id, simulation_id])
    return [dict(row._asdict()) for row in rows] 