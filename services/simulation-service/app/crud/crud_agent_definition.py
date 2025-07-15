from cassandra.cluster import Session
from uuid import UUID

def create_agent_definition(db: Session, *, tenant_id: UUID, simulation_id: UUID, name: str, rules: str, initial_state: str):
    query = "INSERT INTO agent_definitions (tenant_id, simulation_id, agent_type_name, behavior_rules, initial_state_distribution) VALUES (%s, %s, %s, %s, %s)"
    db.execute(db.prepare(query), [tenant_id, simulation_id, name, rules, initial_state]) 