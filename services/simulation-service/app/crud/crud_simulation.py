from cassandra.cluster import Session
from uuid import UUID, uuid4
import datetime

def create_simulation(db: Session, *, tenant_id: UUID, name: str):
    sim_id = uuid4()
    now = datetime.datetime.now(datetime.timezone.utc)
    query = "INSERT INTO simulations (tenant_id, simulation_id, simulation_name, status, created_at) VALUES (%s, %s, %s, %s, %s)"
    db.execute(db.prepare(query), [tenant_id, sim_id, name, 'defined', now])
    return {"simulation_id": sim_id}

def get_simulation(db: Session, *, tenant_id: UUID, simulation_id: UUID):
    query = "SELECT * FROM simulations WHERE tenant_id = %s AND simulation_id = %s"
    return db.execute(db.prepare(query), [tenant_id, simulation_id]).one() 