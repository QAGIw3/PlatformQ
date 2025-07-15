from cassandra.cluster import Session
from uuid import UUID, uuid4
import datetime

def create_tenant(db: Session, name: str) -> dict:
    """Creates a new tenant."""
    tenant_id = uuid4()
    now = datetime.datetime.now(datetime.timezone.utc)
    query = "INSERT INTO tenants (id, name, created_at) VALUES (%s, %s, %s)"
    prepared = db.prepare(query)
    db.execute(prepared, [tenant_id, name, now])
    return get_tenant(db, tenant_id)

def get_tenant(db: Session, tenant_id: UUID) -> dict:
    """Retrieves a tenant by its ID."""
    query = "SELECT id, name, created_at FROM tenants WHERE id = %s"
    prepared = db.prepare(query)
    row = db.execute(prepared, [tenant_id]).one()
    return dict(row._asdict()) if row else None 