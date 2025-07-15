from cassandra.cluster import Session
from uuid import UUID, uuid4
import datetime

def create_proposal(db: Session, *, tenant_id: UUID, user_id: UUID, customer_name: str, document_path: str):
    proposal_id = uuid4()
    now = datetime.datetime.now(datetime.timezone.utc)
    query = "INSERT INTO proposals (tenant_id, proposal_id, status, customer_name, document_path, created_by_user_id, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    prepared = db.prepare(query)
    db.execute(prepared, [tenant_id, proposal_id, 'draft', customer_name, document_path, user_id, now, now])
    return {"proposal_id": proposal_id} 