import datetime
from typing import Optional
from uuid import UUID, uuid4

from cassandra.cluster import Session


def create_audit_log(
    db: Session,
    *,
    tenant_id: UUID,
    event_type: str,
    details: str,
    user_id: Optional[UUID] = None
):
    """Creates an entry in the audit log."""
    query = """
    INSERT INTO audit_log (tenant_id, id, user_id, event_type, details, event_timestamp)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    prepared = db.prepare(query)
    log_id = uuid4()
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    db.execute(prepared, [tenant_id, log_id, user_id, event_type, details, timestamp])
