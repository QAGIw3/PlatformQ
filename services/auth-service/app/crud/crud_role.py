from cassandra.cluster import Session
from uuid import UUID
from typing import List

def assign_role_to_user(db: Session, *, tenant_id: UUID, user_id: UUID, role: str):
    """Assigns a role to a user in the database."""
    query = "INSERT INTO user_roles (tenant_id, user_id, role) VALUES (%s, %s, %s)"
    prepared = db.prepare(query)
    db.execute(prepared, [tenant_id, user_id, role])

def remove_role_from_user(db: Session, *, tenant_id: UUID, user_id: UUID, role: str):
    """Removes a role from a user in the database."""
    query = "DELETE FROM user_roles WHERE tenant_id = %s AND user_id = %s AND role = %s"
    prepared = db.prepare(query)
    db.execute(prepared, [tenant_id, user_id, role])

def get_roles_for_user(db: Session, *, tenant_id: UUID, user_id: UUID) -> List[str]:
    """Retrieves all roles for a given user."""
    query = "SELECT role FROM user_roles WHERE tenant_id = %s AND user_id = %s"
    prepared = db.prepare(query)
    rows = db.execute(prepared, [tenant_id, user_id])
    return [row.role for row in rows]
