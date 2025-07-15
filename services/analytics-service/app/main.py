from platformq_shared.base_service import create_base_app
from fastapi import Depends
from cassandra.cluster import Session
from uuid import UUID

from app.api.deps import get_current_tenant_and_user, get_db_session

# This service uses the shared library, but needs to provide its own dependencies
# as it doesn't have the full auth-service crud modules.
# For simplicity, we will define placeholder functions.
def get_api_key_crud_placeholder(): return None
def get_user_crud_placeholder(): return None
def get_password_verifier_placeholder(): return None

app = create_base_app(
    service_name="analytics-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

@app.get("/api/v1/activity")
def get_activity_stream(
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session)
):
    """
    Fetches the unified activity stream for the current user's tenant.
    """
    tenant_id = context["tenant_id"]
    query = "SELECT * FROM activity_stream WHERE tenant_id = %s LIMIT 50"
    prepared = db.prepare(query)
    rows = db.execute(prepared, [tenant_id])
    return [dict(row._asdict()) for row in rows] 