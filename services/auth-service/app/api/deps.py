from fastapi import Depends, HTTPException, status, Security, Request, Header
from cassandra.cluster import Session
import datetime
from uuid import UUID

from ..crud import crud_user, crud_role, crud_api_key
from ..core.security import verify_password
from shared_lib import security as shared_security

# --- Dependency Provider Functions ---


def get_db_session(request: Request) -> Session:
    """
    FastAPI dependency that provides a Cassandra session from the app state.
    It also ensures the keyspace exists.
    """
    session = request.app.state.db_manager.get_session()
    # This is not ideal, as the keyspace is hardcoded.
    # A better solution would fetch this from config as well.
    try:
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS auth_keyspace
            WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
            AND durable_writes = true;
        """)
        session.set_keyspace('auth_keyspace')
        yield session
    finally:
        pass


def get_event_publisher(request: Request) -> EventPublisher:
    return request.app.state.event_publisher


# --- Dependency Provider Functions ---


def get_api_key_crud():
    return crud_api_key


def get_user_crud():
    return crud_user


def get_password_verifier():
    return verify_password


# --- Main Dependencies ---

def get_current_tenant_and_user(
    tenant_id: UUID = Header(None, alias="X-Tenant-ID"),
    user_id: UUID = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db_session)
):
    """
    Gets the current tenant and user based on trusted headers from the gateway.
    This is the primary security dependency for tenant-scoped operations.
    """
    if not tenant_id or not user_id:
        raise HTTPException(status_code=401, detail="Missing tenant or user information")
    
    user = crud_user.get_user_by_id(db, tenant_id=tenant_id, user_id=user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user or tenant")
    if user.status != 'active':
        raise HTTPException(status_code=403, detail="User is not active")
    
    # Return both for use in endpoints
    return {"tenant_id": tenant_id, "user": user}

def require_role(required_role: str):
    def role_checker(
        context: dict = Depends(get_current_tenant_and_user),
        db: Session = Depends(get_db_session),
    ):
        user_roles = crud_role.get_roles_for_user(
            db, tenant_id=context["tenant_id"], user_id=context["user"].id
        )
        if required_role not in user_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"The user does not have the required '{required_role}' role",
            )
        return context
    return role_checker

# The API Key dependency also needs to be updated, but we will focus on the main flow first.
get_current_user_from_api_key = shared_security.get_user_from_api_key
