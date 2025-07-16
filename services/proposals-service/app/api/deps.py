from fastapi import Request, Depends, Header, HTTPException, status
from cassandra.cluster import Session
from shared_lib import security as shared_security

def get_db_session(request: Request) -> Session:
    session = request.app.state.db_manager.get_session()
    session.set_keyspace('proposals_keyspace') # This should be configured for proposals
    yield session

# For a new service, these dependencies are placeholders.
# A real service would define its own CRUD modules and pass them here.
def get_api_key_crud_placeholder(): return None
def get_user_crud_placeholder(): return None
def get_password_verifier_placeholder(): return None

def get_current_tenant_user_and_reputation(
    user_id: str = Header(None, alias="X-Authenticated-Userid"),
    tenant_id: str = Header(None, alias="X-Tid"),
    reputation: int = Header(0, alias="X-User-Reputation"),
    db: Session = Depends(get_db_session)
):
    """
    This dependency trusts headers injected by an upstream gateway.
    It fetches the user and provides the user object, tenant_id, and reputation score.
    """
    if not user_id or not tenant_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User or Tenant ID missing from trusted headers."
        )
    
    # The proposals service does not have a user table, so we will not validate the user.
    # In a real scenario, this might call the auth service or expect a user object in the header.
    # For now, we construct a mock user object.
    mock_user = type('User', (), {'id': user_id, 'tenant_id': tenant_id})()

    return {"user": mock_user, "tenant_id": tenant_id, "reputation": reputation}

# Alias for use in endpoints
get_current_context = get_current_tenant_user_and_reputation
