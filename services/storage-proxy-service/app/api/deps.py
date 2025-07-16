from fastapi import Request, Depends, HTTPException
from cassandra.cluster import Session
from shared_lib import security as shared_security
from functools import lru_cache
import httpx
from ..core.config import settings

def get_db_session(request: Request) -> Session:
    session = request.app.state.db_manager.get_session()
    session.set_keyspace('auth_keyspace') # This should be configured
    yield session

# For a new service, these dependencies are placeholders.
# A real service would define its own CRUD modules and pass them here.
def get_api_key_crud_placeholder(): return None
def get_user_crud_placeholder(): return None
def get_password_verifier_placeholder(): return None

get_current_tenant_and_user_unverified = shared_security.get_current_user_from_trusted_header

@lru_cache()
def get_auth_service_url():
    return settings.AUTH_SERVICE_URL

def get_user_storage_config(
    user_context: dict = Depends(get_current_tenant_and_user_unverified),
    auth_service_url: str = Depends(get_auth_service_url)
) -> dict:
    """
    Retrieves the user's storage configuration from the auth-service.
    Caches the result.
    """
    user_id = user_context.get("user_id")
    if not user_id:
        raise HTTPException(status_code=400, detail="User ID not found in context.")
        
    try:
        response = httpx.get(f"{auth_service_url}/internal/users/{user_id}")
        response.raise_for_status()
        user_data = response.json()
        return {
            "backend": user_data.get("storage_backend"),
            "config": user_data.get("storage_config")
        }
    except httpx.RequestError:
        # If auth-service is down, maybe fall back to default?
        # For now, we fail.
        raise HTTPException(status_code=503, detail="Could not contact authentication service.")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            # User not found in auth-service, this shouldn't happen if JWT is valid.
            raise HTTPException(status_code=404, detail="User not found.")
        raise HTTPException(status_code=500, detail="Error fetching user storage configuration.")

# This is the dependency that should be used by most endpoints now.
get_current_tenant_and_user = get_user_storage_config
