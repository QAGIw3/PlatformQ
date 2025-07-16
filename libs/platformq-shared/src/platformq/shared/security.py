import datetime
import os

from cassandra.cluster import Session
from fastapi import Depends, HTTPException, Security, status, Header, Request
from fastapi.security.api_key import APIKeyHeader
from jose import jwt, JWTError

# --- Placeholder Dependencies ---
# These functions act as an "interface" that the shared security dependencies
# expect the consuming service to provide via FastAPI's dependency overrides.
# This allows the security logic to remain generic and reusable.

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_api_key_crud_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_user_crud_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_db_session_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_password_verifier_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")

def require_service_token(
    request: Request,
    token: str = Security(api_key_header), # Re-using for X-API-Key header
):
    """
    A dependency to protect service-to-service endpoints.
    It expects a JWT in the X-API-Key header and verifies it.
    """
    if token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Service token required",
        )
    try:
        # In a real app, you would fetch the public key for the auth-service
        # to verify the JWT signature. For now, we'll assume it's symmetric.
        # This secret would be stored in Vault.
        secret_key = os.getenv("S2S_JWT_SECRET")
        if not secret_key:
            raise RuntimeError("S2S_JWT_SECRET not configured.")
            
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
        # The 'sub' of the token is the service's role_id
        service_role_id = payload.get("sub")
        if service_role_id is None:
            raise HTTPException(status_code=403, detail="Invalid service token")
        
        # You could add more checks here, e.g., that the service_role_id
        # is in an approved list of services.
        
        return payload

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate service token",
        )


# --- Authentication Dependencies ---

def get_current_user_from_trusted_header(
    user_id: str = Header(None, alias="X-Authenticated-Userid"), # Kong injects this after a valid JWT
    tenant_id: str = Header(None, alias="X-Tid"), # Our custom claim, injected by Kong
    db: Session = Depends(get_db_session_dependency),
    user_crud = Depends(get_user_crud_dependency)
):
    """
    The primary security dependency for user-facing applications.
    
    This function DOES NOT validate a JWT. It assumes that the request has already
    been authenticated by an upstream gateway (Kong). It trusts the headers that
    Kong injects, retrieves the user from the database, and performs basic checks.
    This is the "Trusted Subsystem" model.
    """
    if not user_id or not tenant_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User or Tenant ID missing from trusted headers."
        )
    
    user = user_crud.get_user_by_id(db, user_id=user_id)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid user")
    if user.status != 'active':
        raise HTTPException(status_code=403, detail="User is not active")
        
    return user


def get_user_from_api_key(
    api_key: str = Security(api_key_header),
    db: Session = Depends(get_db_session_dependency),
    api_key_crud = Depends(get_api_key_crud_dependency),
    user_crud = Depends(get_user_crud_dependency),
    password_verifier = Depends(get_password_verifier_dependency),
):
    """
    The primary security dependency for machine-to-machine communication.
    
    This function handles the entire lifecycle of an API key: parsing,
    database lookup, hash comparison, and user retrieval. It is used
    for programmatic access to the platform.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="API Key required"
        )

    try:
        parts = api_key.split("_")
        if len(parts) != 3:
            raise ValueError("Invalid key format")
        prefix = f"{parts[0]}_{parts[1]}"
        secret = parts[2]
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid API Key format")

    key_data = api_key_crud.get_api_key_by_prefix(db, prefix=prefix)

    if not key_data:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    if not key_data["is_active"]:
        raise HTTPException(status_code=403, detail="API Key is inactive")

    if key_data["expires_at"] and key_data["expires_at"] < datetime.datetime.now(
        datetime.timezone.utc
    ):
        raise HTTPException(status_code=403, detail="API Key has expired")

    if not password_verifier(secret, key_data['hashed_key']):
        raise HTTPException(status_code=401, detail="Invalid API Key")
        
    user = user_crud.get_user_by_id(db, user_id=key_data['user_id'])
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    if user.status != "active":
        raise HTTPException(status_code=403, detail="User is not active")

    return user
