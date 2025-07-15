import datetime

from cassandra.cluster import Session
from fastapi import Depends, HTTPException, Security, status, Header
from fastapi.security.api_key import APIKeyHeader

# We will need to find a way to inject these CRUD functions
# or make the services that use this provide them.
# For now, this highlights the dependency.
# from ...crud import crud_api_key, crud_user
# from ...core.security import verify_password


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# This is a placeholder for a more robust dependency injection system.
# In a real application, you might use a container or a more formal plugin system.
def get_api_key_crud_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_user_crud_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_db_session_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_password_verifier_dependency():
    raise NotImplementedError("This dependency must be overridden by the service.")


def get_current_user_from_trusted_header(
    user_id: str = Header(None, alias="X-User-ID"),
    db: Session = Depends(get_db_session_dependency),
    user_crud = Depends(get_user_crud_dependency)
):
    """
    Gets a user object based on a trusted user ID header from the API gateway.
    """
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
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
