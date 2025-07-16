import requests
from fastapi import Depends, HTTPException, status, Security, Request, Header
from cassandra.cluster import Session
import datetime
from uuid import UUID

from ..crud import crud_user, crud_role, crud_api_key
from ..core.security import verify_password
from platformq_shared import security as shared_security

# --- Dependency Provider Functions ---
# These functions act as the "glue" between our generic, shared security
# dependencies and this specific service's concrete implementation of its
# CRUD modules. They are wired into the shared library in main.py.


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
    """Provides the concrete API Key CRUD module."""
    return crud_api_key


def get_user_crud():
    """Provides the concrete User CRUD module."""
    return crud_user


def get_password_verifier():
    """Provides the concrete password verification function."""
    return verify_password


# --- Main Dependencies ---
# We "import" the fully-wired, generic dependencies from the shared library
# for use in our own API endpoints.

get_current_tenant_and_user = shared_security.get_current_user_from_trusted_header
get_current_user_from_api_key = shared_security.get_user_from_api_key

def require_role(required_role: str):
    """
    A dependency factory that creates a role-checking dependency.
    It takes a required role as an argument and returns a FastAPI dependency
    that will check if the current user has that role.
    It depends on `get_current_tenant_and_user` to get the user first.
    """
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

GRAPH_INTELLIGENCE_SERVICE_URL = "http://graph-intelligence-service:8000" # Placeholder

def require_trust_score(required_score: float):
    """
    A dependency factory that creates a trust score-checking dependency.
    """
    def trust_checker(
        context: dict = Depends(get_current_tenant_and_user),
    ):
        user_id = str(context["user"].id)
        try:
            response = requests.get(f"{GRAPH_INTELLIGENCE_SERVICE_URL}/trust-score/{user_id}")
            response.raise_for_status()
            score_data = response.json()
            if score_data["trust_score"] < required_score:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"User's trust score of {score_data['trust_score']} is below the required score of {required_score}",
                )
        except requests.exceptions.RequestException as e:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Could not connect to the Graph Intelligence Service: {e}",
            )
        return context
    return trust_checker
