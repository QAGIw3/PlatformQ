"""
API dependencies for governance service
"""

from typing import Generator, Optional
from fastapi import Depends, HTTPException, Request, Header
from sqlalchemy.orm import Session
import logging

from ..core import GovernanceManager
from platformq.shared.nextcloud_client import NextcloudClient

logger = logging.getLogger(__name__)


def get_db_session(request: Request) -> Session:
    """Get database session from app state"""
    SessionLocal = request.app.state.db_session
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_governance_manager(request: Request) -> GovernanceManager:
    """Get governance manager from app state"""
    return request.app.state.governance_manager


def get_nextcloud_client(request: Request) -> NextcloudClient:
    """Get Nextcloud client from app state"""
    return request.app.state.nextcloud_client


def get_current_context(
    request: Request,
    x_user_id: str = Header(None),
    x_tenant_id: str = Header(None),
    x_roles: str = Header(None),
    x_reputation: int = Header(None),
    x_voting_power: int = Header(None)
) -> dict:
    """Get current user context from headers"""
    if not x_user_id or not x_tenant_id:
        raise HTTPException(
            status_code=401,
            detail="Missing required authentication headers"
        )
    
    # Parse roles from comma-separated string
    roles = x_roles.split(",") if x_roles else []
    
    # Create user object for compatibility
    user = type('User', (), {
        'id': x_user_id,
        'roles': roles
    })()
    
    return {
        "tenant_id": x_tenant_id,
        "user": user,
        "roles": roles,
        "reputation": x_reputation or 0,
        "voting_power": x_voting_power or 1
    }


# For backward compatibility with proposals-service
get_current_tenant_and_user = get_current_context 