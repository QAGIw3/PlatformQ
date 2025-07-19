"""
FastAPI Dependencies

Common dependencies for the trading platform service.
"""

from typing import Optional
from fastapi import Depends, HTTPException, Header, Request
import logging

from .shared.order_matching import UnifiedMatchingEngine

logger = logging.getLogger(__name__)


def get_matching_engine(request: Request) -> UnifiedMatchingEngine:
    """Get the unified matching engine from app state"""
    matching_engine = getattr(request.app.state, "matching_engine", None)
    if not matching_engine:
        raise HTTPException(status_code=503, detail="Matching engine not available")
    return matching_engine


def get_current_user(
    x_user_id: str = Header(None),
    x_tenant_id: str = Header(None),
    x_roles: str = Header(None)
) -> dict:
    """Get current user from headers"""
    if not x_user_id or not x_tenant_id:
        raise HTTPException(
            status_code=401,
            detail="Missing required authentication headers"
        )
    
    # Parse roles from comma-separated string
    roles = x_roles.split(",") if x_roles else []
    
    return {
        "user_id": x_user_id,
        "tenant_id": x_tenant_id,
        "roles": roles
    } 