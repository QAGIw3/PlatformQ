"""
API dependencies
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any
import logging

from app.core.medallion_architecture import MedallionArchitectureManager
from app.core.lifecycle_manager import DataLifecycleManager

logger = logging.getLogger(__name__)

security = HTTPBearer()


# Global instances (initialized in main.py)
medallion_manager = None
lifecycle_manager = None


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """Get current user from JWT token"""
    # In production, validate JWT token and extract user info
    # For now, return mock user
    return {
        "username": "data_user",
        "tenant_id": "default",
        "roles": ["data_engineer"]
    }


async def get_medallion_manager() -> MedallionArchitectureManager:
    """Get medallion architecture manager instance"""
    if medallion_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Medallion architecture manager not initialized"
        )
    return medallion_manager


async def get_lifecycle_manager() -> DataLifecycleManager:
    """Get lifecycle manager instance"""
    if lifecycle_manager is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lifecycle manager not initialized"
        )
    return lifecycle_manager 