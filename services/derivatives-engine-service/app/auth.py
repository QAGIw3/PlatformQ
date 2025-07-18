from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Optional

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Dict[str, str]:
    """
    Stub authentication dependency
    In production, this would validate JWT tokens and return user info
    """
    # For now, return a mock user
    return {
        "user_id": "mock_user_123",
        "email": "user@example.com",
        "roles": ["trader"]
    }


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[Dict[str, str]]:
    """
    Optional authentication - returns None if no auth provided
    """
    if credentials:
        return await get_current_user(credentials)
    return None 