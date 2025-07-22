"""
Search Service API dependencies
"""

from fastapi import Depends, HTTPException, Header
from typing import Dict, Any

async def get_current_user(authorization: str = Header(None)) -> Dict[str, Any]:
    """Get current user from authorization header"""
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
    
    # In production, validate JWT token
    # For now, return mock user
    return {
        "id": "user123",
        "tenant_id": "default",
        "roles": ["user", "analytics"]
    } 