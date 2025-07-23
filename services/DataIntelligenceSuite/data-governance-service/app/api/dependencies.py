"""
API Dependencies
"""
from typing import Optional
from fastapi import Depends, HTTPException, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# Import from common library
from data_intelligence_common.clients.auth_client import AuthServiceClient
from data_intelligence_common.core.api.middleware import get_request_id

from ..core.container import Container


# Security scheme
security = HTTPBearer()

# Global container instance
_container: Optional[Container] = None


def get_container() -> Container:
    """Get dependency injection container"""
    global _container
    if _container is None:
        _container = Container()
    return _container


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    container: Container = Depends(get_container)
) -> str:
    """Get current authenticated user"""
    try:
        # Get auth client
        auth_client = AuthServiceClient()
        
        # Verify token
        token_info = await auth_client.verify_token(credentials.credentials)
        
        if not token_info or not token_info.get("valid"):
            raise HTTPException(status_code=401, detail="Invalid token")
        
        # Get user info
        user = await auth_client.get_current_user()
        
        return user.username
        
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Authentication failed: {str(e)}")


async def require_permission(permission: str):
    """Require specific permission"""
    async def permission_checker(
        current_user: str = Depends(get_current_user),
        container: Container = Depends(get_container)
    ) -> str:
        auth_client = AuthServiceClient()
        
        has_permission = await auth_client.check_permission(permission)
        if not has_permission:
            raise HTTPException(
                status_code=403,
                detail=f"Permission '{permission}' required"
            )
        
        return current_user
    
    return permission_checker


async def require_role(role: str):
    """Require specific role"""
    async def role_checker(
        current_user: str = Depends(get_current_user),
        container: Container = Depends(get_container)
    ) -> str:
        auth_client = AuthServiceClient()
        
        has_role = await auth_client.check_role(role)
        if not has_role:
            raise HTTPException(
                status_code=403,
                detail=f"Role '{role}' required"
            )
        
        return current_user
    
    return role_checker


def get_request_context(
    request_id: str = Depends(get_request_id),
    user: str = Depends(get_current_user)
) -> dict:
    """Get request context"""
    return {
        "request_id": request_id,
        "user": user
    } 