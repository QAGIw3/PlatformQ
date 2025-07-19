"""
API Dependencies for Quantum Optimization Service

Provides authentication, database, and other dependencies for API endpoints
"""

import logging
from typing import Dict, Any, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import JWTError, jwt
import httpx
import os

from platformq_shared.database import get_db
from platformq_shared.config import ConfigLoader

logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer()

# Configuration
config_loader = ConfigLoader()
JWT_SECRET_KEY = config_loader.get_secret("JWT_SECRET_KEY", "your-secret-key")
JWT_ALGORITHM = config_loader.get_config("JWT_ALGORITHM", "HS256")
AUTH_SERVICE_URL = config_loader.get_config("AUTH_SERVICE_URL", "http://auth-service:8000")


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Verify JWT token and extract user information
    
    Returns:
        Dict containing user_id, tenant_id, and other claims
    """
    token = credentials.credentials
    
    try:
        # Decode JWT token
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        
        # Extract user and tenant information
        user_id = payload.get("sub")
        tenant_id = payload.get("tid")
        
        if not user_id or not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing user or tenant information",
                headers={"WWW-Authenticate": "Bearer"},
            )
            
        return {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "groups": payload.get("groups", []),
            "email": payload.get("email"),
            "exp": payload.get("exp")
        }
        
    except JWTError as e:
        logger.error(f"JWT validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(token_data: Dict[str, Any] = Depends(verify_token)) -> Dict[str, Any]:
    """
    Get current authenticated user
    
    Returns:
        Dict containing user information
    """
    return {
        "id": token_data["user_id"],
        "tenant_id": token_data["tenant_id"],
        "email": token_data.get("email"),
        "groups": token_data.get("groups", [])
    }


async def get_current_tenant_id(user: Dict[str, Any] = Depends(get_current_user)) -> str:
    """
    Get current tenant ID from authenticated user
    
    Returns:
        Tenant ID string
    """
    return user["tenant_id"]


async def get_current_user_id(user: Dict[str, Any] = Depends(get_current_user)) -> str:
    """
    Get current user ID from authenticated user
    
    Returns:
        User ID string
    """
    return user["id"]


def get_db_session() -> Session:
    """
    Get database session
    
    Returns:
        SQLAlchemy database session
    """
    return next(get_db())


async def verify_service_token(credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)) -> bool:
    """
    Verify service-to-service authentication token
    
    Returns:
        True if valid service token
    """
    if not credentials:
        return False
        
    token = credentials.credentials
    
    # Check if it's a service token (different format/validation)
    if token.startswith("service-"):
        # In production, validate against service registry
        # For now, check against known service tokens
        valid_service_tokens = {
            "service-simulation": True,
            "service-cad-collaboration": True,
            "service-graph-intelligence": True,
            "service-neuromorphic": True
        }
        
        service_name = token.replace("-token", "")
        return valid_service_tokens.get(service_name, False)
        
    return False


class RoleChecker:
    """
    Dependency to check if user has required roles
    """
    
    def __init__(self, required_roles: list):
        self.required_roles = required_roles
        
    def __call__(self, user: Dict[str, Any] = Depends(get_current_user)) -> bool:
        """
        Check if user has any of the required roles
        
        Returns:
            True if user has required role
        """
        user_groups = user.get("groups", [])
        
        for role in self.required_roles:
            if role in user_groups:
                return True
                
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions"
        )


async def get_quantum_resource_quota(
    tenant_id: str = Depends(get_current_tenant_id),
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """
    Get quantum resource quota for tenant
    
    Returns:
        Dict containing quota information
    """
    # In production, fetch from database
    # For now, return default quotas
    return {
        "max_qubits": 20,
        "max_jobs_per_day": 100,
        "max_circuit_depth": 10,
        "max_shots": 10000,
        "priority": "standard"
    }


async def validate_api_key(
    api_key: Optional[str] = None,
    db: Session = Depends(get_db_session)
) -> Optional[Dict[str, Any]]:
    """
    Validate API key if provided (alternative to JWT)
    
    Returns:
        Dict containing API key information if valid
    """
    if not api_key:
        return None
        
    # Make request to auth service to validate API key
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{AUTH_SERVICE_URL}/api/v1/api-keys/validate",
                headers={"X-API-Key": api_key}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error validating API key: {e}")
            return None


# Convenience dependencies for common use cases
require_admin = RoleChecker(["admin", "platform_admin"])
require_quantum_access = RoleChecker(["quantum_user", "quantum_admin", "admin"])
require_premium = RoleChecker(["premium_user", "enterprise_user", "admin"])


# Placeholder CRUD dependencies for create_base_app
# These are required by the base service but not used directly in quantum service
def get_api_key_crud_placeholder():
    """Placeholder for API key CRUD operations"""
    class APIKeyCRUD:
        def get_api_key(self, db, api_key: str):
            # In production, this would validate against auth service
            return None
    return APIKeyCRUD()


def get_user_crud_placeholder():
    """Placeholder for user CRUD operations"""
    class UserCRUD:
        def get_user_by_id(self, db, user_id: str):
            # In production, this would fetch from auth service
            return {"id": user_id, "email": f"user_{user_id}@platformq.io"}
    return UserCRUD()


def get_password_verifier_placeholder():
    """Placeholder for password verification"""
    class PasswordVerifier:
        def verify_password(self, plain_password: str, hashed_password: str):
            # Not used in quantum service - auth handled by JWT
            return False
    return PasswordVerifier() 