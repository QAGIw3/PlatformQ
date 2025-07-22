"""
Unified Authentication Module for PlatformQ Services

Provides standardized authentication and authorization across all services.
"""

import os
import jwt
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from functools import wraps
import asyncio
from enum import Enum

from fastapi import HTTPException, Security, Depends, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
import httpx
from pydantic import BaseModel
import hashlib
import hmac

logger = logging.getLogger(__name__)


class AuthType(str, Enum):
    """Supported authentication types"""
    JWT = "jwt"
    API_KEY = "api_key"
    SERVICE_TOKEN = "service_token"
    MIXED = "mixed"  # Supports multiple auth types


class UserRole(str, Enum):
    """Standard user roles across platform"""
    USER = "user"
    TRADER = "trader"
    MARKET_MAKER = "market_maker"
    RISK_MANAGER = "risk_manager"
    ADMIN = "admin"
    SERVICE = "service"  # For service-to-service auth
    OPERATOR = "operator"
    VAULT_CREATOR = "vault_creator"


class AuthConfig(BaseModel):
    """Authentication configuration"""
    auth_service_url: str = os.getenv("AUTH_SERVICE_URL", "http://auth-service:8000")
    jwt_secret_key: str = os.getenv("JWT_SECRET_KEY", "")
    jwt_algorithm: str = "HS256"
    token_expire_minutes: int = 30
    enable_caching: bool = True
    cache_ttl_seconds: int = 300
    require_tenant_isolation: bool = True
    allowed_service_keys: List[str] = []


class AuthenticatedUser(BaseModel):
    """Authenticated user information"""
    user_id: str
    username: Optional[str] = None
    email: Optional[str] = None
    tenant_id: str = "default"
    roles: List[str] = []
    permissions: List[str] = []
    auth_type: AuthType = AuthType.JWT
    metadata: Dict[str, Any] = {}


# Security schemes
bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


class UnifiedAuth:
    """Unified authentication handler for all PlatformQ services"""
    
    def __init__(self, config: Optional[AuthConfig] = None):
        self.config = config or AuthConfig()
        self._auth_cache: Dict[str, Tuple[AuthenticatedUser, datetime]] = {}
        self._http_client: Optional[httpx.AsyncClient] = None
        
    async def __aenter__(self):
        self._http_client = httpx.AsyncClient(timeout=10.0)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._http_client:
            await self._http_client.aclose()
            
    def _get_cache_key(self, auth_type: str, credential: str) -> str:
        """Generate cache key for auth credentials"""
        return hashlib.sha256(f"{auth_type}:{credential}".encode()).hexdigest()
        
    async def _check_cache(self, cache_key: str) -> Optional[AuthenticatedUser]:
        """Check if valid cached auth exists"""
        if not self.config.enable_caching:
            return None
            
        if cache_key in self._auth_cache:
            user, expiry = self._auth_cache[cache_key]
            if datetime.utcnow() < expiry:
                return user
            else:
                del self._auth_cache[cache_key]
        return None
        
    def _cache_auth(self, cache_key: str, user: AuthenticatedUser):
        """Cache authentication result"""
        if self.config.enable_caching:
            expiry = datetime.utcnow() + timedelta(seconds=self.config.cache_ttl_seconds)
            self._auth_cache[cache_key] = (user, expiry)
            
    async def validate_jwt(self, token: str) -> AuthenticatedUser:
        """Validate JWT token"""
        cache_key = self._get_cache_key("jwt", token)
        
        # Check cache
        cached_user = await self._check_cache(cache_key)
        if cached_user:
            return cached_user
            
        try:
            # Try local validation first
            if self.config.jwt_secret_key:
                payload = jwt.decode(
                    token,
                    self.config.jwt_secret_key,
                    algorithms=[self.config.jwt_algorithm]
                )
                
                user = AuthenticatedUser(
                    user_id=payload.get("sub", payload.get("user_id")),
                    username=payload.get("username"),
                    email=payload.get("email"),
                    tenant_id=payload.get("tenant_id", "default"),
                    roles=payload.get("roles", []),
                    permissions=payload.get("permissions", []),
                    auth_type=AuthType.JWT,
                    metadata=payload.get("metadata", {})
                )
                
                self._cache_auth(cache_key, user)
                return user
                
            # Fall back to auth service validation
            if self._http_client:
                response = await self._http_client.post(
                    f"{self.config.auth_service_url}/api/v1/auth/validate",
                    headers={"Authorization": f"Bearer {token}"}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    user = AuthenticatedUser(**data)
                    self._cache_auth(cache_key, user)
                    return user
                    
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token has expired")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Invalid token")
        except Exception as e:
            logger.error(f"JWT validation error: {e}")
            
        raise HTTPException(status_code=401, detail="Could not validate credentials")
        
    async def validate_api_key(self, api_key: str) -> AuthenticatedUser:
        """Validate API key"""
        cache_key = self._get_cache_key("api_key", api_key)
        
        # Check cache
        cached_user = await self._check_cache(cache_key)
        if cached_user:
            return cached_user
            
        # Check if it's a known service key
        if api_key in self.config.allowed_service_keys:
            # Extract service name from API key format: "svc_<service_name>_<random>"
            parts = api_key.split("_")
            service_name = parts[1] if len(parts) > 2 else "unknown"
            
            user = AuthenticatedUser(
                user_id=f"service_{service_name}",
                username=service_name,
                tenant_id="platform",
                roles=[UserRole.SERVICE],
                auth_type=AuthType.API_KEY,
                metadata={"service_name": service_name}
            )
            
            self._cache_auth(cache_key, user)
            return user
            
        # Validate with auth service
        if self._http_client:
            try:
                response = await self._http_client.post(
                    f"{self.config.auth_service_url}/api/v1/auth/validate-api-key",
                    headers={"X-API-Key": api_key}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    user = AuthenticatedUser(**data)
                    self._cache_auth(cache_key, user)
                    return user
            except Exception as e:
                logger.error(f"API key validation error: {e}")
                
        raise HTTPException(status_code=403, detail="Invalid API key")
        
    def has_role(self, user: AuthenticatedUser, required_roles: List[str]) -> bool:
        """Check if user has any of the required roles"""
        return any(role in user.roles for role in required_roles)
        
    def has_permission(self, user: AuthenticatedUser, required_permissions: List[str]) -> bool:
        """Check if user has all required permissions"""
        return all(perm in user.permissions for perm in required_permissions)
        
    def check_tenant_access(self, user: AuthenticatedUser, tenant_id: str) -> bool:
        """Check if user has access to specified tenant"""
        if not self.config.require_tenant_isolation:
            return True
        return user.tenant_id == tenant_id or UserRole.ADMIN in user.roles


# Global auth instance
_auth_instance: Optional[UnifiedAuth] = None


def get_auth_instance() -> UnifiedAuth:
    """Get or create global auth instance"""
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = UnifiedAuth()
    return _auth_instance


# FastAPI Dependencies
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
    api_key: Optional[str] = Security(api_key_header),
    x_user_id: Optional[str] = Header(None),
    x_tenant_id: Optional[str] = Header(None),
    x_roles: Optional[str] = Header(None)
) -> AuthenticatedUser:
    """
    Unified authentication dependency for FastAPI.
    Supports multiple authentication methods.
    """
    auth = get_auth_instance()
    
    # Priority 1: Bearer token (JWT)
    if credentials and credentials.credentials:
        return await auth.validate_jwt(credentials.credentials)
        
    # Priority 2: API Key
    if api_key:
        return await auth.validate_api_key(api_key)
        
    # Priority 3: Pre-authenticated headers (from API Gateway)
    if x_user_id and x_tenant_id:
        return AuthenticatedUser(
            user_id=x_user_id,
            tenant_id=x_tenant_id,
            roles=x_roles.split(",") if x_roles else [],
            auth_type=AuthType.JWT
        )
        
    raise HTTPException(
        status_code=401,
        detail="No valid authentication credentials provided"
    )


def require_roles(*roles: str):
    """Decorator to require specific roles"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, current_user: AuthenticatedUser = Depends(get_current_user), **kwargs):
            auth = get_auth_instance()
            if not auth.has_role(current_user, list(roles)):
                raise HTTPException(
                    status_code=403,
                    detail=f"Required roles: {', '.join(roles)}"
                )
            return await func(*args, current_user=current_user, **kwargs)
        return wrapper
    return decorator


def require_permissions(*permissions: str):
    """Decorator to require specific permissions"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, current_user: AuthenticatedUser = Depends(get_current_user), **kwargs):
            auth = get_auth_instance()
            if not auth.has_permission(current_user, list(permissions)):
                raise HTTPException(
                    status_code=403,
                    detail=f"Required permissions: {', '.join(permissions)}"
                )
            return await func(*args, current_user=current_user, **kwargs)
        return wrapper
    return decorator


# Convenience dependency factories
def get_current_trader() -> AuthenticatedUser:
    """Dependency that requires trader role"""
    async def _get_trader(user: AuthenticatedUser = Depends(get_current_user)) -> AuthenticatedUser:
        if UserRole.TRADER not in user.roles:
            raise HTTPException(status_code=403, detail="Trader access required")
        return user
    return Depends(_get_trader)


def get_current_admin() -> AuthenticatedUser:
    """Dependency that requires admin role"""
    async def _get_admin(user: AuthenticatedUser = Depends(get_current_user)) -> AuthenticatedUser:
        if UserRole.ADMIN not in user.roles:
            raise HTTPException(status_code=403, detail="Admin access required")
        return user
    return Depends(_get_admin)


def get_service_auth() -> AuthenticatedUser:
    """Dependency for service-to-service authentication"""
    async def _get_service(user: AuthenticatedUser = Depends(get_current_user)) -> AuthenticatedUser:
        if UserRole.SERVICE not in user.roles:
            raise HTTPException(status_code=403, detail="Service authentication required")
        return user
    return Depends(_get_service) 