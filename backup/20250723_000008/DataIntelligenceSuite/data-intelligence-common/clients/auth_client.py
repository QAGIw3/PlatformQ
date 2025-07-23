"""
Auth Service Client

Client for authentication and authorization operations.
"""

import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

from .base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


@dataclass
class AuthToken:
    """Authentication token"""
    access_token: str
    token_type: str = "Bearer"
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    scope: Optional[str] = None
    
    @property
    def is_expired(self) -> bool:
        """Check if token is expired"""
        if not self.expires_in:
            return False
        # This is simplified - in real implementation would track issue time
        return False
        
    def to_header(self) -> Dict[str, str]:
        """Convert to authorization header"""
        return {"Authorization": f"{self.token_type} {self.access_token}"}


@dataclass
class User:
    """User model"""
    id: str
    username: str
    email: str
    roles: List[str]
    permissions: List[str]
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None


class AuthServiceClient(BaseServiceClient):
    """
    Client for auth service operations.
    
    Features:
    - User authentication
    - Token management
    - Role/permission checks
    - User management
    """
    
    def __init__(self, config: Optional[ClientConfig] = None, **kwargs):
        if not config:
            config = ClientConfig(service_name="auth-service")
        super().__init__(config, **kwargs)
        
        self._cached_token: Optional[AuthToken] = None
        
    async def login(
        self,
        username: str,
        password: str,
        scope: Optional[str] = None
    ) -> AuthToken:
        """
        Authenticate user and get token.
        
        Args:
            username: Username or email
            password: Password
            scope: Optional scope for token
            
        Returns:
            Authentication token
        """
        data = {
            "username": username,
            "password": password
        }
        
        if scope:
            data["scope"] = scope
            
        response = await self.post("/auth/login", json_data=data)
        
        token = AuthToken(
            access_token=response["access_token"],
            token_type=response.get("token_type", "Bearer"),
            expires_in=response.get("expires_in"),
            refresh_token=response.get("refresh_token"),
            scope=response.get("scope")
        )
        
        # Cache token for subsequent requests
        self._cached_token = token
        self.config.auth_token = token.access_token
        
        return token
        
    async def logout(self, token: Optional[str] = None) -> bool:
        """
        Logout and invalidate token.
        
        Args:
            token: Token to invalidate (uses cached if not provided)
            
        Returns:
            Success status
        """
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            
        response = await self.post("/auth/logout", headers=headers)
        
        # Clear cached token
        self._cached_token = None
        self.config.auth_token = None
        
        return response.get("success", False)
        
    async def refresh_token(
        self,
        refresh_token: str
    ) -> AuthToken:
        """
        Refresh access token.
        
        Args:
            refresh_token: Refresh token
            
        Returns:
            New authentication token
        """
        response = await self.post(
            "/auth/refresh",
            json_data={"refresh_token": refresh_token}
        )
        
        token = AuthToken(
            access_token=response["access_token"],
            token_type=response.get("token_type", "Bearer"),
            expires_in=response.get("expires_in"),
            refresh_token=response.get("refresh_token"),
            scope=response.get("scope")
        )
        
        # Update cached token
        self._cached_token = token
        self.config.auth_token = token.access_token
        
        return token
        
    async def verify_token(
        self,
        token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Verify token validity.
        
        Args:
            token: Token to verify (uses cached if not provided)
            
        Returns:
            Token info including user and permissions
        """
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            
        return await self.get("/auth/verify", headers=headers)
        
    async def get_current_user(self) -> User:
        """
        Get current authenticated user.
        
        Returns:
            Current user
        """
        response = await self.get("/auth/me")
        
        return User(
            id=response["id"],
            username=response["username"],
            email=response["email"],
            roles=response.get("roles", []),
            permissions=response.get("permissions", []),
            is_active=response.get("is_active", True),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            metadata=response.get("metadata")
        )
        
    async def check_permission(
        self,
        permission: str,
        resource: Optional[str] = None
    ) -> bool:
        """
        Check if current user has permission.
        
        Args:
            permission: Permission to check
            resource: Optional resource identifier
            
        Returns:
            Whether user has permission
        """
        params = {"permission": permission}
        if resource:
            params["resource"] = resource
            
        response = await self.get("/auth/check-permission", params=params)
        return response.get("has_permission", False)
        
    async def check_role(self, role: str) -> bool:
        """
        Check if current user has role.
        
        Args:
            role: Role to check
            
        Returns:
            Whether user has role
        """
        response = await self.get(
            "/auth/check-role",
            params={"role": role}
        )
        return response.get("has_role", False)
        
    # User management
    
    async def create_user(
        self,
        username: str,
        email: str,
        password: str,
        roles: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> User:
        """
        Create a new user.
        
        Args:
            username: Username
            email: Email address
            password: Password
            roles: Initial roles
            metadata: Additional metadata
            
        Returns:
            Created user
        """
        data = {
            "username": username,
            "email": email,
            "password": password,
            "roles": roles or [],
            "metadata": metadata or {}
        }
        
        response = await self.post("/users", json_data=data)
        
        return User(
            id=response["id"],
            username=response["username"],
            email=response["email"],
            roles=response.get("roles", []),
            permissions=response.get("permissions", []),
            is_active=response.get("is_active", True),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            metadata=response.get("metadata")
        )
        
    async def get_user(self, user_id: str) -> User:
        """
        Get user by ID.
        
        Args:
            user_id: User ID
            
        Returns:
            User
        """
        response = await self.get(f"/users/{user_id}")
        
        return User(
            id=response["id"],
            username=response["username"],
            email=response["email"],
            roles=response.get("roles", []),
            permissions=response.get("permissions", []),
            is_active=response.get("is_active", True),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            metadata=response.get("metadata")
        )
        
    async def update_user(
        self,
        user_id: str,
        username: Optional[str] = None,
        email: Optional[str] = None,
        roles: Optional[List[str]] = None,
        is_active: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> User:
        """
        Update user.
        
        Args:
            user_id: User ID
            username: New username
            email: New email
            roles: New roles
            is_active: Active status
            metadata: New metadata
            
        Returns:
            Updated user
        """
        data = {}
        if username is not None:
            data["username"] = username
        if email is not None:
            data["email"] = email
        if roles is not None:
            data["roles"] = roles
        if is_active is not None:
            data["is_active"] = is_active
        if metadata is not None:
            data["metadata"] = metadata
            
        response = await self.patch(f"/users/{user_id}", json_data=data)
        
        return User(
            id=response["id"],
            username=response["username"],
            email=response["email"],
            roles=response.get("roles", []),
            permissions=response.get("permissions", []),
            is_active=response.get("is_active", True),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            metadata=response.get("metadata")
        )
        
    async def delete_user(self, user_id: str) -> bool:
        """
        Delete user.
        
        Args:
            user_id: User ID
            
        Returns:
            Success status
        """
        response = await self.delete(f"/users/{user_id}")
        return response.get("success", False)
        
    async def list_users(
        self,
        limit: int = 100,
        offset: int = 0,
        role: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> List[User]:
        """
        List users with filters.
        
        Args:
            limit: Maximum users to return
            offset: Offset for pagination
            role: Filter by role
            is_active: Filter by active status
            
        Returns:
            List of users
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if role is not None:
            params["role"] = role
        if is_active is not None:
            params["is_active"] = is_active
            
        response = await self.get("/users", params=params)
        
        return [
            User(
                id=u["id"],
                username=u["username"],
                email=u["email"],
                roles=u.get("roles", []),
                permissions=u.get("permissions", []),
                is_active=u.get("is_active", True),
                created_at=u.get("created_at"),
                updated_at=u.get("updated_at"),
                metadata=u.get("metadata")
            )
            for u in response.get("users", [])
        ]
        
    # Role management
    
    async def assign_role(
        self,
        user_id: str,
        role: str
    ) -> bool:
        """
        Assign role to user.
        
        Args:
            user_id: User ID
            role: Role to assign
            
        Returns:
            Success status
        """
        response = await self.post(
            f"/users/{user_id}/roles",
            json_data={"role": role}
        )
        return response.get("success", False)
        
    async def revoke_role(
        self,
        user_id: str,
        role: str
    ) -> bool:
        """
        Revoke role from user.
        
        Args:
            user_id: User ID
            role: Role to revoke
            
        Returns:
            Success status
        """
        response = await self.delete(
            f"/users/{user_id}/roles/{role}"
        )
        return response.get("success", False) 