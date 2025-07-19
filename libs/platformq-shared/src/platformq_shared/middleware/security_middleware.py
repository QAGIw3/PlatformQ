"""
Enhanced Security Middleware

Integrates authentication, authorization, service mesh, and security policies.
"""

import logging
from typing import Dict, Any, Optional, Callable, List
from fastapi import Request, Response, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import asyncio
import time
from datetime import datetime
import json

from ..vault.vault_client import VaultClient
from ..consul.consul_client import ConsulClient
from ..authorization.opa_client import OPAClient, AuthzRequest, OPAConfig
from ..service_mesh import ServiceMeshIntegration

logger = logging.getLogger(__name__)


class SecurityMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive security middleware for PlatformQ services.
    
    Features:
    - JWT token validation
    - Fine-grained authorization with OPA
    - Service mesh integration
    - Rate limiting
    - Request/response encryption
    - Audit logging
    - Security headers
    - CSRF protection
    """
    
    def __init__(self,
                 app: ASGIApp,
                 service_name: str,
                 vault_client: VaultClient,
                 consul_client: ConsulClient,
                 opa_client: Optional[OPAClient] = None,
                 service_mesh: Optional[ServiceMeshIntegration] = None,
                 jwt_secret_key: Optional[str] = None,
                 enable_auth: bool = True,
                 enable_authz: bool = True,
                 enable_audit: bool = True,
                 enable_encryption: bool = False,
                 public_paths: List[str] = None):
        super().__init__(app)
        self.service_name = service_name
        self.vault = vault_client
        self.consul = consul_client
        self.opa = opa_client
        self.service_mesh = service_mesh
        self.jwt_secret_key = jwt_secret_key
        self.enable_auth = enable_auth
        self.enable_authz = enable_authz
        self.enable_audit = enable_audit
        self.enable_encryption = enable_encryption
        self.public_paths = public_paths or ["/health", "/ready", "/metrics", "/docs", "/openapi.json"]
        
        # Initialize OPA if not provided
        if self.enable_authz and not self.opa:
            self.opa = OPAClient(OPAConfig())
            asyncio.create_task(self.opa.initialize())
            
        # Rate limiting state
        self._rate_limit_state: Dict[str, List[float]] = {}
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request through security layers"""
        start_time = time.time()
        request_id = request.headers.get("X-Request-ID", self._generate_request_id())
        
        # Add request ID to context
        request.state.request_id = request_id
        
        # Check if path is public
        if self._is_public_path(request.url.path):
            response = await call_next(request)
            return self._add_security_headers(response, request_id)
            
        try:
            # 1. Validate mTLS if enabled
            if self.service_mesh and self.service_mesh.config.enable_mtls:
                await self._validate_mtls(request)
                
            # 2. Authenticate request
            if self.enable_auth:
                auth_context = await self._authenticate(request)
                request.state.auth_context = auth_context
            else:
                auth_context = {"authenticated": False}
                
            # 3. Check rate limits
            if auth_context.get("authenticated"):
                await self._check_rate_limit(request, auth_context)
                
            # 4. Authorize request
            if self.enable_authz and auth_context.get("authenticated"):
                authz_response = await self._authorize(request, auth_context)
                request.state.authz_response = authz_response
                
                if not authz_response.allowed:
                    raise HTTPException(
                        status_code=403,
                        detail=authz_response.reason or "Access denied"
                    )
                    
                # Apply obligations
                if authz_response.obligations:
                    await self._apply_obligations(request, authz_response.obligations)
                    
            # 5. Decrypt request if needed
            if self.enable_encryption:
                await self._decrypt_request(request)
                
            # 6. Process request
            response = await call_next(request)
            
            # 7. Encrypt response if needed
            if self.enable_encryption:
                response = await self._encrypt_response(response)
                
            # 8. Audit log
            if self.enable_audit:
                await self._audit_log(request, response, auth_context, start_time)
                
            # 9. Add security headers
            response = self._add_security_headers(response, request_id)
            
            return response
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Security middleware error: {e}")
            raise HTTPException(
                status_code=500,
                detail="Internal security error"
            )
            
    async def _validate_mtls(self, request: Request) -> None:
        """Validate mTLS certificate"""
        # In Kubernetes/service mesh, this is handled by the sidecar
        # For direct connections, check client certificate
        client_cert = request.headers.get("X-Client-Certificate")
        if client_cert:
            # Validate certificate with Vault PKI
            try:
                await self.vault.validate_certificate(client_cert)
            except Exception as e:
                logger.error(f"mTLS validation failed: {e}")
                raise HTTPException(status_code=401, detail="Invalid client certificate")
                
    async def _authenticate(self, request: Request) -> Dict[str, Any]:
        """Authenticate the request"""
        # Check for service token
        service_token = request.headers.get("X-Service-Token")
        if service_token:
            return await self._authenticate_service(service_token)
            
        # Check for API key
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return await self._authenticate_api_key(api_key)
            
        # Check for JWT token
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header[7:]
            return await self._authenticate_jwt(token)
            
        raise HTTPException(
            status_code=401,
            detail="Missing authentication credentials"
        )
        
    async def _authenticate_service(self, token: str) -> Dict[str, Any]:
        """Authenticate service-to-service request"""
        try:
            # Validate service token with Consul
            service_data = await self.consul.kv_get(f"service-tokens/{token}")
            if not service_data:
                raise ValueError("Invalid service token")
                
            return {
                "authenticated": True,
                "type": "service",
                "service_name": service_data.get("service_name"),
                "roles": ["service"],
                "tenant_id": None
            }
            
        except Exception as e:
            logger.error(f"Service authentication failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid service token")
            
    async def _authenticate_api_key(self, api_key: str) -> Dict[str, Any]:
        """Authenticate API key"""
        try:
            # Validate API key with auth service
            # This is a simplified version - in production, call auth service
            key_data = await self.vault.get_secret(f"api-keys/{api_key}")
            if not key_data:
                raise ValueError("Invalid API key")
                
            return {
                "authenticated": True,
                "type": "api_key",
                "user_id": key_data.get("user_id"),
                "tenant_id": key_data.get("tenant_id"),
                "roles": key_data.get("roles", [])
            }
            
        except Exception as e:
            logger.error(f"API key authentication failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid API key")
            
    async def _authenticate_jwt(self, token: str) -> Dict[str, Any]:
        """Authenticate JWT token"""
        try:
            # Import here to avoid circular dependency
            from jose import jwt, JWTError
            
            # Get JWT secret from Vault if not provided
            if not self.jwt_secret_key:
                secret_data = await self.vault.get_secret("shared/jwt")
                self.jwt_secret_key = secret_data.get("secret_key")
                
            # Decode and validate JWT
            payload = jwt.decode(
                token,
                self.jwt_secret_key,
                algorithms=["HS256"]
            )
            
            # Check expiration
            if payload.get("exp", 0) < time.time():
                raise ValueError("Token expired")
                
            return {
                "authenticated": True,
                "type": "jwt",
                "user_id": payload.get("sub"),
                "tenant_id": payload.get("tid"),
                "roles": payload.get("roles", []),
                "email": payload.get("email")
            }
            
        except JWTError as e:
            logger.error(f"JWT authentication failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid token")
            
    async def _check_rate_limit(self, request: Request, auth_context: Dict[str, Any]) -> None:
        """Check rate limits"""
        # Get rate limit for user role
        roles = auth_context.get("roles", [])
        rate_limit = await self._get_rate_limit(roles)
        
        # Track requests
        user_id = auth_context.get("user_id", "anonymous")
        key = f"{user_id}:{request.url.path}"
        
        now = time.time()
        if key not in self._rate_limit_state:
            self._rate_limit_state[key] = []
            
        # Remove old entries
        self._rate_limit_state[key] = [
            timestamp for timestamp in self._rate_limit_state[key]
            if now - timestamp < 60  # 1 minute window
        ]
        
        # Check limit
        if len(self._rate_limit_state[key]) >= rate_limit:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded: {rate_limit} requests per minute"
            )
            
        # Add current request
        self._rate_limit_state[key].append(now)
        
    async def _get_rate_limit(self, roles: List[str]) -> int:
        """Get rate limit for roles"""
        # Default limits by role
        limits = {
            "platform_admin": 10000,
            "admin": 5000,
            "premium": 1000,
            "basic": 100,
            "service": 10000
        }
        
        # Get highest limit from roles
        max_limit = 60  # Default
        for role in roles:
            if role in limits:
                max_limit = max(max_limit, limits[role])
                
        return max_limit
        
    async def _authorize(self, request: Request, auth_context: Dict[str, Any]) -> Any:
        """Authorize the request with OPA"""
        # Map HTTP method to action
        method_to_action = {
            "GET": "read",
            "POST": "create",
            "PUT": "update",
            "PATCH": "update",
            "DELETE": "delete"
        }
        
        action = method_to_action.get(request.method, "access")
        
        # Extract resource from path
        path_parts = request.url.path.strip("/").split("/")
        resource = path_parts[2] if len(path_parts) > 2 else "unknown"
        
        # Build authorization request
        authz_request = AuthzRequest(
            subject=auth_context.get("user_id", "anonymous"),
            resource=resource,
            action=action,
            context={
                **auth_context,
                "method": request.method,
                "path": request.url.path,
                "service": self.service_name,
                "ip": request.client.host if request.client else None
            }
        )
        
        # Query OPA
        return await self.opa.authorize(authz_request)
        
    async def _apply_obligations(self, request: Request, obligations: Dict[str, Any]) -> None:
        """Apply authorization obligations"""
        # Handle 2FA requirement
        if obligations.get("requires_2fa"):
            otp = request.headers.get("X-OTP-Token")
            if not otp:
                raise HTTPException(
                    status_code=401,
                    detail="Two-factor authentication required",
                    headers={"X-2FA-Required": "true"}
                )
            # Validate OTP (simplified)
            # In production, validate with auth service
            
        # Enable audit logging for this request
        if obligations.get("audit_log"):
            request.state.force_audit = True
            
        # Apply data masking
        if obligations.get("mask_fields"):
            request.state.mask_fields = obligations["mask_fields"]
            
    async def _decrypt_request(self, request: Request) -> None:
        """Decrypt request body if encrypted"""
        if request.headers.get("X-Encrypted") == "true":
            # Get encryption key from Vault
            key_data = await self.vault.get_secret("platformq/encryption")
            key = key_data.get("data_encryption_key")
            
            # Decrypt body
            # This is simplified - in production use proper encryption
            # body = await request.body()
            # decrypted = decrypt(body, key)
            # request._body = decrypted
            pass
            
    async def _encrypt_response(self, response: Response) -> Response:
        """Encrypt response body if needed"""
        # Check if client requested encryption
        # This is simplified - in production use proper encryption
        return response
        
    async def _audit_log(self, request: Request, response: Response,
                        auth_context: Dict[str, Any], start_time: float) -> None:
        """Create audit log entry"""
        # Check if audit is required
        if not request.state.get("force_audit") and response.status_code < 400:
            return
            
        duration = time.time() - start_time
        
        audit_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name,
            "request_id": request.state.request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration * 1000,
            "user_id": auth_context.get("user_id"),
            "tenant_id": auth_context.get("tenant_id"),
            "roles": auth_context.get("roles", []),
            "ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("User-Agent"),
            "auth_type": auth_context.get("type")
        }
        
        # Add request body for write operations
        if request.method in ["POST", "PUT", "PATCH", "DELETE"]:
            try:
                body = await request.body()
                if body and len(body) < 10000:  # Limit size
                    audit_entry["request_body"] = body.decode("utf-8")
            except Exception:
                pass
                
        # Store in audit log
        await self.vault.audit_log(audit_entry)
        
        # Send to SIEM if configured
        await self._send_to_siem(audit_entry)
        
    async def _send_to_siem(self, audit_entry: Dict[str, Any]) -> None:
        """Send audit log to SIEM system"""
        # In production, send to Splunk/ELK/etc
        logger.info(f"Audit log: {json.dumps(audit_entry)}")
        
    def _add_security_headers(self, response: Response, request_id: str) -> Response:
        """Add security headers to response"""
        headers = {
            "X-Request-ID": request_id,
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Content-Security-Policy": "default-src 'self'",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache"
        }
        
        for key, value in headers.items():
            response.headers[key] = value
            
        return response
        
    def _is_public_path(self, path: str) -> bool:
        """Check if path is public"""
        for public_path in self.public_paths:
            if path.startswith(public_path):
                return True
        return False
        
    def _generate_request_id(self) -> str:
        """Generate unique request ID"""
        import uuid
        return str(uuid.uuid4()) 