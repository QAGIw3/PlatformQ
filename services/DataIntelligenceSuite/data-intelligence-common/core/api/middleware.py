"""
API middleware components for cross-cutting concerns.

Provides authentication, rate limiting, request tracking, and error handling.
"""

import time
import uuid
import json
from typing import Callable, Optional, Dict, Any, List, Set
from datetime import datetime, timedelta
import asyncio
from functools import wraps

from fastapi import Request, Response, HTTPException, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from .response_models import ErrorResponse
from ..caching import CacheManager
from ...monitoring import MetricsCollector, StructuredLogger


class RequestTrackingMiddleware(BaseHTTPMiddleware):
    """Middleware for tracking requests with correlation IDs."""
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.logger = StructuredLogger("api.request_tracking")
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Add request tracking headers and logging."""
        # Generate or extract request ID
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        
        # Add to request state
        request.state.request_id = request_id
        request.state.start_time = time.time()
        
        # Log request
        self.logger.info(
            "Request started",
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client_host=request.client.host if request.client else None
        )
        
        # Process request
        response = await call_next(request)
        
        # Add response headers
        response.headers["X-Request-ID"] = request_id
        
        # Calculate duration
        duration = time.time() - request.state.start_time
        
        # Log response
        self.logger.info(
            "Request completed",
            request_id=request_id,
            status_code=response.status_code,
            duration_ms=duration * 1000
        )
        
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Token bucket rate limiting middleware."""
    
    def __init__(
        self,
        app: ASGIApp,
        cache_manager: CacheManager,
        default_limit: int = 100,
        window_seconds: int = 60,
        burst_size: int = 10
    ):
        super().__init__(app)
        self.cache = cache_manager
        self.default_limit = default_limit
        self.window_seconds = window_seconds
        self.burst_size = burst_size
        self.logger = StructuredLogger("api.rate_limit")
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Apply rate limiting."""
        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/metrics"]:
            return await call_next(request)
        
        # Get client identifier
        client_id = self._get_client_id(request)
        
        # Check rate limit
        allowed, remaining, reset_time = await self._check_rate_limit(client_id)
        
        if not allowed:
            self.logger.warning(
                "Rate limit exceeded",
                client_id=client_id,
                path=request.url.path
            )
            
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content=ErrorResponse(
                    error="rate_limit_exceeded",
                    message="Too many requests. Please try again later.",
                    details={"retry_after": reset_time}
                ).dict(),
                headers={
                    "X-RateLimit-Limit": str(self.default_limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(reset_time),
                    "Retry-After": str(reset_time)
                }
            )
        
        # Process request
        response = await call_next(request)
        
        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(self.default_limit)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_time)
        
        return response
    
    def _get_client_id(self, request: Request) -> str:
        """Extract client identifier from request."""
        # Try API key first
        api_key = request.headers.get("X-API-Key")
        if api_key:
            return f"api_key:{api_key}"
        
        # Try authenticated user
        if hasattr(request.state, "user_id"):
            return f"user:{request.state.user_id}"
        
        # Fall back to IP
        if request.client:
            return f"ip:{request.client.host}"
        
        return "anonymous"
    
    async def _check_rate_limit(self, client_id: str) -> tuple[bool, int, int]:
        """Check if request is allowed under rate limit."""
        cache_key = f"rate_limit:{client_id}"
        current_time = int(time.time())
        window_start = current_time - self.window_seconds
        
        # Get current request count
        request_data = await self.cache.get(cache_key) or {
            "requests": [],
            "tokens": self.burst_size
        }
        
        # Remove old requests outside window
        request_data["requests"] = [
            ts for ts in request_data["requests"]
            if ts > window_start
        ]
        
        # Check if under limit
        request_count = len(request_data["requests"])
        
        if request_count >= self.default_limit:
            # Check token bucket for burst
            if request_data["tokens"] > 0:
                request_data["tokens"] -= 1
                allowed = True
            else:
                allowed = False
        else:
            allowed = True
            # Refill tokens
            elapsed = current_time - request_data.get("last_refill", current_time)
            tokens_to_add = int(elapsed * self.burst_size / self.window_seconds)
            request_data["tokens"] = min(
                self.burst_size,
                request_data["tokens"] + tokens_to_add
            )
            request_data["last_refill"] = current_time
        
        if allowed:
            request_data["requests"].append(current_time)
        
        # Save updated data
        await self.cache.set(
            cache_key,
            request_data,
            ttl=self.window_seconds * 2
        )
        
        # Calculate remaining and reset time
        remaining = max(0, self.default_limit - len(request_data["requests"]))
        reset_time = window_start + self.window_seconds
        
        return allowed, remaining, reset_time


class AuthenticationMiddleware(BaseHTTPMiddleware):
    """JWT-based authentication middleware."""
    
    def __init__(
        self,
        app: ASGIApp,
        public_paths: Optional[Set[str]] = None,
        jwt_secret: Optional[str] = None,
        jwt_algorithm: str = "HS256"
    ):
        super().__init__(app)
        self.public_paths = public_paths or {"/health", "/docs", "/openapi.json"}
        self.jwt_secret = jwt_secret or "your-secret-key"
        self.jwt_algorithm = jwt_algorithm
        self.logger = StructuredLogger("api.auth")
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Verify authentication for protected endpoints."""
        # Skip auth for public paths
        if request.url.path in self.public_paths:
            return await call_next(request)
        
        # Extract token
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=ErrorResponse(
                    error="unauthorized",
                    message="Missing or invalid authorization header"
                ).dict()
            )
        
        token = auth_header.split(" ", 1)[1]
        
        # Verify token
        try:
            import jwt
            payload = jwt.decode(
                token,
                self.jwt_secret,
                algorithms=[self.jwt_algorithm]
            )
            
            # Add user info to request state
            request.state.user_id = payload.get("sub")
            request.state.user_roles = payload.get("roles", [])
            request.state.token_exp = payload.get("exp")
            
        except jwt.ExpiredSignatureError:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=ErrorResponse(
                    error="token_expired",
                    message="Authentication token has expired"
                ).dict()
            )
        except jwt.InvalidTokenError:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=ErrorResponse(
                    error="invalid_token",
                    message="Invalid authentication token"
                ).dict()
            )
        
        # Process request
        return await call_next(request)


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Global error handling middleware."""
    
    def __init__(self, app: ASGIApp, debug: bool = False):
        super().__init__(app)
        self.debug = debug
        self.logger = StructuredLogger("api.error_handler")
        self.metrics = MetricsCollector()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Handle exceptions and format error responses."""
        try:
            response = await call_next(request)
            return response
            
        except HTTPException as exc:
            # Let FastAPI handle HTTP exceptions
            raise
            
        except ValueError as exc:
            self.logger.warning(
                "Validation error",
                error=str(exc),
                path=request.url.path
            )
            
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content=ErrorResponse(
                    error="validation_error",
                    message=str(exc)
                ).dict()
            )
            
        except Exception as exc:
            # Log unexpected errors
            request_id = getattr(request.state, "request_id", "unknown")
            
            self.logger.error(
                "Unhandled exception",
                error=str(exc),
                error_type=type(exc).__name__,
                path=request.url.path,
                request_id=request_id,
                exc_info=True
            )
            
            # Track error metrics
            self.metrics.increment(
                "api_errors_total",
                labels={
                    "path": request.url.path,
                    "error_type": type(exc).__name__
                }
            )
            
            # Return error response
            error_details = {"request_id": request_id}
            if self.debug:
                error_details["exception"] = str(exc)
                error_details["type"] = type(exc).__name__
            
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content=ErrorResponse(
                    error="internal_server_error",
                    message="An unexpected error occurred",
                    details=error_details
                ).dict()
            )


class CompressionMiddleware(BaseHTTPMiddleware):
    """Response compression middleware."""
    
    def __init__(
        self,
        app: ASGIApp,
        minimum_size: int = 1000,
        compression_level: int = 6
    ):
        super().__init__(app)
        self.minimum_size = minimum_size
        self.compression_level = compression_level
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Compress responses if client supports it."""
        # Check if client accepts compression
        accept_encoding = request.headers.get("Accept-Encoding", "")
        
        # Process request
        response = await call_next(request)
        
        # Skip compression for small responses or streaming
        if (
            response.status_code == 204 or
            response.headers.get("Content-Length", "0") == "0" or
            "Content-Encoding" in response.headers or
            response.status_code >= 400
        ):
            return response
        
        # Apply gzip compression if supported
        if "gzip" in accept_encoding:
            import gzip
            
            # Read response body
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            
            # Check size threshold
            if len(body) >= self.minimum_size:
                # Compress body
                compressed = gzip.compress(body, compresslevel=self.compression_level)
                
                # Update headers
                headers = dict(response.headers)
                headers["Content-Encoding"] = "gzip"
                headers["Content-Length"] = str(len(compressed))
                
                # Return compressed response
                return Response(
                    content=compressed,
                    status_code=response.status_code,
                    headers=headers,
                    media_type=response.media_type
                )
        
        return response


class CORSMiddleware(BaseHTTPMiddleware):
    """CORS handling middleware with configurable origins."""
    
    def __init__(
        self,
        app: ASGIApp,
        allowed_origins: List[str] = ["*"],
        allowed_methods: List[str] = ["*"],
        allowed_headers: List[str] = ["*"],
        expose_headers: List[str] = [],
        allow_credentials: bool = False,
        max_age: int = 86400
    ):
        super().__init__(app)
        self.allowed_origins = allowed_origins
        self.allowed_methods = allowed_methods
        self.allowed_headers = allowed_headers
        self.expose_headers = expose_headers
        self.allow_credentials = allow_credentials
        self.max_age = max_age
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Handle CORS headers."""
        origin = request.headers.get("Origin")
        
        # Handle preflight requests
        if request.method == "OPTIONS":
            response = Response(status_code=200)
        else:
            response = await call_next(request)
        
        # Add CORS headers
        if origin and self._is_allowed_origin(origin):
            response.headers["Access-Control-Allow-Origin"] = origin
            
            if self.allow_credentials:
                response.headers["Access-Control-Allow-Credentials"] = "true"
            
            if request.method == "OPTIONS":
                response.headers["Access-Control-Allow-Methods"] = ", ".join(
                    self.allowed_methods
                )
                response.headers["Access-Control-Allow-Headers"] = ", ".join(
                    self.allowed_headers
                )
                response.headers["Access-Control-Max-Age"] = str(self.max_age)
            
            if self.expose_headers:
                response.headers["Access-Control-Expose-Headers"] = ", ".join(
                    self.expose_headers
                )
        
        return response
    
    def _is_allowed_origin(self, origin: str) -> bool:
        """Check if origin is allowed."""
        if "*" in self.allowed_origins:
            return True
        
        return origin in self.allowed_origins


# Decorator-based middleware
def require_roles(*roles: str) -> Callable:
    """Decorator to require specific user roles."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            user_roles = getattr(request.state, "user_roles", [])
            
            if not any(role in user_roles for role in roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Insufficient permissions"
                )
            
            return await func(request, *args, **kwargs)
        return wrapper
    return decorator


def rate_limit(
    requests: int = 10,
    window: int = 60,
    key_func: Optional[Callable] = None
) -> Callable:
    """Decorator for custom rate limiting."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            # Get rate limit key
            if key_func:
                key = key_func(request)
            else:
                key = request.client.host if request.client else "anonymous"
            
            # Apply rate limiting logic
            # (Implementation would use cache manager)
            
            return await func(request, *args, **kwargs)
        return wrapper
    return decorator


def validate_request(model: type) -> Callable:
    """Decorator to validate request body against Pydantic model."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            try:
                body = await request.json()
                validated = model(**body)
                request.state.validated_body = validated
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid request body: {str(e)}"
                )
            
            return await func(request, *args, **kwargs)
        return wrapper
    return decorator 