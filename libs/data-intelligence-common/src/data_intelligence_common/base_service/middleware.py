"""Common middleware for DataIntelligenceSuite services."""

from typing import Callable, Optional
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Histogram
import time
import logging
import traceback
import uuid
from datetime import datetime

logger = logging.getLogger(__name__)


# Metrics
request_count = Counter(
    "data_intelligence_http_requests_total",
    "Total HTTP requests",
    ["service", "method", "endpoint", "status"]
)

request_duration = Histogram(
    "data_intelligence_http_request_duration_seconds",
    "HTTP request duration",
    ["service", "method", "endpoint"]
)

active_requests = Counter(
    "data_intelligence_http_requests_active",
    "Active HTTP requests",
    ["service"]
)


class RequestTracingMiddleware(BaseHTTPMiddleware):
    """Middleware for request tracing and correlation."""
    
    def __init__(self, app, service_name: str):
        super().__init__(app)
        self.service_name = service_name
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get or generate request ID
        request_id = request.headers.get("X-Request-ID")
        if not request_id:
            request_id = str(uuid.uuid4())
            
        # Get trace ID for distributed tracing
        trace_id = request.headers.get("X-Trace-ID")
        if not trace_id:
            trace_id = request_id
            
        # Store in request state
        request.state.request_id = request_id
        request.state.trace_id = trace_id
        request.state.start_time = time.time()
        
        # Log request
        logger.info(
            f"Request started",
            extra={
                "request_id": request_id,
                "trace_id": trace_id,
                "method": request.method,
                "path": request.url.path,
                "service": self.service_name
            }
        )
        
        # Process request
        response = await call_next(request)
        
        # Add headers to response
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Trace-ID"] = trace_id
        
        # Log response
        duration = time.time() - request.state.start_time
        logger.info(
            f"Request completed",
            extra={
                "request_id": request_id,
                "trace_id": trace_id,
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_seconds": duration,
                "service": self.service_name
            }
        )
        
        return response


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for collecting metrics."""
    
    def __init__(self, app, service_name: str):
        super().__init__(app)
        self.service_name = service_name
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Track active requests
        active_requests.labels(service=self.service_name).inc()
        
        # Start timer
        start_time = time.time()
        
        try:
            # Process request
            response = await call_next(request)
            
            # Record metrics
            duration = time.time() - start_time
            
            request_count.labels(
                service=self.service_name,
                method=request.method,
                endpoint=request.url.path,
                status=response.status_code
            ).inc()
            
            request_duration.labels(
                service=self.service_name,
                method=request.method,
                endpoint=request.url.path
            ).observe(duration)
            
            return response
            
        finally:
            # Decrement active requests
            active_requests.labels(service=self.service_name).dec()


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Middleware for handling errors consistently."""
    
    def __init__(self, app, service_name: str):
        super().__init__(app)
        self.service_name = service_name
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        try:
            response = await call_next(request)
            return response
            
        except Exception as exc:
            # Log error
            request_id = getattr(request.state, "request_id", "unknown")
            logger.error(
                f"Unhandled exception",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "service": self.service_name,
                    "error": str(exc),
                    "traceback": traceback.format_exc()
                }
            )
            
            # Return error response
            return JSONResponse(
                status_code=500,
                content={
                    "error": "internal_error",
                    "message": "An internal error occurred",
                    "request_id": request_id,
                    "service": self.service_name,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )


class RateLimitingMiddleware(BaseHTTPMiddleware):
    """Middleware for rate limiting."""
    
    def __init__(self, app, service_name: str, max_requests: int = 100, window_seconds: int = 60):
        super().__init__(app)
        self.service_name = service_name
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_counts = {}  # Simple in-memory store
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Get client identifier (IP or user ID)
        client_id = request.client.host if request.client else "unknown"
        
        # Check rate limit
        current_time = time.time()
        window_start = current_time - self.window_seconds
        
        # Clean old entries
        if client_id in self.request_counts:
            self.request_counts[client_id] = [
                timestamp for timestamp in self.request_counts[client_id]
                if timestamp > window_start
            ]
        else:
            self.request_counts[client_id] = []
            
        # Check if limit exceeded
        if len(self.request_counts[client_id]) >= self.max_requests:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limit_exceeded",
                    "message": f"Rate limit exceeded. Max {self.max_requests} requests per {self.window_seconds} seconds",
                    "service": self.service_name
                },
                headers={
                    "X-RateLimit-Limit": str(self.max_requests),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(int(window_start + self.window_seconds))
                }
            )
            
        # Record request
        self.request_counts[client_id].append(current_time)
        
        # Process request
        response = await call_next(request)
        
        # Add rate limit headers
        remaining = self.max_requests - len(self.request_counts[client_id])
        response.headers["X-RateLimit-Limit"] = str(self.max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(window_start + self.window_seconds))
        
        return response


def setup_common_middleware(app: FastAPI, service_name: str):
    """Set up common middleware for DataIntelligenceSuite services."""
    
    # Add middleware in order (executed in reverse order)
    app.add_middleware(ErrorHandlingMiddleware, service_name=service_name)
    app.add_middleware(MetricsMiddleware, service_name=service_name)
    app.add_middleware(RequestTracingMiddleware, service_name=service_name)
    
    logger.info(f"Common middleware configured for {service_name}") 