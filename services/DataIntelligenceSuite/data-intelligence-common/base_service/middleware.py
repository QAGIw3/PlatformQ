"""Common middleware for DataIntelligenceSuite services."""

from typing import Callable, Optional, Dict, Tuple
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Counter, Histogram, Gauge
import time
import logging
import traceback
import uuid
from datetime import datetime, timedelta
from ..core.patterns.resilience import CircuitBreakerPattern, CircuitBreakerConfig

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

circuit_breaker_state = Gauge(
    'circuit_breaker_state', 
    'Circuit breaker state', 
    ['service', 'breaker']
)


class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, requests: int, window: timedelta):
        self.requests = requests
        self.window = window
        self.requests_per_second = requests / window.total_seconds()
        self._buckets: Dict[str, Tuple[float, float]] = {}
        
    async def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        now = time.time()
        
        if key not in self._buckets:
            self._buckets[key] = (self.requests - 1, now)
            return True
            
        tokens, last_update = self._buckets[key]
        
        # Refill tokens based on time elapsed
        elapsed = now - last_update
        tokens = min(self.requests, tokens + elapsed * self.requests_per_second)
        
        if tokens >= 1:
            self._buckets[key] = (tokens - 1, now)
            return True
            
        self._buckets[key] = (tokens, now)
        return False


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware."""
    
    def __init__(self, app, rate_limiter: RateLimiter, service_name: str):
        super().__init__(app)
        self.rate_limiter = rate_limiter
        self.service_name = service_name
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        client_id = request.client.host if request.client else "unknown"
        
        if not await self.rate_limiter.is_allowed(client_id):
            request_count.labels(
                service=self.service_name,
                method=request.method,
                endpoint=str(request.url.path),
                status=429
            ).inc()
            
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded"}
            )
            
        return await call_next(request)


class CircuitBreakerManager:
    """Manages circuit breakers for external service calls."""
    
    def __init__(self, service_name: str, 
                 fail_max: int = 5, 
                 reset_timeout: int = 60,
                 expected_exception: type = Exception):
        self.service_name = service_name
        self.fail_max = fail_max
        self.reset_timeout = reset_timeout
        self.expected_exception = expected_exception
        self._circuit_breakers: Dict[str, CircuitBreakerPattern] = {}
        
    def get_circuit_breaker(self, name: str) -> CircuitBreakerPattern:
        """Get or create circuit breaker"""
        if name not in self._circuit_breakers:
            config = CircuitBreakerConfig(
                failure_threshold=self.fail_max,
                recovery_timeout=self.reset_timeout,
                expected_exceptions=[self.expected_exception]
            )
            self._circuit_breakers[name] = CircuitBreakerPattern(config)
            
            # Set initial state metric
            circuit_breaker_state.labels(
                service=self.service_name,
                breaker=name
            ).set(0)  # Start in closed state
            
        return self._circuit_breakers[name]


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


def setup_common_middleware(app: FastAPI, 
                          service_name: str,
                          enable_rate_limiting: bool = True,
                          rate_limit_requests: int = 100,
                          rate_limit_window: timedelta = timedelta(minutes=1)):
    """Set up common middleware for DataIntelligenceSuite services."""
    
    # Add middleware in order (executed in reverse order)
    app.add_middleware(ErrorHandlingMiddleware, service_name=service_name)
    app.add_middleware(MetricsMiddleware, service_name=service_name)
    app.add_middleware(RequestTracingMiddleware, service_name=service_name)
    
    # Add rate limiting if enabled
    if enable_rate_limiting:
        rate_limiter = RateLimiter(rate_limit_requests, rate_limit_window)
        app.add_middleware(RateLimitMiddleware, 
                          rate_limiter=rate_limiter, 
                          service_name=service_name)
    
    logger.info(f"Common middleware configured for {service_name}") 