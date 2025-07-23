"""Logging middleware

Handles request and response logging with metrics.
"""

import logging
import time
import uuid
from typing import Callable
from datetime import datetime

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.api.metrics import record_api_metric


logger = logging.getLogger(__name__)


async def logging_middleware(request: Request, call_next: Callable) -> Response:
    """Log requests and responses with timing"""
    # Generate request ID if not present
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    
    # Start timing
    start_time = time.time()
    
    # Log request
    logger.info(
        f"Request started: {request.method} {request.url.path}",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "query_params": str(request.query_params),
            "client": request.client.host if request.client else "unknown"
        }
    )
    
    # Add request ID to request state
    request.state.request_id = request_id
    
    try:
        # Process request
        response = await call_next(request)
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Add headers
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time"] = f"{duration:.3f}"
        
        # Log response
        logger.info(
            f"Request completed: {request.method} {request.url.path} - {response.status_code}",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration": duration,
                "client": request.client.host if request.client else "unknown"
            }
        )
        
        # Record metrics
        record_api_metric(
            method=request.method,
            endpoint=request.url.path,
            status_code=response.status_code,
            duration=duration
        )
        
        return response
        
    except Exception as e:
        # Calculate duration
        duration = time.time() - start_time
        
        # Log error
        logger.error(
            f"Request failed: {request.method} {request.url.path} - {type(e).__name__}",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "duration": duration,
                "error": str(e),
                "client": request.client.host if request.client else "unknown"
            }
        )
        
        # Record error metric
        record_api_metric(
            method=request.method,
            endpoint=request.url.path,
            status_code=500,
            duration=duration
        )
        
        # Re-raise the exception
        raise


class LoggingMiddleware(BaseHTTPMiddleware):
    """Logging middleware class with more features"""
    
    def __init__(self, app, log_headers: bool = False, log_body: bool = False):
        super().__init__(app)
        self.log_headers = log_headers
        self.log_body = log_body
        
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Dispatch the request with enhanced logging"""
        # Generate request ID
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        # Start timing
        start_time = time.time()
        
        # Build log context
        log_context = {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "query": str(request.query_params),
            "client_host": request.client.host if request.client else None,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Add headers if enabled
        if self.log_headers:
            log_context["headers"] = dict(request.headers)
            
        # Log request
        logger.info("Incoming request", extra=log_context)
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate duration
            duration = time.time() - start_time
            
            # Update log context
            log_context.update({
                "status_code": response.status_code,
                "duration_ms": round(duration * 1000, 2),
                "response_headers": dict(response.headers) if self.log_headers else None
            })
            
            # Add response headers
            response.headers["X-Request-ID"] = request_id
            response.headers["X-Response-Time-Ms"] = str(round(duration * 1000, 2))
            
            # Log response
            if response.status_code >= 400:
                logger.warning("Request completed with error", extra=log_context)
            else:
                logger.info("Request completed successfully", extra=log_context)
                
            return response
            
        except Exception as e:
            # Calculate duration
            duration = time.time() - start_time
            
            # Update log context
            log_context.update({
                "error_type": type(e).__name__,
                "error_message": str(e),
                "duration_ms": round(duration * 1000, 2)
            })
            
            # Log error
            logger.error("Request failed with exception", extra=log_context, exc_info=True)
            
            # Re-raise
            raise


def get_client_ip(request: Request) -> str:
    """Get the real client IP address"""
    # Check for proxy headers
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Get the first IP in the chain
        return forwarded_for.split(",")[0].strip()
        
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip
        
    # Fall back to direct client
    if request.client:
        return request.client.host
        
    return "unknown" 