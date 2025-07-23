"""Logging middleware

Handles request and response logging with metrics.
"""

import logging
import time
import uuid
from typing import Callable
from datetime import datetime

from fastapi import Request, Response

from app.api.metrics import record_job_submission


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
        
        # Re-raise the exception
        raise 