"""Error handler middleware

Handles exceptions and formats error responses.
"""

import logging
import traceback
from typing import Callable
from datetime import datetime

from fastapi import Request, Response
from fastapi.responses import JSONResponse


logger = logging.getLogger(__name__)


async def error_handler_middleware(request: Request, call_next: Callable) -> Response:
    """Handle exceptions and format error responses"""
    try:
        response = await call_next(request)
        return response
        
    except ValueError as e:
        logger.warning(f"Validation error: {e}")
        return JSONResponse(
            status_code=400,
            content={
                "error": "Bad Request",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "path": str(request.url.path)
            }
        )
        
    except PermissionError as e:
        logger.warning(f"Permission denied: {e}")
        return JSONResponse(
            status_code=403,
            content={
                "error": "Forbidden",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "path": str(request.url.path)
            }
        )
        
    except FileNotFoundError as e:
        logger.warning(f"Resource not found: {e}")
        return JSONResponse(
            status_code=404,
            content={
                "error": "Not Found",
                "message": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "path": str(request.url.path)
            }
        )
        
    except TimeoutError as e:
        logger.error(f"Request timeout: {e}")
        return JSONResponse(
            status_code=408,
            content={
                "error": "Request Timeout",
                "message": "The request took too long to process",
                "timestamp": datetime.utcnow().isoformat(),
                "path": str(request.url.path)
            }
        )
        
    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "error": "Service Unavailable",
                "message": "Unable to connect to required service",
                "timestamp": datetime.utcnow().isoformat(),
                "path": str(request.url.path)
            }
        )
        
    except Exception as e:
        # Log the full exception with traceback
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        
        # In development, include more details
        from app.core.config import settings
        if settings.debug:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal Server Error",
                    "message": str(e),
                    "type": type(e).__name__,
                    "traceback": traceback.format_exc(),
                    "timestamp": datetime.utcnow().isoformat(),
                    "path": str(request.url.path)
                }
            )
        else:
            # In production, don't expose internal details
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal Server Error",
                    "message": "An unexpected error occurred",
                    "timestamp": datetime.utcnow().isoformat(),
                    "path": str(request.url.path),
                    "request_id": request.headers.get("X-Request-ID", "unknown")
                }
            ) 