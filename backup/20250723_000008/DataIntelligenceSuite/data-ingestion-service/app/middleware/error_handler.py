"""
Error handler middleware
"""

import logging
import traceback
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


async def error_handler_middleware(request: Request, call_next: Callable) -> Response:
    """Global error handler middleware"""
    try:
        response = await call_next(request)
        return response
        
    except ValueError as e:
        # Business logic errors
        logger.warning(f"Business error: {e}")
        return JSONResponse(
            status_code=400,
            content={
                "error": "Bad Request",
                "message": str(e),
                "path": str(request.url.path)
            }
        )
        
    except PermissionError as e:
        # Authorization errors
        logger.warning(f"Permission error: {e}")
        return JSONResponse(
            status_code=403,
            content={
                "error": "Forbidden",
                "message": "You don't have permission to access this resource",
                "path": str(request.url.path)
            }
        )
        
    except Exception as e:
        # Unexpected errors
        logger.error(f"Unexpected error: {e}")
        logger.error(traceback.format_exc())
        
        # Don't expose internal errors in production
        if request.app.debug:
            message = str(e)
        else:
            message = "An internal error occurred"
            
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal Server Error",
                "message": message,
                "path": str(request.url.path)
            }
        ) 