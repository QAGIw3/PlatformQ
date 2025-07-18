"""
Error Handling Framework for PlatformQ

Provides standardized error handling, response formats, and middleware
for consistent API error responses across all services.
"""

import logging
import traceback
import uuid
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


class ErrorCode:
    """Standard error codes across the platform"""
    # Client errors
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTHENTICATION_ERROR = "AUTHENTICATION_ERROR"
    AUTHORIZATION_ERROR = "AUTHORIZATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    
    # Server errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    DEPENDENCY_ERROR = "DEPENDENCY_ERROR"
    
    # Business logic errors
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
    INSUFFICIENT_RESOURCES = "INSUFFICIENT_RESOURCES"
    INVALID_STATE = "INVALID_STATE"


class ErrorDetail(BaseModel):
    """Detailed error information"""
    field: Optional[str] = Field(None, description="Field that caused the error")
    message: str = Field(..., description="Human-readable error message")
    code: Optional[str] = Field(None, description="Machine-readable error code")


class ErrorResponse(BaseModel):
    """Standard error response format"""
    error_id: str = Field(..., description="Unique error ID for tracking")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status_code: int = Field(..., description="HTTP status code")
    error_code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[List[ErrorDetail]] = Field(None, description="Additional error details")
    path: Optional[str] = Field(None, description="Request path that caused the error")
    service: Optional[str] = Field(None, description="Service that generated the error")


class ServiceError(Exception):
    """Base exception for service-specific errors"""
    
    def __init__(self,
                 message: str,
                 error_code: str = ErrorCode.INTERNAL_ERROR,
                 status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
                 details: Optional[List[ErrorDetail]] = None):
        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.details = details or []
        super().__init__(message)


class ValidationError(ServiceError):
    """Validation error"""
    def __init__(self, message: str, details: Optional[List[ErrorDetail]] = None):
        super().__init__(
            message=message,
            error_code=ErrorCode.VALIDATION_ERROR,
            status_code=status.HTTP_400_BAD_REQUEST,
            details=details
        )


class NotFoundError(ServiceError):
    """Resource not found error"""
    def __init__(self, resource: str, identifier: Any):
        super().__init__(
            message=f"{resource} with identifier '{identifier}' not found",
            error_code=ErrorCode.NOT_FOUND,
            status_code=status.HTTP_404_NOT_FOUND
        )


class AuthenticationError(ServiceError):
    """Authentication error"""
    def __init__(self, message: str = "Authentication required"):
        super().__init__(
            message=message,
            error_code=ErrorCode.AUTHENTICATION_ERROR,
            status_code=status.HTTP_401_UNAUTHORIZED
        )


class AuthorizationError(ServiceError):
    """Authorization error"""
    def __init__(self, message: str = "Insufficient permissions"):
        super().__init__(
            message=message,
            error_code=ErrorCode.AUTHORIZATION_ERROR,
            status_code=status.HTTP_403_FORBIDDEN
        )


class ConflictError(ServiceError):
    """Resource conflict error"""
    def __init__(self, message: str):
        super().__init__(
            message=message,
            error_code=ErrorCode.CONFLICT,
            status_code=status.HTTP_409_CONFLICT
        )


class RateLimitError(ServiceError):
    """Rate limit exceeded error"""
    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(
            message=message,
            error_code=ErrorCode.RATE_LIMIT_EXCEEDED,
            status_code=status.HTTP_429_TOO_MANY_REQUESTS
        )


class DependencyError(ServiceError):
    """External dependency error"""
    def __init__(self, service: str, message: str):
        super().__init__(
            message=f"Dependency error from {service}: {message}",
            error_code=ErrorCode.DEPENDENCY_ERROR,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE
        )


class BusinessRuleError(ServiceError):
    """Business rule violation"""
    def __init__(self, message: str):
        super().__init__(
            message=message,
            error_code=ErrorCode.BUSINESS_RULE_VIOLATION,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY
        )


def create_error_response(
    request: Request,
    status_code: int,
    error_code: str,
    message: str,
    details: Optional[List[ErrorDetail]] = None,
    service_name: Optional[str] = None
) -> ErrorResponse:
    """Create a standard error response"""
    return ErrorResponse(
        error_id=str(uuid.uuid4()),
        status_code=status_code,
        error_code=error_code,
        message=message,
        details=details,
        path=str(request.url.path),
        service=service_name or "unknown"
    )


def add_error_handlers(app: FastAPI, service_name: str):
    """Add standard error handlers to a FastAPI app"""
    
    @app.exception_handler(ServiceError)
    async def service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
        """Handle service-specific errors"""
        error_response = create_error_response(
            request=request,
            status_code=exc.status_code,
            error_code=exc.error_code,
            message=exc.message,
            details=exc.details,
            service_name=service_name
        )
        
        logger.warning(
            f"Service error: {exc.error_code} - {exc.message}",
            extra={
                "error_id": error_response.error_id,
                "status_code": exc.status_code,
                "path": request.url.path
            }
        )
        
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response.dict()
        )
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        """Handle FastAPI HTTP exceptions"""
        error_code = ErrorCode.INTERNAL_ERROR
        
        if exc.status_code == 404:
            error_code = ErrorCode.NOT_FOUND
        elif exc.status_code == 401:
            error_code = ErrorCode.AUTHENTICATION_ERROR
        elif exc.status_code == 403:
            error_code = ErrorCode.AUTHORIZATION_ERROR
        elif exc.status_code == 409:
            error_code = ErrorCode.CONFLICT
        elif exc.status_code == 429:
            error_code = ErrorCode.RATE_LIMIT_EXCEEDED
        
        error_response = create_error_response(
            request=request,
            status_code=exc.status_code,
            error_code=error_code,
            message=exc.detail,
            service_name=service_name
        )
        
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response.dict()
        )
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        """Handle request validation errors"""
        details = []
        for error in exc.errors():
            field = ".".join(str(loc) for loc in error["loc"])
            details.append(ErrorDetail(
                field=field,
                message=error["msg"],
                code=error["type"]
            ))
        
        error_response = create_error_response(
            request=request,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            error_code=ErrorCode.VALIDATION_ERROR,
            message="Request validation failed",
            details=details,
            service_name=service_name
        )
        
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=error_response.dict()
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        """Handle unexpected errors"""
        error_id = str(uuid.uuid4())
        
        logger.error(
            f"Unexpected error: {str(exc)}",
            extra={
                "error_id": error_id,
                "path": request.url.path,
                "traceback": traceback.format_exc()
            }
        )
        
        error_response = ErrorResponse(
            error_id=error_id,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_code=ErrorCode.INTERNAL_ERROR,
            message="An unexpected error occurred",
            path=str(request.url.path),
            service=service_name
        )
        
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_response.dict()
        )


class ErrorMiddleware:
    """Middleware for error handling and correlation ID"""
    
    def __init__(self, app: FastAPI, service_name: str):
        self.app = app
        self.service_name = service_name
        
    async def __call__(self, request: Request, call_next):
        # Add correlation ID to request
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        request.state.correlation_id = correlation_id
        
        try:
            response = await call_next(request)
            response.headers["X-Correlation-ID"] = correlation_id
            return response
            
        except Exception as e:
            # Log the error with correlation ID
            logger.error(
                f"Request failed: {str(e)}",
                extra={
                    "correlation_id": correlation_id,
                    "path": request.url.path,
                    "method": request.method
                }
            )
            raise 