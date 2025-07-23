"""
Standardized API Response Models for DataIntelligenceSuite
"""

from typing import Any, Dict, Optional, List, Generic, TypeVar
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum

T = TypeVar('T')


class ResponseStatus(str, Enum):
    """API response status"""
    SUCCESS = "success"
    ERROR = "error"
    WARNING = "warning"
    PARTIAL = "partial"


class APIResponse(BaseModel, Generic[T]):
    """
    Standard API response wrapper.
    
    Provides consistent response format across all services.
    """
    status: ResponseStatus = Field(default=ResponseStatus.SUCCESS, description="Response status")
    data: Optional[T] = Field(default=None, description="Response data")
    message: Optional[str] = Field(default=None, description="Human-readable message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp")
    request_id: Optional[str] = Field(default=None, description="Request tracking ID")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
    @classmethod
    def success(cls, data: T, message: Optional[str] = None, **kwargs) -> 'APIResponse[T]':
        """Create success response"""
        return cls(
            status=ResponseStatus.SUCCESS,
            data=data,
            message=message or "Request completed successfully",
            **kwargs
        )
        
    @classmethod
    def error(cls, message: str, data: Optional[T] = None, **kwargs) -> 'APIResponse[T]':
        """Create error response"""
        return cls(
            status=ResponseStatus.ERROR,
            data=data,
            message=message,
            **kwargs
        )
        
    @classmethod
    def warning(cls, data: T, message: str, **kwargs) -> 'APIResponse[T]':
        """Create warning response"""
        return cls(
            status=ResponseStatus.WARNING,
            data=data,
            message=message,
            **kwargs
        )


class PaginationMeta(BaseModel):
    """Pagination metadata"""
    page: int = Field(ge=1, description="Current page number")
    page_size: int = Field(ge=1, le=1000, description="Items per page")
    total_items: int = Field(ge=0, description="Total number of items")
    total_pages: int = Field(ge=0, description="Total number of pages")
    has_next: bool = Field(description="Whether there is a next page")
    has_previous: bool = Field(description="Whether there is a previous page")
    
    @classmethod
    def calculate(cls, page: int, page_size: int, total_items: int) -> 'PaginationMeta':
        """Calculate pagination metadata"""
        total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 0
        
        return cls(
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )


class PaginatedResponse(APIResponse[List[T]], Generic[T]):
    """
    Paginated API response.
    
    Extends standard response with pagination metadata.
    """
    pagination: Optional[PaginationMeta] = Field(default=None, description="Pagination metadata")
    
    @classmethod
    def paginate(
        cls,
        items: List[T],
        page: int,
        page_size: int,
        total_items: int,
        message: Optional[str] = None,
        **kwargs
    ) -> 'PaginatedResponse[T]':
        """Create paginated response"""
        pagination = PaginationMeta.calculate(page, page_size, total_items)
        
        return cls(
            status=ResponseStatus.SUCCESS,
            data=items,
            message=message or f"Retrieved {len(items)} items",
            pagination=pagination,
            **kwargs
        )


class ErrorDetail(BaseModel):
    """Error detail information"""
    code: str = Field(description="Error code")
    field: Optional[str] = Field(default=None, description="Field name if field-specific")
    message: str = Field(description="Error message")
    context: Dict[str, Any] = Field(default_factory=dict, description="Additional context")


class ErrorResponse(BaseModel):
    """
    Standard error response.
    
    Provides detailed error information for debugging.
    """
    status: ResponseStatus = Field(default=ResponseStatus.ERROR)
    error: str = Field(description="Error type/category")
    message: str = Field(description="Human-readable error message")
    details: List[ErrorDetail] = Field(default_factory=list, description="Detailed errors")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: Optional[str] = Field(default=None)
    trace_id: Optional[str] = Field(default=None, description="Distributed trace ID")
    
    # HTTP status code hint
    status_code: int = Field(default=500, description="Suggested HTTP status code")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
    @classmethod
    def validation_error(
        cls,
        errors: List[Dict[str, Any]],
        message: str = "Validation failed",
        **kwargs
    ) -> 'ErrorResponse':
        """Create validation error response"""
        details = [
            ErrorDetail(
                code="validation_error",
                field=error.get("loc", [])[-1] if error.get("loc") else None,
                message=error.get("msg", "Invalid value"),
                context={"type": error.get("type", "unknown")}
            )
            for error in errors
        ]
        
        return cls(
            error="ValidationError",
            message=message,
            details=details,
            status_code=422,
            **kwargs
        )
        
    @classmethod
    def not_found(cls, resource: str, identifier: Any, **kwargs) -> 'ErrorResponse':
        """Create not found error response"""
        return cls(
            error="NotFoundError",
            message=f"{resource} not found",
            details=[
                ErrorDetail(
                    code="not_found",
                    message=f"{resource} with identifier '{identifier}' not found"
                )
            ],
            status_code=404,
            **kwargs
        )
        
    @classmethod
    def unauthorized(cls, message: str = "Unauthorized", **kwargs) -> 'ErrorResponse':
        """Create unauthorized error response"""
        return cls(
            error="UnauthorizedError",
            message=message,
            status_code=401,
            **kwargs
        )
        
    @classmethod
    def forbidden(cls, message: str = "Forbidden", **kwargs) -> 'ErrorResponse':
        """Create forbidden error response"""
        return cls(
            error="ForbiddenError",
            message=message,
            status_code=403,
            **kwargs
        )
        
    @classmethod
    def internal_error(cls, message: str = "Internal server error", **kwargs) -> 'ErrorResponse':
        """Create internal error response"""
        return cls(
            error="InternalServerError",
            message=message,
            status_code=500,
            **kwargs
        )


class HealthStatus(str, Enum):
    """Health check status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class ComponentHealth(BaseModel):
    """Component health status"""
    name: str = Field(description="Component name")
    status: HealthStatus = Field(description="Component status")
    message: Optional[str] = Field(default=None, description="Status message")
    latency_ms: Optional[float] = Field(default=None, description="Check latency in milliseconds")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class HealthResponse(BaseModel):
    """
    Standard health check response.
    
    Provides service and component health information.
    """
    status: HealthStatus = Field(description="Overall health status")
    service: str = Field(description="Service name")
    version: str = Field(description="Service version")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    uptime_seconds: Optional[float] = Field(default=None, description="Service uptime in seconds")
    
    # Component health
    components: List[ComponentHealth] = Field(default_factory=list, description="Component health statuses")
    
    # Additional checks
    checks: Dict[str, Any] = Field(default_factory=dict, description="Additional health checks")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        
    @property
    def is_healthy(self) -> bool:
        """Check if service is healthy"""
        return self.status == HealthStatus.HEALTHY
        
    @classmethod
    def create(
        cls,
        service: str,
        version: str,
        components: List[ComponentHealth],
        uptime_seconds: Optional[float] = None,
        **kwargs
    ) -> 'HealthResponse':
        """Create health response from component statuses"""
        # Determine overall status
        if any(c.status == HealthStatus.UNHEALTHY for c in components):
            overall_status = HealthStatus.UNHEALTHY
        elif any(c.status == HealthStatus.DEGRADED for c in components):
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
            
        return cls(
            status=overall_status,
            service=service,
            version=version,
            components=components,
            uptime_seconds=uptime_seconds,
            **kwargs
        ) 