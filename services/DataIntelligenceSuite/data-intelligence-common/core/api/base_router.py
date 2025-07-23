"""
Base Router for DataIntelligenceSuite APIs

Provides standardized routing patterns and utilities.
"""

import logging
from typing import Any, Dict, Optional, List, Callable, Type, TypeVar
from datetime import datetime
from dataclasses import dataclass
from abc import ABC, abstractmethod
import uuid

from fastapi import APIRouter, Request, Depends, Query, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .response_models import (
    APIResponse,
    PaginatedResponse,
    ErrorResponse,
    HealthResponse,
    ComponentHealth,
    HealthStatus
)
from ...monitoring import MetricsCollector

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)


@dataclass
class RouterConfig:
    """Router configuration"""
    prefix: str
    tags: List[str]
    include_health: bool = True
    include_metrics: bool = True
    
    # Pagination defaults
    default_page_size: int = 20
    max_page_size: int = 100
    
    # Response configuration
    include_request_id: bool = True
    include_response_time: bool = True


class PaginationParams(BaseModel):
    """Standard pagination parameters"""
    page: int = Query(1, ge=1, description="Page number")
    page_size: int = Query(20, ge=1, le=100, description="Items per page")
    
    @property
    def offset(self) -> int:
        """Calculate offset for database queries"""
        return (self.page - 1) * self.page_size


class BaseRouter(ABC):
    """
    Base router class for standardized API endpoints.
    
    Features:
    - Consistent response formatting
    - Built-in pagination
    - Health checks
    - Metrics collection
    - Error handling
    """
    
    def __init__(
        self,
        config: RouterConfig,
        metrics: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.metrics = metrics
        self.router = APIRouter(
            prefix=config.prefix,
            tags=config.tags
        )
        
        # Track start time for uptime
        self._start_time = datetime.utcnow()
        
        # Setup standard routes
        self._setup_routes()
        
    def _setup_routes(self):
        """Setup standard routes"""
        if self.config.include_health:
            self.router.add_api_route(
                "/health",
                self._health_check,
                methods=["GET"],
                response_model=HealthResponse,
                summary="Health check",
                description="Check service health and dependencies"
            )
            
        if self.config.include_metrics:
            self.router.add_api_route(
                "/metrics",
                self._get_metrics,
                methods=["GET"],
                summary="Get metrics",
                description="Get service metrics in Prometheus format"
            )
            
        # Add custom routes
        self.setup_routes()
        
    @abstractmethod
    def setup_routes(self):
        """Setup custom routes - must be implemented by subclasses"""
        pass
        
    @abstractmethod
    async def check_health(self) -> List[ComponentHealth]:
        """Check health of components - must be implemented by subclasses"""
        pass
        
    async def _health_check(self, request: Request) -> HealthResponse:
        """Standard health check endpoint"""
        # Get component health
        components = await self.check_health()
        
        # Calculate uptime
        uptime = (datetime.utcnow() - self._start_time).total_seconds()
        
        # Create response
        response = HealthResponse.create(
            service=self.config.tags[0] if self.config.tags else "unknown",
            version="1.0.0",  # Should be injected
            components=components,
            uptime_seconds=uptime
        )
        
        # Record metrics
        if self.metrics:
            self.metrics.set_gauge(
                "service_health",
                1 if response.is_healthy else 0,
                {"service": response.service}
            )
            
        return response
        
    async def _get_metrics(self) -> str:
        """Get metrics endpoint"""
        if not self.metrics:
            return ""
            
        # Return Prometheus formatted metrics
        return self.metrics.get_prometheus_metrics()
        
    def create_response(
        self,
        data: Any,
        message: Optional[str] = None,
        request: Optional[Request] = None,
        **kwargs
    ) -> APIResponse:
        """Create standard API response"""
        response = APIResponse.success(
            data=data,
            message=message,
            **kwargs
        )
        
        # Add request ID if configured
        if self.config.include_request_id and request:
            response.request_id = request.headers.get(
                "X-Request-ID",
                str(uuid.uuid4())
            )
            
        # Add response time if configured
        if self.config.include_response_time and hasattr(request.state, "start_time"):
            response.metadata["response_time_ms"] = (
                datetime.utcnow() - request.state.start_time
            ).total_seconds() * 1000
            
        return response
        
    def create_paginated_response(
        self,
        items: List[Any],
        total_items: int,
        pagination: PaginationParams,
        message: Optional[str] = None,
        request: Optional[Request] = None,
        **kwargs
    ) -> PaginatedResponse:
        """Create paginated response"""
        response = PaginatedResponse.paginate(
            items=items,
            page=pagination.page,
            page_size=pagination.page_size,
            total_items=total_items,
            message=message,
            **kwargs
        )
        
        # Add standard metadata
        if self.config.include_request_id and request:
            response.request_id = request.headers.get(
                "X-Request-ID",
                str(uuid.uuid4())
            )
            
        return response
        
    def create_error_response(
        self,
        error: Exception,
        request: Optional[Request] = None
    ) -> JSONResponse:
        """Create error response from exception"""
        # Handle different error types
        if isinstance(error, HTTPException):
            error_response = ErrorResponse(
                error=error.__class__.__name__,
                message=error.detail,
                status_code=error.status_code
            )
        elif isinstance(error, ValueError):
            error_response = ErrorResponse.validation_error(
                errors=[{"msg": str(error)}],
                message=str(error)
            )
        else:
            # Generic error
            error_response = ErrorResponse.internal_error(
                message="An unexpected error occurred"
            )
            
        # Add request ID
        if request:
            error_response.request_id = request.headers.get(
                "X-Request-ID",
                str(uuid.uuid4())
            )
            
        # Record error metric
        if self.metrics:
            self.metrics.increment_counter(
                "api_errors_total",
                {
                    "error_type": error_response.error,
                    "status_code": str(error_response.status_code)
                }
            )
            
        return JSONResponse(
            status_code=error_response.status_code,
            content=error_response.dict()
        )
        
    def paginate(
        self,
        model: Type[T],
        pagination: PaginationParams = Depends()
    ) -> PaginationParams:
        """Dependency for pagination parameters"""
        # Enforce max page size
        if pagination.page_size > self.config.max_page_size:
            pagination.page_size = self.config.max_page_size
            
        return pagination
        
    async def get_list(
        self,
        fetch_items: Callable,
        count_items: Callable,
        pagination: PaginationParams,
        filters: Optional[Dict[str, Any]] = None,
        request: Optional[Request] = None
    ) -> PaginatedResponse:
        """
        Generic list endpoint handler.
        
        Args:
            fetch_items: Async function to fetch items
            count_items: Async function to count total items
            pagination: Pagination parameters
            filters: Optional filters
            request: Optional request object
        """
        # Get total count
        total_items = await count_items(filters)
        
        # Fetch items
        items = await fetch_items(
            offset=pagination.offset,
            limit=pagination.page_size,
            filters=filters
        )
        
        # Create response
        return self.create_paginated_response(
            items=items,
            total_items=total_items,
            pagination=pagination,
            request=request
        )
        
    async def get_by_id(
        self,
        fetch_func: Callable,
        resource_id: Any,
        resource_name: str = "Resource",
        request: Optional[Request] = None
    ) -> APIResponse:
        """
        Generic get by ID handler.
        
        Args:
            fetch_func: Async function to fetch resource
            resource_id: Resource identifier
            resource_name: Resource name for error messages
            request: Optional request object
        """
        # Fetch resource
        resource = await fetch_func(resource_id)
        
        if not resource:
            raise HTTPException(
                status_code=404,
                detail=f"{resource_name} not found"
            )
            
        # Create response
        return self.create_response(
            data=resource,
            message=f"{resource_name} retrieved successfully",
            request=request
        )
        
    async def create_resource(
        self,
        create_func: Callable,
        data: BaseModel,
        resource_name: str = "Resource",
        request: Optional[Request] = None
    ) -> APIResponse:
        """
        Generic create resource handler.
        
        Args:
            create_func: Async function to create resource
            data: Resource data
            resource_name: Resource name for messages
            request: Optional request object
        """
        # Create resource
        resource = await create_func(data)
        
        # Create response
        return self.create_response(
            data=resource,
            message=f"{resource_name} created successfully",
            request=request
        )
        
    async def update_resource(
        self,
        update_func: Callable,
        resource_id: Any,
        data: BaseModel,
        resource_name: str = "Resource",
        request: Optional[Request] = None
    ) -> APIResponse:
        """
        Generic update resource handler.
        
        Args:
            update_func: Async function to update resource
            resource_id: Resource identifier
            data: Update data
            resource_name: Resource name for messages
            request: Optional request object
        """
        # Update resource
        resource = await update_func(resource_id, data)
        
        if not resource:
            raise HTTPException(
                status_code=404,
                detail=f"{resource_name} not found"
            )
            
        # Create response
        return self.create_response(
            data=resource,
            message=f"{resource_name} updated successfully",
            request=request
        )
        
    async def delete_resource(
        self,
        delete_func: Callable,
        resource_id: Any,
        resource_name: str = "Resource",
        request: Optional[Request] = None
    ) -> APIResponse:
        """
        Generic delete resource handler.
        
        Args:
            delete_func: Async function to delete resource
            resource_id: Resource identifier
            resource_name: Resource name for messages
            request: Optional request object
        """
        # Delete resource
        deleted = await delete_func(resource_id)
        
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"{resource_name} not found"
            )
            
        # Create response
        return self.create_response(
            data={"id": resource_id, "deleted": True},
            message=f"{resource_name} deleted successfully",
            request=request
        ) 