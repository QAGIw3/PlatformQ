"""
API framework for standardized patterns across DataIntelligenceSuite services.

Provides base routers, response models, request validation, and middleware.
"""

from .response_models import (
    APIResponse,
    PaginatedResponse,
    ErrorResponse,
    HealthResponse,
    BatchResponse,
    StreamResponse
)

from .base_router import (
    BaseRouter,
    RouterConfig,
    create_api_router
)

from .request_validators import (
    BaseRequestModel,
    PaginationRequest,
    FilterRequest,
    BulkOperationRequest,
    ResourceCreateRequest,
    ResourceUpdateRequest,
    ValidationRule,
    DynamicValidator,
    SortOrder,
    # Validators
    validate_email,
    validate_phone,
    validate_url,
    validate_json_path,
    # Constraint types
    NonEmptyStr,
    Identifier,
    SafeString,
    Percentage,
    PositiveInt,
    NonNegativeInt,
    # Factory
    create_request_model
)

from .middleware import (
    RequestTrackingMiddleware,
    RateLimitMiddleware,
    AuthenticationMiddleware,
    ErrorHandlingMiddleware,
    CompressionMiddleware,
    CORSMiddleware,
    # Decorators
    require_roles,
    rate_limit,
    validate_request
)

__all__ = [
    # Response models
    "APIResponse",
    "PaginatedResponse",
    "ErrorResponse",
    "HealthResponse",
    "BatchResponse",
    "StreamResponse",
    
    # Base router
    "BaseRouter",
    "RouterConfig",
    "create_api_router",
    
    # Request models
    "BaseRequestModel",
    "PaginationRequest",
    "FilterRequest",
    "BulkOperationRequest",
    "ResourceCreateRequest",
    "ResourceUpdateRequest",
    "ValidationRule",
    "DynamicValidator",
    "SortOrder",
    
    # Validators
    "validate_email",
    "validate_phone",
    "validate_url",
    "validate_json_path",
    
    # Constraints
    "NonEmptyStr",
    "Identifier",
    "SafeString",
    "Percentage",
    "PositiveInt",
    "NonNegativeInt",
    
    # Factory
    "create_request_model",
    
    # Middleware
    "RequestTrackingMiddleware",
    "RateLimitMiddleware",
    "AuthenticationMiddleware",
    "ErrorHandlingMiddleware",
    "CompressionMiddleware",
    "CORSMiddleware",
    
    # Decorators
    "require_roles",
    "rate_limit",
    "validate_request"
] 