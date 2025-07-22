"""
PlatformQ Shared Library

Common utilities and base classes for all PlatformQ services.
"""

# Version
__version__ = "0.2.0"

# Core modules
from .service_client import ServiceClient
from .utils import generate_correlation_id, setup_logging

# New unified modules
from .auth import (
    AuthType,
    UserRole,
    AuthConfig,
    AuthenticatedUser,
    UnifiedAuth,
    get_auth_instance,
    get_current_user,
    get_current_trader,
    get_current_admin,
    get_service_auth,
    require_roles,
    require_permissions
)

from .service_base import (
    ServiceConfig,
    ServiceMetrics,
    PlatformQService
)

from .monitoring import (
    MetricType,
    AlertSeverity,
    StandardMetrics,
    UnifiedMonitoring,
    monitor_operation
)

__all__ = [
    # Core
    "ServiceClient",
    "generate_correlation_id",
    "setup_logging",
    
    # Auth
    "AuthType",
    "UserRole", 
    "AuthConfig",
    "AuthenticatedUser",
    "UnifiedAuth",
    "get_auth_instance",
    "get_current_user",
    "get_current_trader",
    "get_current_admin",
    "get_service_auth",
    "require_roles",
    "require_permissions",
    
    # Service Base
    "ServiceConfig",
    "ServiceMetrics",
    "PlatformQService",
    
    # Monitoring
    "MetricType",
    "AlertSeverity",
    "StandardMetrics",
    "UnifiedMonitoring",
    "monitor_operation"
] 