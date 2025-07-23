"""
Enhanced Client Framework for DataIntelligenceSuite

Provides base client implementations with built-in patterns and decorators.
"""

from .base import (
    BaseClient,
    RESTClient,
    ClientConfig,
    RetryConfig,
    CircuitBreakerConfig,
    ClientError,
    ConnectionError,
    AuthenticationError,
    RateLimitError,
    CircuitBreakerError,
    retry,
    cached,
    circuit_breaker,
    rate_limited,
    monitored,
    authenticated
)

from .base_client import ServiceClient

from .analytics_client import AnalyticsClient
from .auth_client import AuthServiceClient
from .catalog_client import CatalogClient
from .ml_client import MLPlatformClient
from .processing_client import ProcessingClient

__all__ = [
    # Base classes
    "BaseClient",
    "RESTClient",
    "ServiceClient",
    
    # Configuration
    "ClientConfig",
    "RetryConfig",
    "CircuitBreakerConfig",
    
    # Errors
    "ClientError",
    "ConnectionError",
    "AuthenticationError",
    "RateLimitError",
    "CircuitBreakerError",
    
    # Decorators
    "retry",
    "cached",
    "circuit_breaker",
    "rate_limited",
    "monitored",
    "authenticated",
    
    # Service clients
    "AnalyticsClient",
    "AuthServiceClient",
    "CatalogClient",
    "MLPlatformClient",
    "ProcessingClient"
] 