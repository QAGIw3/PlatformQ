"""
PlatformQ Consul Integration Library

Provides service discovery and configuration management for services in the Consul Connect mesh.
"""

from .service_discovery import (
    ConsulServiceDiscovery,
    ServiceURLHelper
)

from .config import (
    ServiceConfig,
    create_service_config,
    get_auth_client,
    get_data_platform_client,
    get_blockchain_client
)

from .health import (
    HealthStatus,
    HealthCheck,
    HealthCheckRegistry,
    create_database_check,
    create_cache_check,
    create_message_queue_check,
    create_service_dependency_check,
    create_health_endpoint
)

__all__ = [
    # Service discovery
    'ConsulServiceDiscovery',
    'ServiceURLHelper',
    
    # Configuration
    'ServiceConfig',
    'create_service_config',
    
    # Client helpers
    'get_auth_client',
    'get_data_platform_client',
    'get_blockchain_client',
    
    # Health checks
    'HealthStatus',
    'HealthCheck',
    'HealthCheckRegistry',
    'create_database_check',
    'create_cache_check',
    'create_message_queue_check',
    'create_service_dependency_check',
    'create_health_endpoint'
]

__version__ = '0.1.0' 