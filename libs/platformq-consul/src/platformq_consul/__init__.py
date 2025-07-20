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
    'get_blockchain_client'
]

__version__ = '0.1.0' 