"""
Configuration utilities for services using Consul Connect
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from .service_discovery import ConsulServiceDiscovery, ServiceURLHelper


@dataclass
class ServiceConfig:
    """Configuration for a service in the Consul mesh"""
    
    name: str
    port: int = 8000
    tags: list = field(default_factory=list)
    meta: dict = field(default_factory=dict)
    
    # Consul configuration
    consul_host: str = field(default_factory=lambda: os.environ.get('CONSUL_HTTP_ADDR', 'localhost:8500'))
    
    # Upstream services with their local ports
    upstreams: Dict[str, int] = field(default_factory=dict)
    
    # Health check endpoint
    health_endpoint: str = "/health"
    health_interval: str = "10s"
    health_timeout: str = "5s"
    
    def __post_init__(self):
        """Initialize service discovery components"""
        self.discovery = ConsulServiceDiscovery(self.consul_host)
        self.urls = ServiceURLHelper(self.discovery)
        
        # Register service on initialization
        if os.environ.get('CONSUL_REGISTER', 'true').lower() == 'true':
            self.register()
            
    def register(self):
        """Register service with Consul"""
        self.discovery.register_service(
            name=self.name,
            port=self.port,
            tags=self.tags,
            meta=self.meta
        )
        
    def deregister(self):
        """Deregister service from Consul"""
        self.discovery.deregister_service(name=self.name)
        
    def get_upstream_url(self, service_name: str) -> str:
        """
        Get URL for an upstream service
        
        Args:
            service_name: Name of the upstream service
            
        Returns:
            URL for the upstream service
        """
        if service_name in self.upstreams:
            return f"http://localhost:{self.upstreams[service_name]}"
        else:
            return self.urls.get_url(service_name)
            
    def get_config(self, key: str = None) -> Dict[str, Any]:
        """
        Get configuration from Consul KV store
        
        Args:
            key: Specific key to retrieve (default: service name)
            
        Returns:
            Configuration dictionary
        """
        key_prefix = key or f"config/{self.name}"
        return self.discovery.get_service_config(key_prefix)
        

def create_service_config(service_name: str, **kwargs) -> ServiceConfig:
    """
    Create service configuration with defaults for common services
    
    Args:
        service_name: Name of the service
        **kwargs: Additional configuration options
        
    Returns:
        ServiceConfig instance
    """
    # Default configurations for common services
    service_defaults = {
        "auth-service": {
            "tags": ["security", "authentication", "authorization", "api"],
            "upstreams": {
                "vault": 5000,
                "ignite-cache": 5001,
                "pulsar": 5002,
                "cassandra": 5003,
                "compliance-service": 5004,
                "security-service": 5005
            }
        },
        "blockchain-gateway-service": {
            "tags": ["blockchain", "gateway", "web3", "api"],
            "upstreams": {
                "vault": 5100,
                "auth-service": 5101,
                "ignite-cache": 5102,
                "pulsar": 5103,
                "cassandra": 5104,
                "graph-intelligence-service": 5105,
                "compliance-service": 5106
            }
        },
        "data-platform-service": {
            "tags": ["data", "lake", "warehouse", "api"],
            "upstreams": {
                "auth-service": 5200,
                "connector-service": 5201,
                "ignite-cache": 5202,
                "pulsar": 5203,
                "cassandra": 5204,
                "elasticsearch": 5205,
                "minio": 5206,
                "janusgraph": 5207
            }
        },
        "market-data-service": {
            "tags": ["market-data", "pricing", "feeds", "api"],
            "upstreams": {
                "auth-service": 5300,
                "ignite-cache": 5301,
                "pulsar": 5302,
                "cassandra": 5303
            }
        },
        "trading-platform-service": {
            "tags": ["trading", "platform", "exchange", "api"],
            "upstreams": {
                "auth-service": 5400,
                "order-matching-service": 5401,
                "market-data-service": 5402,
                "risk-management-service": 5403,
                "ignite-cache": 5404,
                "pulsar": 5405,
                "cassandra": 5406
            }
        }
    }
    
    # Get defaults for the service
    defaults = service_defaults.get(service_name, {})
    
    # Merge with provided kwargs
    config_args = {
        "name": service_name,
        **defaults,
        **kwargs
    }
    
    return ServiceConfig(**config_args)


# Example usage functions
def get_auth_client(config: ServiceConfig) -> str:
    """Get authenticated client for a service"""
    auth_url = config.get_upstream_url("auth-service")
    return f"{auth_url}/api/v1"


def get_data_platform_client(config: ServiceConfig) -> str:
    """Get data platform client"""
    data_url = config.get_upstream_url("data-platform-service")
    return f"{data_url}/api/v1"


def get_blockchain_client(config: ServiceConfig) -> str:
    """Get blockchain gateway client"""
    blockchain_url = config.get_upstream_url("blockchain-gateway-service")
    return f"{blockchain_url}/api/v1" 