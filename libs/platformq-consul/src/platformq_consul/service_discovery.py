"""
Service discovery utilities for Consul Connect service mesh
"""

import os
import consul
import random
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)


class ConsulServiceDiscovery:
    """Service discovery client for Consul Connect"""
    
    def __init__(self, consul_host: str = None, consul_port: int = 8500):
        """
        Initialize Consul service discovery client
        
        Args:
            consul_host: Consul agent host (default: from env or localhost)
            consul_port: Consul agent port (default: 8500)
        """
        # Get Consul address from environment or use defaults
        if consul_host is None:
            consul_addr = os.environ.get('CONSUL_HTTP_ADDR', 'localhost:8500')
            if ':' in consul_addr:
                consul_host, consul_port = consul_addr.split(':')
                consul_port = int(consul_port)
            else:
                consul_host = consul_addr
                
        self.consul = consul.Consul(host=consul_host, port=consul_port)
        self._service_cache = {}
        
    def get_service(self, service_name: str, passing_only: bool = True) -> Optional[Dict[str, any]]:
        """
        Get a single healthy service instance
        
        Args:
            service_name: Name of the service to discover
            passing_only: Only return services with passing health checks
            
        Returns:
            Service instance dict with host, port, and metadata
        """
        instances = self.get_service_instances(service_name, passing_only)
        if not instances:
            return None
            
        # Return random instance for load balancing
        return random.choice(instances)
        
    def get_service_instances(self, service_name: str, passing_only: bool = True) -> List[Dict[str, any]]:
        """
        Get all instances of a service
        
        Args:
            service_name: Name of the service to discover
            passing_only: Only return services with passing health checks
            
        Returns:
            List of service instances
        """
        try:
            _, services = self.consul.health.service(
                service_name, 
                passing=passing_only
            )
            
            instances = []
            for service in services:
                instance = {
                    'id': service['Service']['ID'],
                    'host': service['Service']['Address'] or service['Node']['Address'],
                    'port': service['Service']['Port'],
                    'tags': service['Service']['Tags'],
                    'meta': service['Service']['Meta'],
                    'node': service['Node']['Node']
                }
                instances.append(instance)
                
            return instances
            
        except Exception as e:
            logger.error(f"Error discovering service {service_name}: {e}")
            return []
            
    def get_service_url(self, service_name: str, scheme: str = 'http') -> Optional[str]:
        """
        Get URL for a service
        
        Args:
            service_name: Name of the service
            scheme: URL scheme (http/https)
            
        Returns:
            Service URL or None if not found
        """
        service = self.get_service(service_name)
        if not service:
            return None
            
        return f"{scheme}://{service['host']}:{service['port']}"
        
    def get_connect_upstream_url(self, service_name: str, local_port: int = None) -> str:
        """
        Get URL for a Connect upstream service
        
        When using Consul Connect, upstream services are exposed on localhost
        at the configured local_bind_port
        
        Args:
            service_name: Name of the upstream service
            local_port: Local port where the upstream is bound
            
        Returns:
            Upstream service URL
        """
        if local_port is None:
            # Try to get from environment variable
            port_env = f"{service_name.upper().replace('-', '_')}_PORT"
            local_port = int(os.environ.get(port_env, 0))
            
            if not local_port:
                raise ValueError(
                    f"Local port not specified for upstream {service_name}. "
                    f"Set {port_env} environment variable or pass local_port parameter."
                )
                
        return f"http://localhost:{local_port}"
        
    def register_service(self, name: str, port: int, tags: List[str] = None, 
                        meta: Dict[str, str] = None, check: Dict[str, any] = None):
        """
        Register a service with Consul
        
        Args:
            name: Service name
            port: Service port
            tags: Service tags
            meta: Service metadata
            check: Health check configuration
        """
        service_id = f"{name}-{os.environ.get('HOSTNAME', 'local')}"
        
        # Default health check
        if check is None:
            check = consul.Check.http(
                f"http://localhost:{port}/health",
                interval="10s",
                timeout="5s"
            )
            
        self.consul.agent.service.register(
            name=name,
            service_id=service_id,
            port=port,
            tags=tags or [],
            meta=meta or {},
            check=check
        )
        
        logger.info(f"Registered service {name} with ID {service_id}")
        
    def deregister_service(self, service_id: str = None, name: str = None):
        """
        Deregister a service from Consul
        
        Args:
            service_id: Service ID to deregister
            name: Service name (will construct ID if service_id not provided)
        """
        if service_id is None and name is not None:
            service_id = f"{name}-{os.environ.get('HOSTNAME', 'local')}"
            
        if service_id:
            self.consul.agent.service.deregister(service_id)
            logger.info(f"Deregistered service {service_id}")
            
    def get_service_config(self, key_prefix: str) -> Dict[str, any]:
        """
        Get service configuration from Consul KV store
        
        Args:
            key_prefix: Key prefix in Consul KV
            
        Returns:
            Configuration dictionary
        """
        try:
            _, values = self.consul.kv.get(key_prefix, recurse=True)
            
            if not values:
                return {}
                
            config = {}
            for value in values:
                # Remove prefix from key
                key = value['Key'].replace(key_prefix, '').lstrip('/')
                # Decode value
                config[key] = value['Value'].decode('utf-8') if value['Value'] else None
                
            return config
            
        except Exception as e:
            logger.error(f"Error getting config from Consul KV: {e}")
            return {}
            
    def watch_service(self, service_name: str, callback: callable, index: int = None):
        """
        Watch for changes to a service
        
        Args:
            service_name: Service to watch
            callback: Function to call when service changes
            index: Consul index for blocking query
        """
        def watch_handler(index, data):
            if data:
                callback(data)
            return self.consul.health.service(service_name, index=index, wait='30s')
            
        return watch_handler(index, None)


class ServiceURLHelper:
    """Helper class for managing service URLs in Consul Connect"""
    
    def __init__(self, discovery: ConsulServiceDiscovery = None):
        """
        Initialize service URL helper
        
        Args:
            discovery: ConsulServiceDiscovery instance
        """
        self.discovery = discovery or ConsulServiceDiscovery()
        
        # Map of service names to their local upstream ports
        # These should match the upstream configurations in Consul service definitions
        self.upstream_ports = {
            # Core services
            "auth-service": 5000,
            "vault": 5000,
            
            # Infrastructure
            "ignite-cache": 5001,
            "pulsar": 5002,
            "cassandra": 5003,
            "elasticsearch": 5004,
            "minio": 5005,
            "janusgraph": 5006,
            "opa": 5007,
            
            # Application services
            "compliance-service": 5004,
            "security-service": 5005,
            "blockchain-gateway-service": 5101,
            "data-platform-service": 5200,
            "market-data-service": 5300,
            "order-matching-service": 5400,
            "risk-management-service": 5500,
            "risk-engine-service": 5501,
            "graph-intelligence-service": 5600,
            "storage-service": 5700,
            "search-service": 5800,
            "mlflow-server": 5900,
            
            # Load from environment overrides
            **self._load_port_overrides()
        }
        
    def _load_port_overrides(self) -> Dict[str, int]:
        """Load port overrides from environment variables"""
        overrides = {}
        
        for key, value in os.environ.items():
            if key.endswith('_UPSTREAM_PORT'):
                service_name = key.replace('_UPSTREAM_PORT', '').lower().replace('_', '-')
                try:
                    overrides[service_name] = int(value)
                except ValueError:
                    pass
                    
        return overrides
        
    def get_url(self, service_name: str, use_connect: bool = True) -> str:
        """
        Get URL for a service
        
        Args:
            service_name: Name of the service
            use_connect: Use Consul Connect upstream (default: True)
            
        Returns:
            Service URL
        """
        if use_connect and service_name in self.upstream_ports:
            # Use local Connect proxy
            return f"http://localhost:{self.upstream_ports[service_name]}"
        else:
            # Use direct service discovery
            url = self.discovery.get_service_url(service_name)
            if not url:
                raise ValueError(f"Service {service_name} not found")
            return url
            
    def __getattr__(self, name: str) -> str:
        """
        Allow attribute-style access to service URLs
        
        Example:
            urls = ServiceURLHelper()
            auth_url = urls.auth_service  # Returns auth service URL
        """
        service_name = name.replace('_', '-')
        return self.get_url(service_name) 