"""
Service Mesh Integration for PlatformQ

Provides Consul Connect integration for automatic mTLS between services.
"""

import os
import logging
import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import httpx
import json

from .consul.consul_client import ConsulClient, ConsulConfig, ServiceDefinition
from .vault.vault_client import VaultClient

logger = logging.getLogger(__name__)


@dataclass
class ServiceMeshConfig:
    """Service mesh configuration"""
    service_name: str
    service_port: int = 8000
    enable_mtls: bool = True
    enable_intentions: bool = True
    enable_observability: bool = True
    health_check_path: str = "/health"
    ready_check_path: str = "/ready"
    metrics_path: str = "/metrics"
    sidecar_port: int = 21000
    admin_port: int = 19000


class ServiceMeshIntegration:
    """
    Integrates services with Consul Connect for zero-trust networking.
    
    Features:
    - Automatic mTLS between services
    - Service intentions (authorization)
    - Observability integration
    - Health checking
    - Sidecar proxy management
    """
    
    def __init__(self, config: ServiceMeshConfig, 
                 consul_client: ConsulClient,
                 vault_client: Optional[VaultClient] = None):
        self.config = config
        self.consul = consul_client
        self.vault = vault_client
        self._proxy_process = None
        self._registered = False
        
    async def initialize(self) -> None:
        """Initialize service mesh integration"""
        await self.consul.initialize()
        
        # Register service with Connect enabled
        await self._register_service()
        
        # Configure service intentions
        if self.config.enable_intentions:
            await self._configure_intentions()
            
        # Start sidecar proxy
        if self.config.enable_mtls:
            await self._start_sidecar_proxy()
            
        logger.info(f"Service mesh initialized for {self.config.service_name}")
        
    async def _register_service(self) -> None:
        """Register service with Consul Connect"""
        # Main service definition
        service = ServiceDefinition(
            name=self.config.service_name,
            port=self.config.service_port,
            tags=["platformq", "connect-enabled"],
            meta={
                "version": os.getenv("SERVICE_VERSION", "1.0.0"),
                "protocol": "http"
            },
            check={
                "http": f"http://localhost:{self.config.service_port}{self.config.health_check_path}",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "60s"
            }
        )
        
        # Register main service
        service_id = await self.consul.register_service(service)
        self._registered = True
        
        # Register sidecar proxy
        await self._register_sidecar_proxy(service_id)
        
        logger.info(f"Registered service {self.config.service_name} with ID {service_id}")
        
    async def _register_sidecar_proxy(self, parent_service_id: str) -> None:
        """Register Envoy sidecar proxy for the service"""
        sidecar = ServiceDefinition(
            name=f"{self.config.service_name}-sidecar-proxy",
            service_id=f"{parent_service_id}-sidecar-proxy",
            port=self.config.sidecar_port,
            tags=["envoy", "sidecar", "connect-proxy"],
            meta={
                "parent_service_id": parent_service_id,
                "parent_service_name": self.config.service_name
            },
            check={
                "tcp": f"localhost:{self.config.sidecar_port}",
                "interval": "10s",
                "timeout": "2s"
            }
        )
        
        # Configure proxy settings
        proxy_config = {
            "destination_service_name": self.config.service_name,
            "destination_service_id": parent_service_id,
            "local_service_address": "127.0.0.1",
            "local_service_port": self.config.service_port,
            "config": {
                "bind_address": "0.0.0.0",
                "bind_port": self.config.sidecar_port,
                "local_connect_timeout_ms": 5000,
                "handshake_timeout_ms": 10000
            },
            "upstreams": await self._get_upstream_config()
        }
        
        # Store proxy config in Consul KV
        await self.consul.kv_put(
            f"connect-proxy/{parent_service_id}/config",
            proxy_config
        )
        
        await self.consul.register_service(sidecar)
        
    async def _get_upstream_config(self) -> List[Dict[str, Any]]:
        """Get upstream service configurations"""
        upstreams = []
        
        # Define upstream services based on service dependencies
        service_dependencies = {
            "digital-asset-service": ["auth-service", "storage-service", "search-service"],
            "workflow-service": ["auth-service", "digital-asset-service", "functions-service"],
            "blockchain-gateway-service": ["auth-service", "compliance-service"],
            "data-platform-service": ["auth-service", "storage-service", "search-service"],
            # Add more service dependencies
        }
        
        dependencies = service_dependencies.get(self.config.service_name, ["auth-service"])
        
        for idx, dep in enumerate(dependencies):
            upstreams.append({
                "destination_name": dep,
                "local_bind_port": 9000 + idx,  # Local ports for upstream services
                "config": {
                    "connect_timeout_ms": 5000,
                    "limits": {
                        "max_connections": 100,
                        "max_pending_requests": 100,
                        "max_concurrent_requests": 100
                    }
                }
            })
            
        return upstreams
        
    async def _configure_intentions(self) -> None:
        """Configure service intentions (authorization rules)"""
        # Default intentions for common services
        default_intentions = [
            {
                "source": self.config.service_name,
                "destination": "auth-service",
                "action": "allow"
            },
            {
                "source": "prometheus",
                "destination": self.config.service_name,
                "action": "allow"  # Allow metrics collection
            },
            {
                "source": "api-gateway",
                "destination": self.config.service_name,
                "action": "allow"  # Allow API gateway access
            }
        ]
        
        # Service-specific intentions
        service_intentions = {
            "auth-service": [
                {"source": "*", "destination": "auth-service", "action": "allow"}
            ],
            "blockchain-gateway-service": [
                {"source": "defi-protocol-service", "destination": "blockchain-gateway-service", "action": "allow"},
                {"source": "derivatives-engine-service", "destination": "blockchain-gateway-service", "action": "allow"}
            ],
            "data-platform-service": [
                {"source": "analytics-service", "destination": "data-platform-service", "action": "allow"},
                {"source": "workflow-service", "destination": "data-platform-service", "action": "allow"}
            ]
            # Add more service-specific intentions
        }
        
        # Apply default intentions
        for intention in default_intentions:
            await self._create_intention(intention)
            
        # Apply service-specific intentions
        specific = service_intentions.get(self.config.service_name, [])
        for intention in specific:
            await self._create_intention(intention)
            
    async def _create_intention(self, intention: Dict[str, str]) -> None:
        """Create a service intention in Consul"""
        try:
            # Use Consul HTTP API to create intention
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{self.consul.config.scheme}://{self.consul.config.host}:{self.consul.config.port}/v1/connect/intentions",
                    json=intention,
                    headers={"X-Consul-Token": self.consul.config.token} if self.consul.config.token else {}
                )
                response.raise_for_status()
                logger.info(f"Created intention: {intention}")
        except Exception as e:
            logger.error(f"Failed to create intention: {e}")
            
    async def _start_sidecar_proxy(self) -> None:
        """Start Envoy sidecar proxy"""
        # In production, this would be handled by Kubernetes/Nomad
        # For development, we'll document the manual process
        logger.info(f"Sidecar proxy configuration ready for {self.config.service_name}")
        logger.info(f"In production, the sidecar will be injected automatically")
        logger.info(f"For local development, run: consul connect proxy -sidecar-for {self.config.service_name}")
        
    async def get_service_client(self, service_name: str, 
                                timeout: float = 30.0) -> httpx.AsyncClient:
        """
        Get an HTTP client configured for service-to-service communication.
        
        In Connect-enabled mode, this will use the local proxy port.
        """
        if self.config.enable_mtls:
            # Find the upstream configuration for this service
            upstreams = await self._get_upstream_config()
            upstream = next((u for u in upstreams if u["destination_name"] == service_name), None)
            
            if upstream:
                # Use local proxy port
                base_url = f"http://localhost:{upstream['local_bind_port']}"
            else:
                # Fallback to direct connection
                logger.warning(f"No upstream configured for {service_name}, using direct connection")
                service_instances = await self.consul.discover_service(service_name)
                if not service_instances:
                    raise ValueError(f"No instances found for service {service_name}")
                instance = service_instances[0]
                base_url = f"http://{instance.address}:{instance.port}"
        else:
            # Direct connection without mTLS
            service_instances = await self.consul.discover_service(service_name)
            if not service_instances:
                raise ValueError(f"No instances found for service {service_name}")
            instance = service_instances[0]
            base_url = f"http://{instance.address}:{instance.port}"
            
        return httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={
                "X-Service-Name": self.config.service_name,
                "X-Service-Version": os.getenv("SERVICE_VERSION", "1.0.0")
            }
        )
        
    async def health_check(self) -> Dict[str, Any]:
        """Check service mesh health"""
        health = {
            "service_registered": self._registered,
            "mtls_enabled": self.config.enable_mtls,
            "consul_healthy": False,
            "proxy_healthy": False
        }
        
        # Check Consul health
        try:
            consul_health = await self.consul.health_check()
            health["consul_healthy"] = consul_health.get("healthy", False)
        except Exception as e:
            logger.error(f"Consul health check failed: {e}")
            
        # Check proxy health (if enabled)
        if self.config.enable_mtls:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"http://localhost:{self.config.admin_port}/ready"
                    )
                    health["proxy_healthy"] = response.status_code == 200
            except Exception:
                health["proxy_healthy"] = False
                
        return health
        
    async def shutdown(self) -> None:
        """Cleanup service mesh resources"""
        if self._registered:
            # Deregister service
            # In production, this is handled by the orchestrator
            logger.info(f"Service {self.config.service_name} deregistration handled by orchestrator") 