"""Consul integration for PlatformQ services."""

import os
import asyncio
import logging
import socket
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import json

import consul.aio
import httpx


logger = logging.getLogger(__name__)


class ConsulServiceRegistry:
    """Service registry using Consul for service discovery and health checks."""
    
    def __init__(
        self,
        consul_host: str = None,
        consul_port: int = 8500,
        consul_token: Optional[str] = None
    ):
        self.consul_host = consul_host or os.getenv("CONSUL_HOST", "localhost")
        self.consul_port = consul_port
        self.consul_token = consul_token or os.getenv("CONSUL_HTTP_TOKEN")
        
        # Initialize Consul client
        self.consul = consul.aio.Consul(
            host=self.consul_host,
            port=self.consul_port,
            token=self.consul_token
        )
        
        # Service registration info
        self.service_id: Optional[str] = None
        self.service_name: Optional[str] = None
        self.service_port: Optional[int] = None
        
        # Health check task
        self._health_check_task: Optional[asyncio.Task] = None
        
    async def register_service(
        self,
        name: str,
        port: int,
        tags: List[str] = None,
        meta: Dict[str, str] = None,
        health_check_endpoint: str = "/health",
        health_check_interval: str = "10s",
        health_check_timeout: str = "5s",
        enable_sidecar: bool = True
    ) -> str:
        """
        Register service with Consul.
        
        Args:
            name: Service name
            port: Service port
            tags: Service tags
            meta: Service metadata
            health_check_endpoint: Health check URL path
            health_check_interval: Health check interval
            health_check_timeout: Health check timeout
            enable_sidecar: Enable Connect sidecar proxy
            
        Returns:
            Service ID
        """
        # Get host IP address
        hostname = socket.gethostname()
        host_ip = socket.gethostbyname(hostname)
        
        # Generate service ID
        self.service_id = f"{name}-{host_ip}-{port}"
        self.service_name = name
        self.service_port = port
        
        # Prepare service definition
        service_def = {
            "ID": self.service_id,
            "Name": name,
            "Tags": tags or [],
            "Meta": meta or {},
            "Port": port,
            "Address": host_ip,
            "Check": {
                "HTTP": f"http://{host_ip}:{port}{health_check_endpoint}",
                "Interval": health_check_interval,
                "Timeout": health_check_timeout,
                "DeregisterCriticalServiceAfter": "5m"
            }
        }
        
        # Add Connect sidecar if enabled
        if enable_sidecar:
            service_def["Connect"] = {
                "SidecarService": {}
            }
        
        # Register service
        try:
            await self.consul.agent.service.register(service_def)
            logger.info(
                f"Registered service {name} with ID {self.service_id} "
                f"at {host_ip}:{port}"
            )
            
            # Start health check updates
            self._health_check_task = asyncio.create_task(
                self._update_health_check()
            )
            
            return self.service_id
            
        except Exception as e:
            logger.error(f"Failed to register service: {e}")
            raise
            
    async def deregister_service(self):
        """Deregister service from Consul."""
        if not self.service_id:
            return
            
        try:
            # Cancel health check task
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass
                    
            # Deregister service
            await self.consul.agent.service.deregister(self.service_id)
            logger.info(f"Deregistered service {self.service_id}")
            
        except Exception as e:
            logger.error(f"Failed to deregister service: {e}")
            
    async def discover_service(
        self,
        service_name: str,
        tag: Optional[str] = None,
        passing_only: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Discover instances of a service.
        
        Args:
            service_name: Name of service to discover
            tag: Optional tag filter
            passing_only: Only return healthy instances
            
        Returns:
            List of service instances
        """
        try:
            # Query service
            _, services = await self.consul.health.service(
                service_name,
                tag=tag,
                passing=passing_only
            )
            
            # Extract service details
            instances = []
            for service in services:
                instances.append({
                    "id": service["Service"]["ID"],
                    "address": service["Service"]["Address"],
                    "port": service["Service"]["Port"],
                    "tags": service["Service"]["Tags"],
                    "meta": service["Service"]["Meta"],
                    "status": service["Checks"][0]["Status"] if service["Checks"] else "unknown"
                })
                
            return instances
            
        except Exception as e:
            logger.error(f"Failed to discover service {service_name}: {e}")
            return []
            
    async def get_service_connection(
        self,
        service_name: str,
        use_sidecar: bool = True
    ) -> Optional[str]:
        """
        Get connection string for a service.
        
        Args:
            service_name: Service to connect to
            use_sidecar: Use Connect sidecar proxy
            
        Returns:
            Connection URL
        """
        if use_sidecar:
            # Use local sidecar proxy
            # Port mapping should be configured in service definition
            port_map = {
                "options-service": 5001,
                "futures-service": 5002,
                "oracle-service": 5010,
                "order-matching-service": 5020,
                "risk-service": 5021,
                # Add more mappings as needed
            }
            
            local_port = port_map.get(service_name)
            if local_port:
                return f"http://localhost:{local_port}"
                
        # Fall back to direct connection
        instances = await self.discover_service(service_name)
        if instances:
            instance = instances[0]  # Use first healthy instance
            return f"http://{instance['address']}:{instance['port']}"
            
        return None
        
    async def _update_health_check(self):
        """Periodically update service health check TTL."""
        while True:
            try:
                # Update health check
                check_id = f"service:{self.service_id}"
                await self.consul.agent.check.ttl_pass(check_id)
                
                # Wait before next update
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to update health check: {e}")
                await asyncio.sleep(5)


class ConsulConfigManager:
    """Configuration management using Consul KV store."""
    
    def __init__(
        self,
        consul_host: str = None,
        consul_port: int = 8500,
        consul_token: Optional[str] = None,
        config_prefix: str = "config/"
    ):
        self.consul_host = consul_host or os.getenv("CONSUL_HOST", "localhost")
        self.consul_port = consul_port
        self.consul_token = consul_token or os.getenv("CONSUL_HTTP_TOKEN")
        self.config_prefix = config_prefix
        
        # Initialize Consul client
        self.consul = consul.aio.Consul(
            host=self.consul_host,
            port=self.consul_port,
            token=self.consul_token
        )
        
        # Config watchers
        self._watchers: Dict[str, asyncio.Task] = {}
        
    async def get_config(self, key: str) -> Optional[Any]:
        """Get configuration value from Consul."""
        full_key = f"{self.config_prefix}{key}"
        
        try:
            _, data = await self.consul.kv.get(full_key)
            if data:
                value = data["Value"]
                if value:
                    # Try to parse as JSON
                    try:
                        return json.loads(value.decode("utf-8"))
                    except json.JSONDecodeError:
                        return value.decode("utf-8")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get config {key}: {e}")
            return None
            
    async def set_config(self, key: str, value: Any) -> bool:
        """Set configuration value in Consul."""
        full_key = f"{self.config_prefix}{key}"
        
        try:
            # Convert to JSON if not string
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            elif not isinstance(value, str):
                value = str(value)
                
            # Store in Consul
            result = await self.consul.kv.put(full_key, value)
            return result
            
        except Exception as e:
            logger.error(f"Failed to set config {key}: {e}")
            return False
            
    async def watch_config(
        self,
        key: str,
        callback: Callable[[str, Any], None],
        poll_interval: int = 5
    ):
        """
        Watch configuration key for changes.
        
        Args:
            key: Configuration key to watch
            callback: Callback function (key, value)
            poll_interval: Polling interval in seconds
        """
        async def _watch_loop():
            last_value = None
            
            while True:
                try:
                    value = await self.get_config(key)
                    
                    if value != last_value:
                        # Value changed
                        await callback(key, value)
                        last_value = value
                        
                    await asyncio.sleep(poll_interval)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error watching config {key}: {e}")
                    await asyncio.sleep(poll_interval)
                    
        # Start watcher
        task = asyncio.create_task(_watch_loop())
        self._watchers[key] = task
        
    async def stop_watch(self, key: str):
        """Stop watching a configuration key."""
        task = self._watchers.get(key)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self._watchers[key]
            
    async def close(self):
        """Close all watchers and connections."""
        # Stop all watchers
        for key in list(self._watchers.keys()):
            await self.stop_watch(key)
            
        # Close Consul client
        await self.consul.close()


class ServiceMeshClient:
    """HTTP client that uses Consul Connect for service-to-service communication."""
    
    def __init__(
        self,
        service_registry: ConsulServiceRegistry,
        timeout: float = 30.0
    ):
        self.service_registry = service_registry
        self.timeout = timeout
        self._clients: Dict[str, httpx.AsyncClient] = {}
        
    async def get_client(self, service_name: str) -> httpx.AsyncClient:
        """Get HTTP client for a service."""
        if service_name not in self._clients:
            # Get service URL
            service_url = await self.service_registry.get_service_connection(
                service_name,
                use_sidecar=True
            )
            
            if not service_url:
                raise ValueError(f"Service {service_name} not found")
                
            # Create client
            self._clients[service_name] = httpx.AsyncClient(
                base_url=service_url,
                timeout=self.timeout
            )
            
        return self._clients[service_name]
        
    async def get(self, service_name: str, path: str, **kwargs) -> httpx.Response:
        """Make GET request to a service."""
        client = await self.get_client(service_name)
        return await client.get(path, **kwargs)
        
    async def post(self, service_name: str, path: str, **kwargs) -> httpx.Response:
        """Make POST request to a service."""
        client = await self.get_client(service_name)
        return await client.post(path, **kwargs)
        
    async def put(self, service_name: str, path: str, **kwargs) -> httpx.Response:
        """Make PUT request to a service."""
        client = await self.get_client(service_name)
        return await client.put(path, **kwargs)
        
    async def delete(self, service_name: str, path: str, **kwargs) -> httpx.Response:
        """Make DELETE request to a service."""
        client = await self.get_client(service_name)
        return await client.delete(path, **kwargs)
        
    async def close(self):
        """Close all HTTP clients."""
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear() 