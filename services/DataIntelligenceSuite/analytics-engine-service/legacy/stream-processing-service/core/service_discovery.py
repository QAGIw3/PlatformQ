"""Service Discovery Integration with Consul

Handles service registration, health checks, and discovery.
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
import aiohttp
import json

from app.core.config import Settings


logger = logging.getLogger(__name__)


async def register_service(settings: Settings):
    """Register service with Consul"""
    service_config = {
        "ID": f"{settings.consul_service_name}-{settings.api_port}",
        "Name": settings.consul_service_name,
        "Tags": [
            "streaming",
            "flink",
            "data-intelligence",
            settings.environment
        ],
        "Address": settings.api_host,
        "Port": settings.api_port,
        "Meta": {
            "version": settings.service_version,
            "environment": settings.environment,
            "service_type": "stream-processing"
        },
        "Check": {
            "HTTP": f"http://{settings.api_host}:{settings.api_port}/api/v1/health",
            "Interval": settings.consul_health_check_interval,
            "Timeout": "5s"
        }
    }
    
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/agent/service/register"
        
        async with aiohttp.ClientSession() as session:
            async with session.put(url, json=service_config) as resp:
                if resp.status == 200:
                    logger.info(f"Successfully registered service with Consul: {settings.consul_service_name}")
                else:
                    logger.error(f"Failed to register service: {resp.status} - {await resp.text()}")
                    
    except Exception as e:
        logger.error(f"Error registering service with Consul: {e}")
        raise


async def deregister_service(settings: Settings):
    """Deregister service from Consul"""
    service_id = f"{settings.consul_service_name}-{settings.api_port}"
    
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/agent/service/deregister/{service_id}"
        
        async with aiohttp.ClientSession() as session:
            async with session.put(url) as resp:
                if resp.status == 200:
                    logger.info(f"Successfully deregistered service from Consul: {service_id}")
                else:
                    logger.error(f"Failed to deregister service: {resp.status}")
                    
    except Exception as e:
        logger.error(f"Error deregistering service from Consul: {e}")


async def discover_service(settings: Settings, service_name: str) -> List[Dict[str, Any]]:
    """Discover service instances from Consul"""
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/health/service/{service_name}?passing=true"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    services = []
                    for entry in data:
                        service = entry.get("Service", {})
                        services.append({
                            "id": service.get("ID"),
                            "address": service.get("Address"),
                            "port": service.get("Port"),
                            "tags": service.get("Tags", []),
                            "meta": service.get("Meta", {})
                        })
                    
                    return services
                else:
                    logger.error(f"Failed to discover service {service_name}: {resp.status}")
                    return []
                    
    except Exception as e:
        logger.error(f"Error discovering service {service_name}: {e}")
        return []


async def get_service_config(settings: Settings, key: str) -> Optional[Dict[str, Any]]:
    """Get configuration from Consul KV store"""
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/kv/{key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        # Consul returns base64 encoded values
                        import base64
                        value = base64.b64decode(data[0]["Value"]).decode("utf-8")
                        return json.loads(value)
                elif resp.status == 404:
                    logger.debug(f"Config key not found: {key}")
                else:
                    logger.error(f"Failed to get config {key}: {resp.status}")
                    
    except Exception as e:
        logger.error(f"Error getting config {key}: {e}")
        
    return None


async def put_service_config(settings: Settings, key: str, value: Dict[str, Any]) -> bool:
    """Put configuration to Consul KV store"""
    try:
        url = f"http://{settings.consul_host}:{settings.consul_port}/v1/kv/{key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.put(url, data=json.dumps(value)) as resp:
                if resp.status == 200:
                    logger.info(f"Successfully stored config: {key}")
                    return True
                else:
                    logger.error(f"Failed to store config {key}: {resp.status}")
                    return False
                    
    except Exception as e:
        logger.error(f"Error storing config {key}: {e}")
        return False


class ServiceDiscoveryClient:
    """Client for service discovery operations"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._cache: Dict[str, List[Dict[str, Any]]] = {}
        self._refresh_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the service discovery client"""
        # Register this service
        if self.settings.consul_enabled:
            await register_service(self.settings)
            
        # Start refresh task
        self._refresh_task = asyncio.create_task(self._refresh_services())
        
    async def stop(self):
        """Stop the service discovery client"""
        # Cancel refresh task
        if self._refresh_task:
            self._refresh_task.cancel()
            
        # Deregister this service
        if self.settings.consul_enabled:
            await deregister_service(self.settings)
            
    async def get_service_url(self, service_name: str) -> Optional[str]:
        """Get URL for a service (load balanced)"""
        services = await self.discover(service_name)
        
        if not services:
            return None
            
        # Simple round-robin selection
        # In production, use proper load balancing
        service = services[0]
        return f"http://{service['address']}:{service['port']}"
        
    async def discover(self, service_name: str) -> List[Dict[str, Any]]:
        """Discover service instances"""
        # Check cache first
        if service_name in self._cache:
            return self._cache[service_name]
            
        # Discover from Consul
        services = await discover_service(self.settings, service_name)
        self._cache[service_name] = services
        
        return services
        
    async def get_config(self, key: str) -> Optional[Dict[str, Any]]:
        """Get configuration value"""
        return await get_service_config(self.settings, key)
        
    async def put_config(self, key: str, value: Dict[str, Any]) -> bool:
        """Put configuration value"""
        return await put_service_config(self.settings, key, value)
        
    async def _refresh_services(self):
        """Periodically refresh service cache"""
        while True:
            try:
                await asyncio.sleep(30)  # Refresh every 30 seconds
                
                # Refresh all cached services
                for service_name in list(self._cache.keys()):
                    services = await discover_service(self.settings, service_name)
                    self._cache[service_name] = services
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error refreshing services: {e}") 