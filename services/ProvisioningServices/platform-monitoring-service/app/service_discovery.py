"""Service Discovery for multi-region monitoring"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

import aiohttp
import consul.aio

from config import settings
from models import ServiceEndpoint, RegionConfig, ServiceType

logger = logging.getLogger(__name__)


class ServiceDiscovery:
    """Multi-region service discovery using Consul"""
    
    def __init__(self):
        self.consul_clients: Dict[str, consul.aio.Consul] = {}
        self.service_cache: Dict[str, List[ServiceEndpoint]] = {}
        self.running = False
        self.discovery_task = None
        self.session = None
        
    async def start(self):
        """Start service discovery"""
        logger.info("Starting Service Discovery")
        self.running = True
        self.session = aiohttp.ClientSession()
        
        # Initialize default Consul client
        self.consul_clients['default'] = consul.aio.Consul(
            host=settings.CONSUL_URL.replace('http://', '').split(':')[0],
            port=int(settings.CONSUL_URL.split(':')[-1]) if ':' in settings.CONSUL_URL else 8500,
            token=settings.CONSUL_TOKEN,
            session=self.session
        )
        
        # Start discovery task
        self.discovery_task = asyncio.create_task(self._discovery_loop())
        
    async def stop(self):
        """Stop service discovery"""
        logger.info("Stopping Service Discovery")
        self.running = False
        
        if self.discovery_task:
            self.discovery_task.cancel()
            try:
                await self.discovery_task
            except asyncio.CancelledError:
                pass
                
        if self.session:
            await self.session.close()
            
    async def add_region(self, region_id: str, config: RegionConfig):
        """Add a new region for service discovery"""
        logger.info(f"Adding region for service discovery: {region_id}")
        
        # Parse Consul URL from region config
        consul_url = f"http://{config.consul_datacenter}-consul:8500"
        host = consul_url.replace('http://', '').split(':')[0]
        port = int(consul_url.split(':')[-1]) if ':' in consul_url else 8500
        
        # Create Consul client for the region
        self.consul_clients[region_id] = consul.aio.Consul(
            host=host,
            port=port,
            token=settings.CONSUL_TOKEN,
            dc=config.consul_datacenter,
            session=self.session
        )
        
        # Trigger immediate discovery
        await self._discover_services(region_id)
        
    async def remove_region(self, region_id: str):
        """Remove a region from service discovery"""
        logger.info(f"Removing region from service discovery: {region_id}")
        
        if region_id in self.consul_clients:
            del self.consul_clients[region_id]
            
        # Remove cached services for the region
        self.service_cache = {
            k: [s for s in v if s.region != region_id]
            for k, v in self.service_cache.items()
        }
        
    async def discover(
        self,
        service_name: str,
        region_id: Optional[str] = None
    ) -> List[ServiceEndpoint]:
        """Discover service endpoints"""
        # Check cache first
        if service_name in self.service_cache:
            services = self.service_cache[service_name]
            if region_id:
                services = [s for s in services if s.region == region_id]
            return services
            
        # If not in cache, try to discover
        endpoints = []
        
        if region_id and region_id in self.consul_clients:
            # Discover in specific region
            endpoints = await self._discover_service_in_region(
                service_name,
                region_id
            )
        else:
            # Discover in all regions
            tasks = []
            for rid in self.consul_clients:
                tasks.append(
                    self._discover_service_in_region(service_name, rid)
                )
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    endpoints.extend(result)
                    
        # Update cache
        self.service_cache[service_name] = endpoints
        
        return endpoints
        
    async def _discovery_loop(self):
        """Background service discovery loop"""
        while self.running:
            try:
                # Discover all Platform Q services
                for service_type in ServiceType:
                    await self.discover(f"platform-{service_type.value}")
                    
                # Also discover monitoring services
                for service in ['prometheus', 'thanos-sidecar', 'alertmanager']:
                    await self.discover(service)
                    
            except Exception as e:
                logger.error(f"Discovery loop error: {e}")
                
            await asyncio.sleep(30)  # Refresh every 30 seconds
            
    async def _discover_services(self, region_id: str):
        """Discover all services in a region"""
        try:
            client = self.consul_clients.get(region_id)
            if not client:
                return
                
            # Get all services
            _, services = await client.catalog.services()
            
            for service_name in services:
                if service_name.startswith('platform-') or service_name in [
                    'prometheus', 'thanos-sidecar', 'alertmanager'
                ]:
                    await self._discover_service_in_region(service_name, region_id)
                    
        except Exception as e:
            logger.error(f"Failed to discover services in {region_id}: {e}")
            
    async def _discover_service_in_region(
        self,
        service_name: str,
        region_id: str
    ) -> List[ServiceEndpoint]:
        """Discover a specific service in a region"""
        endpoints = []
        
        try:
            client = self.consul_clients.get(region_id, self.consul_clients.get('default'))
            if not client:
                return endpoints
                
            # Get service instances
            _, instances = await client.health.service(
                service_name,
                passing=True  # Only healthy instances
            )
            
            for instance in instances:
                service = instance['Service']
                
                # Determine protocol and paths
                protocol = 'https' if 'https' in service.get('Tags', []) else 'http'
                health_path = self._get_health_path(service_name)
                metrics_path = '/metrics'
                
                # Extract additional metadata from tags
                labels = {}
                for tag in service.get('Tags', []):
                    if '=' in tag:
                        key, value = tag.split('=', 1)
                        labels[key] = value
                        
                endpoint = ServiceEndpoint(
                    service_name=service_name,
                    region=region_id,
                    address=service['Address'] or instance['Node']['Address'],
                    port=service['Port'],
                    protocol=protocol,
                    health_check_path=health_path,
                    metrics_path=metrics_path,
                    labels=labels
                )
                endpoints.append(endpoint)
                
        except Exception as e:
            logger.error(f"Failed to discover {service_name} in {region_id}: {e}")
            
        return endpoints
        
    def _get_health_path(self, service_name: str) -> str:
        """Get health check path for a service"""
        health_paths = {
            'platform-cassandra': '/health',
            'platform-ignite': '/ignite?cmd=version',
            'platform-pulsar': '/admin/v2/brokers/health',
            'platform-minio': '/minio/health/live',
            'platform-elasticsearch': '/_cluster/health',
            'platform-janusgraph': '/health',
            'platform-kubernetes': '/healthz',
            'platform-vault': '/v1/sys/health',
            'platform-consul': '/v1/agent/self',
            'prometheus': '/-/healthy',
            'thanos-sidecar': '/-/healthy',
            'alertmanager': '/-/healthy'
        }
        
        return health_paths.get(service_name, '/health')
        
    async def register_service(
        self,
        service_name: str,
        address: str,
        port: int,
        tags: List[str] = None,
        meta: Dict[str, str] = None,
        check_interval: str = "10s"
    ):
        """Register a service with Consul"""
        try:
            client = self.consul_clients.get('default')
            if not client:
                logger.error("No default Consul client available")
                return
                
            # Prepare service definition
            service_def = {
                'Name': service_name,
                'Address': address,
                'Port': port,
                'Tags': tags or [],
                'Meta': meta or {},
                'Check': {
                    'HTTP': f"http://{address}:{port}{self._get_health_path(service_name)}",
                    'Interval': check_interval,
                    'Timeout': "5s"
                }
            }
            
            # Register service
            await client.agent.service.register(**service_def)
            logger.info(f"Registered service: {service_name}")
            
        except Exception as e:
            logger.error(f"Failed to register service {service_name}: {e}")
            
    async def deregister_service(self, service_id: str):
        """Deregister a service from Consul"""
        try:
            client = self.consul_clients.get('default')
            if client:
                await client.agent.service.deregister(service_id)
                logger.info(f"Deregistered service: {service_id}")
        except Exception as e:
            logger.error(f"Failed to deregister service {service_id}: {e}")
            
    async def get_service_config(
        self,
        service_name: str,
        key: str,
        region_id: Optional[str] = None
    ) -> Optional[Any]:
        """Get service configuration from Consul KV"""
        try:
            client = self.consul_clients.get(region_id, self.consul_clients.get('default'))
            if not client:
                return None
                
            # Build key path
            kv_key = f"platform-q/{service_name}/{key}"
            
            # Get value
            _, data = await client.kv.get(kv_key)
            if data:
                return data['Value'].decode('utf-8')
                
        except Exception as e:
            logger.error(f"Failed to get config for {service_name}/{key}: {e}")
            
        return None
        
    async def set_service_config(
        self,
        service_name: str,
        key: str,
        value: str,
        region_id: Optional[str] = None
    ):
        """Set service configuration in Consul KV"""
        try:
            client = self.consul_clients.get(region_id, self.consul_clients.get('default'))
            if not client:
                return
                
            # Build key path
            kv_key = f"platform-q/{service_name}/{key}"
            
            # Set value
            await client.kv.put(kv_key, value)
            logger.info(f"Set config for {service_name}/{key}")
            
        except Exception as e:
            logger.error(f"Failed to set config for {service_name}/{key}: {e}")
            
    async def watch_service(
        self,
        service_name: str,
        callback,
        region_id: Optional[str] = None
    ):
        """Watch for changes to a service"""
        client = self.consul_clients.get(region_id, self.consul_clients.get('default'))
        if not client:
            logger.error(f"No Consul client for region: {region_id}")
            return
            
        index = None
        while self.running:
            try:
                # Watch for changes
                index, data = await client.health.service(
                    service_name,
                    index=index,
                    passing=True
                )
                
                # Convert to endpoints
                endpoints = []
                for instance in data:
                    service = instance['Service']
                    endpoint = ServiceEndpoint(
                        service_name=service_name,
                        region=region_id or 'default',
                        address=service['Address'] or instance['Node']['Address'],
                        port=service['Port'],
                        protocol='http',
                        health_check_path=self._get_health_path(service_name),
                        metrics_path='/metrics'
                    )
                    endpoints.append(endpoint)
                    
                # Call callback with updated endpoints
                await callback(service_name, endpoints)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Watch error for {service_name}: {e}")
                await asyncio.sleep(5) 