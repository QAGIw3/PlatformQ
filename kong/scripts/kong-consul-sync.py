#!/usr/bin/env python3
"""
Kong-Consul Service Synchronization

This script continuously syncs services from Consul to Kong, creating:
- Kong services for each Consul service
- Routes based on service names
- Upstream targets for load balancing
"""

import os
import time
import json
import logging
import consul
import requests
from typing import Dict, List, Set

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KongConsulSync:
    """Synchronize Consul services with Kong"""
    
    def __init__(self):
        self.consul_client = consul.Consul(
            host=os.environ.get('CONSUL_HTTP_ADDR', 'consul-server-1:8500').split(':')[0],
            port=int(os.environ.get('CONSUL_HTTP_ADDR', 'consul-server-1:8500').split(':')[1])
        )
        self.kong_admin_url = os.environ.get('KONG_ADMIN_URL', 'http://kong:8001')
        self.sync_interval = int(os.environ.get('SYNC_INTERVAL', 10))
        
        # Services to exclude from sync
        self.excluded_services = {'consul', 'kong', 'kong-database', 'konga'}
        
        # Service to route path mapping
        self.service_routes = {
            'auth-service': '/auth',
            'blockchain-gateway-service': '/blockchain',
            'data-platform-service': '/data',
            'market-data-service': '/market',
            'trading-platform-service': '/trading',
            'order-matching-service': '/orders',
            'risk-management-service': '/risk',
            'analytics-service': '/analytics',
            'compliance-service': '/compliance',
            'digital-asset-service': '/assets',
            'search-service': '/search',
            'storage-service': '/storage',
            'workflow-service': '/workflow',
            'governance-service': '/governance',
            'graph-intelligence-service': '/graph',
            'mlflow-server': '/ml',
            'unified-ml-platform-service': '/ai'
        }
        
    def get_consul_services(self) -> Dict[str, List[Dict]]:
        """Get all services from Consul"""
        try:
            _, services = self.consul_client.catalog.services()
            
            consul_services = {}
            for service_name in services:
                if service_name in self.excluded_services:
                    continue
                    
                # Get healthy service instances
                _, instances = self.consul_client.health.service(
                    service_name, 
                    passing=True
                )
                
                if instances:
                    consul_services[service_name] = instances
                    
            return consul_services
            
        except Exception as e:
            logger.error(f"Error getting Consul services: {e}")
            return {}
            
    def get_kong_services(self) -> Set[str]:
        """Get all services from Kong"""
        try:
            response = requests.get(f"{self.kong_admin_url}/services")
            response.raise_for_status()
            
            services = response.json().get('data', [])
            return {service['name'] for service in services}
            
        except Exception as e:
            logger.error(f"Error getting Kong services: {e}")
            return set()
            
    def create_kong_service(self, service_name: str, instances: List[Dict]):
        """Create or update a Kong service"""
        try:
            # Use first instance for now (could implement load balancing)
            instance = instances[0]
            host = instance['Service']['Address'] or instance['Node']['Address']
            port = instance['Service']['Port']
            
            service_data = {
                'name': service_name,
                'host': f"{service_name}.service.consul",  # Use Consul DNS
                'port': port,
                'protocol': 'http',
                'connect_timeout': 60000,
                'write_timeout': 60000,
                'read_timeout': 60000,
                'retries': 3,
                'tags': instance['Service']['Tags'] or []
            }
            
            # Create or update service
            response = requests.put(
                f"{self.kong_admin_url}/services/{service_name}",
                json=service_data
            )
            response.raise_for_status()
            
            logger.info(f"Created/updated Kong service: {service_name}")
            
            # Create route for the service
            self.create_kong_route(service_name)
            
            # Create upstream and targets for load balancing
            self.create_kong_upstream(service_name, instances)
            
        except Exception as e:
            logger.error(f"Error creating Kong service {service_name}: {e}")
            
    def create_kong_route(self, service_name: str):
        """Create a route for a Kong service"""
        try:
            # Get the path for this service
            path = self.service_routes.get(service_name, f"/{service_name}")
            
            route_data = {
                'name': f"{service_name}-route",
                'service': {'name': service_name},
                'paths': [path],
                'strip_path': False,
                'preserve_host': True,
                'regex_priority': 0,
                'methods': ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'],
                'protocols': ['http', 'https'],
                'tags': [service_name, 'consul-sync']
            }
            
            # Create or update route
            response = requests.put(
                f"{self.kong_admin_url}/routes/{service_name}-route",
                json=route_data
            )
            response.raise_for_status()
            
            logger.info(f"Created/updated Kong route: {service_name}-route -> {path}")
            
        except Exception as e:
            logger.error(f"Error creating Kong route for {service_name}: {e}")
            
    def create_kong_upstream(self, service_name: str, instances: List[Dict]):
        """Create upstream and targets for load balancing"""
        try:
            # Create upstream
            upstream_data = {
                'name': service_name,
                'algorithm': 'round-robin',
                'hash_on': 'none',
                'healthchecks': {
                    'active': {
                        'type': 'http',
                        'http_path': '/health',
                        'healthy': {
                            'interval': 10,
                            'successes': 2
                        },
                        'unhealthy': {
                            'interval': 5,
                            'http_failures': 3
                        }
                    }
                },
                'tags': [service_name, 'consul-sync']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/upstreams/{service_name}",
                json=upstream_data
            )
            response.raise_for_status()
            
            # Add targets for each instance
            for instance in instances:
                host = instance['Service']['Address'] or instance['Node']['Address']
                port = instance['Service']['Port']
                
                target_data = {
                    'target': f"{host}:{port}",
                    'weight': 100,
                    'tags': [service_name, 'consul-sync']
                }
                
                response = requests.post(
                    f"{self.kong_admin_url}/upstreams/{service_name}/targets",
                    json=target_data
                )
                response.raise_for_status()
                
            logger.info(f"Created/updated Kong upstream: {service_name} with {len(instances)} targets")
            
        except Exception as e:
            logger.error(f"Error creating Kong upstream for {service_name}: {e}")
            
    def delete_kong_service(self, service_name: str):
        """Delete a Kong service and its routes"""
        try:
            # Delete routes first
            response = requests.delete(f"{self.kong_admin_url}/routes/{service_name}-route")
            if response.status_code == 204:
                logger.info(f"Deleted Kong route: {service_name}-route")
                
            # Delete upstream
            response = requests.delete(f"{self.kong_admin_url}/upstreams/{service_name}")
            if response.status_code == 204:
                logger.info(f"Deleted Kong upstream: {service_name}")
                
            # Delete service
            response = requests.delete(f"{self.kong_admin_url}/services/{service_name}")
            if response.status_code == 204:
                logger.info(f"Deleted Kong service: {service_name}")
                
        except Exception as e:
            logger.error(f"Error deleting Kong service {service_name}: {e}")
            
    def configure_global_plugins(self):
        """Configure global Kong plugins"""
        try:
            # Rate limiting plugin
            rate_limit_config = {
                'name': 'rate-limiting',
                'config': {
                    'minute': 100,
                    'hour': 10000,
                    'policy': 'redis',
                    'fault_tolerant': True,
                    'redis_host': 'ignite',
                    'redis_port': 11211
                },
                'tags': ['global', 'consul-sync']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/plugins/global-rate-limiting",
                json=rate_limit_config
            )
            response.raise_for_status()
            logger.info("Configured global rate limiting")
            
            # Prometheus plugin
            prometheus_config = {
                'name': 'prometheus',
                'config': {
                    'per_consumer': True,
                    'status_code_metrics': True,
                    'latency_metrics': True,
                    'bandwidth_metrics': True,
                    'upstream_health_metrics': True
                },
                'tags': ['global', 'consul-sync']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/plugins/global-prometheus",
                json=prometheus_config
            )
            response.raise_for_status()
            logger.info("Configured Prometheus metrics")
            
            # CORS plugin
            cors_config = {
                'name': 'cors',
                'config': {
                    'origins': ['*'],
                    'methods': ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
                    'headers': ['Accept', 'Authorization', 'Content-Type', 'X-Requested-With'],
                    'exposed_headers': ['X-Auth-Token'],
                    'credentials': True,
                    'max_age': 3600
                },
                'tags': ['global', 'consul-sync']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/plugins/global-cors",
                json=cors_config
            )
            response.raise_for_status()
            logger.info("Configured CORS")
            
        except Exception as e:
            logger.error(f"Error configuring global plugins: {e}")
            
    def sync(self):
        """Sync Consul services to Kong"""
        logger.info("Starting sync...")
        
        # Get services from both systems
        consul_services = self.get_consul_services()
        kong_services = self.get_kong_services()
        
        consul_service_names = set(consul_services.keys())
        
        # Create/update services in Kong
        for service_name, instances in consul_services.items():
            self.create_kong_service(service_name, instances)
            
        # Remove services from Kong that are no longer in Consul
        services_to_remove = kong_services - consul_service_names - self.excluded_services
        for service_name in services_to_remove:
            if not any(service_name.startswith(excluded) for excluded in self.excluded_services):
                self.delete_kong_service(service_name)
                
        logger.info(f"Sync completed. Active services: {len(consul_services)}")
        
    def run(self):
        """Run the sync loop"""
        logger.info("Kong-Consul sync started")
        
        # Configure global plugins on startup
        self.configure_global_plugins()
        
        while True:
            try:
                self.sync()
            except Exception as e:
                logger.error(f"Sync error: {e}")
                
            time.sleep(self.sync_interval)


if __name__ == "__main__":
    sync = KongConsulSync()
    sync.run() 