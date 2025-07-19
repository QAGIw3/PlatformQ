"""Configuration manager with Consul and Vault integration for Compute Allocation Service."""

import json
import logging
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timedelta
import asyncio

import consul
import hvac
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration from Consul and secrets from Vault."""
    
    def __init__(self, service_name: str = "compute-allocation-service"):
        self.service_name = service_name
        self.consul_client = None
        self.vault_client = None
        self._config_cache: Dict[str, Any] = {}
        self._secret_cache: Dict[str, Any] = {}
        self._watchers: Dict[str, Callable] = {}
        self._last_index: Dict[str, int] = {}
        self._watch_tasks: Dict[str, asyncio.Task] = {}
        
    async def initialize(self, consul_config: Dict[str, Any], vault_config: Dict[str, Any]):
        """Initialize connections to Consul and Vault."""
        try:
            # Initialize Consul
            self.consul_client = consul.Consul(
                host=consul_config.get("host", "consul"),
                port=consul_config.get("port", 8500),
                token=consul_config.get("token")
            )
            
            # Test Consul connection
            self.consul_client.agent.self()
            logger.info("Connected to Consul")
            
            # Initialize Vault if enabled
            if vault_config.get("enabled", True):
                self.vault_client = hvac.Client(
                    url=vault_config["address"],
                    token=vault_config["token"]
                )
                
                if not self.vault_client.is_authenticated():
                    logger.error("Failed to authenticate with Vault")
                else:
                    logger.info("Connected to Vault")
                    
        except Exception as e:
            logger.error(f"Failed to initialize config manager: {e}")
            raise
    
    async def close(self):
        """Close connections and cancel watchers."""
        for task in self._watch_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        if self._watch_tasks:
            await asyncio.gather(*self._watch_tasks.values(), return_exceptions=True)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value from Consul."""
        try:
            # Check cache first
            if key in self._config_cache:
                return self._config_cache[key]
            
            # Get from Consul
            index, data = self.consul_client.kv.get(f"platformq/{self.service_name}/{key}")
            
            if data:
                value = json.loads(data['Value'].decode('utf-8'))
                self._config_cache[key] = value
                self._last_index[key] = index
                return value
            
            return default
            
        except Exception as e:
            logger.error(f"Failed to get config {key}: {e}")
            return default
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def set_config(self, key: str, value: Any) -> bool:
        """Set configuration value in Consul."""
        try:
            consul_key = f"platformq/{self.service_name}/{key}"
            encoded_value = json.dumps(value).encode('utf-8')
            
            success = self.consul_client.kv.put(consul_key, encoded_value)
            
            if success:
                self._config_cache[key] = value
                logger.info(f"Set config {key}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to set config {key}: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_secret(self, path: str, key: Optional[str] = None) -> Any:
        """Get secret from Vault."""
        if not self.vault_client or not self.vault_client.is_authenticated():
            logger.error("Vault client not authenticated")
            return None
        
        try:
            # Check cache first (with TTL)
            cache_key = f"{path}:{key}" if key else path
            if cache_key in self._secret_cache:
                cached = self._secret_cache[cache_key]
                if cached['expires'] > datetime.utcnow():
                    return cached['value']
            
            # Get from Vault
            mount_path = "secret"  # Default KV v2 mount
            response = self.vault_client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=mount_path
            )
            
            if response:
                data = response['data']['data']
                value = data.get(key) if key else data
                
                # Cache with TTL
                self._secret_cache[cache_key] = {
                    'value': value,
                    'expires': datetime.utcnow() + timedelta(minutes=5)
                }
                
                return value
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get secret {path}: {e}")
            return None
    
    async def watch_config(self, key: str, callback: Callable[[Any], None]):
        """Watch a configuration key for changes."""
        self._watchers[key] = callback
        
        async def watch_loop():
            consul_key = f"platformq/{self.service_name}/{key}"
            index = self._last_index.get(key, 0)
            
            while True:
                try:
                    # Long poll for changes
                    new_index, data = self.consul_client.kv.get(
                        consul_key,
                        index=index,
                        wait='30s'
                    )
                    
                    if new_index != index:
                        index = new_index
                        self._last_index[key] = index
                        
                        if data:
                            value = json.loads(data['Value'].decode('utf-8'))
                            self._config_cache[key] = value
                            
                            # Call callback
                            await asyncio.create_task(
                                asyncio.coroutine(callback)(value)
                            )
                        
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error watching config {key}: {e}")
                    await asyncio.sleep(5)
        
        # Start watch task
        self._watch_tasks[key] = asyncio.create_task(watch_loop())
    
    async def register_service(self, service_host: str, service_port: int, health_check_url: str = "/health"):
        """Register service with Consul."""
        try:
            service_def = {
                "ID": f"{self.service_name}-{service_host}:{service_port}",
                "Name": self.service_name,
                "Tags": [
                    "compute-allocation",
                    "api"
                ],
                "Address": service_host,
                "Port": service_port,
                "Check": {
                    "HTTP": f"http://{service_host}:{service_port}{health_check_url}",
                    "Interval": "10s",
                    "Timeout": "5s"
                }
            }
            
            self.consul_client.agent.service.register(service_def)
            logger.info(f"Registered service with Consul: {service_def['ID']}")
            
        except Exception as e:
            logger.error(f"Failed to register service: {e}")
    
    async def deregister_service(self, service_host: str, service_port: int):
        """Deregister service from Consul."""
        try:
            service_id = f"{self.service_name}-{service_host}:{service_port}"
            self.consul_client.agent.service.deregister(service_id)
            logger.info(f"Deregistered service: {service_id}")
            
        except Exception as e:
            logger.error(f"Failed to deregister service: {e}")
    
    async def get_service_endpoints(self, service_name: str) -> List[Dict[str, Any]]:
        """Get healthy endpoints for a service from Consul."""
        try:
            _, services = self.consul_client.health.service(
                service_name,
                passing=True  # Only healthy services
            )
            
            endpoints = []
            for service in services:
                endpoints.append({
                    "address": service['Service']['Address'],
                    "port": service['Service']['Port'],
                    "tags": service['Service']['Tags']
                })
            
            return endpoints
            
        except Exception as e:
            logger.error(f"Failed to get service endpoints for {service_name}: {e}")
            return []
    
    async def get_provider_credentials(self, provider: str) -> Dict[str, Any]:
        """Get provider credentials from Vault."""
        path = f"providers/{provider}/credentials"
        return await self.get_secret(path) or {}
    
    async def get_database_credentials(self, database: str) -> Dict[str, Any]:
        """Get database credentials from Vault."""
        path = f"database/{database}/credentials"
        return await self.get_secret(path) or {} 