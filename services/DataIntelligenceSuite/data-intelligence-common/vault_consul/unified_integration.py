"""
Unified Vault/Consul Integration for DataIntelligenceSuite

Provides a single interface for all Vault and Consul operations.
"""

import logging
from typing import Any, Dict, Optional, List, Callable
from datetime import datetime, timedelta
import asyncio
import json

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from .vault_integration import VaultIntegration
from .consul_integration import ConsulIntegration
from ..core.config import ConfigManager, ConfigSchema

logger = logging.getLogger(__name__)


class VaultConsulIntegration:
    """
    Unified interface for Vault and Consul operations.
    
    Combines VaultIntegration, ConsulIntegration, and ConfigManager
    to provide a single point of interaction for services.
    """
    
    def __init__(
        self,
        service_name: str,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        self.service_name = service_name
        self.config = config or {}
        
        # Initialize Vault integration
        self.vault = VaultIntegration(vault_client) if vault_client else None
        
        # Initialize Consul integration
        self.consul = ConsulIntegration(
            consul_client,
            service_name,
            config.get("service_port", 8080),
            config.get("service_tags", [])
        ) if consul_client else None
        
        # Initialize Config Manager
        self.config_manager = ConfigManager(
            service_name,
            consul_client,
            vault_client
        ) if consul_client else None
        
        # Combined state
        self._initialized = False
        self._health_check_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize all integrations"""
        try:
            # Initialize Vault
            if self.vault:
                await self.vault.initialize()
                
            # Initialize Consul
            if self.consul:
                await self.consul.initialize()
                
            # Initialize Config Manager
            if self.config_manager:
                await self.config_manager.initialize()
                
                # Register default schemas
                await self._register_default_schemas()
                
            # Start health reporting
            if self.consul and self.vault:
                self._health_check_task = asyncio.create_task(
                    self._health_check_loop()
                )
                
            self._initialized = True
            logger.info(f"Initialized Vault/Consul integration for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault/Consul integration: {e}")
            raise
            
    async def shutdown(self):
        """Shutdown all integrations"""
        # Stop health checks
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
                
        # Shutdown components
        if self.config_manager:
            await self.config_manager.shutdown()
            
        if self.consul:
            await self.consul.shutdown()
            
        if self.vault:
            await self.vault.shutdown()
            
        logger.info(f"Shutdown Vault/Consul integration for {self.service_name}")
        
    async def _register_default_schemas(self):
        """Register default configuration schemas"""
        schemas = [
            ConfigSchema(
                key="database/enabled",
                type=bool,
                default=True,
                description="Enable database connections"
            ),
            ConfigSchema(
                key="database/pool_size",
                type=int,
                default=10,
                description="Database connection pool size",
                validator=lambda x: 1 <= x <= 100
            ),
            ConfigSchema(
                key="cache/enabled",
                type=bool,
                default=True,
                description="Enable caching"
            ),
            ConfigSchema(
                key="cache/ttl_seconds",
                type=int,
                default=300,
                description="Default cache TTL in seconds"
            ),
            ConfigSchema(
                key="security/encryption_enabled",
                type=bool,
                default=True,
                description="Enable data encryption"
            ),
            ConfigSchema(
                key="security/api_key",
                type=str,
                encrypted=True,
                description="Service API key"
            ),
            ConfigSchema(
                key="monitoring/metrics_enabled",
                type=bool,
                default=True,
                description="Enable metrics collection"
            ),
            ConfigSchema(
                key="monitoring/trace_enabled",
                type=bool,
                default=True,
                description="Enable distributed tracing"
            )
        ]
        
        self.config_manager.register_schemas(schemas)
        
    # Vault Operations
    
    async def get_database_credentials(
        self,
        mount_path: str = "database",
        role: str = None
    ) -> Dict[str, str]:
        """Get dynamic database credentials"""
        if not self.vault:
            raise RuntimeError("Vault not configured")
            
        if not role:
            role = f"{self.service_name}-db"
            
        return await self.vault.get_database_credentials(mount_path, role)
        
    async def encrypt_data(self, key_name: str, plaintext: str) -> Dict[str, Any]:
        """Encrypt data using Transit engine"""
        if not self.vault:
            raise RuntimeError("Vault not configured")
            
        return await self.vault.transit_encrypt(key_name, plaintext)
        
    async def decrypt_data(self, key_name: str, ciphertext: str) -> str:
        """Decrypt data using Transit engine"""
        if not self.vault:
            raise RuntimeError("Vault not configured")
            
        return await self.vault.transit_decrypt(key_name, ciphertext)
        
    async def get_secret(self, path: str) -> Optional[Dict[str, Any]]:
        """Get secret from KV store"""
        if not self.vault:
            raise RuntimeError("Vault not configured")
            
        return await self.vault.get_secret(path)
        
    async def store_secret(
        self,
        path: str,
        data: Dict[str, Any],
        cas: Optional[int] = None
    ) -> Dict[str, Any]:
        """Store secret in KV store"""
        if not self.vault:
            raise RuntimeError("Vault not configured")
            
        return await self.vault.store_secret(path, data, cas)
        
    # Consul Operations
    
    async def discover_service(
        self,
        service_name: str,
        tag: Optional[str] = None,
        passing_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Discover service instances"""
        if not self.consul:
            raise RuntimeError("Consul not configured")
            
        return await self.consul.discover_service(service_name, tag, passing_only)
        
    async def get_service_url(
        self,
        service_name: str,
        tag: Optional[str] = None
    ) -> Optional[str]:
        """Get URL for a service"""
        instances = await self.discover_service(service_name, tag)
        if instances:
            instance = instances[0]  # Simple round-robin could be implemented
            return f"http://{instance['address']}:{instance['port']}"
        return None
        
    async def acquire_lock(
        self,
        key: str,
        ttl: int = 15
    ) -> Optional[str]:
        """Acquire distributed lock"""
        if not self.consul:
            raise RuntimeError("Consul not configured")
            
        return await self.consul.acquire_lock(key, ttl)
        
    async def release_lock(self, key: str, session_id: str) -> bool:
        """Release distributed lock"""
        if not self.consul:
            raise RuntimeError("Consul not configured")
            
        return await self.consul.release_lock(key, session_id)
        
    # Configuration Operations
    
    async def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        if not self.config_manager:
            raise RuntimeError("Config manager not configured")
            
        return await self.config_manager.get(key, default)
        
    async def set_config(self, key: str, value: Any, user: Optional[str] = None):
        """Set configuration value"""
        if not self.config_manager:
            raise RuntimeError("Config manager not configured")
            
        await self.config_manager.set(key, value, user)
        
    async def watch_config(
        self,
        key_pattern: str,
        callback: Callable,
        include_value: bool = True,
        recursive: bool = False
    ):
        """Watch for configuration changes"""
        if not self.config_manager:
            raise RuntimeError("Config manager not configured")
            
        self.config_manager.watch(key_pattern, callback, include_value, recursive)
        
    def register_config_schema(self, schema: ConfigSchema):
        """Register configuration schema"""
        if not self.config_manager:
            raise RuntimeError("Config manager not configured")
            
        self.config_manager.register_schema(schema)
        
    # Combined Operations
    
    async def get_secure_database_connection(
        self,
        database_type: str = "postgresql"
    ) -> Dict[str, Any]:
        """Get database connection with dynamic credentials"""
        # Get credentials from Vault
        creds = await self.get_database_credentials()
        
        # Get database host from service discovery
        db_instances = await self.discover_service(f"{database_type}-db")
        if not db_instances:
            raise RuntimeError(f"No {database_type} instances found")
            
        db_instance = db_instances[0]
        
        # Get additional config from Consul KV
        db_config = await self.get_config("database", {})
        
        return {
            "host": db_instance["address"],
            "port": db_instance["port"],
            "username": creds["username"],
            "password": creds["password"],
            "database": db_config.get("name", self.service_name),
            "pool_size": db_config.get("pool_size", 10),
            "ssl_enabled": db_config.get("ssl_enabled", True)
        }
        
    async def get_service_client_config(
        self,
        target_service: str
    ) -> Dict[str, Any]:
        """Get configuration for connecting to another service"""
        # Discover service
        instances = await self.discover_service(target_service)
        if not instances:
            raise RuntimeError(f"Service {target_service} not found")
            
        # Get service-specific config
        service_config = await self.get_config(f"services/{target_service}", {})
        
        # Get API key if needed
        api_key = None
        if service_config.get("auth_required"):
            api_key_secret = await self.get_secret(
                f"services/{target_service}/api-key"
            )
            if api_key_secret:
                api_key = api_key_secret.get("key")
                
        return {
            "instances": instances,
            "api_key": api_key,
            "timeout": service_config.get("timeout", 30),
            "retry_attempts": service_config.get("retry_attempts", 3),
            "circuit_breaker_enabled": service_config.get(
                "circuit_breaker_enabled", True
            )
        }
        
    async def _health_check_loop(self):
        """Report health status to Consul"""
        while True:
            try:
                # Check Vault health
                vault_healthy = True
                if self.vault:
                    try:
                        # Simple health check - try to read a path
                        await self.vault.vault_client.read_secret("sys/health")
                    except Exception:
                        vault_healthy = False
                        
                # Report to Consul
                if vault_healthy:
                    await self.consul.report_healthy()
                else:
                    await self.consul.report_unhealthy("Vault connection failed")
                    
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(10)
                
    def get_status(self) -> Dict[str, Any]:
        """Get integration status"""
        status = {
            "initialized": self._initialized,
            "service_name": self.service_name
        }
        
        if self.vault:
            status["vault"] = {
                "connected": self.vault.vault_client is not None,
                "leases": len(self.vault._active_leases),
                "cached_secrets": len(self.vault._secret_cache)
            }
            
        if self.consul:
            status["consul"] = {
                "registered": self.consul._registered,
                "session_id": self.consul._session_id,
                "health_status": "passing"  # Could be enhanced
            }
            
        if self.config_manager:
            status["config"] = self.config_manager.get_stats()
            
        return status


# Convenience class for service configuration
class DataServiceConfig:
    """Configuration helper for data services"""
    
    def __init__(self, vault_consul: VaultConsulIntegration):
        self.vault_consul = vault_consul
        
    async def get_cassandra_config(self) -> Dict[str, Any]:
        """Get Cassandra configuration"""
        return await self.vault_consul.get_secure_database_connection("cassandra")
        
    async def get_elasticsearch_config(self) -> Dict[str, Any]:
        """Get Elasticsearch configuration"""
        instances = await self.vault_consul.discover_service("elasticsearch")
        config = await self.vault_consul.get_config("elasticsearch", {})
        
        return {
            "hosts": [f"{i['address']}:{i['port']}" for i in instances],
            "use_ssl": config.get("use_ssl", True),
            "verify_certs": config.get("verify_certs", True)
        }
        
    async def get_pulsar_config(self) -> Dict[str, Any]:
        """Get Pulsar configuration"""
        instances = await self.vault_consul.discover_service("pulsar")
        config = await self.vault_consul.get_config("pulsar", {})
        
        broker_urls = [
            f"pulsar://{i['address']}:{i['port']}" for i in instances
        ]
        
        return {
            "broker_url": ",".join(broker_urls),
            "authentication": config.get("authentication"),
            "tls_enabled": config.get("tls_enabled", False)
        }
        
    async def get_ignite_config(self) -> Dict[str, Any]:
        """Get Ignite configuration"""
        instances = await self.vault_consul.discover_service("ignite")
        config = await self.vault_consul.get_config("ignite", {})
        
        return {
            "addresses": [(i["address"], i["port"]) for i in instances],
            "use_ssl": config.get("use_ssl", False),
            "cache_mode": config.get("cache_mode", "PARTITIONED"),
            "backups": config.get("backups", 1)
        } 