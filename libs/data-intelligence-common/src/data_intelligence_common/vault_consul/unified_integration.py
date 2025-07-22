"""Unified Vault and Consul integration for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Set, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import logging
import json
from enum import Enum

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from tenacity import retry, stop_after_attempt, wait_exponential

from .vault_integration import VaultIntegration
from .consul_integration import ConsulIntegration

logger = logging.getLogger(__name__)


@dataclass
class DataServiceConfig:
    """Common configuration for DataIntelligenceSuite services."""
    
    service_name: str
    service_version: str = "1.0.0"
    
    # Service discovery
    service_port: int = 8000
    health_check_path: str = "/health"
    health_check_interval: str = "10s"
    
    # Security
    enable_mtls: bool = True
    enable_encryption: bool = True
    enable_audit: bool = True
    
    # Performance
    max_concurrent_requests: int = 100
    request_timeout_seconds: int = 300
    cache_ttl_seconds: int = 3600
    
    # Data governance
    enable_lineage_tracking: bool = True
    enable_quality_monitoring: bool = True
    enable_compliance_checks: bool = True
    
    # Custom tags and metadata
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ServiceStatus(Enum):
    """Service health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class VaultConsulIntegration:
    """
    Unified Vault and Consul integration for DataIntelligenceSuite services.
    
    This class provides:
    - Service registration and discovery
    - Dynamic secrets management
    - Configuration management
    - Health checking
    - Distributed coordination
    - Encryption and security
    """
    
    def __init__(
        self,
        vault_client: VaultClient,
        consul_client: ConsulClient,
        config: DataServiceConfig
    ):
        self.vault = VaultIntegration(vault_client, config.service_name)
        self.consul = ConsulIntegration(consul_client, config.service_name)
        self.config = config
        
        # Service state
        self._initialized = False
        self._service_id: Optional[str] = None
        self._watchers: Dict[str, asyncio.Task] = {}
        self._active_leases: Dict[str, str] = {}
        
        # Configuration cache
        self._config_cache: Dict[str, Any] = {}
        self._config_version: int = 0
        
    async def initialize(self):
        """Initialize the Vault and Consul integration."""
        if self._initialized:
            logger.warning(f"Service {self.config.service_name} already initialized")
            return
            
        try:
            # Initialize Vault integration
            await self.vault.initialize()
            
            # Initialize Consul integration
            await self.consul.initialize()
            
            # Register service
            self._service_id = await self._register_service()
            
            # Load initial configuration
            await self._load_configuration()
            
            # Start configuration watchers
            await self._start_config_watchers()
            
            # Set up health checks
            await self._setup_health_checks()
            
            self._initialized = True
            logger.info(f"VaultConsul integration initialized for {self.config.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize VaultConsul integration: {e}")
            raise
            
    async def shutdown(self):
        """Gracefully shutdown the integration."""
        try:
            # Stop watchers
            for name, task in self._watchers.items():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                    
            # Deregister service
            if self._service_id:
                await self.consul.deregister_service(self._service_id)
                
            # Revoke active leases
            for lease_id in self._active_leases.values():
                try:
                    await self.vault.client.revoke_lease(lease_id)
                except Exception as e:
                    logger.error(f"Failed to revoke lease {lease_id}: {e}")
                    
            logger.info(f"VaultConsul integration shutdown for {self.config.service_name}")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            
    async def _register_service(self) -> str:
        """Register service with Consul."""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        # Build service definition
        service_def = ServiceDefinition(
            name=self.config.service_name,
            port=self.config.service_port,
            tags=self._build_service_tags(),
            meta=self._build_service_metadata(),
            check={
                "http": f"http://localhost:{self.config.service_port}{self.config.health_check_path}",
                "interval": self.config.health_check_interval,
                "timeout": "5s",
                "deregister_critical_service_after": "60s"
            }
        )
        
        # Register with Consul
        service_id = await self.consul.client.register_service(service_def)
        logger.info(f"Registered service {self.config.service_name} with ID {service_id}")
        
        return service_id
        
    def _build_service_tags(self) -> List[str]:
        """Build service tags for Consul registration."""
        tags = [
            "data-intelligence",
            f"version:{self.config.service_version}",
        ]
        
        if self.config.enable_mtls:
            tags.append("mtls-enabled")
        if self.config.enable_encryption:
            tags.append("encryption-enabled")
        if self.config.enable_lineage_tracking:
            tags.append("lineage-enabled")
            
        tags.extend(self.config.tags)
        
        return tags
        
    def _build_service_metadata(self) -> Dict[str, str]:
        """Build service metadata for Consul registration."""
        meta = {
            "version": self.config.service_version,
            "capabilities": self._get_service_capabilities(),
            "max_concurrent_requests": str(self.config.max_concurrent_requests),
            "request_timeout": str(self.config.request_timeout_seconds),
        }
        
        # Convert metadata values to strings
        for key, value in self.config.metadata.items():
            meta[key] = str(value)
            
        return meta
        
    def _get_service_capabilities(self) -> str:
        """Get service capabilities as a comma-separated string."""
        capabilities = []
        
        if self.config.enable_encryption:
            capabilities.append("encryption")
        if self.config.enable_lineage_tracking:
            capabilities.append("lineage")
        if self.config.enable_quality_monitoring:
            capabilities.append("quality")
        if self.config.enable_compliance_checks:
            capabilities.append("compliance")
            
        return ",".join(capabilities)
        
    async def _load_configuration(self):
        """Load configuration from Consul KV store."""
        try:
            # Load service-specific config
            service_config = await self.consul.get_config(
                f"data-intelligence/{self.config.service_name}/config"
            )
            if service_config:
                self._config_cache["service"] = service_config
                
            # Load common config
            common_config = await self.consul.get_config(
                "data-intelligence/common/config"
            )
            if common_config:
                self._config_cache["common"] = common_config
                
            self._config_version += 1
            logger.info(f"Loaded configuration version {self._config_version}")
            
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            
    async def _start_config_watchers(self):
        """Start configuration watchers."""
        # Watch service-specific config
        self._watchers["service_config"] = asyncio.create_task(
            self._watch_config(f"data-intelligence/{self.config.service_name}/config")
        )
        
        # Watch common config
        self._watchers["common_config"] = asyncio.create_task(
            self._watch_config("data-intelligence/common/config")
        )
        
    async def _watch_config(self, key: str):
        """Watch configuration changes."""
        while True:
            try:
                # Watch for changes
                await self.consul.watch_key(key, self._handle_config_change)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config {key}: {e}")
                await asyncio.sleep(5)
                
    async def _handle_config_change(self, key: str, value: Any):
        """Handle configuration changes."""
        logger.info(f"Configuration changed for key: {key}")
        await self._load_configuration()
        
    async def _setup_health_checks(self):
        """Set up health check endpoints."""
        # This would be implemented by the service using this integration
        pass
        
    # Dynamic credentials management
    @asynccontextmanager
    async def get_database_connection(
        self,
        database: str,
        role: str = "readonly"
    ) -> AsyncIterator[Any]:
        """Get database connection with dynamic credentials."""
        async with self.vault.get_database_credentials(database, role) as (creds, lease_id):
            # Store lease for tracking
            self._active_leases[f"{database}-{role}"] = lease_id
            
            try:
                # Create connection based on database type
                connection = await self._create_database_connection(database, creds)
                yield connection
            finally:
                # Clean up connection
                await self._close_database_connection(database, connection)
                
                # Remove lease tracking
                if f"{database}-{role}" in self._active_leases:
                    del self._active_leases[f"{database}-{role}"]
                    
    async def _create_database_connection(self, database: str, creds: Dict[str, Any]) -> Any:
        """Create database connection based on type."""
        username = creds["username"]
        password = creds["password"]
        
        if database == "postgres":
            import asyncpg
            return await asyncpg.connect(
                host=await self.get_config("databases.postgres.host", "postgres"),
                port=await self.get_config("databases.postgres.port", 5432),
                user=username,
                password=password,
                database=await self.get_config("databases.postgres.database", "platformq"),
                ssl="require"
            )
        elif database == "cassandra":
            from cassandra.cluster import Cluster
            from cassandra.auth import PlainTextAuthProvider
            
            auth_provider = PlainTextAuthProvider(username, password)
            cluster = Cluster(
                [await self.get_config("databases.cassandra.host", "cassandra")],
                auth_provider=auth_provider
            )
            return cluster.connect()
        elif database == "elasticsearch":
            from elasticsearch import AsyncElasticsearch
            
            return AsyncElasticsearch(
                [await self.get_config("databases.elasticsearch.url", "https://elasticsearch:9200")],
                basic_auth=(username, password),
                verify_certs=True
            )
        else:
            raise ValueError(f"Unsupported database type: {database}")
            
    async def _close_database_connection(self, database: str, connection: Any):
        """Close database connection based on type."""
        if not connection:
            return
            
        try:
            if database == "postgres":
                await connection.close()
            elif database == "cassandra":
                connection.shutdown()
            elif database == "elasticsearch":
                await connection.close()
        except Exception as e:
            logger.error(f"Error closing {database} connection: {e}")
            
    # Configuration access
    async def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        # Check service-specific config first
        if "service" in self._config_cache:
            value = self._get_nested_value(self._config_cache["service"], key)
            if value is not None:
                return value
                
        # Check common config
        if "common" in self._config_cache:
            value = self._get_nested_value(self._config_cache["common"], key)
            if value is not None:
                return value
                
        return default
        
    def _get_nested_value(self, data: Dict, key: str) -> Any:
        """Get nested value from dictionary using dot notation."""
        keys = key.split(".")
        value = data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None
                
        return value
        
    # Service discovery
    async def discover_service(self, service_name: str) -> List[Dict[str, Any]]:
        """Discover instances of a service."""
        return await self.consul.discover_service(service_name)
        
    async def get_service_url(self, service_name: str) -> str:
        """Get URL for a service."""
        instances = await self.discover_service(service_name)
        if not instances:
            raise ValueError(f"No instances found for service {service_name}")
            
        # Use first healthy instance
        instance = instances[0]
        return f"http://{instance['address']}:{instance['port']}"
        
    # Distributed coordination
    async def acquire_lock(self, key: str, ttl: int = 60) -> bool:
        """Acquire a distributed lock."""
        return await self.consul.acquire_lock(f"locks/{self.config.service_name}/{key}", ttl)
        
    async def release_lock(self, key: str):
        """Release a distributed lock."""
        await self.consul.release_lock(f"locks/{self.config.service_name}/{key}")
        
    # Health reporting
    async def report_health(self, status: ServiceStatus, message: str = ""):
        """Report service health status."""
        health_data = {
            "status": status.value,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "version": self.config.service_version
        }
        
        await self.consul.put_kv(
            f"health/{self.config.service_name}/{self._service_id}",
            json.dumps(health_data)
        )
        
    # Metrics and monitoring
    def get_integration_metrics(self) -> Dict[str, Any]:
        """Get integration metrics."""
        return {
            "config_version": self._config_version,
            "active_leases": len(self._active_leases),
            "active_watchers": len(self._watchers),
            "cache_size": len(self._config_cache),
            "service_id": self._service_id,
            "initialized": self._initialized
        } 