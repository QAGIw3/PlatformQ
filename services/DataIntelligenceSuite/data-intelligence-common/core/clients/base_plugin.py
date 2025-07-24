"""
Base Plugin Architecture for Service Clients

Provides a plugin-based architecture to reduce code duplication across service clients.
"""

from typing import Any, Dict, List, Optional, Type, Callable, Union
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import importlib
import inspect

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import StructuredLogger, MetricsCollector
from ...clients.base_client import BaseServiceClient, ClientConfig

logger = StructuredLogger.get_logger(__name__)


class PluginCapability(str, Enum):
    """Standard plugin capabilities"""
    CRUD = "crud"
    BATCH = "batch"
    STREAM = "stream"
    QUERY = "query"
    TRANSACTION = "transaction"
    BULK = "bulk"
    SEARCH = "search"
    ANALYTICS = "analytics"


@dataclass
class PluginMetadata:
    """Plugin metadata"""
    name: str
    version: str
    description: str
    author: str
    capabilities: List[PluginCapability]
    dependencies: List[str] = field(default_factory=list)
    config_schema: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "capabilities": [c.value for c in self.capabilities],
            "dependencies": self.dependencies,
            "config_schema": self.config_schema
        }


class ClientPlugin(ABC):
    """
    Base plugin interface for all service clients.
    
    Plugins implement specific client functionality while
    inheriting common patterns from the base.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._initialized = False
        self._metrics = {}
        
    def get_metadata(self) -> PluginMetadata:
        """Get plugin metadata"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_metadata method"
        )
    
    async def initialize(self, vault_client: Optional[VaultClient] = None,
                        consul_client: Optional[ConsulClient] = None) -> None:
        """Initialize plugin with optional Vault/Consul clients"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement initialize method"
        )
    
    async def connect(self, connection_params: Dict[str, Any]) -> None:
        """Connect to the service"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement connect method"
        )
    
    async def disconnect(self) -> None:
        """Disconnect from the service"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement disconnect method"
        )
    
    async def health_check(self) -> bool:
        """Check service health"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement health_check method"
        )
    
    async def execute(self, operation: str, **kwargs) -> Any:
        """Execute an operation"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement execute method"
        )
    
    async def validate_config(self) -> bool:
        """Validate plugin configuration"""
        metadata = self.get_metadata()
        schema = metadata.config_schema
        
        # Basic schema validation
        for key, value_type in schema.items():
            if key not in self.config:
                logger.error(f"Missing required config: {key}")
                return False
            
            if not isinstance(self.config[key], value_type):
                logger.error(f"Invalid type for {key}: expected {value_type}")
                return False
        
        return True
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get plugin metrics"""
        return self._metrics
    
    def record_metric(self, name: str, value: Any):
        """Record a metric"""
        self._metrics[name] = value


class PluginRegistry:
    """
    Dynamic plugin registry for service clients.
    
    Manages plugin discovery, registration, and instantiation.
    """
    
    def __init__(self):
        self._plugins: Dict[str, Type[ClientPlugin]] = {}
        self._instances: Dict[str, ClientPlugin] = {}
        self._metadata: Dict[str, PluginMetadata] = {}
        
    def register_plugin(self, name: str, plugin_class: Type[ClientPlugin]):
        """Register a plugin"""
        if not issubclass(plugin_class, ClientPlugin):
            raise ValueError(f"Plugin must inherit from ClientPlugin: {plugin_class}")
        
        # Get metadata
        temp_instance = plugin_class({})
        metadata = temp_instance.get_metadata()
        
        self._plugins[name] = plugin_class
        self._metadata[name] = metadata
        
        logger.info(f"Registered plugin: {name} v{metadata.version}")
    
    def discover_plugins(self, package_path: str):
        """Discover and register plugins from a package"""
        try:
            package = importlib.import_module(package_path)
            
            for name, obj in inspect.getmembers(package):
                if (inspect.isclass(obj) and 
                    issubclass(obj, ClientPlugin) and 
                    obj != ClientPlugin):
                    
                    plugin_name = obj.__name__.lower().replace("plugin", "")
                    self.register_plugin(plugin_name, obj)
                    
        except Exception as e:
            logger.error(f"Failed to discover plugins from {package_path}: {e}")
    
    def list_plugins(self) -> List[PluginMetadata]:
        """List all registered plugins"""
        return list(self._metadata.values())
    
    def get_plugin(self, name: str) -> Optional[Type[ClientPlugin]]:
        """Get a plugin class"""
        return self._plugins.get(name)
    
    def create_instance(
        self,
        name: str,
        config: Dict[str, Any],
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ) -> ClientPlugin:
        """Create a plugin instance"""
        if name not in self._plugins:
            raise ValueError(f"Plugin not found: {name}")
        
        plugin_class = self._plugins[name]
        instance = plugin_class(config)
        
        # Store instance
        instance_key = f"{name}_{id(instance)}"
        self._instances[instance_key] = instance
        
        return instance
    
    def get_plugins_by_capability(
        self,
        capability: PluginCapability
    ) -> List[PluginMetadata]:
        """Get plugins with specific capability"""
        return [
            metadata
            for metadata in self._metadata.values()
            if capability in metadata.capabilities
        ]


class EnhancedServiceClient(BaseServiceClient):
    """
    Enhanced service client using plugin architecture.
    
    Reduces code duplication by delegating to plugins.
    """
    
    def __init__(
        self,
        plugin_name: str,
        config: ClientConfig,
        plugin_config: Optional[Dict[str, Any]] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        super().__init__(config, vault_client, consul_client, **kwargs)
        
        self.plugin_name = plugin_name
        self.plugin_config = plugin_config or {}
        self._plugin: Optional[ClientPlugin] = None
        
    async def connect(self):
        """Connect using plugin"""
        await super().connect()
        
        # Get plugin from registry
        registry = get_plugin_registry()
        self._plugin = registry.create_instance(
            self.plugin_name,
            self.plugin_config,
            self._vault_client,
            self._consul_client
        )
        
        # Initialize plugin
        await self._plugin.initialize(self._vault_client, self._consul_client)
        
        # Connect plugin
        connection_params = {
            "url": await self._get_service_url(),
            "credentials": await self._get_credentials() if self.config.use_vault_credentials else None,
            "ssl_context": self._create_ssl_context() if self.config.use_mtls else None
        }
        
        await self._plugin.connect(connection_params)
        
        logger.info(f"Connected using plugin: {self.plugin_name}")
    
    async def close(self):
        """Close connection"""
        if self._plugin:
            await self._plugin.disconnect()
        
        await super().close()
    
    async def health_check(self) -> bool:
        """Check health using plugin"""
        if not self._plugin:
            return False
        
        return await self._plugin.health_check()
    
    async def execute(self, operation: str, **kwargs) -> Any:
        """Execute operation using plugin"""
        if not self._plugin:
            raise RuntimeError("Plugin not initialized")
        
        # Add common parameters
        kwargs["timeout"] = kwargs.get("timeout", self.config.read_timeout)
        kwargs["retry_count"] = kwargs.get("retry_count", self.config.max_retries)
        
        # Execute through plugin
        return await self._plugin.execute(operation, **kwargs)
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get plugin-specific configuration"""
        if not self._plugin:
            return {}
        
        metadata = self._plugin.get_metadata()
        return {
            "plugin": metadata.to_dict(),
            "config": self.plugin_config,
            "metrics": self._plugin.get_metrics()
        }


# Global plugin registry
_plugin_registry: Optional[PluginRegistry] = None


def get_plugin_registry() -> PluginRegistry:
    """Get global plugin registry"""
    global _plugin_registry
    if not _plugin_registry:
        _plugin_registry = PluginRegistry()
    return _plugin_registry


def create_client(
    service_name: str,
    config: Optional[ClientConfig] = None,
    plugin_config: Optional[Dict[str, Any]] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None
) -> EnhancedServiceClient:
    """
    Factory function to create service clients.
    
    Args:
        service_name: Name of the service/plugin
        config: Client configuration
        plugin_config: Plugin-specific configuration
        vault_client: Vault client
        consul_client: Consul client
        
    Returns:
        Configured service client
    """
    if not config:
        config = ClientConfig(service_name=service_name)
    
    return EnhancedServiceClient(
        plugin_name=service_name,
        config=config,
        plugin_config=plugin_config,
        vault_client=vault_client,
        consul_client=consul_client
    )


# Example plugin implementation
class ExamplePlugin(ClientPlugin):
    """Example plugin implementation"""
    
    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="example",
            version="1.0.0",
            description="Example plugin implementation",
            author="PlatformQ",
            capabilities=[PluginCapability.CRUD, PluginCapability.QUERY],
            config_schema={
                "database": str,
                "pool_size": int
            }
        )
    
    async def initialize(self, vault_client: Optional[VaultClient] = None,
                        consul_client: Optional[ConsulClient] = None) -> None:
        """Initialize plugin"""
        self._vault_client = vault_client
        self._consul_client = consul_client
        self._initialized = True
    
    async def connect(self, connection_params: Dict[str, Any]) -> None:
        """Connect to service"""
        # Implementation specific connection logic
        self.record_metric("connection_time", datetime.now())
    
    async def disconnect(self) -> None:
        """Disconnect from service"""
        # Implementation specific disconnection logic
        pass
    
    async def health_check(self) -> bool:
        """Check service health"""
        # Implementation specific health check
        return True
    
    async def execute(self, operation: str, **kwargs) -> Any:
        """Execute operation"""
        # Implementation specific operation logic
        if operation == "query":
            return {"result": "example"}
        elif operation == "create":
            return {"id": "12345"}
        else:
            raise ValueError(f"Unknown operation: {operation}") 