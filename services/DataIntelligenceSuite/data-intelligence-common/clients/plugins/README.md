# Client Plugin Architecture

This directory contains plugin implementations for various external services, organized by category for better maintainability and reduced code duplication.

## Overview

The plugin architecture provides a unified way to integrate with external services while maintaining consistency across all clients. Each plugin implements the `ClientPlugin` interface and can be used through the `EnhancedServiceClient`.

## Benefits

1. **Reduced Code Duplication**: Common patterns (retry, caching, monitoring) are handled by the base framework
2. **Consistent Interface**: All plugins follow the same interface pattern
3. **Dynamic Discovery**: Plugins are automatically discovered and registered
4. **Category Organization**: Plugins are organized by their primary function
5. **Enhanced Features**: Built-in support for Vault/Consul, monitoring, and resilience patterns

## Directory Structure

```
plugins/
├── data_stores/       # Database and data storage systems
│   ├── cassandra_plugin.py
│   ├── elasticsearch_plugin.py
│   ├── ignite_plugin.py
│   └── janusgraph_plugin.py
├── messaging/         # Messaging and streaming systems
│   ├── pulsar_plugin.py
│   ├── flink_plugin.py
│   └── flink_sql_plugin.py
├── analytics/         # Analytics and processing engines
│   ├── spark_plugin.py
│   ├── trino_plugin.py
│   └── druid_plugin.py
├── orchestration/     # Workflow and orchestration tools
│   ├── airflow_plugin.py
│   └── seatunnel_plugin.py
├── storage/           # Object and file storage
│   └── minio_plugin.py
├── governance/        # Data governance and metadata
│   ├── atlas_plugin.py
│   ├── datahub_plugin.py
│   └── openlineage_plugin.py
├── quality/           # Data quality tools
│   ├── great_expectations_plugin.py
│   ├── deequ_plugin.py
│   └── soda_core_plugin.py
└── realtime/          # Real-time analytics
    ├── clickhouse_plugin.py
    ├── doris_plugin.py
    └── pinot_plugin.py
```

## Usage

### Using a Plugin

```python
from data_intelligence_common.clients.factory import create_ignite_client

# Create client with plugin
async with create_ignite_client(
    config={
        "nodes": [("localhost", 10800)],
        "partition_aware": True
    },
    vault_client=vault_client,
    consul_client=consul_client
) as client:
    # Use the client
    await client.execute("put", cache_name="my_cache", key="key1", value="value1")
    result = await client.execute("get", cache_name="my_cache", key="key1")
```

### Creating a New Plugin

1. **Choose the appropriate category** for your plugin
2. **Create a new file** following the naming convention: `{service}_plugin.py`
3. **Implement the ClientPlugin interface**:

```python
from typing import Any, Dict, List, Optional
from datetime import datetime
from ...base_plugin import ClientPlugin, PluginMetadata, PluginCapability
from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class MyServicePlugin(ClientPlugin):
    """MyService client plugin."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._client = None
        self._connected = False
        
    def get_metadata(self) -> PluginMetadata:
        """Get plugin metadata"""
        return PluginMetadata(
            name="myservice",
            version="1.0.0",
            description="MyService client plugin",
            author="PlatformQ",
            capabilities=[
                PluginCapability.CRUD,
                PluginCapability.QUERY
            ],
            dependencies=["myservice-client>=1.0.0"],
            config_schema={
                "host": str,
                "port": int,
                "timeout": float
            }
        )
    
    async def initialize(self, vault_client=None, consul_client=None) -> None:
        """Initialize plugin with optional Vault/Consul clients"""
        self._vault_client = vault_client
        self._consul_client = consul_client
        
        # Service discovery via Consul
        if consul_client:
            services = await consul_client.get_service("myservice")
            if services:
                self.config["host"] = services[0]["ServiceAddress"]
                self.config["port"] = services[0]["ServicePort"]
        
        self._initialized = True
        
    async def connect(self, connection_params: Dict[str, Any]) -> None:
        """Connect to the service"""
        # Implement connection logic
        self._connected = True
        self.record_metric("connection_time", datetime.now())
        logger.info("Connected to MyService")
        
    async def disconnect(self) -> None:
        """Disconnect from the service"""
        # Implement disconnection logic
        self._connected = False
        logger.info("Disconnected from MyService")
        
    async def health_check(self) -> bool:
        """Check service health"""
        # Implement health check
        return self._connected
        
    async def execute(self, operation: str, **kwargs) -> Any:
        """Execute an operation"""
        operations = {
            "create": self._create,
            "read": self._read,
            "update": self._update,
            "delete": self._delete,
            "query": self._query
        }
        
        if operation not in operations:
            raise ValueError(f"Unknown operation: {operation}")
            
        return await operations[operation](**kwargs)
    
    # Implement operation methods
    async def _create(self, **kwargs):
        """Create operation"""
        pass
        
    async def _read(self, **kwargs):
        """Read operation"""
        pass
        
    # ... other operations


# Register the plugin
from . import register_plugin
register_plugin("myservice", MyServicePlugin)
```

## Plugin Capabilities

Plugins can declare their capabilities using the `PluginCapability` enum:

- `CRUD`: Basic Create, Read, Update, Delete operations
- `BATCH`: Batch/bulk operations
- `STREAM`: Streaming data support
- `QUERY`: Advanced query capabilities
- `TRANSACTION`: Transaction support
- `BULK`: Bulk import/export
- `SEARCH`: Search functionality
- `ANALYTICS`: Analytics operations

## Configuration

Plugins support configuration through:

1. **Direct configuration**: Pass config dict when creating client
2. **Consul configuration**: Store config in Consul KV
3. **Vault credentials**: Dynamic credentials from Vault
4. **Environment variables**: Fallback configuration

## Monitoring and Metrics

All plugins automatically:
- Record operation metrics
- Log operations with structured logging
- Support distributed tracing
- Provide health check endpoints

## Migration from Old Clients

To migrate from old integration clients:

1. Run the migration script:
   ```bash
   python scripts/migrate_to_plugins.py
   ```

2. Review generated plugin files
3. Complete TODO sections
4. Test thoroughly
5. Update service imports

## Best Practices

1. **Use the execute pattern**: All operations should go through the `execute` method
2. **Handle errors gracefully**: Use proper exception handling and logging
3. **Record metrics**: Use `record_metric` for important operations
4. **Support health checks**: Implement meaningful health checks
5. **Document operations**: Clearly document available operations and parameters
6. **Use type hints**: Add proper type hints for all methods
7. **Test thoroughly**: Write comprehensive tests for your plugin

## Testing

Test your plugin with:

```python
import pytest
from data_intelligence_common.clients.factory import create_client

@pytest.mark.asyncio
async def test_myservice_plugin():
    client = create_client("myservice", config={"host": "localhost"})
    
    async with client:
        # Test operations
        result = await client.execute("create", data={"key": "value"})
        assert result is not None
        
        # Test health check
        health = await client.health_check()
        assert health is True
``` 