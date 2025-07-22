# Data Intelligence Common Library

Common utilities, patterns, and components for all DataIntelligenceSuite services.

## Overview

The `data-intelligence-common` library provides standardized implementations for:

- **Base Service Template**: Common initialization, health checking, and lifecycle management
- **Vault/Consul Integration**: Unified secret management and service discovery
- **Monitoring & Logging**: Structured logging, metrics collection, and distributed tracing
- **Event Processing**: Base event processor and common event handlers

## Installation

```bash
pip install -e libs/data-intelligence-common
```

## Quick Start

### Creating a New DataIntelligenceSuite Service

```python
from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    VaultClient,
    ConsulClient
)

# Define service metadata
metadata = ServiceMetadata(
    name="my-data-service",
    version="1.0.0",
    description="My Data Intelligence Service",
    capabilities=["data-processing", "ml-inference"],
    dependencies=["data-platform-service", "ml-platform-service"],
    data_sources=["postgres", "cassandra"],
    data_outputs=["ignite", "minio"]
)

# Create clients (if using Vault/Consul)
vault_client = VaultClient(...)
consul_client = ConsulClient(...)

# Create FastAPI app with all common setup
app, service = create_data_intelligence_app(
    service_metadata=metadata,
    vault_client=vault_client,
    consul_client=consul_client
)

# Add your service-specific routes
@app.get("/api/v1/process")
async def process_data():
    # Your implementation
    pass
```

### Using the Base Service Class

```python
from data_intelligence_common import DataIntelligenceBaseService, ServiceMetadata

class MyDataService(DataIntelligenceBaseService):
    
    async def initialize_service(self):
        """Initialize service-specific components."""
        # Set up your service components
        self.data_processor = DataProcessor()
        await self.data_processor.initialize()
        
    async def cleanup_service(self):
        """Cleanup service-specific components."""
        # Clean up your service components
        await self.data_processor.shutdown()

# Create and run service
metadata = ServiceMetadata(...)
service = MyDataService(metadata, vault_client, consul_client)

app = FastAPI(lifespan=service.lifespan)
```

## Features

### 1. Vault/Consul Integration

Unified integration with HashiCorp Vault and Consul for:

- **Dynamic Secrets**: Database credentials with automatic rotation
- **Encryption**: Column-level encryption for sensitive data
- **Service Discovery**: Automatic service registration and discovery
- **Configuration Management**: Dynamic configuration with hot-reload
- **Distributed Locks**: For coordination across services

```python
# Get database connection with dynamic credentials
async with service.vault_consul.get_database_connection("postgres") as conn:
    # Use connection
    result = await conn.fetch("SELECT * FROM users")

# Service discovery
ml_service_url = await service.get_service_url("ml-platform-service")

# Distributed lock
if await service.vault_consul.acquire_lock("data-processing-job-1"):
    try:
        # Do exclusive work
        pass
    finally:
        await service.vault_consul.release_lock("data-processing-job-1")
```

### 2. Structured Logging

JSON-formatted structured logging with context propagation:

```python
from data_intelligence_common import get_logger

logger = get_logger(__name__)

# Log with context
logger.info("Processing data", dataset_id="ds-123", row_count=1000)

# Context manager for operations
async with logger.operation("data_transformation") as op_id:
    # Logs start, end, and duration automatically
    await transform_data()
```

### 3. Metrics Collection

Prometheus-compatible metrics with standard and custom metrics:

```python
# Automatic request tracking
service.metrics.track_request("GET", "/api/v1/data", 200, 0.123)

# Database query tracking
with service.metrics.timer("db_query_duration", {"database": "postgres"}):
    result = await db.query("SELECT * FROM datasets")

# Custom metrics
service.metrics.increment_counter("data_processed_bytes", {"format": "parquet"}, 1024)
```

### 4. Health Checking

Comprehensive health check system with custom checks:

```python
# Add custom health check
async def check_ml_service():
    try:
        response = await httpx.get(f"{ml_service_url}/health")
        return response.status_code == 200
    except:
        return False

service.health_manager.add_check("ml_service", check_ml_service)

# Health status available at /health endpoint
```

### 5. Event Processing

Base event processor for building event-driven services:

```python
from data_intelligence_common import BaseEventProcessor

class MyEventProcessor(BaseEventProcessor):
    
    async def register_handlers(self):
        # Register event handlers
        self.event_router.register_handler(
            "data.quality.check.completed",
            self.handle_quality_check
        )
        
    async def handle_quality_check(self, event_data):
        # Process event
        dataset_id = event_data["dataset_id"]
        quality_score = event_data["quality_score"]
        
        if quality_score < 0.8:
            await self.trigger_remediation(dataset_id)
```

## Common Patterns

### Service-to-Service Communication

```python
# Discover and call another service
async def call_ml_service(model_id: str):
    # Get service URL with automatic failover
    ml_url = await service.get_service_url("ml-platform-service")
    
    # Make request with tracing headers
    headers = {"X-Request-ID": request.state.request_id}
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{ml_url}/api/v1/predict",
            json={"model_id": model_id, "data": data},
            headers=headers
        )
    
    return response.json()
```

### Configuration Management

```python
# Get configuration with fallback
batch_size = await service.get_config("processing.batch_size", default=1000)

# Watch for configuration changes
async def on_config_change(key: str, value: Any):
    logger.info(f"Config changed: {key} = {value}")
    # Reload component with new config

await service.vault_consul.consul.watch_config(
    "processing/config",
    on_config_change
)
```

### Error Handling

```python
from data_intelligence_common import DataIntelligenceError

@app.exception_handler(DataIntelligenceError)
async def handle_service_error(request: Request, exc: DataIntelligenceError):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.code,
            "message": exc.message,
            "request_id": request.state.request_id,
            "service": service.metadata.name
        }
    )
```

## Best Practices

1. **Always use structured logging** with appropriate context
2. **Track all external calls** with metrics
3. **Implement health checks** for all dependencies
4. **Use distributed locks** for exclusive operations
5. **Handle configuration changes** gracefully
6. **Propagate request IDs** for tracing
7. **Use event-driven patterns** for loose coupling

## Development

### Running Tests

```bash
cd libs/data-intelligence-common
pytest tests/ -v --cov=src
```

### Code Quality

```bash
# Format code
black src/

# Lint code
ruff src/
```

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 