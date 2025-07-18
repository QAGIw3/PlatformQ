# PlatformQ Shared Library

This package contains shared utilities and base classes for PlatformQ microservices.

## Features

- **Base Service Implementation**: FastAPI base with health checks and OpenTelemetry integration
- **Event Processing Framework**: Standardized Pulsar event consumption with decorators
- **Repository Pattern**: Base repository classes for consistent data access
- **Service Client**: Resilient inter-service communication with circuit breakers
- **Resilience Patterns**: Retry, circuit breaker, rate limiting
- **Configuration Management**: Centralized settings from Consul/Vault
- **Third-party Clients**: Nextcloud, OpenProject, Zulip integrations

## Installation

```bash
pip install platformq-shared
```

## Quick Start

### 1. Event Processing Framework

Simplify Pulsar event consumption with decorators:

```python
from platformq_shared import EventProcessor, event_handler, ProcessingResult, ProcessingStatus
from platformq_events import AssetCreatedEvent, AssetUpdatedEvent

class AssetEventProcessor(EventProcessor):
    def __init__(self):
        super().__init__(
            service_name="my-service",
            pulsar_url="pulsar://localhost:6650"
        )
    
    @event_handler("persistent://platformq/*/asset-created-events", AssetCreatedEvent)
    async def handle_asset_created(self, event: AssetCreatedEvent, msg):
        # Process the event
        await self.index_asset(event)
        return ProcessingResult(status=ProcessingStatus.SUCCESS)
    
    @event_handler("persistent://platformq/*/asset-updated-events", AssetUpdatedEvent)
    async def handle_asset_updated(self, event: AssetUpdatedEvent, msg):
        # Process the event
        await self.update_index(event)
        return ProcessingResult(status=ProcessingStatus.SUCCESS)
    
    async def on_start(self):
        # Initialize resources
        pass
    
    async def on_stop(self):
        # Cleanup resources
        pass

# Usage
processor = AssetEventProcessor()
await processor.start()
```

### 2. Repository Pattern

Standardized database operations with query builder:

```python
from platformq_shared import BaseRepository
from sqlalchemy.orm import Session
from .models import Asset
from .schemas import AssetCreate, AssetUpdate

class AssetRepository(BaseRepository[Asset, AssetCreate, AssetUpdate]):
    def __init__(self, session: Session):
        super().__init__(session, Asset)
    
    async def find_by_type(self, asset_type: str):
        return self.query().filter(type=asset_type).order_by("created_at", descending=True).all()
    
    async def find_active(self, limit: int = 10):
        return self.query().filter(status="active").limit(limit).all()

# Usage
repo = AssetRepository(session)

# CRUD operations
asset = repo.add(AssetCreate(name="Model", type="3d"))
assets = repo.list(skip=0, limit=100)
asset = repo.update(asset_id, AssetUpdate(name="Updated Model"))
repo.delete(asset_id)

# Query builder
active_3d_assets = repo.query()\
    .filter(type="3d", status="active")\
    .order_by("created_at", descending=True)\
    .paginate(page=1, page_size=20)\
    .all()
```

### 3. Service Client

Resilient inter-service communication:

```python
from platformq_shared import ServiceClients, get_service_client
from .schemas import AssetResponse

# Using convenience class
client = ServiceClients.digital_asset()
asset = await client.get(
    f"/api/v1/assets/{asset_id}",
    response_model=AssetResponse
)

# Or get any service client
analytics_client = get_service_client("analytics-service")
results = await analytics_client.post(
    "/api/v1/query",
    data={"query": "SELECT * FROM metrics", "mode": "realtime"}
)

# With context manager for automatic cleanup
from platformq_shared import ServiceClientContext

async with ServiceClientContext("ml-service") as client:
    model = await client.get("/api/v1/models/latest")
```

### 4. Base Service Creation

Create production-ready services:

```python
from platformq_shared import create_base_app
from .deps import get_db, get_api_key_crud, get_user_crud, get_password_verifier

app = create_base_app(
    service_name="my-service",
    db_session_dependency=get_db,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier
)

# Your service is now configured with:
# - Structured JSON logging
# - OpenTelemetry tracing
# - Health check endpoints
# - Security dependencies
# - Prometheus metrics
```

### 5. Batch Event Processing

For efficient batch processing:

```python
from platformq_shared import BatchEventProcessor

class MetricsProcessor(BatchEventProcessor):
    def __init__(self):
        super().__init__(
            service_name="metrics-service",
            pulsar_url="pulsar://localhost:6650",
            batch_size=100,
            batch_timeout=5.0
        )
    
    @event_handler("persistent://platformq/*/metrics", MetricEvent)
    async def handle_metrics_batch(self, events: List[MetricEvent], messages):
        # Process entire batch at once
        await self.store_metrics_bulk(events)
        # All messages acknowledged automatically
```

### 6. Resilience Patterns

Built-in resilience for all service calls:

```python
from platformq_shared.resilience import with_resilience

@with_resilience(
    service_name="external-api",
    circuit_breaker_threshold=5,
    rate_limit=10.0,
    max_retries=3
)
async def call_external_api():
    # Your code here - automatic retry, circuit breaking, rate limiting
    pass
```

## Advanced Features

### Event Processing Modes

- **Standard**: Process events one by one with retries
- **Batch**: Process multiple events together for efficiency  
- **Composite**: Combine multiple processors in one service

### Repository Features

- **Query Builder**: Fluent API for complex queries
- **Bulk Operations**: Efficient batch inserts/updates
- **Async Support**: Full async/await repository implementation
- **Multiple Backends**: PostgreSQL, Cassandra support

### Service Client Features

- **Automatic Service Discovery**: Works with Kubernetes DNS
- **Circuit Breaker**: Prevents cascading failures
- **Rate Limiting**: Protects downstream services
- **Distributed Tracing**: OpenTelemetry integration
- **Connection Pooling**: Reuses HTTP connections
- **Type Safety**: Pydantic model support

## Configuration

Set these environment variables:

```bash
# Pulsar
PULSAR_URL=pulsar://localhost:6650

# Service Discovery  
SERVICE_DISCOVERY_ENABLED=true

# Resilience
CIRCUIT_BREAKER_THRESHOLD=5
RATE_LIMIT_PER_SECOND=100
MAX_RETRIES=3

# Tracing
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

## Testing

The library includes test utilities:

```python
from platformq_shared.testing import ServiceTestCase

class TestMyService(ServiceTestCase):
    def test_event_processing(self):
        # Use built-in mocks for Pulsar, Ignite, etc.
        pass
```

## Migration Guide

If you're updating existing services:

1. Replace manual Pulsar consumers with `EventProcessor`
2. Replace direct SQLAlchemy queries with repositories
3. Replace `httpx`/`requests` calls with `ServiceClient`
4. Use `create_base_app` instead of manual FastAPI setup

## Examples

See the `examples/` directory for complete service implementations using all features.

## Contributing

Please read CONTRIBUTING.md for development guidelines. 