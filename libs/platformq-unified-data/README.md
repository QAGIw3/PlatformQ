# PlatformQ Unified Data Access Layer

A comprehensive data abstraction layer that provides unified access to all data stores in the PlatformQ ecosystem.

## Features

- **Unified API**: Single interface for accessing Cassandra, Ignite, Elasticsearch, JanusGraph, MinIO, and more
- **Repository Pattern**: Clean separation of data access logic from business logic
- **Intelligent Caching**: Multi-level caching with Ignite for performance
- **Cross-Store Queries**: Federated queries across different data stores using Trino
- **Data Consistency**: Eventual consistency with configurable strategies
- **Query Optimization**: Automatic query routing and optimization
- **Tenant Isolation**: Built-in multi-tenancy support
- **Monitoring**: Prometheus metrics for all data operations

## Installation

```bash
pip install platformq-unified-data
```

## Quick Start

```python
from platformq_unified_data import UnifiedDataManager, Repository
from platformq_unified_data.models import DigitalAsset

# Initialize the data manager
data_manager = UnifiedDataManager(
    cassandra_hosts=['cassandra:9042'],
    ignite_hosts=['ignite:10800'],
    elasticsearch_hosts=['elasticsearch:9200'],
    minio_endpoint='minio:9000'
)

# Get a repository for digital assets
asset_repo = data_manager.get_repository(DigitalAsset)

# Create
asset = DigitalAsset(
    name="3D Model",
    type="model",
    owner_id="user123"
)
created_asset = await asset_repo.create(asset)

# Read
asset = await asset_repo.get_by_id("asset123")
assets = await asset_repo.find_by_owner("user123")

# Update
asset.name = "Updated Model"
await asset_repo.update(asset)

# Delete
await asset_repo.delete("asset123")

# Complex queries
results = await asset_repo.query()
    .filter(type="model")
    .filter(created_at__gte="2024-01-01")
    .order_by("-created_at")
    .limit(10)
    .execute()
```

## Architecture

The library follows a layered architecture:

1. **API Layer**: High-level repository interfaces
2. **Query Engine**: Query planning and optimization
3. **Storage Adapters**: Store-specific implementations
4. **Cache Layer**: Multi-level caching with Ignite
5. **Monitoring Layer**: Metrics and observability

## Data Models

Define your data models using the provided base classes:

```python
from platformq_unified_data.models import BaseModel
from platformq_unified_data.fields import StringField, DateTimeField

class MyModel(BaseModel):
    __tablename__ = "my_models"
    __keyspace__ = "my_keyspace"
    
    id = StringField(primary_key=True)
    name = StringField(required=True, indexed=True)
    created_at = DateTimeField(auto_now_add=True)
```

## Caching Strategy

The library implements a multi-level caching strategy:

1. **L1 Cache**: In-memory LRU cache (per service instance)
2. **L2 Cache**: Distributed Ignite cache (shared across services)
3. **L3 Storage**: Persistent storage (Cassandra, Elasticsearch, etc.)

Cache invalidation is handled automatically with configurable TTLs.

## Cross-Store Queries

For complex queries spanning multiple data stores:

```python
# Federated query across Cassandra, Elasticsearch, and MinIO
results = await data_manager.federated_query("""
    SELECT 
        c.asset_id,
        c.asset_name,
        e.search_score,
        m.file_size
    FROM cassandra.platformq.digital_assets c
    JOIN elasticsearch.default.assets e ON c.asset_id = e.asset_id
    JOIN minio.bronze.asset_metadata m ON c.asset_id = m.asset_id
    WHERE c.created_at > CURRENT_DATE - INTERVAL '7' DAY
""")
```

## Configuration

Configure via environment variables or code:

```python
config = {
    'cassandra': {
        'hosts': ['cassandra:9042'],
        'keyspace': 'platformq',
        'consistency': 'QUORUM'
    },
    'ignite': {
        'hosts': ['ignite:10800'],
        'cache_ttl': 3600
    },
    'elasticsearch': {
        'hosts': ['elasticsearch:9200'],
        'index_prefix': 'platformq_'
    }
}

data_manager = UnifiedDataManager(config=config)
```

## Monitoring

All operations are instrumented with Prometheus metrics:

- `platformq_data_operation_duration_seconds`: Operation latency
- `platformq_data_operation_errors_total`: Error count
- `platformq_data_cache_hits_total`: Cache hit rate
- `platformq_data_store_connections`: Active connections

## Contributing

See the main PlatformQ contributing guide for development setup and guidelines. 