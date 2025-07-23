# Digital Integration Hub (DIH) Service

High-performance, in-memory data integration layer built on Apache Ignite for the DataIntelligenceSuite.

## Overview

The DIH Service provides a unified, low-latency data access layer that aggregates data from multiple sources and serves it through high-performance APIs. It acts as a caching and integration layer between backend data stores and consuming applications.

## Features

### Core Capabilities

- **In-Memory Caching**: Leverages Apache Ignite for distributed in-memory storage
- **Multi-Source Integration**: Connects to PostgreSQL, Cassandra, Elasticsearch, MongoDB, and JanusGraph
- **Dynamic Credentials**: Secure database access with Vault-managed credentials
- **Cache Strategies**: Supports cache-aside, read-through, write-through, and write-behind patterns
- **Real-Time Sync**: CDC (Change Data Capture) for real-time data synchronization
- **ACID Transactions**: Full transaction support for critical operations

### Performance Features

- **Partitioned Caching**: Horizontal scaling across nodes
- **Replicated Caching**: High availability for critical data
- **Eviction Policies**: LRU, LFU, FIFO, and custom policies
- **TTL Support**: Time-based expiration for temporal data
- **Bulk Operations**: Efficient batch loading and updates

### Management Features

- **Health Monitoring**: Comprehensive health checks for all components
- **Cache Statistics**: Hit rates, memory usage, and performance metrics
- **Cache Warming**: Pre-load frequently accessed data
- **Auto-Optimization**: Automatic cache tuning based on usage patterns

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        DIH Service                           │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Cache     │  │    Region    │  │      Sync        │  │
│  │    API      │  │  Management  │  │   Orchestrator   │  │
│  └──────┬──────┘  └──────┬───────┘  └────────┬─────────┘  │
│         │                 │                    │             │
│  ┌──────┴─────────────────┴───────────────────┴─────────┐  │
│  │              Cache Manager & DIH Core                 │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │                                   │
│  ┌───────────────────────┴───────────────────────────────┐  │
│  │                  Apache Ignite                         │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                              │
     ┌────────────────────────┼────────────────────────┐
     │                        │                        │
┌────┴────┐           ┌───────┴────┐           ┌──────┴──────┐
│Postgres │           │ Cassandra  │           │Elasticsearch│
└─────────┘           └────────────┘           └─────────────┘
```

## API Endpoints

### Cache Operations

- `GET /api/v1/cache/{region}/{key}` - Get cache entry
- `PUT /api/v1/cache/{region}` - Put cache entry
- `POST /api/v1/cache/{region}/bulk` - Bulk put entries
- `POST /api/v1/cache/{region}/query` - Query multiple entries
- `DELETE /api/v1/cache/{region}/{key}` - Delete entry
- `DELETE /api/v1/cache/{region}` - Clear region
- `GET /api/v1/cache/{region}/stats` - Get cache statistics
- `POST /api/v1/cache/{region}/warm-up` - Start cache warm-up
- `POST /api/v1/cache/{region}/optimize` - Optimize cache configuration

### Region Management

- `GET /api/v1/regions` - List all regions
- `GET /api/v1/regions/{name}` - Get region info
- `POST /api/v1/regions` - Create new region
- `PUT /api/v1/regions/{name}` - Update region
- `DELETE /api/v1/regions/{name}` - Delete region

### Sync Management

- `GET /api/v1/sync/tasks` - List sync tasks
- `POST /api/v1/sync/tasks` - Create sync task
- `GET /api/v1/sync/tasks/{id}` - Get task status
- `POST /api/v1/sync/tasks/{id}/run` - Trigger sync
- `DELETE /api/v1/sync/tasks/{id}` - Delete task

### Health Monitoring

- `GET /health` - Overall service health
- `GET /api/v1/health/ignite` - Ignite cluster health
- `GET /api/v1/health/cache-regions` - Cache regions health
- `GET /api/v1/health/sync` - Sync services health
- `GET /api/v1/health/data-sources` - Data sources health

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=dih-service
SERVICE_PORT=8000

# Vault Configuration
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<your-token>

# Consul Configuration
CONSUL_ADDR=http://consul:8500

# Ignite Configuration
IGNITE_NODES=ignite:10800,ignite2:10800
```

### Cache Region Configuration

```python
# Example region creation
{
    "name": "user-sessions",
    "cache_mode": "PARTITIONED",
    "backups": 1,
    "eviction_policy": "LRU",
    "eviction_max_size": 10000,
    "ttl_seconds": 3600,
    "read_through": true,
    "data_source": "postgres"
}
```

## Usage Examples

### Basic Cache Operations

```python
import httpx

# Put entry
response = await client.put(
    "http://dih-service:8000/api/v1/cache/user-sessions",
    json={
        "key": "user:123",
        "value": {"id": 123, "name": "John Doe"},
        "ttl_seconds": 3600
    }
)

# Get entry
response = await client.get(
    "http://dih-service:8000/api/v1/cache/user-sessions/user:123"
)
```

### Bulk Operations

```python
# Bulk load
response = await client.post(
    "http://dih-service:8000/api/v1/cache/asset-metadata/bulk",
    json={
        "entries": [
            {"key": "asset:1", "value": {...}},
            {"key": "asset:2", "value": {...}},
            {"key": "asset:3", "value": {...}}
        ]
    }
)
```

### Cache Warming

```python
# Start cache warm-up from database
response = await client.post(
    "http://dih-service:8000/api/v1/cache/product-catalog/warm-up",
    json={
        "data_source": "postgres",
        "query": "SELECT id, data FROM products WHERE active = true",
        "refresh_interval": 3600  # Refresh every hour
    }
)
```

## Deployment

### Docker

```bash
docker build -t dih-service .
docker run -p 8000:8000 \
  -e VAULT_TOKEN=$VAULT_TOKEN \
  -e IGNITE_NODES=ignite:10800 \
  dih-service
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dih-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: dih-service
  template:
    metadata:
      labels:
        app: dih-service
    spec:
      containers:
      - name: dih-service
        image: dih-service:latest
        ports:
        - containerPort: 8000
        env:
        - name: VAULT_TOKEN
          valueFrom:
            secretKeyRef:
              name: vault-token
              key: token
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `data_intelligence_cache_hits_total`
- `data_intelligence_cache_misses_total`
- `data_intelligence_cache_evictions_total`
- `data_intelligence_cache_memory_bytes`
- `data_intelligence_sync_tasks_total`
- `data_intelligence_sync_errors_total`

## Development

### Setup

```bash
cd services/DataIntelligenceSuite/dih-service
pip install -r requirements.in
```

### Running Tests

```bash
pytest tests/ -v --cov=app
```

### Local Development

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 