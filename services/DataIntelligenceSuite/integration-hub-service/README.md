# Integration Hub Service

A unified integration platform combining GraphQL federation, graph analytics, and high-performance data integration using Apache Ignite, JanusGraph, and Apache Spark.

## Overview

The Integration Hub Service provides:
- **GraphQL Gateway**: Unified GraphQL API with schema federation across all services
- **Graph Analytics**: Large-scale graph processing with JanusGraph and Apache Spark GraphX
- **Digital Integration Hub (DIH)**: High-performance in-memory data integration with Apache Ignite
- **Temporal Analysis**: Time-aware reasoning and pattern detection
- **Trust Networks**: Trust score calculation and propagation
- **Data Lineage**: Comprehensive lineage tracking and impact analysis

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Integration Hub Service                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌─────────────────┐  ┌───────────────┐ │
│  │   GraphQL    │  │      Graph      │  │     DIH       │ │
│  │   Gateway    │  │    Analytics    │  │   (Ignite)    │ │
│  └──────────────┘  └─────────────────┘  └───────────────┘ │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                    Core Features                         ││
│  │  • Schema Federation    • PageRank & Community Detection ││
│  │  • Query Optimization   • Temporal Pattern Analysis      ││
│  │  • Real-time Updates    • Trust Score Calculation       ││
│  │  • DataLoader Batching  • Lineage & Impact Analysis     ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Features

### GraphQL Gateway
- Unified GraphQL API for all platform services
- Schema federation with automatic stitching
- Query optimization and batching with DataLoaders
- Real-time subscriptions via WebSockets
- Field-level authorization and rate limiting

### Graph Analytics
- **Algorithms**: PageRank, community detection, centrality measures, shortest paths
- **Temporal Analysis**: Time-aware graph operations and pattern detection
- **Trust Networks**: Trust score calculation and trust path finding
- **Lineage Tracking**: Data lineage visualization and impact analysis
- **Custom Algorithms**: Support for user-defined graph algorithms

### Digital Integration Hub
- In-memory caching with Apache Ignite
- Multi-source data aggregation
- CDC (Change Data Capture) synchronization
- Transaction support with ACID guarantees
- API acceleration and query optimization

## API Endpoints

### GraphQL
- `POST /graphql` - Main GraphQL endpoint
- `GET /api/v1/graphql/schema` - Get federated schema
- `GET /api/v1/graphql/federation/status` - Federation health
- `GET /api/v1/graphql/playground` - GraphQL Playground UI

### Graph Operations
- `POST /api/v1/graph/vertices` - Create vertex
- `POST /api/v1/graph/edges` - Create edge
- `POST /api/v1/graph/query` - Execute Gremlin query
- `POST /api/v1/graph/analytics` - Run analytics algorithm
- `GET /api/v1/graph/lineage/{entity_id}` - Get data lineage
- `GET /api/v1/graph/trust/score` - Calculate trust score

### Cache Management
- `GET /api/v1/cache/{region}/{key}` - Get cached value
- `PUT /api/v1/cache/{region}/{key}` - Set cached value
- `DELETE /api/v1/cache/{region}/{key}` - Delete cached value
- `POST /api/v1/regions` - Create cache region
- `POST /api/v1/sync/jobs` - Create sync job

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=integration-hub-service
SERVICE_PORT=8010
LOG_LEVEL=INFO

# GraphQL Configuration
GRAPHQL_MAX_DEPTH=10
GRAPHQL_MAX_TOKENS=1000
GRAPHQL_ENABLE_FEDERATION=true

# JanusGraph Configuration
JANUSGRAPH_URL=ws://janusgraph:8182/gremlin
JANUSGRAPH_TIMEOUT=30000

# Spark Configuration
SPARK_MASTER=spark://spark-master:7077
SPARK_EXECUTOR_MEMORY=4g

# Ignite Configuration
IGNITE_NODES=ignite:10800

# Vault/Consul
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<token>
CONSUL_HOST=consul
CONSUL_PORT=8500
```

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
python -m uvicorn app.main:app --reload --port 8010
```

### Running Tests

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run with coverage
pytest --cov=app tests/
```

## Deployment

### Docker

```bash
# Build image
docker build -t integration-hub-service .

# Run container
docker run -p 8010:8010 \
  -e VAULT_TOKEN=$VAULT_TOKEN \
  -e JANUSGRAPH_URL=ws://janusgraph:8182/gremlin \
  -e SPARK_MASTER=spark://spark-master:7077 \
  integration-hub-service
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: integration-hub-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: integration-hub-service
  template:
    metadata:
      labels:
        app: integration-hub-service
    spec:
      containers:
      - name: integration-hub-service
        image: platformq/integration-hub-service:latest
        ports:
        - containerPort: 8010
        env:
        - name: JANUSGRAPH_URL
          value: ws://janusgraph:8182/gremlin
        - name: SPARK_MASTER
          value: spark://spark-master:7077
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `graphql_queries_total` - Total GraphQL queries
- `graphql_query_duration_seconds` - Query execution time
- `graph_vertices_created_total` - Vertices created
- `graph_analytics_jobs_total` - Analytics jobs run
- `cache_hits_total` - Cache hit rate
- `sync_jobs_completed_total` - Sync jobs completed

## Dependencies

- Apache Ignite 2.14+
- JanusGraph 0.6+
- Apache Spark 3.2+
- GraphFrames 0.8.2
- Python 3.11+
- FastAPI
- Strawberry GraphQL
