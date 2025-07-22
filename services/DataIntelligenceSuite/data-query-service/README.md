# Data Query Service

A focused service for federated query execution across multiple data sources with caching, optimization, and access control.

## Overview

The Data Query Service provides:
- **Federated Query Engine**: Query across multiple databases
- **Query Optimization**: Intelligent query planning and execution
- **Result Caching**: High-performance caching with Ignite
- **Access Control**: Fine-grained permissions and data masking
- **Query History**: Track and analyze query patterns

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Data Query Service                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌─────────────┐  ┌──────────────┐  │
│  │    Query     │  │   Query     │  │    Result    │  │
│  │    Parser    │  │  Optimizer  │  │    Cache     │  │
│  └──────────────┘  └─────────────┘  └──────────────┘  │
│                                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │            Federated Query Engine                   │ │
│  │  • Trino        • Spark SQL    • Direct DB Access  │ │
│  └────────────────────────────────────────────────────┘ │
│                                                         │
│  Storage: Multiple DBs | Cache: Ignite | Auth: Vault   │
└─────────────────────────────────────────────────────────┘
```

## Features

### Query Capabilities
- SQL query support across heterogeneous databases
- Federated joins across data sources
- Query optimization and planning
- Parallel query execution
- Result pagination and streaming

### Performance
- Intelligent query caching
- Query result compression
- Connection pooling
- Query parallelization
- Cost-based optimization

### Security
- Row-level security
- Column-level masking
- Dynamic data masking for PII
- Query audit logging
- Access control integration

## API Endpoints

### Query Execution
- `POST /api/v1/query/execute` - Execute federated query
- `GET /api/v1/query/status/{query_id}` - Get query status
- `GET /api/v1/query/results/{query_id}` - Get query results
- `POST /api/v1/query/cancel/{query_id}` - Cancel running query

### Query Management
- `GET /api/v1/query/history` - Query execution history
- `GET /api/v1/query/stats` - Query statistics
- `POST /api/v1/query/explain` - Explain query plan
- `POST /api/v1/query/validate` - Validate query syntax

### Cache Management
- `GET /api/v1/cache/stats` - Cache statistics
- `POST /api/v1/cache/invalidate` - Invalidate cache entries
- `GET /api/v1/cache/config` - Cache configuration

## Configuration

```yaml
# Service Configuration
SERVICE_NAME: data-query-service
SERVICE_PORT: 8000

# Query Engine
QUERY_ENGINE: trino
TRINO_COORDINATOR: http://trino:8080
SPARK_MASTER: spark://spark-master:7077

# Database Connections
POSTGRES_HOST: postgres
MYSQL_HOST: mysql
CLICKHOUSE_HOST: clickhouse

# Cache Configuration
IGNITE_HOST: ignite
IGNITE_PORT: 10800
CACHE_TTL: 3600
MAX_CACHE_SIZE_MB: 1024

# Performance
MAX_CONCURRENT_QUERIES: 100
QUERY_TIMEOUT_SECONDS: 300
MAX_RESULT_SIZE_MB: 100
```

## Usage Examples

### Execute Federated Query
```python
response = requests.post('http://query-service:8000/api/v1/query/execute', json={
    "sql": """
        SELECT 
            u.user_id, 
            u.name, 
            COUNT(t.transaction_id) as transaction_count,
            SUM(t.amount) as total_amount
        FROM postgres.users u
        JOIN mysql.transactions t ON u.user_id = t.user_id
        WHERE t.created_at > '2024-01-01'
        GROUP BY u.user_id, u.name
    """,
    "federation_hints": {
        "join_strategy": "broadcast",
        "cache_results": true
    }
})
```

### Query with Data Masking
```python
response = requests.post('http://query-service:8000/api/v1/query/execute', json={
    "sql": "SELECT * FROM users WHERE country = 'US'",
    "data_masking": {
        "email": "partial",
        "phone": "full",
        "ssn": "tokenize"
    }
})
```

## Performance Optimization

- **Query Planning**: Cost-based optimizer for federated queries
- **Caching Strategy**: LRU cache with smart invalidation
- **Connection Pooling**: Reuse database connections
- **Result Streaming**: Stream large results
- **Parallel Execution**: Execute sub-queries in parallel

## Security

- **Authentication**: JWT tokens via API Gateway
- **Authorization**: RBAC with fine-grained permissions
- **Encryption**: TLS for data in transit
- **Audit Logging**: Complete query audit trail
- **Data Masking**: Dynamic PII masking

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 