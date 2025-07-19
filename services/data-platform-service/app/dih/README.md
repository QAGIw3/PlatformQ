# Digital Integration Hub (DIH)

A high-performance, in-memory data integration layer built on Apache Ignite that aggregates hot data from multiple sources, providing low-latency API access with ACID transaction support.

## 🎯 Overview

The Digital Integration Hub acts as a unified caching and data access layer that:
- **Aggregates** data from multiple heterogeneous sources (Cassandra, PostgreSQL, Elasticsearch, etc.)
- **Provides** sub-millisecond API access to frequently accessed data
- **Supports** ACID transactions across cache regions
- **Enables** real-time data synchronization and CDC
- **Offloads** backend systems by serving requests from memory

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   API Gateway Layer                          │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Session   │    Asset     │   Metrics   │   Feature  │ │
│  │   Lookup    │   Metadata   │  Real-time  │    Store   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                Digital Integration Hub (Ignite)              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         In-Memory Data Grid (Partitioned)           │   │
│  │  ┌──────────┬──────────┬──────────┬──────────────┐ │   │
│  │  │  Cache   │  Cache   │  Cache   │  Continuous  │ │   │
│  │  │ Region 1 │ Region 2 │ Region 3 │   Queries    │ │   │
│  │  └──────────┴──────────┴──────────┴──────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                  Data Source Connectors                      │
│  ┌──────────┬──────────┬──────────┬──────────┬─────────┐  │
│  │Cassandra │PostgreSQL│  Elastic │  MinIO   │  Pulsar │  │
│  │          │          │  Search  │          │  Stream │  │
│  └──────────┴──────────┴──────────┴──────────┴─────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Key Features

### 1. Cache Regions
- **Partitioned Caches**: Distributed across nodes for scalability
- **Replicated Caches**: Full copy on each node for ultra-fast reads
- **Transactional Support**: ACID transactions with configurable isolation
- **Automatic Expiry**: TTL-based eviction with create/access policies
- **SQL Queries**: Full SQL support with indexes

### 2. Data Sources
- **Multi-Source Integration**: Connect to various databases and APIs
- **Sync Strategies**: Write-through, write-behind, read-through, refresh-ahead
- **CDC Support**: Real-time change data capture
- **Batch Loading**: Efficient bulk data loading
- **Transform Functions**: Data transformation during sync

### 3. API Access
- **RESTful Endpoints**: Pre-configured API patterns
- **SQL Interface**: Cross-region SQL queries
- **Continuous Queries**: Real-time notifications on data changes
- **Cache Key Patterns**: Flexible key generation
- **Multi-Tenancy**: Built-in tenant isolation

### 4. Consistency & Transactions
- **ACID Transactions**: Full transaction support
- **Consistency Levels**: Eventual, Strong, Bounded Staleness
- **Isolation Levels**: From Read Uncommitted to Serializable
- **Two-Phase Commit**: Distributed transaction coordination

## 📡 API Reference

### Cache Region Management

```python
# Create a cache region
POST /api/v1/dih/cache-regions
{
    "name": "user_sessions",
    "cache_mode": "REPLICATED",
    "backups": 1,
    "atomicity_mode": "TRANSACTIONAL",
    "cache_strategy": "WRITE_THROUGH",
    "expiry_policy": {
        "create": 3600,  # 1 hour
        "access": 3600
    },
    "indexes": [
        ["user_id", "STRING"],
        ["tenant_id", "STRING"]
    ]
}

# Get cache statistics
GET /api/v1/dih/cache-regions/{region_name}/stats
```

### Data Source Registration

```python
# Register a data source
POST /api/v1/dih/data-sources
{
    "source_name": "auth_postgres",
    "source_type": "postgresql",
    "connection_params": {
        "host": "postgresql",
        "port": 5432,
        "database": "auth_db"
    },
    "target_regions": ["user_sessions"],
    "sync_interval_seconds": 300,
    "consistency_level": "STRONG"
}
```

### API Endpoint Configuration

```python
# Register an API endpoint
POST /api/v1/dih/api-endpoints
{
    "path": "session/validate",
    "cache_regions": ["user_sessions"],
    "query_template": "SELECT * FROM user_sessions WHERE session_token = '{token}'",
    "cache_key_pattern": "session:{token}",
    "ttl_seconds": 60
}

# Execute API query
POST /api/v1/dih/query/session/validate
{
    "token": "abc123"
}
```

### Transaction Execution

```python
# Execute ACID transaction
POST /api/v1/dih/transactions
{
    "operations": [
        {
            "region": "account_balances",
            "operation": "update",
            "data": {
                "key": "account_123",
                "updates": {"balance": -100.00}
            }
        },
        {
            "region": "account_balances",
            "operation": "update",
            "data": {
                "key": "account_456",
                "updates": {"balance": 100.00}
            }
        }
    ],
    "isolation": "SERIALIZABLE",
    "timeout": 5000
}
```

### SQL Queries

```python
# Execute cross-region SQL query
POST /api/v1/dih/sql-query
{
    "query": "SELECT * FROM asset_metadata WHERE asset_type = ? AND created_at > ?",
    "params": ["model", "2024-01-01"],
    "page_size": 1000
}
```

## 💡 Usage Examples

### Example 1: User Session Management

```python
# 1. Create replicated cache for sessions
await dih.create_cache_region(CacheRegion(
    name="user_sessions",
    cache_mode="REPLICATED",
    expiry_policy={"create": 3600, "access": 3600}
))

# 2. Sync from PostgreSQL
await dih.register_data_source(
    "auth_postgres",
    DataSourceConfig(source_type=DataSource.POSTGRESQL),
    ["user_sessions"]
)

# 3. Query sessions
sessions = await dih.execute_api_query(
    "session/validate",
    {"token": "abc123"}
)
```

### Example 2: Real-time Metrics

```python
# 1. Create partitioned cache with short TTL
await dih.create_cache_region(CacheRegion(
    name="realtime_metrics",
    cache_mode="PARTITIONED",
    cache_strategy=CacheStrategy.WRITE_BEHIND,
    expiry_policy={"create": 300}  # 5 minutes
))

# 2. Stream metrics from Pulsar
await dih.register_data_source(
    "metrics_stream",
    DataSourceConfig(source_type=DataSource.PULSAR_STREAM),
    ["realtime_metrics"]
)
```

### Example 3: ML Feature Store

```python
# 1. Create feature cache
await dih.create_cache_region(CacheRegion(
    name="ml_features_online",
    eviction_max_size=100000000,  # 100M features
    indexes=[("entity_id", "STRING"), ("feature_name", "STRING")]
))

# 2. Batch query features
features = await dih.query_cross_region("""
    SELECT entity_id, feature_name, feature_value
    FROM ml_features_online
    WHERE entity_id IN (?) AND version = ?
""", [entity_ids, version])
```

## 🔧 Configuration

### Environment Variables

```bash
# Ignite cluster nodes
IGNITE_HOST_1=ignite-0.ignite
IGNITE_HOST_2=ignite-1.ignite
IGNITE_HOST_3=ignite-2.ignite

# Default consistency
DIH_DEFAULT_CONSISTENCY=STRONG

# Cache defaults
DIH_DEFAULT_BACKUPS=1
DIH_DEFAULT_EVICTION_SIZE=10000000
```

### Cache Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| WRITE_THROUGH | Synchronous write to cache and source | Critical data requiring consistency |
| WRITE_BEHIND | Async write to source | High-throughput writes |
| READ_THROUGH | Load from source on cache miss | Large datasets |
| REFRESH_AHEAD | Proactive refresh before expiry | Frequently accessed data |

### Consistency Levels

| Level | Description | Trade-off |
|-------|-------------|-----------|
| EVENTUAL | Best effort consistency | Performance |
| STRONG | Immediate consistency | Latency |
| BOUNDED_STALENESS | Max staleness window | Balance |
| SESSION | Session consistency | User experience |

## 📊 Performance

### Benchmarks

| Operation | Latency | Throughput |
|-----------|---------|------------|
| Single Get | <1ms | 1M ops/sec |
| Batch Get (100) | <5ms | 100K ops/sec |
| SQL Query | <10ms | 50K queries/sec |
| Transaction | <20ms | 10K TPS |

### Optimization Tips

1. **Use Affinity Keys**: Co-locate related data
2. **Index Strategically**: Only index query fields
3. **Batch Operations**: Use putAll/getAll for bulk ops
4. **Connection Pooling**: Reuse client connections
5. **Async Operations**: Use async APIs when possible

## 🔍 Monitoring

### Metrics Available

- Cache hit/miss rates
- Query performance
- Transaction success rates
- Data source sync status
- Memory usage per region
- Network throughput

### Health Checks

```bash
# Check DIH health
GET /api/v1/dih/health

# Get detailed metrics
GET /api/v1/dih/metrics
```

## 🚨 Troubleshooting

### Common Issues

1. **High Memory Usage**
   - Check eviction policies
   - Monitor cache sizes
   - Review TTL settings

2. **Slow Queries**
   - Verify indexes exist
   - Check query plans
   - Monitor network latency

3. **Transaction Failures**
   - Check isolation levels
   - Monitor lock contention
   - Review timeout settings

4. **Sync Delays**
   - Check source connectivity
   - Monitor sync intervals
   - Review batch sizes 