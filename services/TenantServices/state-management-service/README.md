# State Management Service

## Overview

The State Management Service provides centralized, distributed state management for PlatformQ using Apache Ignite. It implements the Digital Integration Hub (DIH) pattern for high-performance state access and follows the consistency patterns from the data-platform-service.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  State Management Service                     │
├─────────────────────────────────────────────────────────────┤
│  API Layer                                                   │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │  REST API   │  │  gRPC API    │  │  WebSocket API  │   │
│  └─────────────┘  └──────────────┘  └─────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│  State Management Core                                       │
│  ┌─────────────────────┐  ┌─────────────────────────────┐ │
│  │ Cache Manager        │  │ Consistency Manager         │ │
│  │ - Region Management  │  │ - ACID Transactions        │ │
│  │ - TTL Policies       │  │ - Optimistic Locking       │ │
│  │ - Eviction Policies  │  │ - Event Sourcing           │ │
│  └─────────────────────┘  └─────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│  Storage Backend (Apache Ignite)                            │
│  ┌─────────────────────┐  ┌─────────────────────────────┐ │
│  │ In-Memory Data Grid  │  │ Persistence Layer           │ │
│  │ - Partitioned Caches │  │ - Native Persistence       │ │
│  │ - Replicated Caches  │  │ - Snapshot Management      │ │
│  │ - Near Caches        │  │ - WAL                      │ │
│  └─────────────────────┘  └─────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Core Features

### 1. Cache Management
- **Dynamic Cache Creation**: Create caches on-demand with custom configurations
- **Multiple Cache Modes**: PARTITIONED, REPLICATED, LOCAL
- **Atomicity Modes**: ATOMIC, TRANSACTIONAL
- **Eviction Policies**: LRU, FIFO, RANDOM with configurable sizes

### 2. Consistency Guarantees
- **ACID Transactions**: Full transactional support
- **Optimistic Concurrency**: Version-based conflict detection
- **Pessimistic Locking**: Explicit lock management
- **Read-Through/Write-Through**: Automatic data loading and persistence

### 3. High Availability
- **Automatic Failover**: Node failure detection and recovery
- **Data Replication**: Configurable backup copies
- **Split-Brain Protection**: Network partition handling
- **Rolling Updates**: Zero-downtime deployments

### 4. Performance Optimization
- **Near Caching**: Client-side caching for hot data
- **SQL Queries**: Indexed queries across cached data
- **Collocated Processing**: Compute where data lives
- **Binary Format**: Zero-deserialization access

## API Endpoints

### Cache Operations
```
POST   /api/v1/caches                    # Create cache
GET    /api/v1/caches/{cache}/keys/{key} # Get value
PUT    /api/v1/caches/{cache}/keys/{key} # Put value
DELETE /api/v1/caches/{cache}/keys/{key} # Delete value
POST   /api/v1/caches/{cache}/bulk       # Bulk operations
```

### Transaction Management
```
POST   /api/v1/transactions              # Start transaction
PUT    /api/v1/transactions/{txId}       # Commit/rollback
POST   /api/v1/transactions/{txId}/ops   # Add operation
```

### Query Interface
```
POST   /api/v1/query/sql                 # SQL query
POST   /api/v1/query/scan                # Scan query
POST   /api/v1/query/continuous          # Continuous query
```

### Administrative
```
GET    /api/v1/health                    # Health check
GET    /api/v1/metrics                   # Performance metrics
POST   /api/v1/admin/snapshot            # Create snapshot
POST   /api/v1/admin/rebalance          # Rebalance data
```

## Cache Regions

### Collaboration State
- **Session States**: Active collaboration sessions
- **User Presence**: Real-time user presence data
- **CRDT States**: Conflict-free replicated data types
- **Operation Logs**: Operation history for replay

### Compute Allocation
- **Resource Pool**: Available compute resources
- **Allocation Map**: Active resource allocations
- **Contract Cache**: Futures and spot contracts
- **Performance Metrics**: Real-time performance data

### Feature Store
- **Online Features**: Low-latency feature serving
- **Feature Metadata**: Feature definitions and schemas
- **Feature Statistics**: Computed feature statistics

### General Purpose
- **Application Cache**: Generic application caching
- **Session Store**: User session management
- **Configuration**: Dynamic configuration values

## Integration Patterns

### Event-Driven Updates
```java
// Pulsar integration for cache updates
@EventListener
public void onDataUpdate(UpdateEvent event) {
    cache.put(event.getKey(), event.getValue());
    publishCacheEvent(event);
}
```

### Read-Through Pattern
```java
// Automatic data loading from persistent stores
cache.withCacheLoader(key -> {
    return dataRepository.load(key);
});
```

### Write-Behind Pattern
```java
// Asynchronous persistence with batching
cache.withCacheWriter(entries -> {
    dataRepository.batchSave(entries);
});
```

## Configuration

```yaml
ignite:
  cluster:
    name: platformq-state
    discovery:
      type: kubernetes
      namespace: platformq
      service: ignite-discovery
    
  memory:
    default_region_size: 8GB
    persistence:
      enabled: true
      storage_path: /data/ignite
      wal_mode: BACKGROUND
      checkpoint_frequency: 180000  # 3 minutes
    
  caches:
    default:
      mode: PARTITIONED
      backups: 1
      atomicity: TRANSACTIONAL
      eviction:
        policy: LRU
        max_size: 1000000
      
  security:
    enabled: true
    tls:
      enabled: true
      keystore: /secrets/ignite.keystore
      truststore: /secrets/ignite.truststore
    
  metrics:
    enabled: true
    exporters:
      - prometheus
      - jmx
```

## Client Libraries

### Java
```java
StateManagementClient client = StateManagementClient.builder()
    .endpoint("http://state-service:8000")
    .build();

// Simple operations
client.put("cache1", "key1", value);
Value result = client.get("cache1", "key1");

// Transactions
try (Transaction tx = client.beginTransaction()) {
    tx.put("cache1", "key1", value1);
    tx.put("cache2", "key2", value2);
    tx.commit();
}
```

### Python
```python
from platformq.state import StateClient

client = StateClient(endpoint="http://state-service:8000")

# Simple operations
client.put("cache1", "key1", value)
result = client.get("cache1", "key1")

# Bulk operations
client.put_all("cache1", {
    "key1": value1,
    "key2": value2
})
```

## Monitoring & Operations

### Key Metrics
- Cache hit ratio
- Operation latency (p50, p95, p99)
- Memory utilization
- Rebalancing operations
- Transaction throughput

### Alerts
- Memory pressure (>80% usage)
- Rebalancing failures
- Node disconnections
- Transaction deadlocks
- Performance degradation

### Operational Procedures
- Rolling restart procedure
- Cache warming strategies
- Backup and restore
- Capacity planning 