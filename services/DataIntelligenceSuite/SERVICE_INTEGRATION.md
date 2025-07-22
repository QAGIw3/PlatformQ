# DataIntelligenceSuite Service Integration

This document describes how the refactored DataIntelligenceSuite services integrate with each other.

## Service Architecture

The DataIntelligenceSuite has been refactored into the following microservices:

1. **Digital Integration Hub (DIH) Service** - Port 8002
   - In-memory data integration and caching
   - Change Data Capture (CDC)
   - Data synchronization

2. **Data Quality Service** - Port 8003
   - Data quality monitoring and validation
   - Data profiling and anomaly detection
   - Automated remediation

3. **Pipeline Orchestration Service** - Port 8004
   - Pipeline management and scheduling
   - Execution coordination
   - Performance monitoring

4. **Data Platform Service** (remaining) - Port 8001
   - Data lake management
   - Transformation engine
   - Lineage tracking
   - Other data operations

## Integration Patterns

### 1. Event-Driven Communication

Services communicate primarily through Apache Pulsar events:

```
Pipeline Orchestration Service
    |
    ├── pipeline.extract.requested ──────► Data Platform Service
    ├── pipeline.transform.requested ────► Data Platform Service
    ├── pipeline.load.requested ─────────► Data Platform Service
    ├── data.quality.check.requested ────► Data Quality Service
    └── pipeline.aggregate.requested ────► Data Platform Service

Data Quality Service
    |
    ├── data.quality.issue.detected ─────► Pipeline Orchestration
    └── data.quality.check.completed ────► Pipeline Orchestration

DIH Service
    |
    ├── cache.invalidated ───────────────► Data Platform Service
    └── data.synchronized ───────────────► Data Quality Service
```

### 2. Service Discovery

All services register with Consul for:
- Health checking
- Service discovery
- Configuration management
- Distributed coordination

### 3. Shared Components

#### data-intelligence-common Library
- Base service template
- Vault/Consul integration
- Structured logging
- Metrics collection
- Event processing framework

## Event Flows

### Pipeline Execution Flow

1. **Pipeline Triggered** (Manual/Scheduled/Event)
   ```
   Pipeline Orchestration Service
       ↓
   Creates execution context
       ↓
   Publishes step events
   ```

2. **Data Extraction**
   ```
   Event: pipeline.extract.requested
       ↓
   Data Platform Service extracts data
       ↓
   Event: pipeline.extract.completed
   ```

3. **Data Quality Check**
   ```
   Event: data.quality.check.requested
       ↓
   Data Quality Service validates data
       ↓
   Event: data.quality.check.completed
   ```

4. **Data Transformation**
   ```
   Event: pipeline.transform.requested
       ↓
   Data Platform Service transforms data
       ↓
   Event: pipeline.transform.completed
   ```

5. **Data Loading**
   ```
   Event: pipeline.load.requested
       ↓
   Data Platform Service loads data
       ↓
   Event: pipeline.load.completed
   ```

### Data Quality Monitoring Flow

```
Dataset Updated
    ↓
Event: dataset.updated
    ↓
Data Quality Service profiles dataset
    ↓
If issues detected:
    Event: data.quality.issue.detected
    ↓
    Automated remediation or alert
```

### Cache Synchronization Flow

```
Data Change Detected
    ↓
DIH Service CDC processor
    ↓
Update cache
    ↓
Event: cache.updated
    ↓
Dependent services notified
```

## API Integration

Services also communicate via REST APIs for synchronous operations:

### Pipeline Orchestration → Data Quality
```http
POST /api/v1/quality/check
{
  "dataset": "customer_data",
  "check_type": "full"
}
```

### Pipeline Orchestration → DIH
```http
GET /api/v1/cache/regions/{region}/data/{key}
POST /api/v1/sync/tasks
```

## Configuration Management

### Consul KV Structure
```
/platformq/
├── data-intelligence/
│   ├── dih-service/
│   │   ├── config
│   │   └── cache-regions
│   ├── data-quality-service/
│   │   ├── config
│   │   ├── rules
│   │   └── monitored-datasets
│   └── pipeline-orchestration-service/
│       ├── config
│       ├── pipelines/definitions
│       └── schedules
```

### Dynamic Configuration Updates
Services watch Consul for configuration changes:
- Cache policies
- Quality rules
- Pipeline definitions
- Resource limits

## Security Integration

### Vault Integration
All services use Vault for:
- Dynamic database credentials
- API keys and tokens
- Encryption keys
- Certificate management

### Service-to-Service Authentication
- mTLS between services
- JWT tokens for API authentication
- Service-specific ACL policies in Consul

## Monitoring & Observability

### Distributed Tracing
```
Request → Pipeline Orchestration
    → span: execute_pipeline
        → span: extract_data (Data Platform)
        → span: quality_check (Data Quality)
        → span: transform_data (Data Platform)
        → span: load_data (Data Platform)
```

### Metrics Aggregation
Each service exposes Prometheus metrics:
- Pipeline execution metrics
- Data quality scores
- Cache hit rates
- Service health metrics

### Centralized Logging
All services use structured JSON logging with:
- Correlation IDs
- Service context
- Event tracking

## Development Guidelines

### Adding New Integration

1. **Define Event Schema**
   ```python
   @dataclass
   class NewIntegrationEvent:
       source_service: str
       target_service: str
       payload: Dict[str, Any]
       correlation_id: str
   ```

2. **Implement Event Handler**
   ```python
   async def handle_new_integration(event: Dict[str, Any]):
       # Process event
       # Call target service
       # Publish completion event
   ```

3. **Update Service Discovery**
   ```python
   await consul.register_service(
       name="new-integration",
       service_id=f"new-integration-{instance_id}",
       address=service_host,
       port=service_port,
       check=health_check
   )
   ```

### Testing Integration

1. **Unit Tests**: Mock event publishers/subscribers
2. **Integration Tests**: Use test containers
3. **End-to-End Tests**: Full service deployment

## Deployment Considerations

### Service Dependencies
```yaml
# docker-compose.yml
services:
  pipeline-orchestration:
    depends_on:
      - consul
      - vault
      - pulsar
    
  data-quality:
    depends_on:
      - consul
      - vault
      - pulsar
      - ignite
    
  dih-service:
    depends_on:
      - consul
      - vault
      - ignite
```

### Health Checks
Each service implements:
- `/health` - Basic liveness
- `/ready` - Full readiness including dependencies

### Graceful Shutdown
Services handle SIGTERM:
1. Stop accepting new requests
2. Complete in-flight operations
3. Deregister from Consul
4. Close connections

## Future Enhancements

1. **GraphQL Federation**: Unified API gateway
2. **Service Mesh**: Istio/Linkerd integration
3. **Circuit Breakers**: Resilience patterns
4. **Event Sourcing**: Complete audit trail
5. **CQRS**: Separate read/write models 