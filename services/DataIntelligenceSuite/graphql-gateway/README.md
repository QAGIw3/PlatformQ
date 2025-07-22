# GraphQL Gateway Service

## Overview

The GraphQL Gateway Service provides a unified GraphQL API for all DataIntelligenceSuite services. It acts as a single entry point for clients, aggregating data from multiple backend services and providing a consistent query interface.

## Architecture

### Key Components

1. **GraphQL Schema**: Unified type system representing all domain entities
2. **Service Resolver**: Communicates with backend services via HTTP/gRPC
3. **DataLoader**: Implements batching and caching for efficient data fetching
4. **Query Federation**: Distributes queries across multiple services
5. **Real-time Subscriptions**: WebSocket support for live updates via Pulsar

### Integrated Services

- **Data Ingestion Service**: Source management, schema registry, external connectors
- **Stream Processing Service**: Real-time pipelines, event processing
- **Batch Processing Service**: Large-scale data transformations, file processing
- **Graph Processing Service**: Relationship analytics, network analysis
- **Quality Engine Service**: Data quality checks, profiling
- **MLOps Service**: Model management, training, serving
- **Workflow Engine Service**: Pipeline orchestration, scheduling
- **Data Catalog Service**: Metadata, lineage, discovery
- **Unified Orchestration Service**: Kubernetes job management, workflow automation

## Features

### Query Capabilities
- **Unified Data Access**: Single endpoint for all services
- **Field-level Resolution**: Fetch only requested data
- **Nested Queries**: Deep object traversal with automatic joins
- **Filtering & Pagination**: Built-in support for data filtering
- **Aggregations**: Summary statistics and analytics

### Performance Optimizations
- **Query Batching**: Automatic batching of similar requests
- **Response Caching**: Intelligent caching with TTL management
- **Query Depth Limiting**: Protection against malicious queries
- **Token Limiting**: Prevent resource exhaustion
- **DataLoader Pattern**: N+1 query prevention

### Security Features
- **Authentication**: JWT token validation
- **Authorization**: Field-level access control
- **Query Validation**: Schema-based input validation
- **Rate Limiting**: Per-client request throttling
- **Audit Logging**: Complete query audit trail

## API Schema

### Core Types

```graphql
type Query {
  # Data Catalog
  searchCatalog(query: String!, filters: SearchFilters): SearchResult!
  getEntity(id: ID!): CatalogEntity
  getLineage(entityId: ID!, depth: Int): LineageGraph
  
  # Pipelines
  pipelines(filter: PipelineFilter): [Pipeline!]!
  pipeline(id: ID!): Pipeline
  pipelineExecutions(pipelineId: ID!): [PipelineExecution!]!
  
  # Data Quality
  qualityProfile(dataset: String!): DataQualityProfile
  qualityIssues(filter: QualityFilter): [QualityIssue!]!
  qualityRules: [QualityRule!]!
  
  # ML Models
  models(filter: ModelFilter): [MLModel!]!
  model(id: ID!): MLModel
  modelVersions(modelId: ID!): [ModelVersion!]!
  
  # Connectors & Processing
  connectors: [ConnectorStatus!]!
  connector(connectorId: ID!): ConnectorStatus
  supportedProcessors: [ProcessorInfo!]!
  processorInfo(processorType: String!): ProcessorInfo
  processingJob(jobId: ID!): ProcessingJob
  processingJobs(processorType: String, status: String): [ProcessingJob!]!
  
  # Monitoring
  serviceHealth: [ServiceHealth!]!
  systemMetrics(names: [String!]!): MetricsData
  alerts(filter: AlertFilter): [Alert!]!
}

type Mutation {
  # Pipeline Management
  createPipeline(input: PipelineInput!): Pipeline!
  updatePipeline(id: ID!, input: PipelineUpdateInput!): Pipeline!
  executePipeline(id: ID!, params: JSON): PipelineExecution!
  
  # Data Quality
  runQualityCheck(input: QualityCheckInput!): QualityCheckResult!
  createQualityRule(input: QualityRuleInput!): QualityRule!
  
  # ML Operations
  trainModel(input: TrainModelInput!): TrainingJob!
  deployModel(id: ID!, input: DeploymentInput!): ModelDeployment!
  
  # Data Management
  invalidateCache(region: String!, keys: [String!]): CacheResult!
  triggerLineageUpdate(entityId: ID!): LineageUpdateResult!
  
  # Connector Management
  createConnector(connectorId: ID!, config: ConnectorConfigInput!): MutationResult!
  deleteConnector(connectorId: ID!): MutationResult!
  triggerConnector(connectorId: ID!): MutationResult!
  
  # File Processing
  processFile(input: ProcessFileInput!): ProcessingJobResult!
  processBatch(input: ProcessBatchInput!): BatchProcessingResult!
  receiveWebhook(input: WebhookPayloadInput!): MutationResult!
}

type Subscription {
  # Real-time Updates
  pipelineStatus(pipelineId: ID!): PipelineExecution!
  qualityAlerts(severity: AlertSeverity): QualityAlert!
  modelMetrics(modelId: ID!): ModelMetrics!
  systemEvents(services: [String!]): SystemEvent!
}
```

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=graphql-gateway
SERVICE_PORT=8000
LOG_LEVEL=INFO

# Backend Services
DATA_CATALOG_URL=http://data-catalog-service:8001
INGESTION_SERVICE_URL=http://data-ingestion-service:8002
STREAM_SERVICE_URL=http://stream-processing-service:8003
BATCH_SERVICE_URL=http://batch-processing-service:8004
GRAPH_SERVICE_URL=http://graph-processing-service:8005
QUALITY_SERVICE_URL=http://quality-engine-service:8006
MLOPS_SERVICE_URL=http://mlops-service:8007
WORKFLOW_SERVICE_URL=http://workflow-engine-service:8008

# Security
JWT_SECRET_KEY=your-secret-key
JWT_ALGORITHM=HS256
ENABLE_AUTH=true

# Performance
QUERY_DEPTH_LIMIT=10
QUERY_COMPLEXITY_LIMIT=1000
CACHE_TTL=300
MAX_BATCH_SIZE=100

# Monitoring
PROMETHEUS_PORT=9090
ENABLE_TRACING=true
JAEGER_ENDPOINT=http://jaeger:14268/api/traces

# Consul/Vault Integration
CONSUL_HOST=consul
CONSUL_PORT=8500
VAULT_ADDR=http://vault:8200
```

## Usage Examples

### Basic Query

```graphql
query GetPipelineStatus {
  pipeline(id: "etl-daily-revenue") {
    id
    name
    status
    lastExecution {
      status
      startedAt
      completedAt
      metrics {
        recordsProcessed
        duration
      }
    }
  }
}
```

### Complex Query with Nested Resolution

```graphql
query DataLineageAnalysis {
  searchCatalog(query: "customer", filters: { type: "table" }) {
    entities {
      id
      name
      type
      schema {
        columns {
          name
          dataType
          nullable
        }
      }
      lineage(depth: 3) {
        upstreamEntities {
          id
          name
          transformations
        }
        downstreamEntities {
          id
          name
          impactScore
        }
      }
      qualityProfile {
        completeness
        accuracy
        lastProfiledAt
        issues {
          severity
          description
        }
      }
    }
  }
}
```

### Connector Query Example

```graphql
query GetConnectorsAndJobs {
  connectors {
    connectorId
    type
    enabled
    lastRun
    nextRun
    status
  }
  
  processingJobs(status: "running") {
    jobId
    processorType
    status
    inputFile
    startedAt
    metadata
  }
  
  supportedProcessors {
    processorType
    supportedFormats
    requiresGpu
    maxFileSize
  }
}
```

### Mutation Example

```graphql
mutation CreateAndExecutePipeline {
  createPipeline(input: {
    name: "customer-segmentation"
    type: "batch"
    schedule: "0 2 * * *"
    config: {
      source: "customer_data"
      transformations: ["clean", "enrich", "segment"]
      destination: "customer_segments"
    }
  }) {
    id
    name
    status
  }
}
```

### Subscription Example

```graphql
subscription MonitorPipeline {
  pipelineStatus(pipelineId: "etl-daily-revenue") {
    status
    currentStep
    progress
    estimatedCompletion
    errors {
      message
      timestamp
    }
  }
}
```

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
python -m uvicorn app.main:app --reload --port 8000
```

### Testing GraphQL Queries

The service provides a GraphiQL interface at `http://localhost:8000/graphql` for interactive query development.

### Adding New Resolvers

1. Define types in `schema/types.py`
2. Add queries/mutations in respective schema files
3. Implement resolver logic in `resolvers/`
4. Update DataLoader if needed for batching

## Deployment

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: graphql-gateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: graphql-gateway
  template:
    metadata:
      labels:
        app: graphql-gateway
    spec:
      containers:
      - name: graphql-gateway
        image: platformq/graphql-gateway:latest
        ports:
        - containerPort: 8000
        env:
        - name: CONSUL_HOST
          value: consul-server
        - name: VAULT_ADDR
          value: http://vault:8200
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
```

## Monitoring

### Metrics

The service exposes Prometheus metrics at `/metrics`:

- `graphql_query_duration_seconds`: Query execution time
- `graphql_query_complexity`: Query complexity score
- `graphql_field_resolution_count`: Field resolution frequency
- `graphql_error_count`: Error count by type
- `graphql_cache_hit_rate`: DataLoader cache effectiveness

### Distributed Tracing

OpenTelemetry integration provides end-to-end tracing across all backend services.

### Health Checks

- `/health`: Basic health status
- `/health/ready`: Readiness probe (checks backend connectivity)
- `/health/live`: Liveness probe

## Performance Optimization

### Query Optimization Tips

1. **Use Field Selection**: Only request needed fields
2. **Implement Pagination**: For large result sets
3. **Leverage Caching**: Use cache hints for stable data
4. **Batch Operations**: Group similar queries
5. **Avoid Deep Nesting**: Limit query depth

### Caching Strategy

- **Response Cache**: Full query response caching
- **Field Cache**: Individual field result caching
- **DataLoader Cache**: Request-scoped batching cache
- **CDN Integration**: Static query result caching

## Security Best Practices

1. **Always validate input**: Use strong typing
2. **Implement rate limiting**: Prevent abuse
3. **Monitor query complexity**: Block expensive queries
4. **Use field-level auth**: Restrict sensitive data
5. **Audit all mutations**: Track data changes

## Troubleshooting

### Common Issues

1. **Slow Queries**: Check query complexity and backend service performance
2. **N+1 Problems**: Ensure DataLoaders are properly configured
3. **Connection Errors**: Verify backend service discovery via Consul
4. **Auth Failures**: Check JWT token and permissions

### Debug Mode

Enable debug logging:
```bash
LOG_LEVEL=DEBUG python -m app.main
```

## License

Proprietary - PlatformQ 