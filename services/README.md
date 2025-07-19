# platformQ Services

This directory contains all the microservices that make up the platformQ ecosystem. Each service follows our standardized patterns for configuration, observability, and deployment.

## Core Services

### Authentication & Identity
- **[auth-service](./auth-service/)**: OIDC-compliant authentication service providing SSO, user management, and API key handling for the entire platform.

### Digital Asset Management
- **[digital-asset-service](./digital-asset-service/)**: Central service for managing all digital assets including 3D models, documents, images, and metadata from various tools.

### Data Platform
- **[data-platform-service](./data-platform-service/)**: Comprehensive data management platform consolidating:
  - Federated query execution (Apache Trino)
  - Medallion architecture data lake (Bronze/Silver/Gold)
  - Data governance, quality, and compliance
  - ETL/ELT pipeline orchestration (SeaTunnel)
  - Connection pooling and caching
- **[connector-service](./connector-service/)**: Plugin-based data ingestion framework supporting file processors, API connectors, and webhook handlers.

### Machine Learning Platform
- **[unified-ml-platform-service](./unified-ml-platform-service/)**: Unified ML platform consolidating:
  - Model training, serving, and lifecycle management
  - MLOps and model marketplace
  - Federated learning capabilities
  - Neuromorphic computing
  - Feature store and model registry

### Search & Analytics
- **[search-service](./search-service/)**: Elasticsearch-powered unified search across all platform data with full-text search, facets, and autocomplete.
- **[analytics-service](./analytics-service/)**: Unified analytics platform with batch processing (Trino), real-time analytics (Druid/Ignite), ML operations, and cross-service monitoring.

### Advanced Processing
- **[graph-intelligence-service](./graph-intelligence-service/)**: JanusGraph-powered service for relationship discovery and graph analytics.
- **[simulation-service](./simulation-service/)**: Agent-based simulation engine for complex system modeling.
- **[functions-service](./functions-service/)**: WebAssembly runtime for custom data transformations and automation workflows.

### Document & Storage
- **[storage-service](./storage-service/)**: Enhanced storage platform providing object storage (MinIO/S3), document conversion, preview generation, and license-based access control.

### Trading & Markets
- **[trading-platform-service](./trading-platform-service/)**: Unified trading platform combining social trading, copy trading, prediction markets, and advanced order matching.
- **[derivatives-engine-service](./derivatives-engine-service/)**: Advanced derivatives trading with compute futures, options, and structured products.

### Blockchain & Trust
- **[blockchain-gateway-service](./blockchain-gateway-service/)**: Unified blockchain abstraction layer for multi-chain operations.
- **[verifiable-credential-service](./verifiable-credential-service/)**: Issues and verifies W3C Verifiable Credentials with blockchain anchoring.
- **[compliance-service](./compliance-service/)**: KYC/AML and regulatory compliance with privacy-preserving features.
- **[defi-protocol-service](./defi-protocol-service/)**: DeFi protocols including lending, liquidity pools, and yield farming.

### Workflow & Governance
- **[workflow-service](./workflow-service/)**: Airflow-based workflow orchestration for complex data pipelines and project orchestration.
- **[governance-service](./governance-service/)**: Unified governance system with DAO management, proposals, multi-chain support, and advanced voting strategies.
- **[provisioning-service](./provisioning-service/)**: Automated tenant provisioning and resource allocation.

### Specialized Services
- **[cad-collaboration-service](./cad-collaboration-service/)**: Real-time collaborative CAD editing with CRDT-based conflict resolution.
- **[quantum-optimization-service](./quantum-optimization-service/)**: Quantum-inspired algorithms for complex optimization problems.

## Service Architecture Patterns

### Common Features
All services implement:
- Multi-tenancy with complete data isolation
- OpenTelemetry instrumentation for observability
- Health check endpoints
- Prometheus metrics exposure
- Structured logging with correlation IDs
- Circuit breakers for external dependencies
- Rate limiting and backpressure handling

### API Standards
- RESTful APIs with OpenAPI 3.0 documentation
- gRPC for high-performance inter-service communication
- GraphQL federation support (selected services)
- Consistent error response format
- API versioning strategy

### Event-Driven Patterns
- Apache Pulsar for asynchronous messaging
- Event sourcing for audit trails
- Schema registry for event evolution
- Dead letter queue handling
- Exactly-once processing guarantees

### Data Storage
- PostgreSQL for transactional data
- Cassandra for time-series and wide-column needs
- MinIO for object storage
- Apache Ignite for in-memory computing
- JanusGraph for graph data

## Development Guidelines

### Creating a New Service
Use the template service as a starting point:
```bash
cp -r template-service services/my-new-service
cd services/my-new-service
# Update configuration and implement business logic
```

### Testing
Each service includes:
- Unit tests with pytest
- Integration tests with TestContainers
- Load tests with k6
- Contract tests for API compatibility

### Local Development
All services can be run locally with Docker Compose:
```bash
# Run all infrastructure dependencies
docker-compose -f infra/docker-compose/docker-compose.yml up -d

# Run a specific service
cd services/my-service
uvicorn app.main:app --reload
```

### Deployment
Services are packaged as Docker containers and deployed to Kubernetes:
- Helm charts for each service
- Horizontal pod autoscaling
- Rolling updates with zero downtime
- Service mesh integration with Istio

## Service Communication

### Synchronous Communication
- REST APIs for request-response patterns
- gRPC for low-latency service calls
- Circuit breakers prevent cascade failures
- Retries with exponential backoff

### Asynchronous Communication
- Pulsar topics for event streaming
- Pub/sub patterns for loose coupling
- Event-carried state transfer
- Choreography over orchestration

### Service Discovery
- Kubernetes DNS for internal services
- Istio service mesh for advanced routing
- Health checks ensure only healthy instances receive traffic

## Monitoring & Observability

### Metrics
- Prometheus metrics for all services
- Custom business metrics
- RED metrics (Rate, Errors, Duration)
- Grafana dashboards

### Logging
- Structured JSON logging
- Centralized log aggregation
- Correlation IDs for request tracing
- Log levels: DEBUG, INFO, WARN, ERROR

### Tracing
- OpenTelemetry distributed tracing
- Jaeger for trace visualization
- Automatic context propagation
- Performance bottleneck identification

## Security

### Authentication & Authorization
- JWT tokens from auth-service
- API key authentication for external access
- RBAC with fine-grained permissions
- Multi-tenant data isolation

### Data Protection
- Encryption at rest and in transit
- Secrets management with Kubernetes Secrets
- GDPR compliance features
- Audit logging for all data access

## Shared Libraries

The platform includes several shared libraries to promote code reuse:

### Core Libraries
- **[platformq-shared](../libs/platformq-shared/)**: Base service utilities, event processing, resilience patterns
- **[platformq-events](../libs/platformq-events/)**: Shared event schemas for inter-service communication
- **[platformq-blockchain-common](../libs/platformq-blockchain-common/)**: Blockchain types, interfaces, and adapters
- **[platformq-unified-data](../libs/platformq-unified-data/)**: Unified data access patterns

### Feature Libraries
- **[platformq-notifications](../libs/platformq-notifications/)**: Multi-channel notification capabilities (replaces notification-service)
- **[platformq-event-framework](../libs/platformq-event-framework/)**: Advanced event processing framework

## Best Practices

1. **API Design**: Follow REST conventions and use OpenAPI specification
2. **Error Handling**: Use consistent error codes and messages
3. **Validation**: Validate all inputs using Pydantic models
4. **Documentation**: Keep README and API docs up to date
5. **Testing**: Maintain >80% code coverage
6. **Performance**: Profile and optimize critical paths
7. **Security**: Follow OWASP guidelines
8. **Deployment**: Use GitOps for configuration management
9. **Consolidation**: Prefer shared libraries over duplicate service functionality 