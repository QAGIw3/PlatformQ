# platformQ Services

This directory contains all the microservices that make up the platformQ ecosystem. Each service follows our standardized patterns for configuration, observability, and deployment.

## Core Services

### Authentication & Identity
- **[auth-service](./auth-service/)**: OIDC-compliant authentication service providing SSO, user management, and API key handling for the entire platform.

### Digital Asset Management
- **[digital-asset-service](./digital-asset-service/)**: Central service for managing all digital assets including 3D models, documents, images, and metadata from various tools.

### Data Integration
- **[connector-service](./connector-service/)**: Plugin-based data ingestion framework supporting file processors, API connectors, and webhook handlers.
- **[seatunnel-service](./seatunnel-service/)**: Unified data integration layer with 100+ connectors for batch, streaming, and CDC data synchronization.

### Search & Analytics
- **[search-service](./search-service/)**: Elasticsearch-powered unified search across all platform data with full-text search, facets, and autocomplete.
- **[realtime-analytics-service](./realtime-analytics-service/)**: Apache Ignite-based real-time analytics with sub-millisecond queries and live dashboards.
- **[analytics-service](./analytics-service/)**: Business intelligence and reporting service for complex analytical queries.

### Collaboration & Projects
- **[projects-service](./projects-service/)**: Integrated project management orchestrating Nextcloud, OpenProject, and Zulip for seamless collaboration.
- **[notification-service](./notification-service/)**: Multi-channel notification delivery system supporting email, Zulip, and webhooks.

### Advanced Processing
- **[graph-intelligence-service](./graph-intelligence-service/)**: JanusGraph-powered service for relationship discovery and graph analytics.
- **[simulation-service](./simulation-service/)**: Agent-based simulation engine for complex system modeling.
- **[functions-service](./functions-service/)**: WebAssembly runtime for custom data transformations and automation workflows.

### Document & Storage
- **[storage-proxy-service](./storage-proxy-service/)**: Unified interface for MinIO object storage with multi-tenancy support and document conversion capabilities (PDF, DOCX, etc.).

### Trust & Verification
- **[verifiable-credential-service](./verifiable-credential-service/)**: Issues and verifies W3C Verifiable Credentials with blockchain anchoring.

### Workflow & Proposals
- **[workflow-service](./workflow-service/)**: Airflow-based workflow orchestration for complex data pipelines.
- **[proposals-service](./proposals-service/)**: DAO proposal management integrated with smart contracts.
- **[provisioning-service](./provisioning-service/)**: Automated tenant provisioning and resource allocation.

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

## Best Practices

1. **API Design**: Follow REST conventions and use OpenAPI specification
2. **Error Handling**: Use consistent error codes and messages
3. **Validation**: Validate all inputs using Pydantic models
4. **Documentation**: Keep README and API docs up to date
5. **Testing**: Maintain >80% code coverage
6. **Performance**: Profile and optimize critical paths
7. **Security**: Follow OWASP guidelines
8. **Deployment**: Use GitOps for configuration management 