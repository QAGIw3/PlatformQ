# platformQ Services

This directory contains all the microservices that make up the platformQ ecosystem. Each service follows our standardized patterns for configuration, observability, and deployment.

## Core Services

### Authentication & Identity
- **[auth-service](./auth-service/)**: OIDC-compliant authentication service providing SSO, user management, and API key handling for the entire platform.

### Digital Asset Management
- **[digital-asset-service](./digital-asset-service/)**: Central service for managing all digital assets including 3D models, documents, images, and metadata from various tools.

### Data Platform (Consolidated)
- **[data-platform-service](./data-platform-service/)**: Comprehensive data management platform providing:
  
  **Query & Federation**
  - Federated query execution across multiple data sources (Apache Trino)
  - SQL interface for unified data access
  - Query optimization and caching
  - Schema discovery and exploration
  
  **Data Catalog**
  - Centralized metadata management
  - Asset discovery and search (Elasticsearch)
  - Column-level metadata and statistics
  - Data lineage tracking (JanusGraph)
  - Tagging and classification
  
  **Data Governance**
  - Policy-based access control
  - GDPR, CCPA, HIPAA compliance frameworks
  - Data privacy controls (masking, encryption, tokenization)
  - Access request workflows
  - Audit logging and compliance reporting
  
  **Data Quality**
  - Automated data profiling
  - Quality rule definition and monitoring
  - Anomaly detection
  - Data validation pipelines
  - Quality score tracking
  
  **Data Lake Management**
  - Medallion architecture (Bronze/Silver/Gold)
  - MinIO-based object storage
  - Data ingestion and transformation
  - Compaction and optimization
  - Retention policies
  
  **Pipeline Orchestration**
  - ETL/ELT pipeline management (Apache SeaTunnel)
  - Connector library for various data sources
  - Real-time and batch processing
  - Pipeline monitoring and metrics
  - Visual pipeline builder
  
  **Administration**
  - System health monitoring
  - Resource quota management
  - User and permission management
  - Backup and recovery
  - Performance tuning recommendations

### Machine Learning Platform (Consolidated)
- **[unified-ml-platform-service](./unified-ml-platform-service/)**: Unified ML platform providing:
  
  **Model Registry & Lifecycle**
  - Centralized model storage and versioning (MLflow)
  - Model artifact management
  - Model promotion workflows (dev→staging→production)
  - Model comparison and evaluation
  - Dependency tracking
  
  **Training & Experimentation**
  - Distributed training orchestration (Apache Spark)
  - Hyperparameter optimization
  - AutoML capabilities
  - Experiment tracking and comparison
  - Resource allocation and scheduling
  
  **Model Serving**
  - Real-time inference endpoints (Knative/Seldon)
  - Batch prediction jobs
  - Multi-model serving
  - A/B testing framework
  - Auto-scaling based on load
  - Model monitoring and drift detection
  
  **Federated Learning**
  - Privacy-preserving distributed training
  - Multiple aggregation strategies (FedAvg, FedProx, SCAFFOLD)
  - Differential privacy support
  - Secure aggregation protocols
  - Participant management
  
  **Neuromorphic Computing**
  - Spiking neural network support
  - Event-driven processing
  - Hardware acceleration integration
  - Energy-efficient inference
  - Real-time anomaly detection
  
  **Feature Store**
  - Feature definition and management
  - Online/offline feature serving
  - Feature versioning and lineage
  - Materialization pipelines
  - Feature validation and monitoring
  - Time-travel queries
  
  **Model Marketplace**
  - Model listing and discovery
  - Usage-based pricing
  - Model reviews and ratings
  - Revenue sharing
  - License management
  
  **ML Monitoring**
  - Model performance tracking
  - Data drift detection
  - Prediction monitoring
  - Alert management
  - Custom metrics and dashboards

### Search & Analytics
- **[search-service](./search-service/)**: Elasticsearch-powered unified search across all platform data with full-text search, facets, and autocomplete.
- **[analytics-service](./analytics-service/)**: Real-time analytics platform with stream processing (Apache Flink), time-series analysis, and cross-service monitoring.

### Advanced Processing
- **[graph-intelligence-service](./graph-intelligence-service/)**: JanusGraph-powered service for relationship discovery, community detection, and graph analytics.
- **[simulation-service](./simulation-service/)**: Agent-based simulation engine for complex system modeling.
- **[functions-service](./functions-service/)**: WebAssembly runtime for custom data transformations and automation workflows.

### Storage
- **[storage-proxy-service](./storage-proxy-service/)**: Unified storage abstraction providing object storage (MinIO), CDN integration, and access control.

### Trading & Markets
- **[trading-platform-service](./trading-platform-service/)**: Unified trading platform combining social trading, copy trading, and order matching.
- **[derivatives-engine-service](./derivatives-engine-service/)**: Advanced derivatives trading with compute futures, options, and structured products.

### Blockchain & Trust
- **[blockchain-gateway-service](./blockchain-gateway-service/)**: Unified blockchain abstraction layer for multi-chain operations.
- **[verifiable-credential-service](./verifiable-credential-service/)**: Issues and verifies W3C Verifiable Credentials with blockchain anchoring.
- **[compliance-service](./compliance-service/)**: KYC/AML and regulatory compliance with privacy-preserving features.
- **[defi-protocol-service](./defi-protocol-service/)**: DeFi protocols including lending, liquidity pools, and yield farming.

### Workflow & Governance
- **[workflow-service](./workflow-service/)**: Apache Airflow-based workflow orchestration for complex data pipelines.
- **[governance-service](./governance-service/)**: DAO management with proposals, voting, and treasury management.
- **[provisioning-service](./provisioning-service/)**: Automated tenant provisioning and resource allocation.

### Collaboration
- **[cad-collaboration-service](./cad-collaboration-service/)**: Real-time collaborative CAD editing with CRDT-based conflict resolution.

### Specialized Computing
- **[quantum-optimization-service](./quantum-optimization-service/)**: Quantum-inspired algorithms for optimization problems.

### Event Routing
- **[event-router-service](./event-router-service/)**: Central event routing and transformation hub for the platform.

### Data Integration
- **[seatunnel-service](./seatunnel-service/)**: Apache SeaTunnel integration for data synchronization and ETL pipelines.
- **[connector-service](./connector-service/)**: Plugin-based data ingestion framework.

## Infrastructure Services
- **[mlflow-server](./mlflow-server/)**: MLflow server for ML experiment tracking and model registry (used by unified-ml-platform-service).

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
- Elasticsearch for search and analytics
- Milvus for vector embeddings

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
- Secrets management with HashiCorp Vault
- GDPR compliance features
- Audit logging for all data access

## Shared Libraries

The platform includes several shared libraries to promote code reuse:

### Core Libraries
- **[platformq-shared](../libs/platformq-shared/)**: Base service utilities including logging, caching (Ignite), error handling, and resilience patterns
- **[platformq-events](../libs/platformq-events/)**: Shared event schemas and publishers for inter-service communication
- **[platformq-blockchain-common](../libs/platformq-blockchain-common/)**: Blockchain types, interfaces, and adapters
- **[platformq-unified-data](../libs/platformq-unified-data/)**: Unified data access patterns and models

### Feature Libraries
- **[platformq-event-framework](../libs/platformq-event-framework/)**: Advanced event processing framework with routing and transformation

## Best Practices

1. **API Design**: Follow REST conventions and use OpenAPI specification
2. **Error Handling**: Use consistent error codes and messages across services
3. **Validation**: Validate all inputs using Pydantic models
4. **Documentation**: Keep README and API docs up to date
5. **Testing**: Maintain >80% code coverage
6. **Performance**: Profile and optimize critical paths
7. **Security**: Follow OWASP guidelines
8. **Deployment**: Use GitOps for configuration management
9. **Consolidation**: Prefer service consolidation and shared libraries over duplicate functionality
10. **Event-Driven**: Use events for loose coupling between services 