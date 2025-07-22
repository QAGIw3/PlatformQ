# DataIntelligenceSuite

A comprehensive suite of data intelligence services providing advanced data platform capabilities including data lake management, quality assurance, pipeline orchestration, and real-time analytics.

## Architecture Overview

The DataIntelligenceSuite has been refactored into a microservices architecture for improved maintainability, scalability, and deployment flexibility:

### Core Services

1. **Digital Integration Hub (DIH) Service** (Port 8002)
   - Apache Ignite-based in-memory data integration
   - Real-time Change Data Capture (CDC)
   - Multi-source data synchronization
   - High-performance caching layer

2. **Data Quality Service** (Port 8003)
   - Autonomous data quality management
   - Real-time monitoring and alerting
   - Data profiling and anomaly detection
   - Automated issue remediation

3. **Pipeline Orchestration Service** (Port 8004)
   - Centralized pipeline management
   - Flexible scheduling (cron, interval, event-driven)
   - Execution coordination across services
   - Performance monitoring and optimization

4. **Data Platform Service** (Port 8001)
   - Medallion lake architecture (Bronze/Silver/Gold)
   - Data transformation engine
   - Lineage tracking and governance
   - Feature store and ML operations

### Shared Components

- **data-intelligence-common**: Shared library providing:
  - Base service template with standardized lifecycle
  - Vault/Consul integration for secrets and configuration
  - Structured logging and metrics collection
  - Event processing framework
  - Common middleware and utilities

## Key Technologies

- **Apache Ignite**: In-memory computing for DIH
- **Apache Spark**: Large-scale data processing
- **Apache Flink**: Stream processing
- **Apache Pulsar**: Event streaming and messaging
- **Apache SeaTunnel**: Data integration
- **HashiCorp Vault**: Secrets management
- **HashiCorp Consul**: Service discovery and configuration
- **FastAPI**: REST API framework
- **Prometheus/Grafana**: Monitoring and visualization

## Getting Started

### Prerequisites
- Python 3.9+
- Docker and Docker Compose
- Access to Vault and Consul

### Installation

1. **Install shared libraries**:
   ```bash
   cd libs/data-intelligence-common
   pip install -e .
   ```

2. **Install service dependencies**:
   ```bash
   # For each service
   cd services/DataIntelligenceSuite/[service-name]
   pip install -r requirements.txt
   ```

### Running Services

#### Using Docker Compose
```bash
# Start all services
docker-compose -f docker-compose.data-intelligence.yml up

# Start specific service
docker-compose -f docker-compose.data-intelligence.yml up dih-service
```

#### Running Individually
```bash
# DIH Service
cd services/DataIntelligenceSuite/dih-service
uvicorn app.main:app --port 8002

# Data Quality Service
cd services/DataIntelligenceSuite/data-quality-service
uvicorn app.main:app --port 8003

# Pipeline Orchestration Service
cd services/DataIntelligenceSuite/pipeline-orchestration-service
uvicorn app.main:app --port 8004

# Data Platform Service
cd services/DataIntelligenceSuite/data-platform-service
uvicorn app.main:app --port 8001
```

## Service Integration

Services communicate through:
- **Event-driven architecture** using Apache Pulsar
- **REST APIs** for synchronous operations
- **Service discovery** via Consul
- **Shared configuration** in Consul KV

See [SERVICE_INTEGRATION.md](SERVICE_INTEGRATION.md) for detailed integration patterns.

## API Documentation

Each service exposes OpenAPI documentation at:
- DIH Service: http://localhost:8002/docs
- Data Quality Service: http://localhost:8003/docs
- Pipeline Orchestration Service: http://localhost:8004/docs
- Data Platform Service: http://localhost:8001/docs

## Configuration

### Environment Variables
```bash
# Common across all services
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=<your-token>
CONSUL_HOST=localhost
CONSUL_PORT=8500
PULSAR_SERVICE_URL=pulsar://localhost:6650

# Service-specific ports
DIH_SERVICE_PORT=8002
DATA_QUALITY_SERVICE_PORT=8003
PIPELINE_ORCHESTRATION_SERVICE_PORT=8004
DATA_PLATFORM_SERVICE_PORT=8001
```

### Consul Configuration Structure
```
/platformq/data-intelligence/
├── common/                    # Shared configuration
├── dih-service/              # DIH-specific config
├── data-quality-service/     # Quality service config
├── pipeline-orchestration/   # Pipeline service config
└── data-platform-service/    # Platform service config
```

## Development

### Code Organization
```
services/DataIntelligenceSuite/
├── dih-service/              # Digital Integration Hub
├── data-quality-service/     # Data Quality Management
├── pipeline-orchestration/   # Pipeline Orchestration
├── data-platform-service/    # Core Data Platform
└── SERVICE_INTEGRATION.md    # Integration documentation

libs/
└── data-intelligence-common/ # Shared library
```

### Testing
```bash
# Run unit tests for a service
cd services/DataIntelligenceSuite/[service-name]
pytest tests/

# Run integration tests
pytest tests/integration/

# Run end-to-end tests
pytest tests/e2e/
```

### Adding New Features

1. **Identify the appropriate service** based on domain
2. **Define events** if cross-service communication needed
3. **Implement API endpoints** following REST conventions
4. **Add event handlers** for async processing
5. **Update tests** and documentation

## Monitoring & Observability

### Metrics
All services expose Prometheus metrics at `/metrics`:
- Request latency and throughput
- Service-specific business metrics
- Resource utilization
- Error rates and types

### Logging
Structured JSON logging with:
- Correlation IDs for request tracing
- Service and component context
- Event tracking
- Error details with stack traces

### Health Checks
- `/health` - Basic liveness check
- `/ready` - Full readiness including dependencies
- `/info` - Service metadata and version

## Deployment

### Kubernetes
```yaml
# Example deployment for a service
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dih-service
  namespace: data-intelligence
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
        image: platformq/dih-service:latest
        ports:
        - containerPort: 8002
        env:
        - name: VAULT_ADDR
          valueFrom:
            secretKeyRef:
              name: vault-config
              key: address
```

### Scaling Considerations

- **Horizontal scaling**: All services designed to scale horizontally
- **Load balancing**: Use Consul for service discovery
- **Caching**: DIH service provides distributed caching
- **Async processing**: Event-driven architecture for decoupling

## Security

- **Authentication**: JWT tokens via auth-service
- **Authorization**: Policy-based with OPA
- **Encryption**: TLS for transport, Vault for at-rest
- **Secrets**: Dynamic credentials from Vault
- **Audit**: Comprehensive logging of all operations

## Troubleshooting

### Common Issues

1. **Service Discovery Failed**
   - Check Consul agent status
   - Verify service registration

2. **Event Processing Delays**
   - Check Pulsar broker health
   - Monitor consumer lag

3. **Performance Issues**
   - Review service metrics
   - Check resource allocation
   - Analyze slow queries

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
```

## Contributing

1. Fork the repository
2. Create feature branch
3. Make changes with tests
4. Submit pull request

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for details.

## License

Copyright © PlatformQ. All rights reserved. 