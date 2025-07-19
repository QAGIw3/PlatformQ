
# Provisioning Service

The Provisioning Service is responsible for orchestrating the provisioning of all necessary resources for tenants and services, including infrastructure resources, compute capacity allocation, and dynamic resource scaling.

## Architecture

The service has been refactored to use a clean, modular architecture:

- **Shared Compute Library** - Common models, providers, and cost management
- **Configuration Management** - Consul for dynamic config, Vault for secrets
- **Event-Driven Processing** - Apache Pulsar for async event handling
- **Distributed Caching** - Apache Ignite for state management
- **Multi-Provider Support** - Unified interface for AWS, CloudStack, Kubernetes, etc.

## Features

### Tenant Provisioning
- Automated resource quota assignment based on subscription tiers
- Multi-tenant isolation with Kubernetes namespaces
- Infrastructure provisioning across multiple services:
  - Cassandra keyspace creation with proper replication
  - MinIO bucket setup with lifecycle policies
  - Pulsar namespace configuration
  - Apache Ignite cache initialization
  - Elasticsearch index creation
  - JanusGraph schema setup

### Compute Provisioning
- **Dynamic Allocation** - On-demand compute resources from multiple providers
- **Provider Abstraction** - Unified interface for cloud and on-premise resources
- **Cost Optimization** - Automatic selection of most cost-effective providers
- **Partner Integration** - Leverage capacity through derivatives engine
- **Access Management** - Secure credential generation and distribution

### Dynamic Scaling
- **Auto-scaling** - Horizontal and vertical scaling based on metrics
- **Predictive Scaling** - ML-based demand prediction using scikit-learn
- **Cost-aware Scaling** - Balance performance and cost objectives
- **Policy-driven** - Configurable scaling policies per service
- **Business Rules** - Time-based and compliance-aware scaling

### Resource Monitoring
- Real-time metrics collection from Prometheus
- Anomaly detection and alerting
- Resource usage tracking per tenant
- Cost monitoring and budget enforcement

## API Endpoints

### Tenant Management
- `POST /api/v1/tenants/provision` - Provision new tenant
- `GET /api/v1/tenants/{tenant_id}/quota` - Get tenant quota
- `GET /api/v1/tenants/{tenant_id}/usage` - Get current usage

### Compute Provisioning
- `POST /api/v1/compute/provision` - Provision compute resources
- `GET /api/v1/compute/provision/{allocation_id}` - Get provisioning status
- `DELETE /api/v1/compute/provision/{allocation_id}` - Terminate resources
- `GET /api/v1/compute/capacity` - Get available capacity

### Scaling Management
- `GET /api/v1/scaling/policies` - List scaling policies
- `POST /api/v1/scaling/policies` - Create/update scaling policy
- `GET /api/v1/scaling/decisions` - Get recent scaling decisions

### Monitoring
- `GET /api/v1/metrics/resources` - Get resource metrics
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

## Configuration

The service uses a combination of environment variables, Consul KV, and Vault secrets:

### Environment Variables
```bash
# Service Configuration
SERVICE_NAME=provisioning-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8000
ENVIRONMENT=production

# Consul Configuration
CONSUL_HOST=consul
CONSUL_PORT=8500
CONSUL_TOKEN=<consul-token>

# Vault Configuration
VAULT_ENABLED=true
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<vault-token>

# Database Configuration
CASSANDRA_HOSTS=cassandra1,cassandra2,cassandra3
ELASTICSEARCH_HOSTS=elasticsearch:9200

# Messaging
PULSAR_URL=pulsar://pulsar:6650

# Monitoring
PROMETHEUS_URL=http://prometheus:9090

# Integration Services
DERIVATIVES_ENGINE_URL=http://derivatives-engine-service:8000
```

### Consul Configuration
Dynamic configuration is stored in Consul under:
- `platformq/provisioning-service/compute_providers` - Provider configurations
- `platformq/provisioning-service/scaling_policies` - Default scaling policies
- `platformq/provisioning-service/tenant_budgets` - Budget limits

### Vault Secrets
Sensitive credentials are stored in Vault:
- `providers/{provider}/credentials` - Cloud provider credentials
- `database/{db}/credentials` - Database credentials

## Dependencies

- **FastAPI** - REST API framework
- **Apache Ignite** - Distributed caching and compute
- **Apache Pulsar** - Event streaming
- **Kubernetes Client** - Container orchestration
- **Prometheus Client** - Metrics collection
- **scikit-learn** - Predictive scaling models
- **Consul** - Service discovery and configuration
- **Vault** - Secret management

## Development

### Installation
```bash
pip install -r requirements.txt
```

### Running Locally
```bash
uvicorn app.main:app --reload --port 8000
```

### Running Tests
```bash
pytest tests/
```

## Deployment

The service is deployed as a Kubernetes deployment with:
- Horizontal Pod Autoscaling
- Resource limits and requests
- Health checks and readiness probes
- Prometheus monitoring
- Distributed tracing with Jaeger

## Security

- JWT authentication via shared auth service
- Role-based access control (RBAC)
- Tenant isolation at infrastructure level
- Encrypted secrets with Vault
- mTLS for service-to-service communication via Consul Connect

## Monitoring & Observability

- **Metrics** - Prometheus metrics exposed at `/metrics`
- **Logging** - Structured JSON logging
- **Tracing** - OpenTelemetry integration
- **Dashboards** - Grafana dashboards for resource utilization
- **Alerts** - Prometheus alerts for anomalies and SLA violations 