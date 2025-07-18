
# Provisioning Service

This service is responsible for orchestrating the provisioning of all necessary resources for tenants and services, including infrastructure resources and compute capacity allocation.

## Features

### Tenant Provisioning
- Cassandra keyspace creation with proper replication
- MinIO bucket setup with lifecycle policies
- Pulsar namespace configuration
- OpenProject workspace creation
- Apache Ignite cache initialization
- Elasticsearch index creation
- Automated resource quota assignment

### Compute Provisioning
- **Dynamic Compute Allocation** - Provision compute resources on-demand
- **Partner Integration** - Allocate resources from cloud partners via derivatives engine
- **Multi-Resource Support** - GPU, CPU, memory, and storage provisioning
- **Cost Optimization** - Automatic selection of most cost-effective providers
- **Access Management** - Secure access details generation (SSH, Jupyter, monitoring)
- **Lifecycle Management** - Provision, monitor, and terminate resources

### Dynamic Scaling
- **Auto-scaling** - Automatic resource scaling based on demand
- **Predictive Scaling** - ML-based demand prediction
- **Cost-aware Scaling** - Balance performance and cost
- **Multi-tenant Management** - Fair resource sharing across tenants
- **Business Rules** - Time-based and policy-driven scaling

## Architecture

The service uses:
- **Event-driven architecture** with Apache Pulsar
- **Apache Ignite** for distributed caching
- **Kubernetes** for container orchestration
- **Prometheus** for metrics collection
- **FastAPI** for REST APIs

## API Endpoints

### Tenant Management
- `POST /api/v1/provision` - Manual tenant provisioning
- `GET /api/v1/tenants/{tenant_id}/provisioning-status` - Get provisioning status
- `GET /api/v1/tenants/{tenant_id}/quota` - Get tenant quota
- `PUT /api/v1/tenants/{tenant_id}/quota` - Update tenant quota

### Compute Provisioning
- `POST /api/v1/compute/provision` - Provision compute resources
- `GET /api/v1/compute/provision/{request_id}` - Get provisioning status
- `DELETE /api/v1/compute/provision/{request_id}` - Terminate resources
- `GET /api/v1/compute/capacity` - Get available capacity

### Scaling Policies
- `GET /api/v1/scaling/policies` - List scaling policies
- `POST /api/v1/scaling/policies` - Create scaling policy
- `PUT /api/v1/scaling/policies/{policy_id}` - Update policy
- `DELETE /api/v1/scaling/policies/{policy_id}` - Delete policy

## Configuration

Environment variables:
```
PULSAR_URL=pulsar://pulsar:6650
CASSANDRA_HOSTS=cassandra1,cassandra2,cassandra3
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
IGNITE_HOST=ignite
IGNITE_PORT=10800
ELASTICSEARCH_HOST=elasticsearch
PROMETHEUS_URL=http://prometheus:9090
DERIVATIVES_ENGINE_URL=http://derivatives-engine-service:8000
K8S_NAMESPACE=platformq
```

## Integration

### Derivatives Engine Service
- Requests capacity allocation through cross-service coordinator
- Leverages partner capacity management
- Benefits from wholesale pricing
- Automatic failover to alternative providers

### MLOps Service
- Automatic GPU provisioning for model training
- Resource reservation for scheduled jobs
- Cost estimation and optimization

### Event Processing
The service processes events:
- `tenant-created-events` - Provision tenant resources
- `user-created-events` - Create user-specific resources
- `resource-anomaly-events` - Handle resource issues
- `scaling-request-events` - Execute scaling operations
- `quota-exceeded-events` - Enforce resource limits

## Monitoring

- Health check at `/health`
- Readiness at `/ready`
- Prometheus metrics at `/metrics`
- Resource utilization dashboards
- Scaling event tracking
- Cost monitoring

## Development

To install dependencies:
```bash
pip install -r requirements.txt
```

To run locally:
```bash
uvicorn app.main:app --reload
```

To run tests:
```bash
pytest tests/
```

## Security

- JWT authentication
- Role-based access control
- Tenant isolation
- Resource quota enforcement
- Audit logging

## Performance

- Async event processing
- Distributed caching
- Efficient resource pooling
- Optimized API responses
- Horizontal scaling support 