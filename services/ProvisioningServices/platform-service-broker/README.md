# Platform Service Broker

Open Service Broker API (OSB) implementation for Platform Q cloud brokerage platform. This service provides a unified interface for provisioning cloud resources and Platform Q services.

## Overview

The Platform Service Broker implements the Open Service Broker API v2.16 specification and provides:

- **OpenStack Resource Provisioning**: Compute, storage, and network resources
- **Platform Q Services**: Cassandra, Ignite, Pulsar, MinIO, Elasticsearch, JanusGraph
- **Hierarchical Multi-tenancy**: Reseller → Customer → Tenant structure
- **Cloudify Integration**: Blueprint-based orchestration
- **Metering Integration**: CloudKitty and OpenMeter for usage tracking

## Architecture

```
┌─────────────────────┐
│   Client Apps       │
└──────────┬──────────┘
           │ OSB API
┌──────────▼──────────┐
│  Platform Service   │
│      Broker         │
├─────────────────────┤
│ • OpenStack Broker  │
│ • Platform Broker   │
│ • Metering Client   │
└──────────┬──────────┘
           │
    ┌──────┴──────┬─────────┬──────────┐
    ▼             ▼         ▼          ▼
┌────────┐  ┌─────────┐ ┌────────┐ ┌────────┐
│OpenStack│  │Cloudify │ │CloudKit│ │Platform│
│         │  │         │ │   ty   │ │Services│
└────────┘  └─────────┘ └────────┘ └────────┘
```

## Features

### Service Catalog

#### OpenStack Services
- **Compute**: Virtual machines with various flavors (small to 2xlarge, GPU options)
- **Storage**: Block storage volumes (standard and premium)
- **Network**: Virtual networks and security groups

#### Platform Q Services
- **Cassandra**: Distributed NoSQL database
- **Ignite**: In-memory computing platform
- **Pulsar**: Distributed messaging and streaming
- **MinIO**: S3-compatible object storage
- **Elasticsearch**: Search and analytics (coming soon)
- **JanusGraph**: Graph database (coming soon)

### Multi-tenancy
- Hierarchical structure: Reseller → Customer → Tenant
- Quota management per tier
- Resource isolation
- Usage tracking and billing

## API Endpoints

### OSB Standard Endpoints
- `GET /v2/catalog` - Get service catalog
- `PUT /v2/service_instances/{instance_id}` - Provision service
- `PATCH /v2/service_instances/{instance_id}` - Update service
- `DELETE /v2/service_instances/{instance_id}` - Deprovision service
- `PUT /v2/service_instances/{instance_id}/service_bindings/{binding_id}` - Create binding
- `DELETE /v2/service_instances/{instance_id}/service_bindings/{binding_id}` - Remove binding
- `GET /v2/service_instances/{instance_id}/last_operation` - Get operation status

### Health & Monitoring
- `GET /health` - Health check
- `GET /ready` - Readiness check
- `GET /metrics` - Prometheus metrics

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_PORT=8080
ENABLE_OPENSTACK_BROKER=true
ENABLE_PLATFORM_BROKER=true

# OpenStack Configuration
OS_AUTH_URL=http://keystone:5000/v3
OS_REGION_NAME=RegionOne
OS_INTERFACE=public
OS_SERVICE_PROJECT=service
OS_SERVICE_USER=platform-broker
OS_SERVICE_PASSWORD=<password>
OS_SERVICE_DOMAIN=default

# Cloudify Configuration
CLOUDIFY_URL=http://cloudify-manager
CLOUDIFY_USERNAME=admin
CLOUDIFY_PASSWORD=admin
CLOUDIFY_TENANT=default_tenant

# CloudKitty Configuration
CLOUDKITTY_ENABLED=true
CLOUDKITTY_URL=http://cloudkitty:8889

# OpenMeter Configuration
OPENMETER_ENABLED=true
OPENMETER_URL=http://openmeter:8080
OPENMETER_API_KEY=<api-key>

# Platform Services
CASSANDRA_HOSTS=cassandra:9042
IGNITE_HOST=ignite
IGNITE_PORT=10800
PULSAR_URL=pulsar://pulsar:6650
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
ELASTICSEARCH_URL=http://elasticsearch:9200
JANUSGRAPH_URL=http://janusgraph:8182

# Service Discovery
CONSUL_HOST=consul
CONSUL_PORT=8500

# Secrets Management
VAULT_ENABLED=true
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<token>
```

## Deployment

### Docker

```bash
# Build image
docker build -t platform-service-broker:latest .

# Run container
docker run -d \
  --name platform-service-broker \
  -p 8080:8080 \
  --env-file .env \
  platform-service-broker:latest
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: platform-service-broker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: platform-service-broker
  template:
    metadata:
      labels:
        app: platform-service-broker
    spec:
      containers:
      - name: broker
        image: platform-service-broker:latest
        ports:
        - containerPort: 8080
        envFrom:
        - configMapRef:
            name: broker-config
        - secretRef:
            name: broker-secrets
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
```

### Helm Chart

```bash
helm install platform-service-broker ./charts/platform-service-broker \
  --set openstack.authUrl=http://keystone:5000/v3 \
  --set openstack.servicePassword=secret \
  --set cloudify.url=http://cloudify-manager
```

## Usage Examples

### List Available Services

```bash
curl -H "X-Broker-API-Version: 2.16" \
  http://broker.example.com/v2/catalog
```

### Provision an OpenStack VM

```bash
curl -X PUT \
  -H "X-Broker-API-Version: 2.16" \
  -H "Content-Type: application/json" \
  -d '{
    "service_id": "openstack-compute",
    "plan_id": "compute-medium",
    "organization_guid": "customer-123",
    "space_guid": "tenant-456",
    "context": {
      "platform": "platform-q",
      "reseller_id": "reseller-001",
      "customer_id": "customer-123",
      "tenant_id": "tenant-456"
    },
    "parameters": {
      "image": "ubuntu-22.04",
      "use_cloudify": true
    }
  }' \
  http://broker.example.com/v2/service_instances/my-instance-001
```

### Create a Cassandra Keyspace

```bash
curl -X PUT \
  -H "X-Broker-API-Version: 2.16" \
  -H "Content-Type: application/json" \
  -d '{
    "service_id": "platform-cassandra",
    "plan_id": "cassandra-prod",
    "organization_guid": "customer-123",
    "space_guid": "tenant-456"
  }' \
  http://broker.example.com/v2/service_instances/cassandra-001
```

### Bind to Service

```bash
curl -X PUT \
  -H "X-Broker-API-Version: 2.16" \
  -H "Content-Type: application/json" \
  -d '{
    "service_id": "platform-cassandra",
    "plan_id": "cassandra-prod"
  }' \
  http://broker.example.com/v2/service_instances/cassandra-001/service_bindings/binding-001
```

## Development

### Setup

```bash
# Clone repository
git clone https://github.com/platformq/platform-service-broker
cd platform-service-broker

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run locally
uvicorn app.main:app --reload --port 8080
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run with coverage
pytest --cov=app tests/

# Run linting
black app/ tests/
pylint app/
```

### Adding New Services

1. Create a new broker class inheriting from `BasePlatformBroker`
2. Implement required OSB methods
3. Add to service catalog
4. Update configuration
5. Add tests

## Monitoring

### Metrics

The broker exports Prometheus metrics:
- `osb_catalog_requests_total` - Catalog API calls
- `osb_provision_requests_total` - Provision requests by service
- `osb_bind_requests_total` - Binding requests
- `osb_operation_duration_seconds` - Operation latency

### Logging

Structured JSON logging with levels:
- `INFO` - Normal operations
- `WARNING` - Degraded functionality
- `ERROR` - Failed operations
- `DEBUG` - Detailed troubleshooting

### Alerts

Example Prometheus alerts:

```yaml
groups:
- name: platform-service-broker
  rules:
  - alert: BrokerHighErrorRate
    expr: rate(osb_errors_total[5m]) > 0.05
    annotations:
      summary: "High error rate in service broker"
  
  - alert: BrokerSlowResponse
    expr: histogram_quantile(0.95, osb_operation_duration_seconds) > 5
    annotations:
      summary: "Slow broker response times"
```

## Troubleshooting

### Common Issues

1. **OpenStack Authentication Failed**
   - Verify Keystone URL and credentials
   - Check service project exists
   - Ensure network connectivity

2. **Service Provisioning Timeout**
   - Check Cloudify blueprint execution
   - Verify resource quotas
   - Review OpenStack logs

3. **Binding Creation Failed**
   - Ensure instance exists
   - Check credential generation
   - Verify service connectivity

### Debug Mode

Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
```

## Security

- All API endpoints require valid OSB API version header
- OpenStack credentials stored in Vault
- Service bindings generate unique credentials
- Network isolation enforced per tenant
- TLS encryption for all external communication

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit pull request

## License

Apache License 2.0 