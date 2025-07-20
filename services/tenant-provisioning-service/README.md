# Tenant Provisioning Service

## Overview

The Tenant Provisioning Service orchestrates the provisioning of all infrastructure resources required for new tenants in PlatformQ. It ensures that all necessary resources are created across multiple systems in the correct order, with proper error handling and rollback capabilities.

## Features

- **Multi-System Orchestration**: Provisions resources across 11+ infrastructure systems
- **Parallel Provisioning**: Provisions independent resources in parallel for faster deployment
- **Rollback on Failure**: Automatically deprovisions resources if any step fails
- **Idempotent Operations**: Safe to retry failed provisioning attempts
- **Event-Driven**: Responds to tenant lifecycle events automatically
- **Comprehensive Logging**: Detailed logging for troubleshooting

## Supported Resources

The service provisions the following resources for each tenant:

1. **Kubernetes Namespace** - Isolated compute environment
2. **Vault Secrets** - Secure credential storage
3. **Consul Configuration** - Service configuration
4. **Cassandra Keyspace** - Time-series and event data storage
5. **MinIO Buckets** - Object storage for datasets and models
6. **Apache Ignite Caches** - High-performance caching
7. **Elasticsearch Indices** - Full-text search and analytics
8. **JanusGraph Schema** - Graph database for relationships
9. **Apache Pulsar Namespace** - Message streaming
10. **OpenProject Project** - Project management
11. **Nextcloud User** - File sharing and collaboration

## API Endpoints

### Provision Tenant
```http
POST /api/v1/tenants/provision
Content-Type: application/json

{
  "tenant_id": "tenant-123",
  "tenant_name": "Acme Corp",
  "tier": "professional",
  "resources": ["CASSANDRA_KEYSPACE", "MINIO_BUCKET"],  // Optional: specific resources
  "metadata": {
    "organization": "Acme Corp",
    "admin_email": "admin@acme.com"
  }
}
```

### Deprovision Tenant
```http
DELETE /api/v1/tenants/{tenant_id}/deprovision
```

### Get Provisioning Status
```http
GET /api/v1/provisioning/{request_id}
```

### Retry Failed Provisioning
```http
POST /api/v1/provisioning/{request_id}/retry
```

## Configuration

Environment variables:

- `CASSANDRA_HOSTS`: Comma-separated list of Cassandra hosts
- `MINIO_ENDPOINT`: MinIO endpoint (default: minio:9000)
- `PULSAR_URL`: Pulsar broker URL (default: pulsar://pulsar:6650)
- `IGNITE_HOST`: Apache Ignite host
- `CONSUL_HOST`: Consul host
- `VAULT_ADDR`: Vault address
- `OPENPROJECT_URL`: OpenProject URL
- `PROVISIONING_TIMEOUT`: Timeout for provisioning operations (seconds)
- `MAX_RETRY_ATTEMPTS`: Maximum retry attempts for failed operations
- `PARALLEL_PROVISIONING`: Enable parallel provisioning (default: true)

## Development

### Running Locally
```bash
cd services/tenant-provisioning-service
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t tenant-provisioning-service .
```

## Architecture

The service follows a modular architecture:

- **Orchestrator**: Coordinates provisioning across all systems
- **Provisioners**: Individual provisioners for each infrastructure system
- **Repository**: Handles data persistence using Cassandra and Ignite
- **Event Processor**: Handles tenant lifecycle events from Pulsar

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `tenant_provisioning_requests_total`: Total provisioning requests by status
- `tenant_provisioning_duration_seconds`: Provisioning duration histogram

## Error Handling

- Automatic rollback on provisioning failure
- Detailed error messages for troubleshooting
- Support for retrying failed resources only
- Circuit breaker pattern for external service calls 