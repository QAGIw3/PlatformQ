# Infrastructure Provisioning Service

The Infrastructure Provisioning Service manages the provisioning and lifecycle of infrastructure resources for tenants in the Platform Q ecosystem.

## Overview

This service handles provisioning of:
- **Apache Cassandra** - Distributed NoSQL database
- **Elasticsearch** - Search and analytics engine
- **Apache Ignite** - In-memory computing platform
- **MinIO** - Object storage
- **Apache Pulsar** - Messaging and streaming platform
- **Consul** - Service mesh and configuration
- **HashiCorp Vault** - Secrets management
- **JanusGraph** - Graph database

## Features

- **Multi-tenant Infrastructure**: Isolated resources per tenant
- **Tier-based Provisioning**: Different resource sets based on tenant tier
- **Parallel Provisioning**: Provision multiple resources concurrently
- **Event-driven**: Responds to tenant lifecycle events
- **Resource Validation**: Verify provisioned resources are healthy
- **Rollback Support**: Automatic rollback on provisioning failures

## API Endpoints

### Provisioning
- `POST /api/v1/infrastructure/provision` - Provision infrastructure resources
- `POST /api/v1/infrastructure/deprovision` - Deprovision infrastructure resources
- `GET /api/v1/infrastructure/{tenant_id}` - Get all infrastructure for a tenant
- `GET /api/v1/infrastructure/{tenant_id}/{resource_type}` - Get specific resource

### Validation & Management
- `POST /api/v1/infrastructure/validate/{tenant_id}` - Validate infrastructure
- `POST /api/v1/infrastructure/cleanup` - Cleanup orphaned resources
- `GET /api/v1/provisioners` - List available provisioners
- `GET /api/v1/infrastructure/status/{request_id}` - Get provisioning status

### Health & Metrics
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8004)
- `CASSANDRA_HOSTS` - Cassandra contact points
- `ELASTICSEARCH_HOSTS` - Elasticsearch hosts
- `IGNITE_HOST` - Apache Ignite host
- `MINIO_ENDPOINT` - MinIO endpoint
- `PULSAR_URL` - Pulsar broker URL
- `CONSUL_HOST` - Consul host
- `VAULT_ADDR` - Vault address
- `JANUSGRAPH_HOST` - JanusGraph host

## Tier-based Resource Allocation

### Free Tier
- Cassandra keyspace
- MinIO bucket
- Consul KV store

### Starter Tier
- All Free tier resources
- Pulsar namespace
- Ignite caches

### Professional Tier
- All Starter tier resources
- Elasticsearch indices
- Vault secret engine

### Enterprise/Custom Tier
- All Professional tier resources
- JanusGraph graph database
- Enhanced resource limits

## Development

### Running Locally
```bash
cd services/infrastructure-provisioning-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8004
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t infrastructure-provisioning-service:latest -f services/infrastructure-provisioning-service/Dockerfile .
```

## Architecture

The service follows a modular architecture:

- **Orchestrator**: Coordinates provisioning across multiple resources
- **Provisioners**: Individual resource provisioners (one per infrastructure type)
- **Repository**: Handles data persistence in Cassandra
- **Event Processor**: Handles tenant lifecycle events

## Dependencies

- **platformq-resource-common**: Shared models and interfaces
- **Apache Cassandra**: For storing provisioning state
- **Apache Pulsar**: For event-driven communication

## Monitoring

The service exposes Prometheus metrics for:
- Provisioning request count and duration
- Resource provisioning success/failure rates
- Infrastructure validation status
- API endpoint latencies 