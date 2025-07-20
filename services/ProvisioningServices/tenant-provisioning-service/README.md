# Tenant Provisioning Service

The Tenant Provisioning Service orchestrates the complete lifecycle of tenant onboarding, management, and deprovisioning across the Platform Q ecosystem.

## Overview

This service provides:
- Automated tenant onboarding workflow
- Cross-service tenant provisioning
- Tenant lifecycle management
- Resource quota initialization
- Event-driven provisioning

## Features

- **Automated Onboarding**: Complete tenant setup across all services
- **Tier-based Provisioning**: Different resources based on subscription tier
- **Orchestrated Workflow**: Coordinated provisioning across services
- **Rollback Support**: Automatic rollback on failures
- **Event Integration**: Responds to tenant lifecycle events
- **Status Tracking**: Real-time provisioning status

## API Endpoints

### Tenant Provisioning
- `POST /api/v1/tenants/provision` - Provision new tenant
- `POST /api/v1/tenants/deprovision` - Deprovision tenant
- `GET /api/v1/tenants/{tenant_id}/status` - Get provisioning status
- `POST /api/v1/tenants/{tenant_id}/retry` - Retry failed provisioning

### Tenant Management
- `PUT /api/v1/tenants/{tenant_id}/upgrade` - Upgrade tenant tier
- `PUT /api/v1/tenants/{tenant_id}/downgrade` - Downgrade tenant tier
- `POST /api/v1/tenants/{tenant_id}/suspend` - Suspend tenant
- `POST /api/v1/tenants/{tenant_id}/activate` - Activate tenant

### Provisioning Status
- `GET /api/v1/provisioning/active` - List active provisioning
- `GET /api/v1/provisioning/failed` - List failed provisioning
- `GET /api/v1/provisioning/{request_id}` - Get specific request

### Health & Metrics
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8008)
- `PULSAR_URL` - Pulsar broker URL
- `INFRASTRUCTURE_SERVICE_URL` - Infrastructure provisioning service
- `USER_SERVICE_URL` - User provisioning service
- `QUOTA_SERVICE_URL` - Quota management service
- `PROVISIONING_TIMEOUT` - Total provisioning timeout
- `RETRY_ATTEMPTS` - Number of retry attempts

## Tenant Tiers

### Free Tier
- Basic infrastructure resources
- Limited quotas
- Community support
- Single user

### Starter Tier
- Enhanced infrastructure
- Increased quotas
- Email support
- Up to 5 users

### Professional Tier
- Full infrastructure suite
- High quotas
- Priority support
- Up to 25 users

### Enterprise Tier
- Unlimited infrastructure
- Custom quotas
- Dedicated support
- Unlimited users

## Provisioning Workflow

1. **Request Validation**: Validate tenant information
2. **Infrastructure Setup**: Provision infrastructure resources
3. **Service Configuration**: Configure tenant in each service
4. **User Creation**: Create initial admin user
5. **Quota Initialization**: Set resource quotas
6. **Notification**: Send welcome email
7. **Verification**: Validate all provisioning

## Services Provisioned

### Infrastructure
- Cassandra keyspace
- Elasticsearch indices
- Ignite caches
- MinIO buckets
- Pulsar namespaces
- Consul KV store
- Vault secrets
- JanusGraph (Enterprise)

### Platform Services
- Authentication setup
- Project workspace
- API keys generation
- Monitoring dashboards
- Cost tracking

### Collaboration Tools
- Nextcloud storage
- OpenProject workspace
- GitLab namespace
- Communication channels

## Event Handling

### Incoming Events
- `TenantCreatedEvent` - New tenant signup
- `TenantUpgradedEvent` - Tier upgrade
- `TenantDowngradedEvent` - Tier downgrade
- `TenantSuspendedEvent` - Account suspension
- `TenantDeletedEvent` - Account deletion

### Outgoing Events
- `TenantProvisionedEvent` - Provisioning complete
- `TenantDeprovisionedEvent` - Deprovisioning complete
- `ProvisioningFailedEvent` - Provisioning failed
- `TenantStatusChangedEvent` - Status update

## Error Handling

### Retry Strategy
- Exponential backoff
- Service-specific retries
- Partial provisioning recovery
- Dead letter queues

### Rollback Process
- Reverse provisioning order
- Clean up partial resources
- Restore previous state
- Notify administrators

## Integration

### Service Dependencies
- Infrastructure Provisioning Service
- User Provisioning Service
- Quota Management Service
- Cost Optimization Service
- Notification Service

### External Systems
- Payment gateway
- CRM system
- Support ticketing
- Analytics platform

## Development

### Running Locally
```bash
cd services/tenant-provisioning-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8008
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t tenant-provisioning-service:latest -f services/tenant-provisioning-service/Dockerfile .
```

## Architecture

The service consists of:

- **Orchestrator**: Coordinates provisioning workflow
- **Service Clients**: Communicate with other services
- **Event Processor**: Handle lifecycle events
- **State Manager**: Track provisioning state
- **Repository**: Persist provisioning data

## Provisioning State Machine

States:
- `PENDING` - Request received
- `IN_PROGRESS` - Provisioning started
- `PARTIALLY_COMPLETED` - Some services provisioned
- `COMPLETED` - All services provisioned
- `FAILED` - Provisioning failed
- `ROLLED_BACK` - Changes reversed

## Monitoring

The service exposes Prometheus metrics for:
- Provisioning request count
- Provisioning duration by tier
- Success/failure rates
- Service-specific provisioning times
- Rollback frequency 