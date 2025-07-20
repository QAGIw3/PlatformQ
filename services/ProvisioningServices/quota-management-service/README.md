# Quota Management Service

The Quota Management Service provides centralized resource quota management, usage tracking, and limit enforcement across the Platform Q ecosystem.

## Overview

This service enables:
- Fine-grained resource quotas per tenant
- Real-time usage tracking
- Quota enforcement and validation
- Usage forecasting and alerts
- Hierarchical quota management

## Features

- **Multi-Resource Quotas**: Support for compute, storage, network, and service-specific quotas
- **Flexible Periods**: Hourly, daily, weekly, monthly, yearly quotas
- **Soft and Hard Limits**: Warning thresholds and absolute limits
- **Usage Tracking**: Real-time resource consumption monitoring
- **Quota Inheritance**: Hierarchical quota management for organizations
- **Event-driven Updates**: Automatic usage updates via event streams

## API Endpoints

### Quota Management
- `POST /api/v1/quotas` - Create quota
- `GET /api/v1/quotas/{tenant_id}` - Get tenant quotas
- `PUT /api/v1/quotas/{tenant_id}/{resource_type}` - Update quota
- `DELETE /api/v1/quotas/{tenant_id}/{resource_type}` - Delete quota

### Usage Tracking
- `GET /api/v1/usage/{tenant_id}` - Get current usage
- `GET /api/v1/usage/{tenant_id}/history` - Get usage history
- `POST /api/v1/usage/{tenant_id}/update` - Update usage (internal)

### Quota Validation
- `POST /api/v1/check` - Check if operation is allowed
- `GET /api/v1/remaining/{tenant_id}/{resource_type}` - Get remaining quota

### Reports
- `GET /api/v1/reports/usage-summary` - Usage summary report
- `GET /api/v1/reports/quota-utilization` - Quota utilization report

### Health & Metrics
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8003)
- `CASSANDRA_HOSTS` - Cassandra contact points
- `IGNITE_HOST` - Ignite cache host
- `PULSAR_URL` - Pulsar broker URL
- `USAGE_UPDATE_INTERVAL` - Usage update interval (seconds)
- `QUOTA_CHECK_CACHE_TTL` - Quota check cache TTL (seconds)

## Resource Types

Supported resource types:
- **Compute**: CPU cores, memory, instances
- **Storage**: Block storage, object storage, snapshots
- **Network**: Bandwidth, floating IPs, load balancers
- **Database**: Instances, storage, connections
- **Services**: API calls, functions, queues

## Quota Models

### Basic Quota
```json
{
  "tenant_id": "tenant-123",
  "resource_type": "cpu",
  "limit": 100,
  "period": "monthly",
  "unit": "cores"
}
```

### Advanced Quota
```json
{
  "tenant_id": "tenant-123",
  "resource_type": "storage",
  "soft_limit": 900,
  "hard_limit": 1000,
  "period": "monthly",
  "unit": "GB",
  "metadata": {
    "notification_threshold": 0.8,
    "auto_scale": true
  }
}
```

## Usage Tracking

### Real-time Updates
- Event-driven updates from resource services
- Aggregated usage calculations
- Cache-backed for performance

### Historical Tracking
- Time-series data storage
- Configurable retention periods
- Usage trend analysis

## Quota Enforcement

### Pre-allocation Checks
- Validate before resource provisioning
- Reserve quota during provisioning
- Rollback on failure

### Post-allocation Tracking
- Monitor actual usage
- Alert on threshold breaches
- Automatic enforcement actions

## Integration

### Service Integration
- Compute Allocation Service
- Infrastructure Provisioning Service
- Resource Scaling Service
- Cost Optimization Service

### Event Streams
- Resource allocation events
- Usage update events
- Quota exceeded events
- Threshold alert events

## Development

### Running Locally
```bash
cd services/quota-management-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8003
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t quota-management-service:latest -f services/quota-management-service/Dockerfile .
```

## Architecture

The service consists of:

- **Quota Manager**: Core quota management logic
- **Usage Tracker**: Real-time usage tracking
- **Enforcement Engine**: Quota validation and enforcement
- **Event Processor**: Handles usage update events
- **Repository**: Data persistence layer

## Caching Strategy

- **Ignite Cache**: Hot data and frequent lookups
- **Local Cache**: Per-request quota checks
- **TTL Management**: Configurable cache expiration

## Monitoring

The service exposes Prometheus metrics for:
- Quota check request rates
- Usage update frequencies
- Quota exceeded events
- Cache hit/miss rates
- API response times 