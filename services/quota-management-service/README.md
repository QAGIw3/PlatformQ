# Quota Management Service

The Quota Management Service provides resource quota enforcement and usage tracking for multi-tenant environments. It ensures fair resource allocation, prevents resource exhaustion, and provides real-time usage monitoring.

## Features

### Quota Management
- Flexible resource quotas (CPU, Memory, Storage, Instances, Networks, Databases)
- Per-tenant quota configuration with defaults
- Soft and hard limit enforcement
- Multiple quota periods (hourly, daily, weekly, monthly, yearly)
- Real-time quota status tracking (OK, Warning, Exceeded)

### Usage Tracking
- Real-time resource usage monitoring
- Historical usage tracking with configurable retention
- Usage pattern analysis and reporting
- Automatic usage updates from resource lifecycle events
- In-memory caching for performance

### Quota Enforcement
- Pre-allocation quota checks
- Three-tier enforcement (Allow, Warn, Block)
- Configurable soft/hard thresholds
- Grace periods for soft limits
- Integration with provisioning services

### Alerts and Notifications
- Multi-threshold alerts (50%, 75%, 90%, 95%, 100%)
- Alert deduplication (24-hour window)
- Real-time event streaming via Pulsar
- Alert history tracking
- Customizable alert messages

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Resource Events │────▶│ Quota Manager    │────▶│   Repository    │
│   (Pulsar)      │     │                  │     │ (Cassandra/     │
└─────────────────┘     └──────────────────┘     │  Ignite)        │
                                │                 └─────────────────┘
                                ▼
                        ┌──────────────────┐
                        │  Usage Tracker   │
                        │  (In-Memory +    │
                        │   Persistent)    │
                        └──────────────────┘
                                │
                                ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │ Alert Generator  │────▶│ Quota Events    │
                        └──────────────────┘     │   (Pulsar)      │
                                                 └─────────────────┘
```

## API Endpoints

### Quota Check
- `POST /api/v1/quota/check` - Check if resource allocation is allowed

### Quota Management
- `GET /api/v1/quotas/{tenant_id}` - Get all quotas for tenant
- `GET /api/v1/quotas/{tenant_id}/{resource_type}` - Get specific quota
- `POST /api/v1/quotas/{tenant_id}` - Set quota for tenant
- `PUT /api/v1/quotas/{tenant_id}/{resource_type}` - Update existing quota

### Usage Tracking
- `GET /api/v1/usage/{tenant_id}` - Get current usage for all resources
- `GET /api/v1/usage/{tenant_id}/{resource_type}` - Get usage for specific resource
- `PUT /api/v1/usage/{tenant_id}` - Update usage (manual adjustment)
- `GET /api/v1/usage-history/{tenant_id}/{resource_type}` - Get usage history

### Alerts
- `GET /api/v1/alerts/{tenant_id}` - Get quota alerts

### Status and Summary
- `GET /api/v1/status/{tenant_id}` - Get comprehensive quota status
- `GET /api/v1/summary/{tenant_id}` - Get quota summary
- `POST /api/v1/initialize/{tenant_id}` - Initialize default quotas

## Configuration

Key environment variables:

```bash
# Service Configuration
SERVICE_NAME=quota-management-service
SERVICE_PORT=8091

# Database Configuration
CASSANDRA_HOSTS=cassandra:9042
CASSANDRA_KEYSPACE=platformq
IGNITE_HOST=ignite
IGNITE_PORT=10800
IGNITE_CACHE_NAME=quota-cache

# Pulsar Configuration
PULSAR_URL=pulsar://pulsar:6650
PULSAR_TOPIC_PREFIX=persistent://public/default/
PULSAR_QUOTA_EVENTS_TOPIC=quota-events
PULSAR_RESOURCE_EVENTS_TOPIC=resource-events

# Quota Management
QUOTA_CHECK_INTERVAL_SECONDS=60
QUOTA_ENFORCEMENT_ENABLED=true
QUOTA_SOFT_LIMIT_THRESHOLD=0.8  # 80%
QUOTA_HARD_LIMIT_THRESHOLD=1.0  # 100%

# Default Quotas (per tenant)
DEFAULT_QUOTA_CPU_CORES=100
DEFAULT_QUOTA_MEMORY_GB=256
DEFAULT_QUOTA_STORAGE_GB=1000
DEFAULT_QUOTA_INSTANCES=50
DEFAULT_QUOTA_NETWORKS=10
DEFAULT_QUOTA_DATABASES=5

# Usage Tracking
USAGE_TRACKING_ENABLED=true
USAGE_CACHE_TTL_SECONDS=300  # 5 minutes
USAGE_HISTORY_RETENTION_DAYS=90

# Alerts
QUOTA_ALERT_ENABLED=true
QUOTA_ALERT_THRESHOLDS=50,75,90,95  # Comma-separated percentages
```

## Resource Types

The service supports the following resource types:

- **COMPUTE**: CPU cores/vCPUs
- **MEMORY**: RAM in GB
- **STORAGE**: Disk space in GB
- **INSTANCES**: Number of compute instances/VMs
- **NETWORK**: Number of networks/VPCs
- **DATABASE**: Number of database instances

## Quota Check Flow

1. Service requests quota check with resource type and amount
2. Quota Manager retrieves current quota and usage
3. Calculates new usage percentage
4. Determines action based on thresholds:
   - < 80%: ALLOW
   - 80-100%: WARN (but allow)
   - ≥ 100%: BLOCK
5. Publishes quota event
6. Checks and generates alerts if needed
7. Returns action and message

## Event Processing

### Incoming Resource Events

```json
{
  "event_type": "created|updated|deleted",
  "tenant_id": "tenant-001",
  "resource_id": "vm-12345",
  "resource_type": "compute",
  "resource_size": {
    "cpu": 4,
    "memory": 8
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Outgoing Quota Events

```json
{
  "event_type": "quota_check",
  "tenant_id": "tenant-001",
  "resource_type": "compute",
  "current_usage": 45.0,
  "quota_limit": 100.0,
  "percentage_used": 45.0,
  "action": "allow|warn|block",
  "message": "Optional message",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## Deployment

### Docker

```bash
docker build -t quota-management-service .
docker run -p 8091:8091 --env-file .env quota-management-service
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: quota-management-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: quota-management-service
  template:
    metadata:
      labels:
        app: quota-management-service
    spec:
      containers:
      - name: quota-management-service
        image: quota-management-service:latest
        ports:
        - containerPort: 8091
        env:
        - name: CASSANDRA_HOSTS
          value: "cassandra:9042"
        # ... other environment variables
```

## Scheduled Tasks

1. **Quota Status Check** - Every 60 seconds
   - Updates quota status based on current usage
   - Updates Prometheus metrics
   - Triggers status change events

2. **Usage History Cleanup** - Daily at 2 AM
   - Removes usage history older than retention period
   - Optimizes database storage

## Monitoring

### Prometheus Metrics

- `quota_checks_total` - Total quota checks by tenant, resource, and action
- `quota_exceeded_total` - Count of quota exceeded events
- `quota_alerts_total` - Alerts generated by threshold
- `resource_usage_current` - Current resource usage
- `quota_utilization_percentage` - Quota utilization percentage

### Health Checks

- `/health` - Basic health check
- `/ready` - Readiness check (validates DB connections)
- `/metrics` - Prometheus metrics endpoint

## Integration with Other Services

- **Provisioning Services**: Enforce quotas before resource creation
- **Resource Monitoring Service**: Receive usage updates
- **Cost Optimization Service**: Provide usage data for cost analysis
- **Notification Service**: Send quota alerts to users
- **Dashboard Service**: Display quota status and usage

## Security

- Service-to-service authentication via mTLS
- API authentication via JWT tokens
- Row-level security for multi-tenancy
- Encrypted data at rest in Cassandra
- Audit logging for all quota changes

## Performance Considerations

- In-memory caching reduces database load
- Batch processing for resource events
- Asynchronous event processing
- Connection pooling for databases
- Configurable cache TTL

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export CASSANDRA_HOSTS=localhost:9042
export IGNITE_HOST=localhost
# ... other variables

# Run the service
python app/main.py
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/

# Run with coverage
pytest --cov=app tests/
``` 