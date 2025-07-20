# Cost Optimization Service

The Cost Optimization Service analyzes cloud costs across multiple providers, generates optimization recommendations, manages budgets, and provides cost insights to help reduce cloud spending.

## Features

### Cost Analysis
- Multi-cloud cost collection (AWS, CloudStack, Kubernetes)
- Real-time cost tracking and aggregation
- Cost breakdown by resource type, provider, and tags
- Historical cost trends and comparisons
- Anomaly detection using statistical methods

### Optimization Recommendations
- **Rightsizing**: Identifies over/under-provisioned resources
- **Reserved Instances**: Recommends RI purchases based on usage patterns
- **Unused Resources**: Detects idle resources for removal
- **Scheduling**: Suggests start/stop schedules for predictable workloads
- **Storage Optimization**: Recommends lifecycle policies and archival

### Budget Management
- Flexible budget periods (daily, weekly, monthly, quarterly, yearly)
- Multi-threshold alerts (50%, 75%, 90%, 100%)
- Resource-specific budgets with filters
- Real-time budget tracking and enforcement
- Budget event streaming via Pulsar

### Integration
- AWS Cost Explorer for detailed AWS costs
- CloudStack API for private cloud costs
- Kubernetes metrics for container costs
- Prometheus for resource utilization data
- Consul for service discovery
- Pulsar for event-driven architecture

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Cloud APIs    │────▶│ Cost Analyzer    │────▶│   Repository    │
│ (AWS/K8s/CS)    │     │                  │     │ (Cassandra/     │
└─────────────────┘     └──────────────────┘     │  Ignite)        │
                                │                 └─────────────────┘
                                ▼
                        ┌──────────────────┐
                        │ Recommendation   │
                        │ Engine           │
                        └──────────────────┘
                                │
                                ▼
                        ┌──────────────────┐     ┌─────────────────┐
                        │ Budget Manager   │────▶│ Pulsar Events   │
                        └──────────────────┘     └─────────────────┘
```

## API Endpoints

### Cost Analysis
- `POST /api/v1/cost-analysis` - Analyze costs for a tenant
- `GET /api/v1/cost-analysis/{tenant_id}/{analysis_id}` - Get specific analysis
- `GET /api/v1/cost-history/{tenant_id}` - Get historical cost data
- `GET /api/v1/cost-breakdown/{tenant_id}` - Get cost breakdown by dimension
- `GET /api/v1/resource-costs/{tenant_id}` - Get detailed resource costs

### Recommendations
- `GET /api/v1/recommendations/{tenant_id}` - Get optimization recommendations
- `PUT /api/v1/recommendations/{tenant_id}/{recommendation_id}` - Update recommendation status

### Budgets
- `POST /api/v1/budgets/{tenant_id}` - Create budget
- `GET /api/v1/budgets/{tenant_id}` - List budgets
- `GET /api/v1/budgets/{tenant_id}/{budget_id}` - Get budget details
- `PUT /api/v1/budgets/{tenant_id}/{budget_id}` - Update budget
- `DELETE /api/v1/budgets/{tenant_id}/{budget_id}` - Delete budget

### Alerts
- `GET /api/v1/alerts/{tenant_id}` - Get budget alerts

### Summary
- `GET /api/v1/summary/{tenant_id}` - Get cost optimization summary

## Configuration

Key environment variables:

```bash
# Service Configuration
SERVICE_NAME=cost-optimization-service
SERVICE_PORT=8090

# AWS Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
AWS_COST_EXPLORER_ENABLED=true

# CloudStack Configuration
CLOUDSTACK_API_URL=https://cloudstack.example.com/client/api
CLOUDSTACK_API_KEY=your-api-key
CLOUDSTACK_SECRET_KEY=your-secret-key

# Kubernetes Configuration
KUBERNETES_CONFIG_TYPE=incluster  # or kubeconfig
KUBERNETES_METRICS_ENABLED=true

# Database Configuration
CASSANDRA_HOSTS=cassandra:9042
CASSANDRA_KEYSPACE=platformq
IGNITE_HOST=ignite
IGNITE_PORT=10800

# Pulsar Configuration
PULSAR_URL=pulsar://pulsar:6650
PULSAR_TOPIC_PREFIX=persistent://public/default/

# Cost Analysis Configuration
COST_ANALYSIS_INTERVAL_HOURS=24
COST_ANOMALY_THRESHOLD_PERCENT=20.0
COST_OPTIMIZATION_MIN_SAVINGS_PERCENT=5.0

# Budget Configuration
BUDGET_CHECK_INTERVAL_HOURS=6
BUDGET_ALERT_THRESHOLDS=50,75,90,100

# Recommendation Configuration
RECOMMENDATION_LOOKBACK_DAYS=30
RECOMMENDATION_CONFIDENCE_THRESHOLD=0.7
RI_RECOMMENDATION_MIN_SAVINGS=100.0
RI_RECOMMENDATION_MIN_USAGE_DAYS=20
```

## Deployment

### Docker

```bash
docker build -t cost-optimization-service .
docker run -p 8090:8090 --env-file .env cost-optimization-service
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cost-optimization-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: cost-optimization-service
  template:
    metadata:
      labels:
        app: cost-optimization-service
    spec:
      containers:
      - name: cost-optimization-service
        image: cost-optimization-service:latest
        ports:
        - containerPort: 8090
        env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: access-key-id
        # ... other environment variables
```

## Scheduled Tasks

The service runs several scheduled tasks:

1. **Cost Analysis** - Runs every 24 hours (configurable)
   - Collects costs from all providers
   - Generates recommendations
   - Checks budgets

2. **Recommendation Refresh** - Daily
   - Updates existing recommendations
   - Removes obsolete recommendations

3. **Budget Checks** - Every 6 hours (configurable)
   - Evaluates spending against budgets
   - Generates alerts

## Monitoring

### Prometheus Metrics

- `cost_analysis_total` - Total cost analyses performed
- `recommendations_generated_total` - Recommendations by type
- `budget_alerts_total` - Budget alerts triggered
- `analysis_duration_seconds` - Cost analysis duration
- `tenant_total_cost_daily` - Daily cost per tenant

### Health Checks

- `/health` - Basic health check
- `/ready` - Readiness check (validates DB connections)
- `/metrics` - Prometheus metrics endpoint

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

## Integration with Other Services

- **Tenant Provisioning Service**: Receives tenant lifecycle events
- **Resource Monitoring Service**: Gets resource metrics for recommendations
- **Notification Service**: Sends budget alerts and recommendations
- **Dashboard Service**: Provides cost visualization data

## Security

- Uses Vault for AWS credentials rotation
- Service-to-service authentication via mTLS
- API authentication via JWT tokens
- Encrypted data at rest in Cassandra
- Row-level security for multi-tenancy 