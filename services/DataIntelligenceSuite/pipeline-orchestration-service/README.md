# Pipeline Orchestration Service

A comprehensive pipeline orchestration service for the DataIntelligenceSuite, providing centralized pipeline management, scheduling, execution, and monitoring capabilities.

## Features

### Core Capabilities
- **Pipeline Management**: Create, update, and manage data pipeline definitions
- **Flexible Scheduling**: Support for cron, interval, event-driven, and manual triggers
- **Execution Orchestration**: Coordinate pipeline execution across distributed services
- **Dependency Management**: Handle complex pipeline dependencies and workflows
- **Template System**: Pre-built templates for common pipeline patterns

### Advanced Features
- **Pipeline Optimization**: Optimize execution based on cost, performance, or resources
- **Real-time Monitoring**: Track pipeline execution, performance, and health
- **Alert Management**: Configurable alerts for failures and performance degradation
- **Event-driven Architecture**: React to data changes and system events
- **Resource Management**: Control and limit pipeline resource usage

## Architecture

The service follows a microservice architecture pattern:

```
pipeline-orchestration-service/
├── app/
│   ├── main.py                # FastAPI application and service initialization
│   ├── core/
│   │   ├── pipeline_coordinator.py  # Core coordination logic
│   │   └── pipeline_optimizer.py    # Optimization engine
│   ├── pipelines/
│   │   ├── pipeline_repository.py   # Pipeline storage
│   │   ├── pipeline_scheduler.py    # Scheduling engine
│   │   └── pipeline_executor.py     # Execution management
│   ├── monitoring/
│   │   ├── pipeline_monitor.py      # Performance monitoring
│   │   └── pipeline_metrics.py      # Metrics collection
│   ├── api/                         # REST API endpoints
│   └── events/                      # Event processing
```

## API Endpoints

### Pipeline Management
- `POST /api/v1/pipelines` - Create a new pipeline
- `GET /api/v1/pipelines` - List pipelines with filtering
- `GET /api/v1/pipelines/{id}` - Get pipeline details
- `PUT /api/v1/pipelines/{id}` - Update pipeline
- `DELETE /api/v1/pipelines/{id}` - Delete pipeline

### Execution Management
- `POST /api/v1/executions` - Execute a pipeline
- `GET /api/v1/executions/{id}` - Get execution status
- `POST /api/v1/executions/{id}/cancel` - Cancel execution
- `GET /api/v1/executions` - List executions

### Monitoring & Metrics
- `GET /api/v1/monitoring/metrics/{pipeline_id}` - Get pipeline metrics
- `GET /api/v1/monitoring/alerts` - Get active alerts
- `POST /api/v1/monitoring/alerts/{id}/acknowledge` - Acknowledge alert
- `GET /api/v1/monitoring/schedules` - Get scheduled tasks

### Templates
- `GET /api/v1/templates` - List available templates
- `GET /api/v1/templates/{id}` - Get template details
- `POST /api/v1/templates/create-pipeline` - Create pipeline from template

## Configuration

### Environment Variables
```bash
# Service Configuration
SERVICE_NAME=pipeline-orchestration-service
SERVICE_PORT=8004
LOG_LEVEL=INFO

# Vault Configuration
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=<your-token>
VAULT_NAMESPACE=platformq

# Consul Configuration
CONSUL_HOST=localhost
CONSUL_PORT=8500
CONSUL_TOKEN=<your-token>

# Event System
PULSAR_SERVICE_URL=pulsar://localhost:6650
EVENT_TOPIC_PREFIX=persistent://platformq/data-intelligence
```

### Consul Configuration
Store configuration in Consul KV:
```
pipeline-orchestration/
├── pipelines/
│   ├── definitions/          # Pipeline definitions
│   └── templates/            # Pipeline templates
├── schedules/                # Schedule configurations
├── optimization/
│   ├── rules/               # Optimization rules
│   └── resource-limits/     # Resource constraints
└── metrics/                 # Historical metrics
```

## Usage Examples

### Creating a Pipeline
```python
import httpx

# Create a data transformation pipeline
response = await client.post(
    "http://localhost:8004/api/v1/pipelines",
    json={
        "name": "Daily Sales Aggregation",
        "type": "transformation",
        "description": "Aggregate daily sales data",
        "config": {
            "steps": [
                {
                    "name": "extract",
                    "type": "extract",
                    "config": {"source": "sales_db", "table": "transactions"}
                },
                {
                    "name": "transform",
                    "type": "transform",
                    "config": {"operations": ["aggregate", "calculate_metrics"]}
                },
                {
                    "name": "load",
                    "type": "load",
                    "config": {"target": "analytics_db", "table": "daily_sales"}
                }
            ]
        },
        "schedule": {
            "type": "cron",
            "cron_expression": "0 2 * * *"  # Daily at 2 AM
        }
    }
)
```

### Executing a Pipeline
```python
# Manually trigger pipeline execution
response = await client.post(
    "http://localhost:8004/api/v1/executions",
    json={
        "pipeline_id": "pipeline-123",
        "parameters": {"date": "2024-01-15"}
    }
)

execution_id = response.json()["execution_id"]

# Check execution status
status = await client.get(
    f"http://localhost:8004/api/v1/executions/{execution_id}"
)
```

### Creating from Template
```python
# Create pipeline from template
response = await client.post(
    "http://localhost:8004/api/v1/templates/create-pipeline",
    json={
        "template_id": "bronze_to_silver",
        "name": "Customer Data Bronze to Silver",
        "overrides": {
            "source_dataset": "raw_customers",
            "target_dataset": "clean_customers"
        }
    }
)
```

## Pipeline Templates

### Available Templates

1. **Bronze to Silver Pipeline**
   - Data quality checks
   - Cleansing and normalization
   - Deduplication

2. **Silver to Gold Pipeline**
   - Business aggregations
   - Metric calculations
   - Optimization

3. **Batch Ingestion Pipeline**
   - Extract from source
   - Schema validation
   - Load to data lake

4. **Streaming Ingestion Pipeline**
   - Real-time consumption
   - Transformation
   - Micro-batch processing

## Development

### Setup
```bash
# Install dependencies
pip install -r requirements.in

# Run locally
cd services/DataIntelligenceSuite/pipeline-orchestration-service
uvicorn app.main:app --reload --port 8004
```

### Testing
```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/
```

## Deployment

### Docker
```bash
# Build image
docker build -t pipeline-orchestration-service:latest .

# Run container
docker run -p 8004:8004 \
  -e VAULT_ADDR=$VAULT_ADDR \
  -e CONSUL_HOST=$CONSUL_HOST \
  pipeline-orchestration-service:latest
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipeline-orchestration-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: pipeline-orchestration-service
  template:
    metadata:
      labels:
        app: pipeline-orchestration-service
    spec:
      containers:
      - name: pipeline-orchestration-service
        image: pipeline-orchestration-service:latest
        ports:
        - containerPort: 8004
        env:
        - name: VAULT_ADDR
          valueFrom:
            secretKeyRef:
              name: vault-config
              key: address
```

## Monitoring & Observability

### Metrics
The service exposes Prometheus metrics at `/metrics`:
- `pipeline_executions_started_total` - Total executions started
- `pipeline_executions_completed_total` - Total executions completed
- `pipeline_execution_duration_seconds` - Execution duration
- `pipelines_active` - Number of active pipelines
- `pipeline_schedule_lag_seconds` - Schedule execution lag

### Health Checks
- `/health` - Basic health check
- `/ready` - Readiness check with component status

### Logging
Structured JSON logging with trace correlation:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "service": "pipeline-orchestration-service",
  "trace_id": "abc123",
  "message": "pipeline_execution_started",
  "pipeline_id": "pipeline-123",
  "execution_id": "exec-456"
}
```

## Integration

### Event Integration
The service publishes and subscribes to events:
- `pipeline.execution.started/completed/failed`
- `pipeline.step.started/completed`
- `pipeline.alert.created`
- Service-specific step events (extract, transform, load, etc.)

### Service Dependencies
- **Data Platform Service**: For data operations
- **Data Quality Service**: For quality checks
- **Vault**: Dynamic secrets and configuration
- **Consul**: Service discovery and configuration
- **Pulsar**: Event streaming

## Pipeline DSL

Pipelines are defined using a declarative configuration:

```yaml
name: Customer Data Pipeline
type: transformation
config:
  steps:
    - name: extract_customers
      type: extract
      config:
        source: customer_db
        query: "SELECT * FROM customers WHERE updated_at > :last_run"
    
    - name: validate
      type: quality_check
      config:
        checks: [null_check, duplicate_check]
    
    - name: transform
      type: transform
      config:
        operations:
          - clean_data
          - normalize_addresses
          - enrich_demographics
    
    - name: load
      type: load
      config:
        target: analytics_db
        mode: upsert
        key_columns: [customer_id]

schedule:
  type: cron
  cron_expression: "0 */4 * * *"  # Every 4 hours

dependencies:
  - upstream_pipeline_id

tags: [customer, daily, critical]
```

## Support

For issues or questions:
- Check the logs at `/var/log/pipeline-orchestration-service/`
- View metrics in Grafana dashboards
- Contact the Data Intelligence team 