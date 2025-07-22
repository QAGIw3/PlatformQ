# Data Quality Service

A comprehensive data quality management service for the DataIntelligenceSuite, providing autonomous data quality monitoring, validation, profiling, and remediation capabilities.

## Features

### Core Capabilities
- **Autonomous Quality Management**: Self-healing data quality with automated issue detection and remediation
- **Real-time Monitoring**: Continuous quality monitoring with alerting and trend analysis
- **Data Profiling**: Comprehensive profiling with statistics, patterns, and anomaly detection
- **Rule Engine**: Flexible rule-based validation with multiple condition operators and actions
- **Quality Remediation**: Automated and manual remediation strategies for common issues

### Advanced Features
- **Drift Detection**: Identify changes in data distribution over time
- **Anomaly Detection**: Multiple methods for outlier and anomaly identification
- **Pattern Recognition**: Detect and validate data patterns and formats
- **Cross-dataset Validation**: Ensure consistency across related datasets
- **Pipeline Integration**: Quality gates for data pipelines

## Architecture

The service follows a microservice architecture pattern:

```
data-quality-service/
├── app/
│   ├── main.py              # FastAPI application and service initialization
│   ├── core/
│   │   ├── quality_engine.py # Core quality management engine
│   │   └── profiler.py      # Data profiling implementation
│   ├── remediation/
│   │   └── orchestrator.py  # Remediation orchestration
│   ├── monitoring/
│   │   └── quality_monitor.py # Real-time monitoring
│   ├── rules/
│   │   ├── rule_engine.py   # Rule execution engine
│   │   └── rule_repository.py # Rule persistence
│   ├── api/                 # REST API endpoints
│   └── events/              # Event processing
```

## API Endpoints

### Quality Operations
- `POST /api/v1/quality/check` - Run quality checks on a dataset
- `POST /api/v1/quality/validate` - Validate data against schema and rules
- `POST /api/v1/quality/remediate` - Remediate quality issues
- `GET /api/v1/quality/issues/{dataset}` - Get quality issues for a dataset
- `GET /api/v1/quality/history/{dataset}` - Get quality history

### Rule Management
- `POST /api/v1/rules` - Create a new rule
- `GET /api/v1/rules` - List rules with filtering
- `GET /api/v1/rules/{rule_id}` - Get specific rule
- `PUT /api/v1/rules/{rule_id}` - Update rule
- `DELETE /api/v1/rules/{rule_id}` - Delete rule
- `POST /api/v1/rules/execute` - Execute rules against data

### Monitoring
- `GET /api/v1/monitoring/metrics/{dataset}` - Get quality metrics
- `GET /api/v1/monitoring/trends` - Get quality trends
- `GET /api/v1/monitoring/alerts` - Get active alerts
- `POST /api/v1/monitoring/config/alerts` - Configure alerts

### Profiling
- `POST /api/v1/profiling/profile` - Profile a dataset
- `GET /api/v1/profiling/profile/{dataset}` - Get latest profile
- `POST /api/v1/profiling/anomalies/detect` - Detect anomalies
- `GET /api/v1/profiling/drift/{dataset}` - Detect data drift

## Configuration

### Environment Variables
```bash
# Service Configuration
SERVICE_NAME=data-quality-service
SERVICE_PORT=8003
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
data-quality/
├── monitored-datasets        # List of datasets to monitor
├── rules/                    # Quality rules
├── monitoring-config         # Monitoring settings
└── alerts/                   # Alert configurations
```

## Usage Examples

### Running Quality Checks
```python
import httpx

# Check data quality
response = await client.post(
    "http://localhost:8003/api/v1/quality/check",
    json={
        "dataset": "customer_data",
        "check_type": "full",
        "auto_remediate": True
    }
)
```

### Creating Quality Rules
```python
# Create a validation rule
response = await client.post(
    "http://localhost:8003/api/v1/rules",
    json={
        "name": "Email Format Validation",
        "type": "validation",
        "conditions": [{
            "field": "email",
            "operator": "matches",
            "value": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        }],
        "actions": [{
            "type": "flag",
            "params": {"flag_name": "invalid_email"}
        }]
    }
)
```

### Monitoring Quality Metrics
```python
# Get quality trends
response = await client.get(
    "http://localhost:8003/api/v1/monitoring/trends",
    params={"dataset": "customer_data", "hours": 24}
)
```

## Development

### Setup
```bash
# Install dependencies
pip install -r requirements.in

# Run locally
cd services/DataIntelligenceSuite/data-quality-service
uvicorn app.main:app --reload --port 8003
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
docker build -t data-quality-service:latest .

# Run container
docker run -p 8003:8003 \
  -e VAULT_ADDR=$VAULT_ADDR \
  -e CONSUL_HOST=$CONSUL_HOST \
  data-quality-service:latest
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-quality-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: data-quality-service
  template:
    metadata:
      labels:
        app: data-quality-service
    spec:
      containers:
      - name: data-quality-service
        image: data-quality-service:latest
        ports:
        - containerPort: 8003
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
- `data_quality_score` - Quality scores by dataset and metric
- `data_quality_alerts_active` - Number of active alerts
- `dq_rule_executions_total` - Total rule executions
- `dq_rule_violations_total` - Rule violations

### Health Checks
- `/health` - Basic health check
- `/ready` - Readiness check
- `/api/v1/monitoring/health/components` - Component health status

### Logging
Structured JSON logging with trace correlation:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "service": "data-quality-service",
  "trace_id": "abc123",
  "message": "quality_check_completed",
  "dataset": "customer_data",
  "issues_found": 5
}
```

## Quality Metrics

The service tracks these quality dimensions:
- **Completeness**: Percentage of non-null values
- **Accuracy**: Data correctness based on rules
- **Consistency**: Cross-dataset consistency
- **Timeliness**: Data freshness
- **Validity**: Format and constraint compliance
- **Uniqueness**: Duplicate detection
- **Integrity**: Referential integrity
- **Conformity**: Schema compliance

## Integration

### Event Integration
The service publishes and subscribes to events:
- `data.quality.check.requested`
- `data.quality.issue.detected`
- `data.quality.alert`
- `dataset.created/updated`
- `pipeline.stage.completed`

### Service Dependencies
- **Vault**: Dynamic secrets and encryption
- **Consul**: Service discovery and configuration
- **Pulsar**: Event streaming
- **Ignite**: Caching quality metrics
- **Various databases**: For data access

## Support

For issues or questions:
- Check the logs at `/var/log/data-quality-service/`
- View metrics in Grafana dashboards
- Contact the Data Intelligence team 