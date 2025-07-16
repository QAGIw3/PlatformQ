# PlatformQ Airflow Helm Chart

This Helm chart deploys Apache Airflow integrated with the PlatformQ ecosystem, providing advanced workflow orchestration capabilities while maintaining the platform's event-driven architecture and multi-tenancy model.

## Prerequisites

- Kubernetes 1.23+
- Helm 3.8+
- PlatformQ core services deployed (auth-service, Pulsar, Consul, Vault)
- PVC support for persistent storage

## Installation

### Add the Airflow repository

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

### Install the chart

```bash
# Create namespace
kubectl create namespace airflow

# Install with default values
helm install airflow ./airflow-platformq -n airflow

# Install with custom values
helm install airflow ./airflow-platformq -n airflow -f custom-values.yaml
```

## Configuration

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `airflow.executor` | Executor type | `CeleryKubernetesExecutor` |
| `airflow.workers.replicas` | Number of Celery workers | `3` |
| `platformq.enabled` | Enable PlatformQ integration | `true` |
| `platformq.multiTenancy.enabled` | Enable multi-tenant support | `true` |
| `platformq.eventBridge.enabled` | Enable event-to-DAG bridge | `true` |
| `platformq.security.oidc.enabled` | Enable OIDC authentication | `false` |

### PlatformQ Integration Features

1. **Custom Operators**: Pre-installed operators for interacting with PlatformQ services
   - `PulsarEventOperator`: Publish events to Pulsar
   - `PulsarSensorOperator`: Wait for Pulsar events
   - `PlatformQServiceOperator`: Call PlatformQ microservices
   - `ProcessorJobOperator`: Launch Kubernetes processor jobs
   - `WASMFunctionOperator`: Trigger WASM function execution

2. **Event Bridge**: Automatically trigger DAGs from platform events
   - Digital asset creation
   - Project creation requests
   - Proposal approvals
   - Document updates

3. **Multi-Tenancy**: DAG isolation and resource quotas per tenant

4. **Pre-configured DAGs**:
   - `digital_asset_processing`: Process uploaded files
   - `create_collaborative_project`: Orchestrate cross-system project creation
   - `verifiable_credential_issuance`: Issue trust credentials

## Usage Examples

### Triggering a DAG via the Workflow Service API

```bash
# List available workflows
curl -X GET http://workflow-service:8000/api/v1/workflows

# Trigger a workflow
curl -X POST http://workflow-service:8000/api/v1/workflows/create_collaborative_project/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "project_name": "New Research Project",
    "creator_id": "user-123",
    "description": "Quantum computing research",
    "team_members": ["user-456", "user-789"]
  }'

# Check workflow status
curl -X GET http://workflow-service:8000/api/v1/workflows/create_collaborative_project/runs/manual_20240115_120000
```

### Creating Custom DAGs

Create a new DAG that integrates with PlatformQ:

```python
from datetime import datetime, timedelta
from airflow import DAG
from platformq_airflow.operators import (
    PulsarSensorOperator,
    PlatformQServiceOperator,
    PulsarEventOperator
)

default_args = {
    'owner': 'platformq',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_custom_workflow',
    default_args=default_args,
    description='Custom workflow for PlatformQ',
    schedule_interval=None,
    catchup=False,
)

# Wait for an event
wait_for_event = PulsarSensorOperator(
    task_id='wait_for_custom_event',
    topic_pattern='persistent://platformq/.*/custom-events',
    schema_class=MyEventSchema,
    subscription_name='airflow-custom',
    dag=dag,
)

# Process the event
process_event = PlatformQServiceOperator(
    task_id='process_custom_event',
    service_name='my-service',
    endpoint='/api/v1/process',
    method='POST',
    payload="{{ ti.xcom_pull(task_ids='wait_for_custom_event', key='event') }}",
    dag=dag,
)

# Publish completion
publish_result = PulsarEventOperator(
    task_id='publish_completion',
    topic_base='custom-processing-completed',
    schema_class=CompletionEvent,
    event_data={
        'status': 'completed',
        'processed_at': '{{ ts }}'
    },
    tenant_id="{{ ti.xcom_pull(task_ids='wait_for_custom_event', key='tenant_id') }}",
    dag=dag,
)

wait_for_event >> process_event >> publish_result
```

## Monitoring

### Prometheus Metrics

The chart exposes Airflow metrics on port 9102:

```yaml
# Example ServiceMonitor for Prometheus Operator
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: airflow-metrics
spec:
  selector:
    matchLabels:
      app: airflow
  endpoints:
  - port: metrics
    interval: 30s
```

### Grafana Dashboards

Import the included dashboards:
- Airflow Overview: `dashboards/airflow-overview.json`
- DAG Performance: `dashboards/dag-performance.json`
- Worker Status: `dashboards/worker-status.json`

## Troubleshooting

### Common Issues

1. **DAGs not appearing**: Check ConfigMap mounting and DAG folder permissions
2. **Event bridge not working**: Verify Pulsar connectivity and topic permissions
3. **Kubernetes executor failing**: Check RBAC and service account configuration

### Debug Commands

```bash
# Check Airflow scheduler logs
kubectl logs -n airflow deployment/airflow-scheduler

# Check worker logs
kubectl logs -n airflow deployment/airflow-worker

# Access Airflow CLI
kubectl exec -n airflow deployment/airflow-scheduler -- airflow dags list

# Test database connection
kubectl exec -n airflow deployment/airflow-scheduler -- airflow db check
```

## Upgrade

```bash
# Update chart dependencies
helm dependency update ./airflow-platformq

# Upgrade release
helm upgrade airflow ./airflow-platformq -n airflow
```

## Uninstall

```bash
helm uninstall airflow -n airflow
kubectl delete namespace airflow
``` 