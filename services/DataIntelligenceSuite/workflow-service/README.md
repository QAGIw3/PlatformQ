
# Workflow Service

## Purpose

The **Workflow Service** acts as a central orchestrator for complex, cross-application business processes. It has been enhanced with Apache Airflow integration to provide advanced workflow capabilities including visual DAG design, complex scheduling, retry mechanisms, and comprehensive monitoring. The service maintains backward compatibility while offering powerful new orchestration features.

### Key Features

- **Apache Airflow Integration**: Optional integration with Airflow for advanced workflow orchestration
- **Event-Driven Architecture**: Consumes and produces events via Pulsar
- **Webhook Gateway**: Translates webhooks from external systems into platform events
- **Multi-Tenant Support**: Workflow isolation and resource management per tenant
- **Backward Compatibility**: Can run in legacy mode without Airflow

## Event-Driven Architecture

This service is a central hub in the platform's event-driven architecture.

### Events Consumed

- **Topic**: `persistent://public/default/project-events`
- **Topic Pattern**: `persistent://platformq/.*/proposal-approved-events`
- **Description**: The service listens to a variety of events to trigger workflows. For example, it listens to proposal approval events to kick off the Verifiable Credential issuance process.

### Events Produced

- **Topic**: `persistent://platformq/<tenant_id>/verifiable-credential-issuance-requests`
- **Schema**: `IssueVerifiableCredential`
- **Description**: Published when a proposal is approved, this event requests that the `verifiable-credential-service` issue a new credential for the approval.

- **Topic**: `persistent://platformq/<tenant_id>/project-events`
- **Topic**: `persistent://platformq/<tenant_id>/document-events`
- **Description**: Publishes various events based on incoming webhooks.

## API Endpoints

The service exposes webhook endpoints to integrate with external systems and workflow management APIs when Airflow is enabled.

### Webhooks

- `POST /webhooks/openproject`: Receives webhook notifications from OpenProject. It transforms these into a standard `ProjectCreated` platform event and publishes it to Pulsar.
- `POST /webhooks/onlyoffice`: Receives callbacks from OnlyOffice/Nextcloud when a document is saved. It transforms these into a standard `DocumentUpdated` platform event and publishes it to Pulsar.

### Workflow Management (Airflow Integration)

When `AIRFLOW_ENABLED=true`, the following endpoints are available:

- `GET /api/v1/workflows`: List all available workflows (DAGs)
  - Query param: `only_active=true` to show only active workflows
- `POST /api/v1/workflows/{workflow_id}/trigger`: Manually trigger a workflow
  - Body: JSON configuration to pass to the DAG
- `GET /api/v1/workflows/{workflow_id}/runs/{run_id}`: Get workflow run status
- `PATCH /api/v1/workflows/{workflow_id}`: Pause or unpause a workflow
  - Query param: `is_paused=true/false`
- `GET /api/v1/event-mappings`: Get all event to workflow mappings
- `POST /api/v1/event-mappings`: Register new event to workflow mapping
  - Query params: `event_type`, `workflow_id`

## Configuration

### Environment Variables

- `PULSAR_URL`: The URL for the Pulsar broker (default: `pulsar://localhost:6650`)
- `AIRFLOW_ENABLED`: Enable Airflow integration (default: `false`)
- `AIRFLOW_API_URL`: Airflow API URL (default: `http://airflow_webserver:8080`)
- `AIRFLOW_USERNAME`: Airflow username for API access (default: `airflow`)
- `AIRFLOW_PASSWORD`: Airflow password for API access (default: `airflow`)

### Airflow Mode vs Legacy Mode

The service can run in two modes:

1. **Legacy Mode** (`AIRFLOW_ENABLED=false`): Traditional event processing with hardcoded workflows
2. **Airflow Mode** (`AIRFLOW_ENABLED=true`): Advanced orchestration with Airflow DAGs

## How to Run Locally

### Option 1: With Docker Compose (Recommended)

1. **Start all services including Airflow**:
   ```bash
   cd infra/docker-compose
   docker-compose up -d
   ./setup_airflow.sh
   ```

2. **Enable Airflow in workflow service**:
   ```bash
   docker-compose exec workflow-service sh -c "export AIRFLOW_ENABLED=true"
   docker-compose restart workflow-service
   ```

### Option 2: Standalone

1. **Ensure Dependencies are Running**: 
   - Apache Pulsar
   - Apache Airflow (if using Airflow mode)

2. **Install Requirements**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Environment Variables**:
   ```bash
   export PULSAR_URL=pulsar://localhost:6650
   export AIRFLOW_ENABLED=true
   export AIRFLOW_API_URL=http://localhost:8080
   ```

4. **Run the Service**:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## Examples

### Triggering a Workflow via API

```bash
# Create a collaborative project
curl -X POST http://localhost:8000/api/v1/workflows/create_collaborative_project/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "project_name": "Q1 Research Initiative",
    "creator_id": "user-123",
    "description": "Quantum computing research project",
    "team_members": ["user-456", "user-789"]
  }'

# Response:
{
  "workflow_id": "create_collaborative_project",
  "run_id": "create_collaborative_project_20240115_143022",
  "state": "running",
  "execution_date": "2024-01-15T14:30:22Z"
}
```

### Registering Event to DAG Mapping

```bash
# Map custom events to workflows
curl -X POST "http://localhost:8000/api/v1/event-mappings?event_type=CustomApprovalEvent&workflow_id=approval_workflow"

# Response:
{
  "event_type": "CustomApprovalEvent",
  "workflow_id": "approval_workflow",
  "message": "Mapping registered successfully"
}
```

### Event-Driven Workflow Triggering

When an event is published to Pulsar that matches a registered mapping, the corresponding DAG is automatically triggered:

```python
# Publishing an event that triggers a workflow
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.events import DigitalAssetCreated

publisher = EventPublisher(pulsar_url="pulsar://localhost:6650")
publisher.connect()

# This event will trigger the 'digital_asset_processing' DAG
event = DigitalAssetCreated(
    tenant_id="12345",
    asset_id="asset-001",
    asset_type="blender",
    raw_data_uri="s3://bucket/models/scene.blend",
    created_by="user-123",
    created_at=int(time.time() * 1000)
)

publisher.publish(
    topic_base="digital-asset-created-events",
    tenant_id="12345",
    schema_class=DigitalAssetCreated,
    data=event
)
``` 