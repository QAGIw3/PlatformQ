
# Workflow Service

## Purpose

The **Workflow Service** acts as a central orchestrator for complex, cross-application business processes. It consumes events from various services and produces new events to trigger actions in other parts of the platform. It also serves as a webhook gateway, translating incoming webhooks from third-party systems (like OpenProject and OnlyOffice) into standardized platform events.

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

The service exposes webhook endpoints to integrate with external systems.

### Webhooks

- `POST /webhooks/openproject`: Receives webhook notifications from OpenProject. It transforms these into a standard `ProjectCreated` platform event and publishes it to Pulsar.
- `POST /webhooks/onlyoffice`: Receives callbacks from OnlyOffice/Nextcloud when a document is saved. It transforms these into a standard `DocumentUpdated` platform event and publishes it to Pulsar.

## How to Run Locally

1.  **Ensure Dependencies are Running**: This service requires a running instance of Apache Pulsar.
2.  **Install Requirements**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Set Environment Variables**:
    - `PULSAR_URL`: The URL for the Pulsar broker (e.g., `pulsar://localhost:6650`).
4.  **Run the Service**:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ``` 