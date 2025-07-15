# Verifiable Credential Service

## Purpose

The **Verifiable Credential Service** provides a "Trust Engine" for the platform. It issues W3C Verifiable Credentials to create immutable, cryptographically-provable records of key business events. It can be triggered via a REST API or by platform events.

Conceptually, it records the hash of each issued credential to a private blockchain (e.g., Hyperledger Fabric) to create a tamper-proof audit trail.

## Event-Driven Architecture

The service can be driven asynchronously via platform events for background workflows.

### Events Consumed

- **Topic Pattern**: `persistent://platformq/.*/verifiable-credential-issuance-requests`
- **Schema**: `IssueVerifiableCredential`
- **Description**: Consumes requests to issue new credentials, typically published by the `workflow-service` when a business process requires a formal, auditable record (e.g., a proposal approval).

### Events Produced

- **Topic**: `persistent://platformq/<tenant_id>/verifiable-credential-issued-events`
- **Schema**: `VerifiableCredentialIssued`
- **Description**: Published after a credential has been successfully created and recorded. This allows other services to be notified of the completion of the issuance workflow.

## API Endpoints

The service also provides a synchronous REST API for issuance.

- `POST /api/v1/issue`: Issues a new Verifiable Credential based on the provided subject and credential type.

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