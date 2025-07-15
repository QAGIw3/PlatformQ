
# Provisioning Service

## Purpose

The **Provisioning Service** is responsible for creating user accounts in various third-party applications integrated into the platform. It listens for `UserCreatedEvent` messages and triggers the appropriate provisioning logic for each downstream application.

This service is designed to be extensible, allowing new applications to be integrated by adding new provisioning modules.

## Event-Driven Architecture

This service is a key component in the user-onboarding workflow and operates entirely in the background.

### Events Consumed

- **Topic**: `persistent://platformq/<tenant_id>/user-events`
- **Schema**: `UserCreatedEvent`
- **Description**: When a new user is created in the `auth-service`, a `UserCreatedEvent` is published. The Provisioning Service consumes this event to kick off the provisioning process.

### Events Produced

This service does not produce any events. It is a terminal consumer in its workflows.

## How to Run Locally

1.  **Ensure Dependencies are Running**: This service requires a running instance of Apache Pulsar.
2.  **Install Requirements**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Set Environment Variables**: The service relies on environment variables for configuration. Ensure the following are set:
    - `PULSAR_URL`: The URL for the Pulsar broker (e.g., `pulsar://localhost:6650`).
    - `NEXTCLOUD_URL`: The base URL for the Nextcloud instance.
    - `NEXTCLOUD_ADMIN_USER`: The username for the Nextcloud admin account.
    - `NEXTCLOUD_ADMIN_PASS`: The password for the Nextcloud admin account.

4.  **Run the Service**:
    ```bash
    python -m app.main
    ``` 