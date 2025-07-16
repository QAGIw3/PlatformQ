# Notification Service

## Purpose

The **Notification Service** is a FastAPI application responsible for broadcasting system events and insights to relevant communication channels, primarily Zulip. It operates as an event consumer, processing various event types from Pulsar, and also periodically fetches data from other services via gRPC to generate actionable notifications.

Its primary responsibilities are:
-   Consuming `UserCreatedEvent` and `DocumentUpdatedEvent` from Pulsar to send notifications about user activity and document changes.
-   Periodically querying the `Graph Intelligence Service` via gRPC for new community insights and sending these as notifications to Zulip.
-   Providing a webhook endpoint for external systems (e.g., Nextcloud) to trigger event processing.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### Webhooks

-   `POST /webhooks/nextcloud`: A placeholder webhook endpoint for receiving notifications from Nextcloud. In a full implementation, this would parse the webhook payload, transform it into a structured event, and publish it to a Pulsar topic (e.g., `content-events`).

## Event-Driven Architecture

### Events Consumed

-   **Topic Pattern**: `persistent://platformq/.*/user-events`
-   **Schema**: `UserCreatedEvent`
-   **Description**: Triggers a notification to Zulip when a new user signs up.

-   **Topic Pattern**: `persistent://platformq/.*/document-events`
-   **Schema**: `DocumentUpdatedEvent`
-   **Description**: Triggers a notification to Zulip when a document is updated by a user.

### External Integrations

-   **Zulip**: The primary communication platform for sending notifications. The service interacts with the Zulip API to post messages to specific streams and topics.
-   **Pulsar**: Used for consuming various system events (e.g., user creations, document updates) from different topics.
-   **Graph Intelligence Service (gRPC)**: Periodically queried to retrieve community insights, which are then formatted and sent as notifications to Zulip.

## Configuration

The service relies on the following environment variables (or configurations loaded via Consul/Vault in a production environment):

-   `PULSAR_URL`: The URL for the Pulsar broker (e.g., `pulsar://localhost:6650`).
-   `ZULIP_API_KEY`: API key for authenticating with the Zulip API (retrieved from Vault).
-   `ZULIP_EMAIL`: Email associated with the Zulip bot/user (retrieved from Vault).
-   `ZULIP_SITE`: The base URL of the Zulip instance (e.g., `https://your-zulip-instance.com`).
-   `GRAPH_INTELLIGENCE_SERVICE_GRPC_TARGET`: The gRPC target for the Graph Intelligence Service (e.g., `graph-intelligence-service:50052`).

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/notification-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    Before running the service, ensure the necessary environment variables for Pulsar, Zulip, and the Graph Intelligence Service are configured. For Zulip API Key and Email, these are expected to be retrieved from a secrets management system like Vault, so for local testing you might need to mock them or provide dummy values.

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/notification-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```
    The Pulsar consumers and the Graph Intelligence scheduler will start in background threads upon application startup.

## Dependencies

-   `FastAPI`: Web framework for building the API and handling webhooks.
-   `python-zulip-api`: Client library for interacting with the Zulip API.
-   `apache-pulsar`: Client library for interacting with Pulsar.
-   `schedule`: Library for in-process scheduling of periodic tasks.
-   `grpcio`, `grpcio-tools`: For gRPC communication with the Graph Intelligence Service.
-   `platformq_shared.base_service`: Shared library for common service functionalities.
-   `platformq_shared.config`: Shared library for loading configuration from Consul and Vault.
-   `platformq_shared.events`: Defines the event schemas (e.g., `UserCreatedEvent`, `DocumentUpdatedEvent`, `GraphIntelligenceEvent`) consumed and processed by the service. 