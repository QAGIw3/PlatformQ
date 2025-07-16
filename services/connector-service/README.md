# Connector Service

## Purpose

The **Connector Service** is the primary entry point for all external data into the platformQ ecosystem. It is a highly extensible service responsible for fetching, transforming, and orchestrating the processing of data from a wide variety of sources.

## Core Architecture

The service is built on a **plugin-based architecture**. All specific connection and processing logic is encapsulated within individual "connector" modules located in the `app/plugins/` directory. The main service acts as a lightweight host that discovers, loads, and runs these plugins.

This architecture is designed for maintainability and extensibility. To add support for a new data source, a developer simply needs to create a new plugin file; no changes are needed to the core service.

### Connector Patterns

The service supports multiple patterns for data ingestion:

1.  **Scheduled Connectors**: For API-based services (like CRMs or ERPs), connectors can define a `schedule` property (a cron string), and the service's internal scheduler will run them periodically.
2.  **Event-Driven Connectors**: For real-time data, the service exposes a generic `/webhooks/{connector_name}` endpoint that can trigger a specific connector to run.
3.  **Processor Job Orchestration**: For complex file formats that require dedicated software (e.g., Blender, FreeCAD), the connectors do not process the files themselves. Instead, they orchestrate the processing by creating a short-lived **Kubernetes Job** that runs a specialized "processor" container. This keeps the main `connector-service` lightweight and scalable.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### REST API Endpoints

-   `GET /connectors`: An administrative endpoint that returns a list of all discovered and loaded connectors by their `connector_type`.
-   `POST /connectors/{connector_name}/run`: An administrative endpoint to manually trigger a specific connector's `run` method. The request body is passed as the `context` to the connector's `run` method.
-   `POST /webhooks/{connector_name}`: A generic webhook endpoint that receives real-time events. It triggers the corresponding connector's `run` method in the background, passing the entire webhook payload as `context`.

### gRPC API Endpoints

-   **Service**: `ConnectorService`
-   **Method**: `CreateAssetFromURI(CreateAssetFromURIRequest) returns (CreateAssetFromURIResponse)`
-   **Description**: (Conceptual) Initiates the creation of a digital asset from a given URI and tenant ID. The current implementation is a placeholder and would require full logic to find the appropriate connector and trigger asset creation.

## Configuration

The service relies on shared configuration settings, which are typically loaded via Consul/Vault in a production environment. Key configurations include:

-   `GRPC_PORT`: The port on which the gRPC server listens (default: `50051`).

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/connector-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Generate gRPC Code**:
    If you've made changes to the `.proto` files or are setting up for the first time, you'll need to generate the gRPC Python code. This is typically done via a script like `scripts/generate_grpc.sh`:
    ```bash
    ./scripts/generate_grpc.sh
    ```

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/connector-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```
    Upon startup, the service will discover and schedule plugins, and the gRPC server will begin listening.

## Adding a New Connector Plugin

To extend the Connector Service with a new data source or integration:
1.  Create a new Python file (e.g., `my_new_connector.py`) in the `app/plugins/` directory.
2.  Define a class within this file that inherits from `app.plugins.base.BaseConnector`.
3.  Implement the required `connector_type` property and the `run` method, which contains your connector's specific logic for data ingestion, transformation, and orchestration.
4.  If the connector requires periodic execution, define a `schedule` property (a cron string) within your connector class.
5.  If the connector orchestrates external processing jobs (e.g., for complex file formats), ensure the corresponding processor container and Kubernetes Job definitions are in place.

## Dependencies

-   `FastAPI`: Web framework for building REST APIs and handling webhooks.
-   `APScheduler`: For scheduling periodic connector runs.
-   `grpcio`, `grpcio-tools`: For gRPC communication.
-   `platformq_shared.base_service`: Shared library for common service functionalities.
-   `platformq_shared.config`: Shared library for loading configuration.
-   `app.plugins.base`: The base class for all connector plugins. 