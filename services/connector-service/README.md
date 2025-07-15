# Connector Service

The `connector-service` is the primary entry point for all external data into the platformQ ecosystem. It is a highly extensible service responsible for fetching, transforming, and orchestrating the processing of data from a wide variety of sources.

## Core Architecture

The service is built on a **plugin-based architecture**. All specific connection and processing logic is encapsulated within individual "connector" modules located in the `app/plugins/` directory. The main service acts as a lightweight host that discovers, loads, and runs these plugins.

This architecture is designed for maintainability and extensibility. To add support for a new data source, a developer simply needs to create a new plugin file; no changes are needed to the core service.

### Connector Patterns

The service supports multiple patterns for data ingestion:

1.  **Scheduled Connectors**: For API-based services (like CRMs or ERPs), connectors can define a `schedule` property (a cron string), and the service's internal scheduler will run them periodically.
2.  **Event-Driven Connectors**: For real-time data, the service exposes a generic `/webhooks/{connector_name}` endpoint that can trigger a specific connector to run.
3.  **Processor Job Orchestration**: For complex file formats that require dedicated software (e.g., Blender, FreeCAD), the connectors do not process the files themselves. Instead, they orchestrate the processing by creating a short-lived **Kubernetes Job** that runs a specialized "processor" container. This keeps the main `connector-service` lightweight and scalable.

## Adding a New Connector

To add a new connector:
1.  Create a new Python file in `app/plugins/`.
2.  Define a class that inherits from `BaseConnector`.
3.  Implement the required `connector_type` property and `run` method.
4.  If the connector requires a dedicated processor, create a new directory under `processing/` for its `Dockerfile` and scripts. 