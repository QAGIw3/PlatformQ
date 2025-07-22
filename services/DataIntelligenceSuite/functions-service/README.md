
# Functions Service

## Purpose

The **Functions Service** provides a platform for executing custom, sandboxed code in response to platform events. It has evolved into a generic, event-driven WebAssembly (WASM) execution engine.

Its primary responsibilities are:
-   Providing an API to manage a library of WASM modules.
-   Listening for `ExecuteWasmFunction` events.
-   Executing the specified WASM module against data fetched from a URI.
-   Publishing the results of the execution as a `FunctionExecutionCompleted` event.

This enables a powerful and flexible dynamic data processing pipeline, allowing developers to add custom validation, analysis, or transformation logic without modifying the core services.

## Event-Driven Architecture

### Events Consumed

-   **Topic Pattern**: `persistent://platformq/.*/wasm-function-execution-requests`
-   **Schema**: `ExecuteWasmFunction`
-   **Description**: This is the primary trigger for the service. It contains the ID of the asset to process, a URI to the asset's raw data, and the ID of the WASM module to execute.

### Events Produced

-   **Topic**: `persistent://platformq/<tenant_id>/function-execution-completed-events`
-   **Schema**: `FunctionExecutionCompleted`
-   **Description**: Published after a WASM function has been executed. It includes a `status` ("SUCCESS" or "FAILURE") and a `results` map containing new metadata to be applied to the asset, or an `error_message` if the execution failed.

## API Endpoints

### WASM Module Management

-   `POST /api/v1/wasm-modules`: Uploads a new `.wasm` module. Requires a multipart form with `module_id` (string), `description` (optional string), and `file` (the binary file).
-   `GET /api/v1/wasm-modules`: Lists all registered WASM modules.
-   `GET /api/v1/wasm-modules/{module_id}`: Retrieves the metadata for a single registered module.
-   `DELETE /api/v1/wasm-modules/{module_id}`: Deletes a module from the registry and the filesystem.

### Knative Deployment

The service also retains its original capability to deploy functions to Knative, though this is separate from the event-driven WASM workflow.

-   `POST /api/v1/functions`: Deploys user code (as a string) as a Knative Service using a pre-built Python runtime.
-   `POST /api/v1/wasm-functions`: Deploys a pre-compiled WASM module from a container image URL as a Knative Service.

### Embedded WASM Execution

-   `POST /api/v1/functions/wasm/run-embedded`: Executes a WASM module directly within the service instance. The module is passed as a Base64-encoded string in the request body. This is intended for quick, trusted, short-lived computations.

## Code Structure

The service's `main.py` has been refactored to delegate responsibilities to smaller, more focused modules:
-   `app/crud/wasm_module_crud.py`: Handles all database interactions for the WASM module registry.
-   `app/api/endpoints/wasm_modules.py`: Defines the CRUD API for managing WASM modules.
-   `app/pulsar_consumer.py`: Contains the logic for the background consumer that listens for execution events.
-   `app/kubernetes_deployment.py`: Contains the endpoints for deploying functions to Knative.
-   `app/wasm_execution.py`: Contains the endpoint for running embedded WASM functions.
-   `app/wasm_runtime.py`: Initializes the shared Wasmtime engine.

## How to Run Locally

1.  **Install Requirements**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Set Environment Variables**:
    -   `PULSAR_URL`: The URL for the Pulsar broker (e.g., `pulsar://localhost:6650`).
3.  **Run the Service**:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```
    On the first run, this will create a `functions.db` SQLite file in the service's root directory to store the WASM module registry.
4.  **Populate with Modules**: Use the API (e.g., via `curl` or Postman) to upload `.wasm` files and register them with a `module_id`.
    ```bash
    curl -X POST -F "module_id=my-test-module" \
         -F "file=@/path/to/your/module.wasm" \
         http://localhost:8000/api/v1/wasm-modules
    ``` 