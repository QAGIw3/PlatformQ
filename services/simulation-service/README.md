# Simulation Service

## Purpose

The **Simulation Service** provides the API for defining, managing, and launching large-scale agent-based simulations. It serves as the user-facing entry point to the platform's simulation engine, allowing users to define simulation parameters, initiate runs, and monitor their progress.

Its primary responsibilities are:
-   Providing an API to create and manage simulation definitions, including agent types and their initial states.
-   Triggering the start of simulations and publishing corresponding events.
-   Receiving completion reports from simulation workers and updating simulation status.
-   Providing real-time access to the state of agents within a running simulation.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### Simulation Management

-   `POST /api/v1/simulations`: Creates a new simulation definition. Requires a JSON payload with `simulation_name` and a list of `agent_definitions`.
-   `POST /api/v1/simulations/{simulation_id}/start`: Initiates a simulation run for a given `simulation_id`. Publishes a `SimulationStartedEvent`.
-   `POST /api/v1/simulations/{simulation_id}/complete`: (Internal) An endpoint for simulation workers to report the completion of a simulation run. Publishes a `SimulationRunCompleted` event.
-   `GET /api/v1/simulations/{simulation_id}/state`: Returns the real-time state of all agents for a running simulation.

## Event-Driven Architecture

### Events Consumed

(No explicit events consumed by the service directly via Pulsar, rather it acts as a publisher and API endpoint for external triggers.)

### Events Produced

-   **Topic Base**: `simulation-control-events`
-   **Schema**: `SimulationStartedEvent`
-   **Description**: Published when a user initiates a simulation run, signaling the simulation engine to begin processing.

-   **Topic Base**: `simulation-lifecycle-events`
-   **Schema**: `SimulationRunCompleted`
-   **Description**: Published by simulation workers upon completion of a simulation run, indicating its status (success/failure) and providing a log URI.

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/simulation-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    Ensure your environment is configured for `platformq_shared.base_service` dependencies, including database (Cassandra) and Pulsar connection settings. This service also relies on a running IPFS node for `log_uri` in `SimulationRunCompleted` events.

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/simulation-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

## Dependencies

-   `FastAPI`: Web framework for building the API.
-   `SQLAlchemy`: ORM for database interactions.
-   `platformq_shared.base_service`: Shared library for common service functionalities, including security and event publishing.
-   `platformq_shared.events`: Defines the event schemas used by the service. 