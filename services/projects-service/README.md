# Projects Service

## Purpose

The **Projects Service** acts as a central orchestrator, creating a unified "Project" that links resources across multiple applications like OpenProject, Nextcloud, and Zulip. Its primary goal is to simplify the creation and management of collaborative projects by providing a single API endpoint to provision resources across these integrated platforms.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### Project Management

-   `POST /api/v1/projects`: Orchestrates the creation of a new collaborative project. It takes a `name` for the project and conceptually creates corresponding entities in OpenProject, Nextcloud, and Zulip, then stores the links in its own database. (Note: The integration logic with OpenProject, Nextcloud, and Zulip is currently conceptual/commented out and would require actual client implementations.)

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/projects-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    This service conceptually relies on configuration for external services (OpenProject, Nextcloud, Zulip) which would typically be loaded via Consul/Vault in a production environment. For local testing, mock clients or a local setup of these external services would be required.

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/projects-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

## Dependencies

-   `FastAPI`: Web framework for building the API.
-   `SQLAlchemy`: ORM for database interactions.
-   `platformq_shared.base_service`: Shared library for common service functionalities, including security and event publishing. 