# Template Service

## Purpose

The **Template Service** is a FastAPI application designed to manage and provide access to a collection of service templates. Its primary function is to clone a Git repository containing these templates and expose them via a RESTful API.

This service acts as a central repository for various service templates, enabling other parts of the platform to discover and utilize standardized project structures for new services.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### Template Management

-   `GET /api/v1/templates`: Clones the configured template repository and returns a list of available service templates (directories in the repository root).
-   `GET /api/v1/templates/{template_name}`: (Conceptual) Fetches and returns details about a specific template. The current implementation is a placeholder and would require further development to list files or provide more comprehensive metadata.

## Configuration

The service relies on the following environment variable (or a configuration loaded via Consul/Vault in a production environment):

-   `TEMPLATE_REPO_URL`: The URL of the Git repository containing the service templates (e.g., `https://github.com/your-company/service-templates.git`).

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/template-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    Before running the service, set the `TEMPLATE_REPO_URL` environment variable. For example:
    ```bash
    export TEMPLATE_REPO_URL="https://github.com/your-company/service-templates.git" # Replace with your actual template repo
    ```

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/template-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

## Dependencies

-   `FastAPI`: Web framework for building the API.
-   `python-git`: Library for interacting with Git repositories.
-   `platformq_shared.base_service`: Shared library for common service functionalities. 