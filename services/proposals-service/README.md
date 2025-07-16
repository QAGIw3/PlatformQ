# Proposals Service

## Purpose

The **Proposals Service** is a FastAPI application responsible for managing customer proposals within the platform. Its core functionality involves:

-   Creating new proposal records in a database.
-   Integrating with Nextcloud to generate and store associated proposal documents.

This service streamlines the proposal generation process by connecting the platform's data management with document storage, ensuring that each proposal has a corresponding document in Nextcloud.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### Proposal Management

-   `POST /api/v1/proposals`: Creates a new proposal. This endpoint requires `customer_name` in the request body. It creates a record in the database and an empty document in Nextcloud, returning the new proposal's details.

## Configuration

The service relies on the following configurations for Nextcloud integration (in a production environment, these would be loaded via Consul/Vault):

-   `NEXTCLOUD_URL`: The base URL for the Nextcloud instance (e.g., `http://platformq-nextcloud`).
-   `NEXTCLOUD_ADMIN_USER`: The username for a Nextcloud administrator account used for API interactions.
-   `NEXTCLOUD_ADMIN_PASS`: The password for the Nextcloud administrator account.

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/proposals-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    Before running the service, ensure that the Nextcloud configuration environment variables are set:
    ```bash
    export NEXTCLOUD_URL="http://your-nextcloud-instance"
    export NEXTCLOUD_ADMIN_USER="your-admin-user"
    export NEXTCLOUD_ADMIN_PASS="your-admin-password"
    ```

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/proposals-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

## Dependencies

-   `FastAPI`: Web framework for building the API.
-   `SQLAlchemy`: ORM for database interactions.
-   `shared_lib.base_service`: Shared library for common service functionalities.
-   `shared_lib.nextcloud_client`: Client for interacting with Nextcloud. 