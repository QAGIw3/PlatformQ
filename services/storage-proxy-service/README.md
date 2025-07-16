# Storage Proxy Service

## Purpose

The **Storage Proxy Service** is a FastAPI application that acts as an intermediary for interacting with an IPFS (InterPlanetary File System) node. Its primary responsibilities include:

-   Providing API endpoints for uploading files to IPFS.
-   Providing API endpoints for downloading files from IPFS.

This service simplifies file storage and retrieval by abstracting the direct interaction with IPFS, allowing other services to easily store and access decentralized content.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### IPFS Operations

-   `POST /upload`: Uploads a file to the configured IPFS node. Requires a `multipart/form-data` request with a `file` field. It returns the Content Identifier (CID) and filename of the uploaded file.
-   `GET /download/{cid}`: Downloads a file from IPFS using its Content Identifier (CID). It returns the raw file content as `application/octet-stream`.

## Configuration

The service relies on the following environment variable (or a configuration loaded via Consul/Vault in a production environment):

-   `IPFS_API_URL`: The URL for connecting to the IPFS API. (e.g., `/dns/platformq-ipfs-kubo/tcp/5001/http`).

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/storage-proxy-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    Before running the service, set the `IPFS_API_URL` environment variable. For example:
    ```bash
    export IPFS_API_URL="/ip4/127.0.0.1/tcp/5001"
    ```
    (Note: This assumes a local IPFS node is running and accessible. In a deployed environment, this would point to the cluster's IPFS gateway.)

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/storage-proxy-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```

## Dependencies

-   `FastAPI`: Web framework for building the API.
-   `ipfshttpclient`: Python client library for the IPFS HTTP API.
-   `platformq_shared.base_service`: Shared library for common service functionalities. 