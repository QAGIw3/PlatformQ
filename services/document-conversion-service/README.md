# Document Conversion Service

## Purpose

The **Document Conversion Service** is a Python application designed to convert documents from one format to another using LibreOffice. It operates as a Pulsar consumer, listening for conversion requests, performing the conversion, and conceptually, would then publish a completion event or upload the converted file.

Its primary responsibilities are:
-   Consuming `document-conversion-requests` events from Pulsar.
-   Downloading source documents from a provided URL.
-   Converting documents to specified output formats using LibreOffice (soffice).
-   Handling temporary file storage during the conversion process.

## Event-Driven Architecture

### Events Consumed

-   **Topic**: `persistent://public/default/document-conversion-requests`
-   **Schema Path**: `schemas/conversion_requested.avsc`
-   **Description**: This event triggers the document conversion process. It is expected to contain `source_document_url` (the URL of the document to convert) and `output_format` (the desired output format, e.g., "pdf", "docx").

### Events Produced

(The current implementation does not explicitly publish a "conversion_completed" event or upload the converted file. This would be a future enhancement.)

## Configuration

The service relies on the following environment variable (or a configuration loaded via Consul/Vault in a production environment):

-   `PULSAR_URL`: The URL for the Pulsar broker (e.g., `pulsar://localhost:6650`).

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/document-conversion-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Ensure LibreOffice is Available**:
    This service relies on `soffice` (LibreOffice) being installed and available in the system's PATH for document conversion. If running in a Docker container, ensure your Dockerfile includes LibreOffice installation.

3.  **Set Environment Variables**:
    Before running the service, set the `PULSAR_URL` environment variable:
    ```bash
    export PULSAR_URL="pulsar://localhost:6650" # Replace with your Pulsar broker URL
    ```

4.  **Run the Service**:
    Execute the `main.py` script:
    ```bash
    python app/main.py
    ```
    The service will start consuming messages from the `document-conversion-requests` Pulsar topic.

## Dependencies

-   `apache-pulsar`: Client library for interacting with Pulsar.
-   `requests`: For downloading files from URLs.
-   `shared_lib.config`: Shared library for loading configuration.
-   `LibreOffice (soffice)`: External dependency for document conversion. 