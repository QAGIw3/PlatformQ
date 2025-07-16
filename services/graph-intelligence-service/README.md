# Graph Intelligence Service

## Purpose

The **Graph Intelligence Service** analyzes the real-time graph of relationships within the platform, stored in JanusGraph, to discover complex patterns, communities, and insights that would not be visible in traditional analytics. It provides both RESTful and gRPC interfaces for accessing these insights.

Its primary responsibilities are:
-   Connecting to JanusGraph to traverse and query the graph data.
-   Providing a gRPC endpoint (`GetCommunityInsights`) to retrieve community detection results, which are conceptually calculated by a background Spark GraphComputer job.
-   Exposing REST endpoints for various graph insights, such as centrality.

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### REST API Endpoints

-   `GET /api/v1/insights/{insight_type}`: Retrieves various graph insights based on the `insight_type`.
    -   `insight_type = "community-detection"`: Returns a message indicating that community detection is now performed via the gRPC `GetCommunityInsights` method.
    -   `insight_type = "centrality"`: Returns a list of the top 10 most central documents (by in-degree) within a tenant's graph.

### gRPC API Endpoints

-   **Service**: `GraphIntelligenceService`
-   **Method**: `GetCommunityInsights(GetCommunityInsightsRequest) returns (GetCommunityInsightsResponse)`
-   **Description**: Analyzes the graph for a given `tenant_id` to identify and return newly formed communities of users. The results are transformed from Gremlin query output into a protobuf message format.

## Configuration

The service relies on the following environment variable (or a configuration loaded via Consul/Vault in a production environment):

-   `JANUSGRAPH_URL`: The WebSocket URL for connecting to the JanusGraph server (e.g., `ws://platformq-janusgraph:8182/gremlin`).
-   `GRAPH_INTELLIGENCE_SERVICE_GRPC_TARGET`: The gRPC port for the service (default: `50052`).

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/graph-intelligence-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set Environment Variables**:
    Before running the service, set the `JANUSGRAPH_URL` environment variable:
    ```bash
    export JANUSGRAPH_URL="ws://localhost:8182/gremlin" # Replace with your JanusGraph instance URL
    ```

3.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/graph-intelligence-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000
    ```
    The gRPC server will also start on port `50052` upon application startup.

## Dependencies

-   `FastAPI`: Web framework for building REST APIs.
-   `gremlinpython`: Python client for Apache TinkerPop Gremlin.
-   `grpcio`, `grpcio-tools`: For gRPC communication.
-   `platformq_shared.base_service`: Shared library for common service functionalities. 