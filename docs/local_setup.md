# Local Development Environment Setup

This guide provides a detailed walkthrough for setting up the complete platformQ development environment on your local machine.

## 1. Prerequisites

- **Docker Desktop**: Ensure Docker Desktop is installed and running with at least 8GB of RAM allocated to it. Our stack is powerful but resource-intensive.
- **`make`**: The `make` utility is used to run common commands. It is pre-installed on macOS and most Linux distributions.
- **Python 3.11+**: While services run in Docker, some tooling and scripts may be run locally.
- **`platformqctl`**: Our internal CLI. Install it by running `pip install -e ./platformqctl` from the project root.

## 2. The "One-Time" Bootstrap

The first time you set up the environment, you must run a series of bootstrapping commands. These commands only need to be run once.

**It is recommended to run them in order, waiting for each to complete.**

1.  **Start all infrastructure containers**:
    This command starts the core infrastructure (Pulsar, Cassandra, Vault, Consul, etc.) in the background.
    ```bash
    make services-up
    ```
    You can check the status with `docker-compose -f infra/docker-compose/docker-compose.yml ps`.

2.  **Bootstrap Centralized Configuration**:
    This command populates the Consul KV store with all non-secret configuration.
    ```bash
    make bootstrap-config
    ```

3.  **Bootstrap Secrets**:
    This command populates Vault with the necessary secrets (passwords, API keys).
    ```bash
    make bootstrap-secrets
    ```

4.  **Bootstrap OIDC Clients**:
    This command registers our integrated applications (Nextcloud, Superset, etc.) as OIDC clients with our `auth-service`.
    ```bash
    make bootstrap-oidc-clients
    ```

5.  **Bootstrap API Gateway**:
    This command configures the Kong API Gateway with the necessary routes and security plugins.
    ```bash
    make bootstrap-platform
    ```

## 3. Daily Development Workflow

Once the one-time bootstrap is complete, your daily workflow is much simpler.

- **Checking your work**: Before committing code, always run the linter and tests.
  ```bash
  make lint
  make test
  ```

- **Formatting code**: To automatically format your code according to project standards:
  ```bash
  make format
  ```

- **Stopping the environment**:
  ```bash
  make services-down
  ```

## 4. Accessing Services

Once the stack is running, you can access the various UIs at these addresses:

- **Kong Gateway (Main Entry Point)**: `http://localhost:8000`
- **Kong Admin API**: `http://localhost:8001`
- **Consul UI**: `http://localhost:8500`
- **Vault UI**: `http://localhost:8200` (Token: `root`)
- **Pulsar UI**: `http://localhost:8080`
- **Jaeger UI (Traces)**: `http://localhost:16686`
- **Grafana UI (Metrics & Dashboards)**: `http://localhost:3000`
- **MLflow UI**: `http://localhost:5000`
- **Superset UI**: `http://localhost:8088` 