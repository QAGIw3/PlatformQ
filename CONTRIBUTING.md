# Contributing to platformQ

Thank you for your interest in contributing to the platformQ project! This document outlines our architectural principles, development workflow, and coding standards to help you get started.

## 1. Core Architectural Principles

Our platform is built on a set of core principles that guide our technical decisions. Understanding these is key to contributing effectively.

- **Everything as Code**: All infrastructure, deployment manifests, configurations, and policies are stored in version control.
- **Event-Driven**: Services communicate asynchronously via a central message bus (Pulsar) using versioned Avro schemas. This promotes loose coupling and scalability.
- **Centralized Identity**: All authentication and authorization are handled by a single OIDC provider (`auth-service`). Services do not manage their own credentials.
- **Zero-Trust Networking**: All internal traffic is encrypted via a service mesh (Istio), and services trust headers from the gateway, not network location.
- **Immutable Infrastructure**: We do not manually change running environments. All changes are deployed through our automated CI/CD pipeline via Terraform and Helm.
- **Multi-Tenancy First**: All data and APIs are partitioned by `tenant_id` from the database up.

## 2. Local Development Setup

To get started, you need Docker and `make` installed.

1.  **Start all services**:
    ```bash
    make services-up
    ```
2.  **Bootstrap the platform**:
    This only needs to be done once. It populates Consul, Vault, and registers our OIDC clients.
    ```bash
    make bootstrap-config
    make bootstrap-secrets
    make bootstrap-oidc-clients
    make bootstrap-platform
    ```
3.  **Run linters and tests**:
    ```bash
    make lint
    make test
    ```

## 3. Creating a New Service (The "Golden Path")

We use a CLI tool to scaffold new services.

1.  **Install the CLI**:
    ```bash
    pip install -e ./platformqctl
    ```
2.  **Create your service**:
    ```bash
    platformqctl create service --name=my-new-service
    ```
    This creates a new directory in `services/` with a complete, best-practice skeleton.

## 4. Architectural Decision Log (ADR)

*   **ADR-001: Microservices over Monolith**: Chosen for independent scaling, deployment, and team autonomy.
*   **ADR-002: Event-Driven over Request/Reply**: Chosen for resilience and scalability. Pulsar was chosen over Kafka for its native multi-tenancy and tiered storage.
*   **ADR-003: Centralized Identity (OIDC)**: Chosen to avoid each service implementing its own auth logic.
*   **ADR-004: Service Mesh (Istio)**: Chosen for transparent mTLS and advanced traffic management (canary releases).
*   **ADR-005: GitOps (GitHub Actions)**: Chosen to provide a single, version-controlled workflow for testing and deploying all platform components.
*   **ADR-006: Lakehouse Architecture**: Chosen for cost-effective, long-term storage of raw events and unified analytics.

---

We look forward to your contributions! 