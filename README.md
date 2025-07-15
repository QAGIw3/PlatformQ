# platformQ: A Cloud-Native Digital Ecosystem

Welcome to platformQ. This repository contains the source code for a complete, enterprise-grade, multi-tenant cloud-native platform. It is designed from the ground up to be secure, scalable, observable, and automated, providing a robust foundation for building a suite of integrated digital applications.

## Core Capabilities

The platform is more than just a collection of microservices; it is a unified ecosystem that provides:

*   **Centralized Identity & SSO**: A full OIDC provider (`auth-service`) for unified authentication.
*   **Integrated Open-Source Suite**: Seamless integration with powerful tools like Nextcloud, OpenProject, Zulip, and OnlyOffice.
*   **Digital Asset Management**: A central `digital-asset-service` and a rich web interface for managing a unified model of all data on the platform, from 3D models and CAD files to business contacts and simulation results.
*   **Extensible Data Connector Framework**: A powerful `connector-service` with a plugin-based architecture for ingesting data from any source, including:
    *   **Complex File Formats**: Deep metadata extraction for tools like Blender, GIMP, FreeCAD, and Audacity using a scalable Kubernetes Job pattern.
    *   **API-Driven Services**: Scheduled data synchronization from external systems like SuiteCRM and Metasfresh.
    *   **Real-time Event Streams**: Ingestion from generic webhooks and other push-based sources.
*   **Event-Driven Architecture**: A Pulsar-based message bus for asynchronous, resilient communication between services.
*   **Advanced Data & Analytics**: A complete Lakehouse architecture with Flink, Trino, and Cassandra for real-time analytics and deep data exploration.
*   **Cutting-Edge Engines**:
    *   **Simulation Engine**: For large-scale, agent-based modeling.
    *   **Trust Engine**: For creating cryptographically verifiable workflows with Verifiable Credentials.
    *   **Intelligence Engine**: For discovering emergent patterns in platform data using Graph Analytics.
*   **WASM-Powered Workflows**: A secure, embedded WebAssembly runtime in the `functions-service` enables intelligent automation, such as analyzing simulation logs and automatically creating issues in project management tools.
*   **"Golden Path" Developer Experience**: A complete DevOps automation suite, including a CLI (`platformqctl`) with "superpowers" for interacting directly with all core platform engines, and a "Golden Path" service template.

## Getting Started

For developers looking to contribute or build on the platform, the best place to start is the **[Contribution Guide](CONTRIBUTING.md)**.

For operators looking to deploy and manage the platform, refer to the **[IaC & Deployment Guide](iac/terraform/README.md)**.

## Documentation Portal

For complete, in-depth documentation on every aspect of the platform, you can build and serve the documentation portal locally:

```bash
# Install documentation tools
pip install -r requirements.txt

# Build API docs and other generated content
make docs-build

# Serve the site locally
make docs-serve
```
This will make the full documentation available at `http://127.0.0.1:8000`. 