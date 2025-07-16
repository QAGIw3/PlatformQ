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

## Advanced Data Infrastructure

*   **Unified Data Integration**: `seatunnel-service` provides 100+ pre-built connectors for seamless data synchronization between systems, supporting batch, streaming, and CDC modes.
*   **Data Lake & Warehouse**: Complete lakehouse architecture with:
    *   **Apache Hive Metastore**: Centralized metadata management for all data lake tables
    *   **MinIO**: S3-compatible object storage for raw and processed data
    *   **Trino**: High-performance distributed SQL for interactive analytics
    *   **Apache Cassandra**: Wide-column store for time-series and operational data
*   **Stream Processing**: Real-time data pipelines with:
    *   **Apache Flink**: Fully implemented stream processing jobs for activity stream aggregation and graph ingestion
    *   **Apache Pulsar**: Multi-tenant messaging with schema registry
    *   **Event Sourcing**: Complete audit trail of all platform activities

## Intelligent Services

*   **Unified Search**: `search-service` with Elasticsearch providing:
    *   Full-text search across all platform data
    *   Faceted search and aggregations
    *   Real-time autocomplete
    *   Multi-language support
*   **Real-time Analytics**: `realtime-analytics-service` powered by Apache Ignite offering:
    *   Sub-millisecond metric queries
    *   Live dashboards with WebSocket updates
    *   Time-series data management
    *   Prometheus-compatible metric export
*   **Graph Intelligence**: Enhanced with JanusGraph + Elasticsearch for:
    *   Real-time relationship discovery
    *   Community detection and centrality analysis
    *   Multi-hop traversals with millisecond response times
*   **Project Collaboration**: Fully integrated `projects-service` that:
    *   Creates Nextcloud folders with proper permissions
    *   Sets up OpenProject workspaces with initial tasks
    *   Provisions Zulip discussion streams
    *   Maintains synchronized state across all tools

## Enterprise Connectors

*   **SuiteCRM Connector**: Complete bi-directional sync of:
    *   Contacts with full relationship mapping
    *   Accounts with industry classification
    *   Opportunities with sales stage tracking
    *   Custom field support
*   **Metasfresh ERP Connector**: Real-time synchronization of:
    *   Products with category hierarchy
    *   Business partners (customers/vendors)
    *   Sales and purchase orders
    *   Invoices with payment status

## Platform Engines

*   **Simulation Engine**: For large-scale, agent-based modeling
*   **Trust Engine**: Verifiable credentials and cryptographic workflows
*   **WASM Functions**: Secure, embedded WebAssembly runtime for:
    *   Custom data transformations
    *   Intelligent automation workflows
    *   Real-time scoring and analysis

## Developer Experience

*   **Golden Path Templates**: Service templates with all best practices built-in
*   **Platform CLI**: `platformqctl` with direct access to all platform capabilities
*   **Comprehensive APIs**: RESTful and gRPC APIs for all services
*   **Event-Driven SDKs**: Libraries for publishing and consuming platform events

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