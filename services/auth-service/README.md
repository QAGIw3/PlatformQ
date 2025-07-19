# `auth-service`

The Authentication and Authorization Service is the identity and access management backbone of the platformQ ecosystem, providing unified authentication, authorization, and policy management.

## Core Responsibilities

- **OIDC Provider**: Acts as a full OpenID Connect provider for all other applications, enabling Single Sign-On (SSO).
- **Multi-Tenant Identity**: Manages users, roles, and subscriptions, all strictly partitioned by `tenant_id`.
- **Authentication Methods**: Supports both traditional passwordless login and decentralized "Sign-In with Ethereum" (SIWE).
- **API Key Management**: Allows users to generate API keys for programmatic access.
- **Event Publishing**: Publishes Avro-encoded events to Pulsar for significant identity events (e.g., `UserCreated`, `SubscriptionChanged`).
- **Unified Authorization**: Provides centralized policy evaluation for access control decisions across all services.
- **Policy Management**: Supports both Role-Based Access Control (RBAC) and attribute-based policies with future OPA integration.

## API Specification

The full OpenAPI specification for this service is automatically generated and can be found in the main [API Reference documentation](.././docs/index.html).

### Key Endpoints

- `/evaluate`: Evaluate authorization policies for access control decisions
- `/check-permission`: Check if the current user can perform a specific action
- `/policies`: Manage authorization policies (admin only)
- `/policies/actions`: List available actions and their required roles

## Key Data Models

- `tenants`: The root table for all organizations on the platform.
- `users`: Stores user profile information, partitioned by tenant.
- `user_roles`: Maps users to their roles within a specific tenant.
- `oidc_clients`: Stores the configuration for applications (like Nextcloud or Superset) that use this service for SSO.

Refer to the master `schema.cql` file for the complete data model. 