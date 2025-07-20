# User Provisioning Service

The User Provisioning Service handles automated user provisioning across various Platform Q services including authentication systems, collaboration tools, and development platforms.

## Overview

This service provides centralized user management with:
- Multi-service user provisioning
- Bulk user operations
- Role and group management
- Password synchronization
- User lifecycle management

## Features

- **Multi-Service Support**: Provision users across Keycloak, Nextcloud, OpenProject, Vault, GitLab
- **Bulk Operations**: Provision multiple users efficiently with batching
- **Automated Credentials**: Generate and manage user credentials securely
- **Role Synchronization**: Keep roles consistent across services
- **Group Management**: Manage user groups across platforms
- **Event-driven Updates**: Respond to user lifecycle events

## API Endpoints

### User Provisioning
- `POST /api/v1/users/provision` - Provision a single user
- `POST /api/v1/users/provision/bulk` - Bulk provision users
- `POST /api/v1/users/deprovision` - Deprovision a user
- `GET /api/v1/users/{tenant_id}/{user_id}/status` - Get user status

### User Management
- `POST /api/v1/users/{tenant_id}/{user_id}/sync` - Sync user across services
- `PUT /api/v1/users/{tenant_id}/{user_id}/roles` - Update user roles
- `PUT /api/v1/users/{tenant_id}/{user_id}/groups` - Update user groups
- `POST /api/v1/users/{tenant_id}/{user_id}/reset-password` - Reset password

### Service Management
- `GET /api/v1/provisioners` - List available provisioners

### Health & Metrics
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8005)
- `PULSAR_URL` - Pulsar broker URL
- `AUTH_SERVICE_URL` - Authentication service URL
- `NEXTCLOUD_URL` - Nextcloud URL
- `NEXTCLOUD_ADMIN_USER` - Nextcloud admin username
- `NEXTCLOUD_ADMIN_PASS` - Nextcloud admin password
- `OPENPROJECT_URL` - OpenProject URL
- `OPENPROJECT_API_KEY` - OpenProject API key
- `VAULT_ADDR` - Vault address
- `VAULT_TOKEN` - Vault token
- `KEYCLOAK_URL` - Keycloak URL
- `KEYCLOAK_REALM` - Keycloak realm
- `KEYCLOAK_CLIENT_ID` - Keycloak client ID
- `KEYCLOAK_CLIENT_SECRET` - Keycloak client secret
- `GITLAB_URL` - GitLab URL
- `GITLAB_TOKEN` - GitLab token

## Supported Services

### Keycloak
- User creation with roles and groups
- Password management
- Multi-realm support
- Federation configuration

### Nextcloud
- User account creation
- Quota assignment
- Group membership
- App permissions

### OpenProject
- User account creation
- Project membership
- Role assignment
- API access

### HashiCorp Vault
- User authentication setup
- Policy assignment
- Secret access configuration
- AppRole creation

### GitLab
- User account creation
- Project access
- Group membership
- SSH key management

## User Provisioning Flow

1. **Request Reception**: API receives provisioning request
2. **Validation**: Validate user data and permissions
3. **Service Provisioning**: Provision user in each service
4. **Credential Generation**: Generate passwords and API keys
5. **Secret Storage**: Store credentials in Vault
6. **Notification**: Send provisioning status events
7. **Synchronization**: Ensure consistency across services

## Bulk Provisioning

The service supports efficient bulk user provisioning:
- Batched operations to reduce API calls
- Parallel provisioning where possible
- Progress tracking and reporting
- Rollback on failures
- CSV/JSON import support

## Development

### Running Locally
```bash
cd services/user-provisioning-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8005
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t user-provisioning-service:latest -f services/user-provisioning-service/Dockerfile .
```

## Architecture

The service follows a modular architecture:

- **Orchestrator**: Coordinates user provisioning across services
- **Service Provisioners**: Individual provisioners for each service
- **Event Processor**: Handles user lifecycle events
- **Credential Manager**: Generates and manages user credentials

## Event Handling

The service listens for:
- `UserCreatedEvent` - Provision new user across services
- `UserUpdatedEvent` - Update user information
- `UserDeletedEvent` - Deprovision user
- `RoleChangedEvent` - Update user roles
- `GroupChangedEvent` - Update user groups

## Security

- **Credential Generation**: Secure password generation with configurable complexity
- **Secret Storage**: All credentials stored in Vault
- **Service Authentication**: Secure authentication to each service
- **Audit Logging**: All provisioning actions logged
- **Data Encryption**: Sensitive data encrypted in transit

## Monitoring

The service exposes Prometheus metrics for:
- User provisioning success/failure rates
- Service-specific provisioning times
- Bulk operation performance
- API endpoint latencies
- Service availability status 