# Quantum Optimization Service Authentication

## Overview

The Quantum Optimization Service uses JWT-based authentication integrated with the PlatformQ Auth Service. All API endpoints require valid authentication tokens that contain user identity, tenant information, and role-based permissions.

## Authentication Flow

```mermaid
sequenceDiagram
    participant Client
    participant AuthService
    participant QuantumService
    participant JWT
    
    Client->>AuthService: Login with credentials
    AuthService->>JWT: Generate token with claims
    AuthService->>Client: Return JWT token
    Client->>QuantumService: API request with Bearer token
    QuantumService->>JWT: Validate & extract claims
    JWT->>QuantumService: User ID, Tenant ID, Roles
    QuantumService->>Client: Process request with user context
```

## JWT Token Structure

The JWT tokens contain the following claims:

```json
{
  "sub": "user-123",           // User ID
  "tid": "tenant-456",         // Tenant ID
  "email": "user@example.com", // User email
  "groups": [                  // User roles/groups
    "quantum_user",
    "premium_user"
  ],
  "exp": 1234567890,          // Expiration timestamp
  "iat": 1234567800,          // Issued at timestamp
  "iss": "platformq-auth"     // Issuer
}
```

## Required Roles

Different endpoints require different roles:

| Endpoint | Required Roles | Description |
|----------|---------------|-------------|
| `/api/v1/problems` | `quantum_user`, `quantum_admin`, `admin` | Basic quantum access |
| `/api/v1/problems/{id}/solve` | `quantum_user`, `quantum_admin`, `admin` | Submit optimization jobs |
| `/api/v1/admin/*` | `admin`, `platform_admin` | Administrative operations |
| `/api/v1/benchmarks` | Any authenticated user | View benchmark results |

## Making Authenticated Requests

### Using Bearer Token

Include the JWT token in the Authorization header:

```bash
curl -X POST http://quantum-service:8000/api/v1/problems \
  -H "Authorization: Bearer <your-jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Portfolio Optimization",
    "problem_type": "portfolio",
    "objective_function": {...},
    "variables": {...}
  }'
```

### Python Example

```python
import httpx
from datetime import datetime

# Get token from auth service first
auth_response = await client.post(
    "http://auth-service:8000/api/v1/login",
    json={"email": "user@example.com", "password": "password"}
)
token = auth_response.json()["access_token"]

# Use token for quantum service requests
headers = {"Authorization": f"Bearer {token}"}

response = await client.post(
    "http://quantum-service:8000/api/v1/problems",
    headers=headers,
    json={
        "name": "Route Optimization",
        "problem_type": "routing",
        "objective_function": {
            "type": "minimize",
            "expression": "total_distance"
        },
        "variables": {
            "nodes": ["A", "B", "C", "D"],
            "edges": [...]
        }
    }
)
```

## Service-to-Service Authentication

For internal service communication, use service tokens:

```python
# Service tokens have format: service-<service-name>-token
headers = {"Authorization": "Bearer service-simulation-token"}

response = await client.post(
    "http://quantum-service:8000/api/v1/internal/optimize",
    headers=headers,
    json={...}
)
```

## Resource Quotas

The service enforces resource quotas based on tenant subscription:

| Resource | Basic | Premium | Enterprise |
|----------|-------|---------|------------|
| Max Qubits | 10 | 20 | 50 |
| Jobs/Day | 10 | 100 | Unlimited |
| Max Circuit Depth | 5 | 10 | 20 |
| Priority | Low | Medium | High |

## Error Handling

Authentication errors return appropriate HTTP status codes:

- `401 Unauthorized`: Invalid or expired token
- `403 Forbidden`: Insufficient permissions or quota exceeded

Example error response:

```json
{
  "detail": "Could not validate credentials",
  "status_code": 401,
  "headers": {
    "WWW-Authenticate": "Bearer"
  }
}
```

## Security Best Practices

1. **Token Expiration**: Tokens expire after 1 hour by default
2. **Secure Storage**: Never store tokens in plain text
3. **HTTPS Only**: Always use HTTPS in production
4. **Token Refresh**: Implement token refresh before expiration
5. **Audit Logging**: All authenticated requests are logged

## Configuration

The service uses the following environment variables:

```bash
# JWT Configuration
JWT_SECRET_KEY=<secret-key>        # Shared with auth service
JWT_ALGORITHM=HS256                # JWT signing algorithm

# Auth Service
AUTH_SERVICE_URL=http://auth-service:8000

# Service Authentication
SERVICE_NAME=quantum-optimization-service
SERVICE_TOKEN=<service-token>
```

## Troubleshooting

### Common Issues

1. **"Could not validate credentials"**
   - Check token expiration
   - Verify JWT_SECRET_KEY matches auth service
   - Ensure token has required claims

2. **"Insufficient permissions"**
   - Verify user has required roles
   - Check role names match exactly

3. **"Quota exceeded"**
   - Check current usage against limits
   - Consider upgrading subscription

### Debug Mode

Enable debug logging for authentication:

```python
import logging
logging.getLogger("app.api.deps").setLevel(logging.DEBUG)
```

## Migration from Placeholder Auth

The service has been migrated from placeholder authentication to full JWT integration:

### Before (Placeholder)
```python
tenant_id: str = Depends(lambda: "default-tenant")
user_id: str = Depends(lambda: "user")
```

### After (JWT Auth)
```python
tenant_id: str = Depends(get_current_tenant_id)
user_id: str = Depends(get_current_user_id)
```

All endpoints now properly extract user context from JWT tokens, ensuring proper multi-tenancy and access control. 