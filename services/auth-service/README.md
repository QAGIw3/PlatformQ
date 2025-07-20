# PlatformQ Auth Service

A comprehensive authentication and authorization service providing JWT-based authentication, OAuth2/OIDC support, role-based access control (RBAC), and integration with HashiCorp Vault and Consul.

## Features

### Core Authentication
- **JWT Authentication**: Secure token-based authentication
- **OAuth2/OIDC Provider**: Full OpenID Connect implementation
- **Multi-Tenant Support**: Isolated authentication per tenant
- **Role-Based Access Control**: Fine-grained permissions
- **API Key Management**: For service-to-service authentication

### OAuth2/OIDC Support
- **Authorization Code Flow**: With PKCE support
- **Client Credentials Flow**: For service accounts
- **OpenID Connect Discovery**: `.well-known/openid-configuration`
- **Dynamic Client Registration**: OAuth2 DCR support
- **Token Introspection**: RFC 7662 compliant
- **Token Revocation**: RFC 7009 compliant
- **JWKS Endpoint**: Public key distribution

### Security Features
- **HashiCorp Vault Integration**: 
  - Secure storage of JWT signing keys
  - Dynamic secret rotation
  - OAuth2 client credentials management
- **Consul Integration**:
  - Service discovery
  - Health checking
  - Configuration management
- **Passwordless Authentication**: Email-based login
- **Sign-In with Ethereum (SIWE)**: Web3 authentication

## API Endpoints

### OAuth2/OIDC Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/oauth2/.well-known/openid-configuration` | GET | OpenID Connect discovery |
| `/oauth2/authorize` | GET | Authorization endpoint |
| `/oauth2/token` | POST | Token endpoint |
| `/oauth2/userinfo` | GET | UserInfo endpoint |
| `/oauth2/jwks` | GET | JSON Web Key Set |
| `/oauth2/introspect` | POST | Token introspection |
| `/oauth2/revoke` | POST | Token revocation |

### Authentication Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/login` | POST | Username/password login |
| `/api/v1/login/passwordless` | POST | Request passwordless login |
| `/api/v1/token/passwordless` | POST | Exchange passwordless token |
| `/api/v1/token/refresh` | POST | Refresh access token |
| `/api/v1/logout` | POST | Revoke refresh token |
| `/api/v1/register` | POST | User registration |

### User Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/users` | GET | List users (admin) |
| `/api/v1/users/{user_id}` | GET | Get user details |
| `/api/v1/users/{user_id}` | PATCH | Update user |
| `/api/v1/users/{user_id}` | DELETE | Delete user |
| `/api/v1/users/{user_id}/roles` | GET | Get user roles |
| `/api/v1/users/{user_id}/roles` | PUT | Assign roles |

### API Key Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/apikeys` | POST | Create API key |
| `/api/v1/apikeys` | GET | List API keys |
| `/api/v1/apikeys/{key_id}` | DELETE | Revoke API key |

## OAuth2/OIDC Configuration

### Supported Flows
1. **Authorization Code Flow** (recommended for web apps)
   - With PKCE for public clients
   - Standard flow for confidential clients

2. **Client Credentials Flow** (for service-to-service)
   - Machine-to-machine authentication
   - No user context required

### Scopes
- `openid`: OpenID Connect support
- `profile`: User profile information
- `email`: Email address access
- `offline_access`: Refresh token support
- `trade:read`, `trade:write`: Trading permissions
- `analytics:read`: Analytics access
- `ml:access`: Machine learning platform access

### Example: Authorization Code Flow

1. **Redirect to authorization endpoint:**
```
GET /oauth2/authorize?
  response_type=code&
  client_id=your-client-id&
  redirect_uri=https://app.example.com/callback&
  scope=openid profile email&
  state=random-state&
  code_challenge=challenge&
  code_challenge_method=S256
```

2. **Exchange authorization code for tokens:**
```bash
curl -X POST http://localhost:8000/oauth2/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=authorization_code" \
  -d "code=AUTHORIZATION_CODE" \
  -d "redirect_uri=https://app.example.com/callback" \
  -d "client_id=your-client-id" \
  -d "client_secret=your-secret" \
  -d "code_verifier=verifier"
```

3. **Get user information:**
```bash
curl -H "Authorization: Bearer ACCESS_TOKEN" \
  http://localhost:8000/oauth2/userinfo
```

### Example: Client Credentials Flow

```bash
curl -X POST http://localhost:8000/oauth2/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=service-client-id" \
  -d "client_secret=service-secret" \
  -d "scope=service"
```

## Integration with Kong

The auth service serves as the identity provider for Kong's OAuth2/OIDC plugins:

1. Kong OIDC plugin configuration:
```json
{
  "issuer": "http://auth-service:8000/oauth2",
  "discovery": "http://auth-service:8000/oauth2/.well-known/openid-configuration"
}
```

2. Token validation flow:
   - Client → Kong → Auth Service (token validation)
   - Kong caches validation results
   - Auth service provides JWKS for signature verification

## Running the Service

### With Docker Compose

```bash
docker-compose -f docker-compose.service-mesh.yml up auth-service
```

### Environment Variables

```bash
# Vault Configuration
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=your-vault-token

# Consul Configuration  
CONSUL_HTTP_ADDR=consul-agent:8500

# Database
DATABASE_URL=cassandra://cassandra:9042/platformq

# JWT Settings
JWT_SECRET_KEY=your-secret-key  # Overridden by Vault
JWT_ALGORITHM=RS256
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=30

# OAuth2 Settings
OAUTH2_ENABLED=true
OAUTH2_ISSUER=http://localhost:8000/oauth2
```

## Health Checks

- `/health` - Overall service health
- `/health/live` - Liveness probe
- `/health/ready` - Readiness probe (includes Vault/Consul connectivity)

## Security Considerations

1. **Token Storage**: 
   - Access tokens are stateless JWTs
   - Refresh tokens stored in database
   - Signing keys rotated via Vault

2. **Client Credentials**:
   - Stored hashed in database
   - Can be rotated via API
   - Vault integration for secure storage

3. **Rate Limiting**:
   - Implemented at Kong gateway level
   - Per-user and per-IP limits

4. **Audit Logging**:
   - All authentication events logged
   - Integration with centralized logging

## Development

### Running Tests
```bash
pytest tests/
```

### Adding New OAuth2 Clients
```python
from app.crud import crud_oidc

client = crud_oidc.create_client(
    db,
    client_name="My App",
    redirect_uris=["https://myapp.com/callback"],
    grant_types=["authorization_code", "refresh_token"],
    response_types=["code"],
    scope="openid profile email"
)
```

## Monitoring

### Metrics
- Authentication success/failure rates
- Token issuance counts
- OAuth2 flow completion rates
- API response times

### Alerts
- High authentication failure rate
- Vault/Consul connectivity issues
- Token signing key rotation failures
- Database connection pool exhaustion 