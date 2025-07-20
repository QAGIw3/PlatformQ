# Kong API Gateway with Consul Service Mesh

This directory contains the Kong API Gateway configuration integrated with Consul Connect service mesh, providing a secure, scalable API management layer for PlatformQ.

## Architecture Overview

```
                           ┌─────────────────┐
                           │   Internet      │
                           └────────┬────────┘
                                    │
                           ┌────────▼────────┐
                           │  Kong Gateway   │
                           │  (Port 8000)    │
                           └────────┬────────┘
                                    │
                      ┌─────────────┴─────────────┐
                      │     Kong Plugins          │
                      │  • OAuth2/OIDC            │
                      │  • Rate Limiting          │
                      │  • JWT Validation         │
                      │  • ACL                   │
                      └─────────────┬─────────────┘
                                    │
                      ┌─────────────▼─────────────┐
                      │   Consul DNS Resolution   │
                      │  service.consul:port      │
                      └─────────────┬─────────────┘
                                    │
                 ┌──────────────────┴──────────────────┐
                 │          Service Mesh               │
                 │  ┌────────┐  ┌────────┐  ┌────────┐│
                 │  │Service │  │Service │  │Service ││
                 │  │   A    │  │   B    │  │   C    ││
                 │  └────────┘  └────────┘  └────────┘│
                 └─────────────────────────────────────┘
```

## Features

### Dynamic Service Discovery
- Automatic synchronization with Consul service registry
- Real-time updates when services are added/removed
- Health-aware routing with automatic failover

### Authentication & Authorization
- **OAuth2/OIDC**: Integration with auth-service for user authentication
- **JWT**: Service-to-service authentication
- **ACL**: Role-based access control
- **API Keys**: For external API consumers

### API Management
- **Rate Limiting**: Protect services from overload
- **Request/Response Transformation**: Modify headers and payloads
- **CORS**: Configurable cross-origin resource sharing
- **Prometheus Metrics**: Detailed API metrics

### Security
- **mTLS**: Via Consul Connect for backend services
- **Request Validation**: Schema validation for API requests
- **IP Restriction**: Whitelist/blacklist IP addresses
- **Bot Detection**: Protect against automated attacks

## Quick Start

### 1. Start Kong with Consul

```bash
# Start the Kong gateway
docker-compose -f docker-compose.kong-consul.yml up -d

# Wait for Kong to be ready
curl -i http://localhost:8001/status
```

### 2. Configure OAuth2/OIDC

```bash
# Run the OAuth configuration script
docker exec kong-config-sync python /app/configure-oauth.py
```

### 3. Access Services

Services are available at:
- Auth Service: `http://localhost:8000/auth`
- Trading Platform: `http://localhost:8000/trading`
- Market Data: `http://localhost:8000/market`
- Analytics: `http://localhost:8000/analytics`

### 4. Admin Interfaces

- Kong Admin API: http://localhost:8001
- Kong Manager UI: http://localhost:8002
- Konga UI: http://localhost:1337

## Service Routes

| Service | Path | Authentication | Description |
|---------|------|----------------|-------------|
| auth-service | `/auth` | None | Authentication provider |
| market-data-service | `/market` | Optional | Public market data |
| trading-platform-service | `/trading` | Required | Trading operations |
| order-matching-service | `/orders` | Required | Order management |
| analytics-service | `/analytics` | Required | Analytics and reporting |
| blockchain-gateway-service | `/blockchain` | Required | Blockchain operations |
| risk-management-service | `/risk` | Required | Risk monitoring |
| compliance-service | `/compliance` | Admin only | Compliance tools |

## Authentication Flows

### User Authentication (OIDC)

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Client  │────▶│   Kong   │────▶│   Auth   │────▶│   IDP    │
│          │◀────│          │◀────│ Service  │◀────│          │
└──────────┘     └──────────┘     └──────────┘     └──────────┘
     │                 │                                   
     └─────────────────┴── JWT Token ─────────────────────┘
```

### API Authentication (JWT)

```
POST /trading/api/v1/orders
Authorization: Bearer <jwt-token>

Kong validates JWT → Adds user context → Forwards to service
```

## Plugin Configuration

### Rate Limiting

```json
{
  "name": "rate-limiting",
  "config": {
    "minute": 100,
    "hour": 10000,
    "policy": "redis",
    "redis_host": "ignite"
  }
}
```

### OAuth2 Scopes

| Service | Required Scopes |
|---------|----------------|
| trading-platform | `trade:read`, `trade:write` |
| order-matching | `order:read`, `order:write`, `order:cancel` |
| analytics | `analytics:read`, `report:generate` |
| risk-management | `risk:read`, `risk:override` |

### ACL Groups

| Group | Access Level |
|-------|-------------|
| admin | Full access to all services |
| trader | Trading and market data access |
| analyst | Analytics and reporting access |
| user | Basic platform access |
| api | API-only access |

## Monitoring

### Prometheus Metrics

Kong exposes metrics at `http://localhost:8001/metrics`:

- Request rate by service/route
- Response time percentiles
- Error rates
- Upstream health status

### Health Checks

```bash
# Kong health
curl http://localhost:8001/status

# Service health via Kong
curl http://localhost:8000/auth/health
```

## Security Best Practices

1. **Production Secrets**: Replace example secrets with secure values
2. **TLS/SSL**: Enable HTTPS in production
3. **Token Rotation**: Implement regular token rotation
4. **Audit Logging**: Enable comprehensive audit logs
5. **WAF Rules**: Configure Web Application Firewall rules

## Troubleshooting

### Service Not Found

```bash
# Check if service is registered in Consul
curl http://localhost:8500/v1/catalog/service/my-service

# Check Kong services
curl http://localhost:8001/services
```

### Authentication Failed

```bash
# Check consumer credentials
curl http://localhost:8001/consumers/my-user

# Verify OAuth2 application
curl http://localhost:8001/consumers/my-user/oauth2
```

### Rate Limit Exceeded

Check response headers:
- `X-RateLimit-Limit-Minute`
- `X-RateLimit-Remaining-Minute`
- `Retry-After`

## Scripts

- `kong-consul-sync.py`: Synchronizes Consul services with Kong
- `configure-oauth.py`: Sets up OAuth2/OIDC authentication
- `Dockerfile.config-sync`: Container for running sync scripts

## Configuration Files

- `docker-compose.kong-consul.yml`: Kong deployment with Consul
- `plugins/`: Custom Kong plugin implementations
- `config/`: Kong declarative configuration files

## Development

### Adding a New Service

1. Register service in Consul
2. Sync script will automatically create Kong service and route
3. Configure authentication if needed

### Custom Plugins

Place custom plugins in `plugins/` directory:

```lua
-- plugins/my-plugin/handler.lua
local MyPlugin = {
    PRIORITY = 1000,
    VERSION = "1.0.0"
}

function MyPlugin:access(conf)
    -- Plugin logic
end

return MyPlugin
```

### Testing

```bash
# Test without auth (public endpoint)
curl http://localhost:8000/market/api/v1/prices

# Test with JWT
TOKEN=$(curl -X POST http://localhost:8000/auth/api/v1/token \
  -d "username=test&password=test" | jq -r .token)
  
curl http://localhost:8000/trading/api/v1/positions \
  -H "Authorization: Bearer $TOKEN"
``` 