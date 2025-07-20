# PlatformQ Consul Connect Service Mesh

This directory contains the configuration and setup for PlatformQ's Consul Connect service mesh, providing secure service-to-service communication, service discovery, and configuration management.

## Overview

The service mesh provides:
- **Mutual TLS (mTLS)** encryption between all services
- **Service discovery** with health checking
- **Configuration management** via Consul KV store
- **Access control** with ACL policies and intentions
- **Observability** through Envoy proxy metrics

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Consul Server 1 │────▶│ Consul Server 2 │────▶│ Consul Server 3 │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┴───────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
         ┌──────────▼────────┐   ┌─────────▼──────────┐
         │   Service A       │   │   Service B        │
         │ ┌───────────────┐ │   │ ┌────────────────┐ │
         │ │ Consul Agent  │ │   │ │ Consul Agent   │ │
         │ └───────┬───────┘ │   │ └────────┬───────┘ │
         │ ┌───────▼───────┐ │   │ ┌────────▼───────┐ │
         │ │ Envoy Sidecar │◀────┼─▶│ Envoy Sidecar  │ │
         │ └───────┬───────┘ │   │ └────────┬───────┘ │
         │ ┌───────▼───────┐ │   │ ┌────────▼───────┐ │
         │ │  Application  │ │   │ │  Application   │ │
         │ └───────────────┘ │   │ └────────────────┘ │
         └───────────────────┘   └────────────────────┘
```

## Quick Start

### 1. Start the Service Mesh

```bash
# Start Consul servers and all services
./scripts/start-service-mesh.sh

# Or manually with docker-compose
docker-compose -f docker-compose.service-mesh.yml up -d
```

### 2. Bootstrap ACLs and Policies

```bash
# Run the bootstrap script
python3 scripts/bootstrap-service-mesh.py

# This will:
# - Initialize ACL system
# - Create service policies
# - Set up service intentions
# - Load initial configurations
# - Generate consul-tokens.env file
```

### 3. Access Consul UI

Open http://localhost:8500 and login with the bootstrap token.

## Service Integration

### Using the PlatformQ Consul Library

```python
from platformq_consul import create_service_config, HealthCheckRegistry, create_health_endpoint

# Create service configuration
config = create_service_config("my-service",
    port=8000,
    upstreams={
        "auth-service": 5000,
        "data-platform-service": 5001,
    }
)

# Use service discovery
auth_url = config.get_upstream_url("auth-service")

# Register health checks
health_registry = HealthCheckRegistry()
health_registry.register("database", check_database_connection)
health_registry.register("cache", check_cache_connection)

# Add health endpoints to FastAPI
app.include_router(create_health_endpoint(health_registry))
```

### Environment Variables

Services should be configured with these environment variables:

```bash
# Consul agent address
CONSUL_HTTP_ADDR=consul-agent-myservice:8500

# Service name and port
SERVICE_NAME=my-service
SERVICE_PORT=8000

# Consul token (from consul-tokens.env)
CONSUL_HTTP_TOKEN=<service-token>

# Upstream service ports (for Connect)
AUTH_SERVICE_PORT=5000
DATA_PLATFORM_PORT=5001
```

## Service Definitions

Service definitions are stored in `consul/services/` as JSON files. Each definition includes:

- Service metadata (name, tags, port)
- Health check configuration
- Connect sidecar configuration
- Upstream service dependencies

Example service definition:

```json
{
  "service": {
    "name": "my-service",
    "tags": ["api", "platform"],
    "port": 8000,
    "check": {
      "http": "http://localhost:8000/health",
      "interval": "10s"
    },
    "connect": {
      "sidecar_service": {
        "port": 21000,
        "proxy": {
          "upstreams": [
            {
              "destination_name": "auth-service",
              "local_bind_port": 5000
            }
          ]
        }
      }
    }
  }
}
```

## Service Intentions

Service intentions define which services can communicate. They are configured in:
- `consul/policies/service-intentions-comprehensive.hcl`

Key principles:
- All services can access `auth-service`
- All services can access infrastructure (Ignite, Pulsar)
- Specific services have restricted access to databases
- Default deny policy for undefined connections

## Configuration Management

Service configurations are stored in Consul KV under `config/<service-name>/`.

### Reading Configuration

```python
# Get service configuration
config_data = config.get_config()  # Gets config/<service-name>/

# Or specific key
api_config = config.get_config("config/my-service/api")
```

### Writing Configuration

```bash
# Via CLI
consul kv put config/my-service/settings '{"key": "value"}'

# Via API
curl -X PUT -d '{"key": "value"}' \
  http://localhost:8500/v1/kv/config/my-service/settings
```

## Health Checks

Services must expose health endpoints:

- `/health` - Comprehensive health status
- `/health/live` - Liveness probe (is service running)
- `/health/ready` - Readiness probe (is service ready to accept traffic)

The health check should return:
- HTTP 200 for healthy
- HTTP 503 for unhealthy

## Security

### ACL Policies

Each service has its own ACL policy granting:
- Write access to its own service registration
- Read access to all services (for discovery)
- Write access to its configuration namespace

### mTLS Encryption

All service-to-service communication is encrypted using mTLS via Envoy proxies. Certificates are automatically managed by Consul.

### Service Tokens

Service tokens are generated during bootstrap and saved to `consul-tokens.env`. These should be:
- Stored securely (use Vault in production)
- Rotated regularly
- Never committed to version control

## Monitoring

### Metrics

Envoy sidecars expose Prometheus metrics on port 9102+:
- Request rates and latencies
- Error rates
- Connection pool statistics

### Tracing

Envoy can be configured to send traces to Jaeger:
- Distributed request tracing
- Service dependency visualization
- Performance bottleneck identification

## Troubleshooting

### Check Service Registration

```bash
# List all services
consul catalog services

# Check specific service health
consul health checks <service-name>
```

### View Envoy Configuration

```bash
# Access Envoy admin interface
curl http://localhost:19000/clusters
curl http://localhost:19000/config_dump
```

### Debug Intentions

```bash
# List all intentions
consul intention list

# Check specific intention
consul intention check auth-service data-platform-service
```

### Common Issues

1. **Service cannot connect to upstream**
   - Check service intentions
   - Verify upstream port configuration
   - Check Envoy logs

2. **Health check failing**
   - Verify service is running
   - Check health endpoint accessibility
   - Review health check timeout

3. **ACL permission denied**
   - Verify service token is set
   - Check ACL policy includes required permissions
   - Review Consul agent logs

## Scripts

- `scripts/generate_consul_services.py` - Generate service definitions
- `scripts/generate_docker_compose_mesh.py` - Generate docker-compose file
- `scripts/bootstrap-service-mesh.py` - Bootstrap ACLs and configurations
- `scripts/start-service-mesh.sh` - Start the entire service mesh

## Files

- `config/consul.hcl` - Consul server configuration
- `config/consul-client.hcl` - Consul client configuration
- `services/*.json` - Service definitions
- `policies/service-intentions-comprehensive.hcl` - Service communication policies 