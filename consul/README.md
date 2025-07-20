# Consul Service Mesh for PlatformQ

This directory contains the configuration and setup for HashiCorp Consul, which provides service discovery, health checking, and secure service-to-service communication for the PlatformQ microservices architecture.

## Features

- **Service Discovery**: Automatic service registration and discovery
- **Health Checking**: Built-in health checks for all services
- **Service Mesh**: mTLS encryption between services via Envoy proxy
- **Configuration Management**: Centralized configuration with hot-reload
- **ACL Security**: Fine-grained access control for services
- **Observability**: Metrics and tracing through Envoy sidecars

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Consul Cluster                           │
│  ┌───────────┐    ┌───────────┐    ┌───────────┐          │
│  │  Server 1  │────│  Server 2  │────│  Server 3  │          │
│  └───────────┘    └───────────┘    └───────────┘          │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────▼────────┐  ┌───────▼────────┐  ┌──────▼─────────┐
│  Service Node   │  │  Service Node   │  │  Service Node   │
│ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐│
│ │Consul Agent │ │  │ │Consul Agent │ │  │ │Consul Agent ││
│ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘│
│ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐│
│ │   Service   │ │  │ │   Service   │ │  │ │   Service   ││
│ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘│
│ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐│
│ │Envoy Sidecar│ │  │ │Envoy Sidecar│ │  │ │Envoy Sidecar││
│ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘│
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

## Quick Start

### 1. Start Consul Cluster

```bash
# Start the Consul cluster with example services
docker-compose -f docker-compose.consul.yml up -d

# Check cluster status
docker exec consul-server-1 consul members

# Access Consul UI
open http://localhost:8500
```

### 2. Register a Service

Services can register themselves using the Consul integration library:

```python
from platformq_consul import ConsulServiceRegistry

# Initialize registry
registry = ConsulServiceRegistry()

# Register service
await registry.register_service(
    name="my-service",
    port=8000,
    tags=["api", "v1"],
    meta={"version": "1.0.0"}
)
```

### 3. Discover Services

```python
# Discover healthy instances of a service
instances = await registry.discover_service("amm-service")

# Get connection URL (uses sidecar proxy if available)
url = await registry.get_service_connection("amm-service")
```

### 4. Service-to-Service Communication

```python
from platformq_consul import ServiceMeshClient

# Initialize client
mesh_client = ServiceMeshClient(registry)

# Make secure requests through the mesh
response = await mesh_client.get("oracle-service", "/api/v1/price/BTC/USD")
```

## Configuration

### Server Configuration (`consul.hcl`)

- **Datacenter**: `platformq-dc1`
- **ACL**: Enabled with deny-by-default
- **Connect**: Enabled for service mesh
- **UI**: Enabled on port 8500
- **Telemetry**: Prometheus metrics enabled

### Client Configuration (`consul-client.hcl`)

- **Mode**: Client (non-voting)
- **Retry Join**: Automatically join cluster
- **Connect**: Enabled for sidecar proxy

### Service Definitions

Each service has a JSON configuration file in `services/` that defines:
- Service name and port
- Health checks
- Connect sidecar configuration
- Upstream dependencies

## Service Intentions (ACLs)

Service intentions define which services can communicate:

```hcl
# Allow market-data-service to call options-service
service_intentions = [
  {
    name = "options-service"
    sources = [{
      name   = "market-data-service"
      action = "allow"
    }]
  }
]
```

## Key Features Implemented

### 1. Automatic Service Registration
- Services register on startup
- Health checks keep registration active
- Deregister on shutdown

### 2. Configuration Management
- Store configuration in Consul KV
- Watch for changes and hot-reload
- Environment-specific configs

### 3. Service Mesh Security
- mTLS between all services
- No plaintext communication
- Certificate rotation handled by Consul

### 4. Load Balancing
- Client-side load balancing
- Health-aware routing
- Circuit breaking support

### 5. Observability
- Prometheus metrics from Envoy
- Distributed tracing support
- Service topology visualization

## Monitoring

### Consul UI
- Service health status
- Service topology
- Key/Value store browser

### Metrics
- Envoy exposes Prometheus metrics on port 9102
- Consul exposes metrics on `/v1/agent/metrics`

### Health Checks
- HTTP health endpoints
- TCP connection checks
- Script-based checks

## Security Best Practices

1. **Enable ACLs**: Always use ACLs in production
2. **Encrypt Communication**: Use TLS for Consul agent communication
3. **Rotate Tokens**: Regularly rotate ACL tokens
4. **Limit Access**: Use least-privilege for service permissions
5. **Audit Logs**: Enable audit logging for compliance

## Troubleshooting

### Service Not Discoverable
```bash
# Check service registration
consul catalog services

# Check service health
consul health checks <service-name>
```

### Connection Refused
```bash
# Check sidecar proxy status
consul connect proxy -sidecar-for <service-name> -admin-bind localhost:19000

# Check Envoy admin interface
curl http://localhost:19000/clusters
```

### ACL Denied
```bash
# Check token permissions
consul acl token read -id <token-id>

# Update service intentions
consul intention create -allow <source> <destination>
```

## Production Considerations

1. **High Availability**: Run 3-5 Consul servers
2. **Backup**: Regular snapshots of Consul data
3. **Monitoring**: Set up alerts for cluster health
4. **Performance**: Tune Raft parameters for your workload
5. **Security**: Use Vault for certificate management

## Integration with PlatformQ Services

All PlatformQ services can leverage Consul for:

- **Service Discovery**: Find other services dynamically
- **Configuration**: Load settings from Consul KV
- **Health Checking**: Report service health
- **Secure Communication**: mTLS via Connect
- **Load Balancing**: Distribute requests across instances

Example service integration:
- Market Data Service → Options Service (pricing data)
- AMM Service → Oracle Service (price feeds)
- Social Trading → Order Matching Service (copy trades) 