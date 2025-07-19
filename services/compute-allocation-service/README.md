# Compute Allocation Service

The Compute Allocation Service provides centralized compute resource allocation across multiple cloud providers and on-premise infrastructure. It implements intelligent provider selection, cost optimization, and unified resource management.

## Architecture

The service has been refactored with the following improvements:

- **Shared Compute Library** - Reusable models, providers, and cost management
- **Provider Abstraction** - Unified interface for all compute providers
- **Configuration Management** - Dynamic configuration via Consul, secrets via Vault
- **Event-Driven** - Asynchronous operations with Apache Pulsar
- **Cost Optimization** - Real-time pricing and budget management
- **Multi-Tenant** - Tenant isolation and resource quotas

## Features

### Multi-Provider Support
- **AWS EC2** - Full EC2 instance management with spot/reserved pricing
- **CloudStack** - On-premise cloud infrastructure
- **Kubernetes** - Container-based workloads
- **Rackspace** - Additional cloud capacity
- **Extensible** - Easy to add new providers

### Intelligent Allocation
- **Strategy-Based** - Cost-optimized, performance-optimized, or balanced
- **Availability Checking** - Real-time capacity verification
- **Best-Fit Selection** - Automatic provider and instance type selection
- **Multi-Region** - Cross-region resource allocation
- **Fallback Options** - Alternative suggestions when primary fails

### Cost Management
- **Real-Time Pricing** - Current pricing from all providers
- **Budget Enforcement** - Per-tenant spending limits
- **Cost Forecasting** - Predictive cost analysis
- **Optimization Suggestions** - Recommendations for cost savings
- **Usage Tracking** - Detailed resource consumption metrics

### Resource Management
- **Lifecycle Management** - Provision, resize, and terminate
- **Health Monitoring** - Continuous resource health checks
- **Auto-Cleanup** - Expired resource termination
- **Modification Support** - Extend duration or scale resources
- **Access Control** - Secure credential management

## API Endpoints

### Resource Allocation
- `POST /api/v1/allocations` - Allocate compute resources
- `GET /api/v1/allocations/{allocation_id}` - Get allocation details
- `PUT /api/v1/allocations/{allocation_id}` - Modify allocation
- `DELETE /api/v1/allocations/{allocation_id}` - Release resources
- `GET /api/v1/allocations` - List allocations

### Cost Analysis
- `GET /api/v1/costs/estimate` - Estimate costs for requirements
- `GET /api/v1/metrics/allocations` - Get allocation metrics

### Provider Management
- `GET /api/v1/providers/capabilities` - Get provider capabilities

### Monitoring
- `GET /health` - Health check
- `GET /metrics` - Prometheus metrics

## Request/Response Models

### Allocation Request
```json
{
  "workload_type": "ml-training",
  "workload_id": "model-123",
  "requirements": {
    "cpu_cores": 8,
    "memory_gb": 32,
    "gpu_count": 1,
    "gpu_type": "nvidia-v100",
    "storage_gb": 100,
    "regions": ["us-east-1", "us-west-2"]
  },
  "strategy": "COST_OPTIMIZED",
  "duration_hours": 24,
  "pricing_preferences": ["SPOT", "ON_DEMAND"],
  "tags": {
    "project": "ml-research",
    "team": "data-science"
  }
}
```

### Allocation Response
```json
{
  "success": true,
  "allocation": {
    "allocation_id": "alloc-123456",
    "provider": "AWS",
    "region": "us-east-1",
    "instance_type": "p3.2xlarge",
    "status": "ACTIVE",
    "cost_per_hour": 3.06,
    "access_details": {
      "instance_id": "i-0a1b2c3d",
      "public_ip": "54.123.45.67",
      "dns_name": "ec2-54-123-45-67.compute-1.amazonaws.com"
    }
  }
}
```

## Configuration

### Environment Variables
```bash
# Service Configuration
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8000

# Consul Configuration
CONSUL_HOST=consul
CONSUL_PORT=8500
CONSUL_TOKEN=<consul-token>

# Vault Configuration
VAULT_ENABLED=true
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<vault-token>
```

### Consul Configuration Structure
```
platformq/compute-allocation-service/
├── compute_providers/
│   ├── aws/
│   │   ├── enabled: true
│   │   ├── regions: ["us-east-1", "us-west-2"]
│   │   └── instance_types: {...}
│   ├── cloudstack/
│   │   ├── enabled: true
│   │   ├── api_url: "http://cloudstack:8080/client/api"
│   │   └── zone_id: "1"
│   └── kubernetes/
│       ├── enabled: true
│       └── namespace: "platformq"
└── tenant_budgets/
    └── tenant-123/
        ├── monthly_limit: 10000
        └── alert_thresholds: [0.5, 0.75, 0.9]
```

### Vault Secret Structure
```
secret/
├── providers/
│   ├── aws/credentials
│   │   ├── access_key: "..."
│   │   └── secret_key: "..."
│   └── cloudstack/credentials
│       ├── api_key: "..."
│       └── secret_key: "..."
└── database/
    └── postgres/credentials
        ├── username: "..."
        └── password: "..."
```

## Provider Integration

### Adding a New Provider
1. Implement the `ResourceProvider` interface
2. Add provider-specific configuration
3. Register with `ProviderRegistry`
4. Add credentials to Vault
5. Enable in Consul configuration

Example provider implementation:
```python
from platformq_compute_common.providers import ResourceProvider

class MyProvider(ResourceProvider):
    async def get_capabilities(self) -> ProviderCapabilities:
        # Return provider capabilities
        
    async def check_availability(self, requirements, region):
        # Check if resources are available
        
    async def allocate(self, allocation):
        # Allocate resources
        
    async def deallocate(self, allocation):
        # Release resources
```

## Development

### Installation
```bash
pip install -r requirements.txt
```

### Running Locally
```bash
# Start dependencies
docker-compose up -d consul vault

# Configure Consul/Vault
python scripts/setup_config.py

# Run service
uvicorn app.main:app --reload --port 8000
```

### Running Tests
```bash
pytest tests/ -v
```

## Deployment

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: compute-allocation-service
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: service
        image: platformq/compute-allocation-service:latest
        env:
        - name: CONSUL_HOST
          value: consul.default.svc.cluster.local
        - name: VAULT_ADDR
          value: http://vault.default.svc.cluster.local:8200
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
```

## Monitoring

### Metrics
The service exposes Prometheus metrics:
- `compute_allocations_total` - Total allocation requests
- `compute_allocation_duration_seconds` - Allocation latency
- `compute_allocations_active` - Currently active allocations

### Logging
Structured JSON logging with correlation IDs for request tracing.

### Alerts
Example Prometheus alerts:
```yaml
- alert: HighAllocationFailureRate
  expr: rate(compute_allocations_total{status="failure"}[5m]) > 0.1
  annotations:
    summary: High allocation failure rate
    
- alert: ProviderUnavailable
  expr: up{job="compute-allocation-service"} == 0
  annotations:
    summary: Compute allocation service is down
```

## Security

- **Authentication** - JWT tokens via shared auth service
- **Authorization** - Tenant-based access control
- **Secrets** - All credentials stored in Vault
- **Encryption** - TLS for all external communications
- **Audit** - All allocation actions logged

## Performance

- **Caching** - Provider capabilities cached
- **Connection Pooling** - Reused HTTP connections
- **Async Operations** - Non-blocking I/O
- **Health Checks** - Continuous provider monitoring
- **Circuit Breakers** - Fault tolerance for provider failures 