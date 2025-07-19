# PlatformQ CloudStack Integration

## Overview

This implementation provides a seamless integration of Apache CloudStack into PlatformQ, creating a unified compute management layer for the compute futures physical settlement system. CloudStack manages heterogeneous infrastructure including partner capacity, on-premise resources, and edge locations through a single API.

## Architecture

The integration consists of several key components:

### 1. **CloudStack Infrastructure Layer**
- **Management Server**: Central control plane for all compute resources
- **Multiple Zones**: Partner zones (Rackspace, Equinix), Edge zones, On-premise zones
- **API Gateway**: Kong-based gateway for secure, rate-limited access
- **Usage Tracking**: Built-in metering for billing and SLA monitoring

### 2. **Enhanced Provisioning Service**
- **CloudStack Provider**: Unified provider implementation replacing individual provider code
- **Automatic Zone Selection**: Intelligent placement based on requirements and costs
- **Resource Abstraction**: Maps compute futures requirements to CloudStack resources
- **Failover Orchestration**: Automatic failover between zones

### 3. **Real-time Stream Processing**
- **Flink Jobs**: Process settlement events, monitor SLAs, calculate penalties
- **Pulsar Integration**: Event streaming for all compute events
- **SeaTunnel Pipelines**: Data synchronization between systems
- **CDC Integration**: Real-time database change capture

### 4. **Data Integration Layer**
- **Multi-Store Sync**: Pulsar → Ignite/Cassandra/Elasticsearch pipelines
- **Data Quality Monitoring**: Automated quality checks
- **Transformation Templates**: Reusable data transformation patterns

## Key Features

### Unified Compute Management
- Single API for all compute resources (cloud, on-premise, edge, partner)
- Consistent provisioning across heterogeneous infrastructure
- Automated resource discovery and capacity planning

### Physical Settlement Automation
- Automatic provisioning when futures contracts expire
- SLA monitoring with real-time metrics
- Automatic failover with priority-based provider selection
- Penalty calculation and enforcement

### Partner Integration
- Wholesale capacity management through CloudStack zones
- Usage-based billing and settlement
- Multi-tenant isolation with VPC support

### Advanced Capabilities
- GPU passthrough for ML workloads
- Network isolation per settlement
- Resource tagging for tracking and billing
- Built-in monitoring and metrics export

## Deployment

### Prerequisites
- Docker and Docker Compose
- Java 11+ and Maven (for Flink jobs)
- 16GB+ RAM, 8+ CPU cores
- 100GB+ storage

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/platformq/platformq.git
   cd platformq
   ```

2. **Run the deployment script**
   ```bash
   ./deploy-cloudstack-integration.sh
   ```

3. **Configure CloudStack** (after initial setup)
   ```bash
   python3 configure-cloudstack.py
   ```

4. **Test the integration**
   ```bash
   python3 examples/compute_futures_settlement_demo.py
   ```

## Configuration

### Environment Variables
```bash
# CloudStack
CLOUDSTACK_API_URL=http://localhost:8080/client/api
CLOUDSTACK_API_KEY=your-api-key
CLOUDSTACK_SECRET_KEY=your-secret-key

# Databases
POSTGRES_PASSWORD=your-password
CASSANDRA_PASSWORD=your-password
ELASTIC_PASSWORD=your-password
```

### CloudStack Zones

Configure zones in `infra/cloudstack/config/zones.yml`:

```yaml
zones:
  - name: partner-rackspace
    type: partner
    partner_id: rackspace
    location: us-central
    capacity:
      cpu: 10000
      gpu: 100
      memory_gb: 50000
      
  - name: edge-nyc
    type: edge
    location: us-east-edge
    capacity:
      cpu: 500
      memory_gb: 2000
```

### SeaTunnel Jobs

Configure data pipelines in `infra/seatunnel/config/`:

- `compute-futures-sync.conf`: Syncs compute events across systems
- `platform-cdc-sync.conf`: Real-time database synchronization
- Custom transformation templates

## API Usage

### Provision Compute Resources
```python
# Called automatically during settlement
response = requests.post(
    "http://localhost:8000/api/v1/compute/provision",
    json={
        "settlement_id": "CS_123",
        "resource_type": "gpu",
        "quantity": "10",
        "duration_hours": 24,
        "start_time": "2024-01-15T10:00:00Z",
        "provider_id": "cloudstack",
        "buyer_id": "buyer_123"
    }
)
```

### Check SLA Metrics
```python
response = requests.get(
    "http://localhost:8000/api/v1/metrics/compute/CS_123"
)
metrics = response.json()
# {
#     "uptime_percent": 99.95,
#     "latency_ms": 12,
#     "performance_score": 0.98,
#     "error_rate": 0.001
# }
```

## Monitoring

### Dashboards
- **CloudStack UI**: http://localhost:8080/client/
- **SeaTunnel Web**: http://localhost:5071
- **Flink Web UI**: http://localhost:8081
- **Grafana**: http://localhost:3000

### Key Metrics
- Settlement success rate
- Resource utilization by zone
- SLA compliance percentage
- Failover frequency
- Cost per compute hour

### Alerts
- SLA violations
- Capacity exhaustion
- Failed provisioning
- Network issues

## Security

### Network Security
- Isolated VPCs per settlement
- Firewall rules per tenant
- Encrypted API communication

### Access Control
- API key authentication
- Role-based access control
- Audit logging

### Data Protection
- Encrypted storage
- Secure credentials management
- Compliance with data regulations

## Troubleshooting

### Common Issues

1. **CloudStack not starting**
   ```bash
   docker logs cloudstack-management
   # Check database connectivity
   ```

2. **Provisioning failures**
   ```bash
   # Check zone capacity
   curl http://localhost:8080/client/api?command=listCapacity
   ```

3. **SeaTunnel job failures**
   ```bash
   docker logs seatunnel-worker-1
   # Check connector availability
   ```

### Health Checks
```bash
# Check all services
curl http://localhost:8000/api/v1/health
curl http://localhost:8080/client/api?command=listZones
curl http://localhost:8081/jobs/overview
```

## Performance Optimization

### CloudStack Tuning
- Increase JVM heap for management server
- Configure connection pools
- Enable caching

### Flink Optimization
- Adjust parallelism based on load
- Configure checkpointing interval
- Optimize state backend

### Database Tuning
- Cassandra: Adjust compaction strategy
- Ignite: Configure memory regions
- Elasticsearch: Optimize shard allocation

## Future Enhancements

1. **Kubernetes Integration**
   - CloudStack CKS (Container Service)
   - Kubernetes as a zone type

2. **Advanced Scheduling**
   - ML-based placement optimization
   - Cost-aware scheduling
   - Carbon-aware compute

3. **Enhanced Monitoring**
   - Custom CloudStack plugins
   - Advanced analytics
   - Predictive maintenance

4. **Multi-Cloud Federation**
   - Cross-CloudStack federation
   - Global resource marketplace
   - Unified billing

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the Apache License 2.0. 