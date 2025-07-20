# Compute Market Service

## Overview

The Compute Market Service manages the decentralized compute resource marketplace within PlatformQ, enabling users to buy, sell, and trade computational resources such as GPU/CPU time, storage, and specialized hardware access. It provides dynamic pricing, resource allocation, quality of service guarantees, and automated settlement.

## Architecture

### Key Components

1. **Resource Manager**: Tracks available compute resources and their specifications
2. **Pricing Engine**: Dynamic pricing based on supply, demand, and resource characteristics
3. **Allocation Manager**: Handles resource reservation and allocation
4. **QoS Monitor**: Ensures quality of service and SLA compliance
5. **Settlement Engine**: Automated billing and payment processing
6. **Burst Manager**: Handles burst capacity and spot instances
7. **Scheduler**: Optimizes resource allocation across the network

### Technology Stack

- **FastAPI**: REST API framework
- **Apache Ignite**: Distributed state for resource inventory
- **Apache Pulsar**: Event streaming for resource events
- **Apache Flink**: Real-time analytics for pricing and utilization
- **Kubernetes**: Resource orchestration
- **Prometheus**: Metrics and monitoring

## Features

- **Resource Types**: GPU, CPU, Storage, Bandwidth, Specialized Hardware
- **Dynamic Pricing**: Market-based pricing with spot and reserved instances
- **Quality Tiers**: Bronze, Silver, Gold, Platinum with different SLAs
- **Auto-scaling**: Automatic resource scaling based on demand
- **Resource Pooling**: Aggregate resources from multiple providers
- **Futures Contracts**: Lock in compute prices for future use
- **Burst Capacity**: Handle temporary spikes in demand
- **Multi-region**: Global resource allocation

## API Endpoints

### Resource Discovery
- `GET /api/v1/resources` - List available resources
- `GET /api/v1/resources/{resource_id}` - Get resource details
- `GET /api/v1/resources/types` - List resource types
- `GET /api/v1/resources/regions` - List available regions

### Pricing and Quotes
- `GET /api/v1/pricing/current` - Get current spot prices
- `POST /api/v1/pricing/quote` - Get price quote for resources
- `GET /api/v1/pricing/history` - Historical pricing data

### Resource Allocation
- `POST /api/v1/allocations` - Request resource allocation
- `GET /api/v1/allocations/{allocation_id}` - Get allocation status
- `PUT /api/v1/allocations/{allocation_id}` - Modify allocation
- `DELETE /api/v1/allocations/{allocation_id}` - Release resources

### Marketplace
- `POST /api/v1/market/orders` - Place buy/sell order
- `GET /api/v1/market/orders/{order_id}` - Get order details
- `GET /api/v1/market/orderbook` - Get market orderbook
- `GET /api/v1/market/trades` - Recent trades

## Resource Types

### GPU Resources
```json
{
  "type": "GPU",
  "model": "NVIDIA A100",
  "memory": "80GB",
  "compute_capability": 8.0,
  "tensor_cores": true
}
```

### CPU Resources
```json
{
  "type": "CPU",
  "model": "AMD EPYC 7763",
  "cores": 64,
  "threads": 128,
  "base_clock": "2.45GHz"
}
```

### Storage Resources
```json
{
  "type": "STORAGE",
  "class": "NVMe SSD",
  "capacity": "2TB",
  "iops": 1000000,
  "bandwidth": "7GB/s"
}
```

## Pricing Models

### Spot Pricing
- Real-time market-based pricing
- Can be interrupted with notice
- Best for fault-tolerant workloads

### Reserved Instances
- Fixed pricing for committed usage
- Guaranteed availability
- Discounts for longer commitments

### Burst Pricing
- Premium pricing for immediate access
- No commitment required
- Automatic scaling

## Quality of Service

### Service Tiers

| Tier | Availability | Support | Price Multiplier |
|------|-------------|---------|------------------|
| Bronze | 99.0% | Best effort | 1.0x |
| Silver | 99.5% | 8x5 | 1.2x |
| Gold | 99.9% | 24x7 | 1.5x |
| Platinum | 99.99% | Dedicated | 2.0x |

### SLA Guarantees
- Uptime commitments
- Performance benchmarks
- Network latency limits
- Data locality options

## Configuration

```python
# Resource Types
SUPPORTED_RESOURCE_TYPES = ["GPU", "CPU", "STORAGE", "BANDWIDTH", "FPGA"]

# Pricing Configuration
BASE_GPU_PRICE = 1.50  # per hour
BASE_CPU_PRICE = 0.10  # per core-hour
SPOT_DISCOUNT = 0.7
BURST_PREMIUM = 1.5

# Quality of Service
DEFAULT_QOS_TIER = "silver"
SLA_CHECK_INTERVAL = 60  # seconds

# Allocation Limits
MAX_ALLOCATION_DURATION = 720  # hours
MIN_ALLOCATION_DURATION = 1  # hour
```

## Integration

### Dependencies
- **Trading Core Service**: For futures trading
- **Auth Service**: User authentication
- **Billing Service**: Payment processing
- **Resource Providers**: Physical resource APIs
- **Monitoring Service**: Resource utilization

### Event Streams
- Resource availability updates
- Price change events
- Allocation status changes
- SLA violation alerts

## Monitoring

Prometheus metrics at `/metrics`:

- `compute_resources_available`: Available resources by type
- `compute_resources_allocated`: Allocated resources
- `compute_utilization_percent`: Resource utilization
- `compute_pricing_spot`: Current spot prices
- `compute_allocations_total`: Total allocations
- `compute_sla_violations`: SLA breach count

## Development

### Project Structure

```
compute-market-service/
├── app/
│   ├── api/           # REST endpoints
│   ├── core/          # Core business logic
│   ├── models/        # Data models
│   ├── pricing/       # Pricing algorithms
│   ├── allocation/    # Resource allocation
│   ├── qos/          # Quality monitoring
│   ├── config.py     # Configuration
│   └── main.py       # FastAPI app
├── scripts/          # Utility scripts
├── tests/           # Tests
└── requirements.in  # Dependencies
```

### Testing

```bash
# Unit tests
pytest tests/unit

# Integration tests
pytest tests/integration

# Load testing
locust -f tests/load/locustfile.py
```

## Algorithms

### Dynamic Pricing
- Supply-demand curves
- Time-of-day adjustments
- Regional price differences
- Provider reputation factors

### Resource Matching
- Best-fit allocation
- Geographic optimization
- Performance matching
- Cost optimization

## Security

- **Authentication**: JWT tokens
- **Authorization**: Resource quotas
- **Encryption**: TLS for data in transit
- **Isolation**: Resource sandboxing
- **Audit**: All transactions logged

## Provider Integration

### Becoming a Provider
1. Register provider account
2. Install resource agent
3. Configure resource specs
4. Set pricing preferences
5. Start accepting jobs

### Provider Requirements
- Minimum uptime: 95%
- Network bandwidth: 1Gbps+
- Security compliance
- Regular benchmarking

## Future Roadmap

- Federated learning support
- Confidential computing
- Quantum resource integration
- Carbon-neutral options
- Cross-chain settlements

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines. 