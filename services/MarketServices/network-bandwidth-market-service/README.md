# Network Bandwidth Market Service

Real-time marketplace for trading network bandwidth, dedicated circuits, and latency guarantees on the PlatformQ Infrastructure DeFi ecosystem.

## Overview

The Network Bandwidth Market Service provides:
- **Network Path Registry**: Registration and management of network paths and circuits
- **Bandwidth Trading**: Spot and futures markets for network bandwidth
- **QoS Management**: Quality of Service classes and guarantees
- **Burst Capacity**: On-demand burst bandwidth allocation
- **Dedicated Circuits**: Reserved network paths with guaranteed performance
- **Latency Futures**: Contracts for guaranteed low-latency connections
- **Congestion Pricing**: Dynamic pricing based on network utilization

## Architecture

### Core Components

#### Path Registry
- Network path registration and discovery
- Route optimization and selection
- Path availability monitoring
- Multi-path redundancy management

#### Bandwidth Manager
- Bandwidth allocation and reservation
- QoS class enforcement
- Burst capacity management
- Usage tracking and metering

#### Circuit Manager
- Dedicated circuit provisioning
- Circuit lifecycle management
- Performance monitoring
- SLA enforcement

#### Pricing Engine
- Congestion-based dynamic pricing
- Time-of-day pricing models
- Route quality multipliers
- Burst premium calculations

### Integration Points

- **Apache Ignite**: Distributed caching for path state and bandwidth allocations
- **Apache Pulsar**: Event streaming for bandwidth events and circuit updates
- **Apache Flink**: Real-time congestion analytics and traffic prediction
- **Blockchain**: NetworkBandwidthExchange contract for tokenization
- **Elasticsearch**: Network path search and analytics
- **Vault/Consul**: Secure configuration and service discovery

## API Endpoints

### Path Management
- `POST /api/v1/paths` - Register network path
- `GET /api/v1/paths/{path_id}` - Get path details
- `PUT /api/v1/paths/{path_id}/availability` - Update path availability
- `GET /api/v1/paths/search` - Search available paths

### Bandwidth Trading
- `POST /api/v1/bandwidth/allocate` - Allocate bandwidth
- `POST /api/v1/bandwidth/release` - Release bandwidth
- `GET /api/v1/bandwidth/available` - Check available bandwidth
- `POST /api/v1/bandwidth/burst` - Request burst capacity

### Dedicated Circuits
- `POST /api/v1/circuits` - Provision dedicated circuit
- `GET /api/v1/circuits/{circuit_id}` - Get circuit details
- `PUT /api/v1/circuits/{circuit_id}/modify` - Modify circuit parameters
- `DELETE /api/v1/circuits/{circuit_id}` - Decommission circuit

### Latency Futures
- `POST /api/v1/latency/futures` - Create latency future contract
- `GET /api/v1/latency/futures/{contract_id}` - Get contract details
- `POST /api/v1/latency/futures/{contract_id}/exercise` - Exercise contract
- `GET /api/v1/latency/current` - Get current latency metrics

### Pricing
- `GET /api/v1/pricing/bandwidth` - Get bandwidth pricing
- `GET /api/v1/pricing/burst` - Get burst pricing
- `GET /api/v1/pricing/circuits` - Get circuit pricing
- `GET /api/v1/pricing/congestion` - Get congestion metrics

## Resource Types

### Bandwidth Classes
```python
class BandwidthClass(str, Enum):
    BEST_EFFORT = "best_effort"      # No guarantees
    BRONZE = "bronze"                # Basic QoS
    SILVER = "silver"                # Enhanced QoS
    GOLD = "gold"                    # Premium QoS
    PLATINUM = "platinum"            # Ultra-low latency
```

### Network Paths
```python
class NetworkPath:
    path_id: str
    source: str
    destination: str
    hops: List[str]
    latency_ms: float
    available_bandwidth_mbps: int
    reliability_score: float
```

### QoS Parameters
```python
class QoSParameters:
    bandwidth_mbps: int
    latency_ms: float
    jitter_ms: float
    packet_loss_rate: float
    priority: int
```

## Pricing Models

### Base Bandwidth Pricing
```
price = base_rate * bandwidth_mbps * duration_hours * qos_multiplier
```

### Burst Pricing
```
burst_price = burst_rate * burst_bandwidth_mbps * burst_duration_minutes * urgency_multiplier
```

### Congestion Pricing
```
congestion_multiplier = 1 + (utilization / capacity) ^ 2
final_price = base_price * congestion_multiplier
```

### Latency Premium
```
latency_premium = base_price * (guaranteed_latency / actual_latency) ^ 1.5
```

## Background Tasks

### Congestion Monitoring
- Real-time network utilization tracking
- Congestion prediction using ML models
- Dynamic pricing adjustments
- Automatic burst allocation

### Circuit Health Monitoring
- Continuous performance monitoring
- SLA violation detection
- Automatic failover triggers
- Quality degradation alerts

### Path Optimization
- Route efficiency analysis
- Alternative path discovery
- Load balancing recommendations
- Cost optimization suggestions

### Settlement Processing
- Bandwidth usage reconciliation
- Billing calculations
- SLA credit processing
- Contract settlement

## Security

### Access Control
- Role-based bandwidth allocation limits
- Circuit provisioning authorization
- API rate limiting per tier
- Resource quota enforcement

### Network Isolation
- VLAN segregation for dedicated circuits
- Traffic encryption options
- DDoS protection integration
- Firewall rule management

## Configuration

```python
# Network Path Configuration
MAX_PATH_HOPS = 10
PATH_DISCOVERY_INTERVAL = 300  # seconds
PATH_RELIABILITY_THRESHOLD = 0.95

# Bandwidth Configuration
MIN_BANDWIDTH_ALLOCATION = 10  # Mbps
MAX_BANDWIDTH_ALLOCATION = 10000  # Mbps
BURST_MULTIPLIER = 2.0
BURST_DURATION_LIMIT = 3600  # seconds

# Circuit Configuration
CIRCUIT_SETUP_TIME = 300  # seconds
CIRCUIT_MIN_DURATION = 3600  # seconds
CIRCUIT_MAX_DURATION = 2592000  # 30 days

# Pricing Configuration
BASE_BANDWIDTH_RATE = 0.001  # per Mbps per hour
BURST_RATE_MULTIPLIER = 3.0
CONGESTION_THRESHOLD = 0.8
LATENCY_PREMIUM_FACTOR = 1.5
```

## Monitoring and Metrics

### Key Metrics
- Total bandwidth allocated
- Circuit utilization rates
- Congestion events
- Latency SLA violations
- Revenue per path
- Burst request frequency

### Alerts
- Network congestion warnings
- Circuit failure notifications
- SLA breach alerts
- Capacity exhaustion warnings

## Development

### Setup
```bash
cd services/MarketServices/network-bandwidth-market-service
pip install -r requirements.txt
```

### Running
```bash
uvicorn app.main:app --reload --port 8026
```

### Testing
```bash
pytest tests/
```

## Integration Examples

### Allocate Bandwidth
```python
response = requests.post(
    "http://localhost:8026/api/v1/bandwidth/allocate",
    json={
        "path_id": "path_123",
        "bandwidth_mbps": 1000,
        "qos_class": "gold",
        "duration_hours": 24,
        "start_time": "2024-01-15T10:00:00Z"
    }
)
```

### Provision Dedicated Circuit
```python
response = requests.post(
    "http://localhost:8026/api/v1/circuits",
    json={
        "source": "datacenter_a",
        "destination": "datacenter_b",
        "bandwidth_mbps": 10000,
        "latency_requirement_ms": 5,
        "redundancy": true,
        "duration_days": 30
    }
)
``` 