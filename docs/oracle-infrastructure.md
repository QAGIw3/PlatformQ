# Oracle Infrastructure for Compute Resources

## Overview

This document describes the comprehensive Oracle Infrastructure that provides essential data feeds for PlatformQ's DeFi protocols. The infrastructure includes four main components that work together to provide reliable, real-time data for compute resource DeFi operations.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Oracle Infrastructure Layer                          │
├──────────────────┬──────────────────┬────────────────┬─────────────────┤
│ Quality          │ Availability     │ Price          │ Performance     │
│ Aggregator       │ Monitor          │ Aggregator     │ Oracle          │
│                  │                  │                │                 │
│ • Quality scores │ • Uptime tracking│ • Multi-source │ • Benchmarking  │
│ • Confidence     │ • Downtime logs  │ • TWAP/VWAP    │ • Verification  │
│ • History        │ • SLA compliance │ • Volatility   │ • Metrics       │
└──────────────────┴──────────────────┴────────────────┴─────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          DeFi Protocols                                  │
│  Vaults | Lending | Derivatives | Insurance | AMMs | Markets            │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Quality Aggregator Oracle

**Purpose**: Aggregates quality scores from quantum, AI, and network oracles to provide unified quality metrics for DeFi protocols.

**Key Features**:
- Weighted quality score calculation based on resource type
- Component-level breakdown (fidelity, coherence, performance, etc.)
- Historical quality tracking with volatility metrics
- Confidence scoring based on data availability
- On-chain signature and submission capabilities

**API Endpoints**:
- `GET /api/v1/defi-oracles/quality/{resource_id}` - Get current quality score
- `GET /api/v1/defi-oracles/quality/{resource_id}/history` - Get historical quality data
- `POST /api/v1/defi-oracles/quality/{resource_id}/sign` - Sign quality data for on-chain use

### 2. Availability Monitor

**Purpose**: Tracks uptime/downtime for compute resources, essential for insurance claims and SLA verification.

**Key Features**:
- Continuous availability monitoring with configurable intervals
- Automatic downtime detection and recording
- SLA compliance tracking (99%+ uptime requirements)
- Resource-specific health checks (quantum, AI, network)
- On-chain downtime event recording for insurance claims

**API Endpoints**:
- `POST /api/v1/defi-oracles/availability/monitor/start` - Start monitoring a resource
- `GET /api/v1/defi-oracles/availability/{resource_id}/status` - Current availability
- `GET /api/v1/defi-oracles/availability/{resource_id}/metrics` - SLA metrics
- `GET /api/v1/defi-oracles/availability/{resource_id}/downtime` - Downtime records

### 3. Price Aggregator Oracle

**Purpose**: Combines price feeds from multiple sources to provide accurate, manipulation-resistant pricing.

**Key Features**:
- Multi-source price aggregation (markets, AMMs, external oracles)
- Outlier detection and removal using IQR method
- Time-Weighted Average Price (TWAP) calculations
- Price volatility metrics for risk assessment
- Weighted averaging with confidence scoring

**Price Sources**:
- Direct from compute markets (40-50% weight)
- AMM pools (30-35% weight)
- External oracles like Chainlink (20-30% weight)
- Centralized exchanges (optional)

**API Endpoints**:
- `GET /api/v1/defi-oracles/price/{resource_type}` - Get aggregated price
- `GET /api/v1/defi-oracles/price/{resource_type}/twap` - Get TWAP
- `GET /api/v1/defi-oracles/price/{resource_type}/volatility` - Price volatility
- `POST /api/v1/defi-oracles/price/{resource_type}/sign` - Sign price data

### 4. Performance Benchmark Oracle

**Purpose**: Verifies compute resource performance for insurance claims and quality guarantees.

**Key Features**:
- Multiple benchmark types (standard, stress, verification)
- Resource-specific performance metrics
- Performance claim verification for insurance
- Historical performance tracking with trends
- Automated performance scoring

**Benchmark Types**:
- **Standard**: Regular performance testing
- **Stress**: Maximum load testing
- **Verification**: Quick checks for claims
- **Endurance**: Long-running stability tests

**Metrics by Resource Type**:

**Quantum**:
- Gate speed (microseconds)
- Circuit depth capability
- Error rates
- Coherence time

**AI**:
- Throughput (TFLOPS)
- Inference latency
- Model accuracy
- Power efficiency (TFLOPS/W)

**Network**:
- Bandwidth (Mbps)
- Packet loss rate
- Jitter (ms)
- Connection stability

**API Endpoints**:
- `POST /api/v1/defi-oracles/performance/benchmark` - Run benchmark
- `POST /api/v1/defi-oracles/performance/verify` - Verify performance claims
- `GET /api/v1/defi-oracles/performance/{resource_id}/history` - Performance history

## Integration with DeFi Protocols

### Vaults
- Use quality scores for yield optimization strategies
- Monitor price feeds for arbitrage opportunities
- Track performance for resource selection

### Lending
- Quality scores affect interest rates
- Availability metrics for collateral valuation
- Performance verification for loan terms

### Derivatives
- Price feeds for futures and options pricing
- Volatility data for options Greeks calculation
- Quality scores for structured products

### Insurance
- Availability monitoring for downtime claims
- Quality degradation tracking for coverage triggers
- Performance verification for guarantee claims
- Price feeds for premium calculation

## Data Flow

1. **Collection**: Resource-specific oracles collect raw metrics
2. **Aggregation**: DeFi oracles aggregate and normalize data
3. **Validation**: Outlier detection and confidence scoring
4. **Signing**: Cryptographic signatures for on-chain verification
5. **Submission**: Signed data submitted to blockchain
6. **Consumption**: DeFi protocols use verified oracle data

## Security Features

1. **Multi-Source Validation**: Never rely on single data source
2. **Outlier Detection**: IQR-based outlier removal
3. **Cryptographic Signatures**: All data signed before on-chain submission
4. **Access Control**: API key authentication for write operations
5. **Rate Limiting**: Prevent spam and DOS attacks

## Configuration

Key environment variables:
```bash
# Oracle Contract Addresses
QUALITY_ORACLE_ADDRESS=0x...
AVAILABILITY_MONITOR_ADDRESS=0x...
PRICE_ORACLE_ADDRESS=0x...
PERFORMANCE_ORACLE_ADDRESS=0x...

# Market Addresses
QUANTUM_MARKET_ADDRESS=0x...
AI_MARKET_ADDRESS=0x...
NETWORK_MARKET_ADDRESS=0x...

# AMM Addresses
QUANTUM_AMM_ADDRESS=0x...
AI_AMM_ADDRESS=0x...
NETWORK_AMM_ADDRESS=0x...

# Oracle Configuration
ORACLE_SIGNING_KEY=...
AVAILABILITY_CHECK_INTERVAL=60
```

## Monitoring and Metrics

All oracles expose Prometheus metrics:
- `oracle_quality_updates_total` - Quality score updates
- `oracle_availability_checks_total` - Availability checks
- `oracle_price_updates_total` - Price updates
- `oracle_benchmark_runs_total` - Performance benchmarks
- `oracle_sla_compliance_percent` - SLA compliance tracking

## Future Enhancements

1. **Machine Learning Integration**: Predictive quality and price models
2. **Cross-Chain Oracle Bridge**: Share oracle data across chains
3. **Reputation System**: Track oracle node reliability
4. **Decentralized Oracle Network**: Multiple oracle nodes for consensus
5. **Zero-Knowledge Proofs**: Private performance verification
6. **Real-time Streaming**: WebSocket feeds for live data

## API Usage Examples

### Get All Oracle Data for a Resource
```bash
curl -X GET "http://localhost:8027/api/v1/defi-oracles/resource/101/all?resource_type=quantum"
```

Response includes quality, availability, price, and performance data in one call.

### Start Monitoring a Resource
```bash
curl -X POST "http://localhost:8027/api/v1/defi-oracles/availability/monitor/start" \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_id": 101,
    "resource_type": "quantum",
    "endpoint": "http://quantum-provider.com/health",
    "check_config": {}
  }'
```

### Run Performance Benchmark
```bash
curl -X POST "http://localhost:8027/api/v1/defi-oracles/performance/benchmark" \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "resource_id": 101,
    "resource_type": "quantum",
    "benchmark_type": "standard"
  }'
```

## Conclusion

The Oracle Infrastructure provides a robust, secure, and comprehensive data layer for PlatformQ's compute resource DeFi ecosystem. By aggregating multiple data sources, implementing security measures, and providing reliable feeds, it enables trustworthy DeFi operations on compute resources. 