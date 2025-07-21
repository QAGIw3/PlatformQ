# Oracle Service

Decentralized oracle service for compute resource quality verification and measurement in the PlatformQ ecosystem.

## Overview

The Oracle Service provides trusted, real-time measurements and quality scores for:
- **Quantum Resources**: Fidelity, coherence times, error rates
- **AI Accelerators**: Performance benchmarks, thermal monitoring, power efficiency
- **Network Paths**: Latency, bandwidth, packet loss, jitter

## Architecture

### Core Components
- **Quantum Oracle**: QPU measurements and verification
- **AI Oracle**: Accelerator benchmarking and monitoring
- **Network Oracle**: Path quality and SLA compliance
- **Blockchain Integration**: On-chain oracle data feeds
- **Quality Scoring**: Aggregated quality scores with confidence intervals

### Integration Points
- Apache Ignite for distributed caching
- Elasticsearch for measurement history
- Apache Pulsar for event streaming
- Blockchain smart contracts for data feeds
- Consul for service discovery

## API Endpoints

### Measurements
- `POST /api/v1/measurements/quantum/fidelity` - Measure quantum gate fidelity
- `POST /api/v1/measurements/ai/benchmark` - Run AI accelerator benchmark
- `POST /api/v1/measurements/network/latency` - Measure network latency

### Quality Scores
- `POST /api/v1/quality/quantum/{qpu_id}` - Calculate quantum processor quality
- `POST /api/v1/quality/ai/{accelerator_id}` - Calculate AI accelerator quality
- `POST /api/v1/quality/network/{path_id}` - Calculate network path quality

### Verification
- `POST /api/v1/quality/verify/quantum` - Verify quantum computation results
- `POST /api/v1/quality/verify/ai-training` - Verify AI training completion
- `POST /api/v1/quality/verify/network-sla` - Verify network SLA compliance

## Running the Service

```bash
cd services/oracle-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8027
```

## Configuration

Key environment variables:
- `BLOCKCHAIN_RPC_URL`: Blockchain RPC endpoint
- `ORACLE_CONTRACT_ADDRESS`: Deployed oracle contract address
- `REQUIRE_API_KEY`: Enable API key authentication
- `MEASUREMENT_INTERVAL`: Periodic measurement interval (seconds) 