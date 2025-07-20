# Blockchain Connector Service

A high-performance microservice that provides unified blockchain connectivity and operations across multiple blockchain networks including Ethereum, Solana, Cosmos, and NEAR.

## Overview

The Blockchain Connector Service acts as an abstraction layer between PlatformQ services and various blockchain networks. It provides a consistent interface for blockchain operations while handling the complexities of different blockchain protocols, RPC endpoints, and network-specific requirements.

## Key Features

- **Multi-Chain Support**: Native support for Ethereum (EVM), Solana, Cosmos, and NEAR blockchains
- **Unified Interface**: Consistent API across different blockchain types
- **Connection Management**: Automatic connection pooling, failover, and load balancing
- **Chain Information**: Real-time chain status, block information, and network metrics
- **Account Management**: Balance queries, nonce tracking, and account validation
- **Transaction Support**: Transaction preparation, gas estimation, and broadcasting
- **Event Monitoring**: Block and transaction event streaming via Pulsar
- **High Availability**: Built-in redundancy with multiple RPC endpoints per chain
- **Caching**: Intelligent caching with Apache Ignite for improved performance
- **Metrics & Monitoring**: Prometheus metrics and health endpoints

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                  Blockchain Manager                      │
├──────────────┬────────────┬───────────┬────────────────┤
│ EVM Adapter  │ Solana     │  Cosmos   │ NEAR Adapter  │
│              │ Adapter    │  Adapter  │                │
├──────────────┴────────────┴───────────┴────────────────┤
│           Connection Pool & Load Balancer               │
├─────────────────────────────────────────────────────────┤
│     Caching Layer          │      Event Publisher       │
│   (Apache Ignite)          │    (Apache Pulsar)         │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Chain Operations
- `GET /api/v1/chains` - List all supported chains
- `GET /api/v1/chains/{chain_id}` - Get specific chain information
- `GET /api/v1/chains/{chain_id}/status` - Get chain status and latest block

### Account Operations
- `GET /api/v1/chains/{chain_id}/accounts/{address}/balance` - Get account balance
- `GET /api/v1/chains/{chain_id}/accounts/{address}/nonce` - Get account nonce
- `POST /api/v1/chains/{chain_id}/accounts/validate` - Validate account address

### Transaction Operations
- `POST /api/v1/chains/{chain_id}/transactions/prepare` - Prepare transaction
- `POST /api/v1/chains/{chain_id}/transactions/estimate-gas` - Estimate gas costs
- `POST /api/v1/chains/{chain_id}/transactions/broadcast` - Broadcast signed transaction
- `GET /api/v1/chains/{chain_id}/transactions/{tx_hash}` - Get transaction details

### Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics endpoint

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=blockchain-connector-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8000
ENVIRONMENT=production

# Blockchain RPC Endpoints
ETHEREUM_RPC_URLS=https://eth-mainnet.g.alchemy.com/v2/key,https://mainnet.infura.io/v3/key
SOLANA_RPC_URLS=https://api.mainnet-beta.solana.com,https://solana-api.projectserum.com
COSMOS_RPC_URLS=https://cosmos-rpc.polkachu.com,https://rpc-cosmoshub.blockapsis.com
NEAR_RPC_URLS=https://rpc.mainnet.near.org,https://near.lava.build

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_TOPIC_PREFIX=blockchain-events

# Ignite Cache Configuration
IGNITE_ENDPOINTS=ignite-node1:10800,ignite-node2:10800
IGNITE_CACHE_NAME=blockchain-cache

# Consul Configuration
CONSUL_HOST=consul
CONSUL_PORT=8500

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
```

## Dependencies

- **FastAPI**: REST API framework
- **web3.py**: Ethereum/EVM blockchain interaction
- **solana-py**: Solana blockchain interaction
- **cosmpy**: Cosmos blockchain interaction
- **py-near**: NEAR blockchain interaction
- **aiopulsar**: Event streaming
- **pyignite**: Distributed caching
- **prometheus-client**: Metrics collection
- **python-consul**: Service discovery

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t blockchain-connector-service .

# Run the container
docker run -d \
  --name blockchain-connector \
  -p 8000:8000 \
  -e ETHEREUM_RPC_URLS="your-ethereum-rpc-urls" \
  -e SOLANA_RPC_URLS="your-solana-rpc-urls" \
  -e PULSAR_SERVICE_URL="pulsar://pulsar:6650" \
  -e IGNITE_ENDPOINTS="ignite:10800" \
  blockchain-connector-service
```

### Using Docker Compose

```yaml
services:
  blockchain-connector:
    build: ./services/blockchain/blockchain-connector-service
    ports:
      - "8000:8000"
    environment:
      - ETHEREUM_RPC_URLS=${ETHEREUM_RPC_URLS}
      - SOLANA_RPC_URLS=${SOLANA_RPC_URLS}
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
      - IGNITE_ENDPOINTS=ignite:10800
    depends_on:
      - pulsar
      - ignite
      - consul
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ETHEREUM_RPC_URLS="your-ethereum-rpc-urls"
export SOLANA_RPC_URLS="your-solana-rpc-urls"

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Testing

```bash
# Run unit tests
pytest tests/unit -v

# Run integration tests
pytest tests/integration -v

# Run with coverage
pytest --cov=app --cov-report=html

# Load testing
locust -f tests/load/locustfile.py --host http://localhost:8000
```

## Monitoring

### Health Checks

The service provides health checks at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "chains": {
    "ethereum": "connected",
    "solana": "connected",
    "cosmos": "connected",
    "near": "connected"
  },
  "dependencies": {
    "pulsar": "connected",
    "ignite": "connected",
    "consul": "registered"
  }
}
```

### Metrics

Prometheus metrics are available at `/metrics`:

- `blockchain_rpc_requests_total` - Total RPC requests by chain and method
- `blockchain_rpc_request_duration_seconds` - RPC request duration histogram
- `blockchain_rpc_errors_total` - RPC errors by chain and error type
- `blockchain_connection_pool_size` - Active connections per chain
- `blockchain_cache_hits_total` - Cache hit rate
- `blockchain_block_height` - Latest block height per chain

### Logging

Structured JSON logging with correlation IDs:

```json
{
  "timestamp": "2024-01-10T10:00:00Z",
  "level": "INFO",
  "service": "blockchain-connector-service",
  "trace_id": "abc123",
  "message": "RPC request completed",
  "chain": "ethereum",
  "method": "eth_getBalance",
  "duration_ms": 45
}
```

## Integration with Other Services

### Transaction Processor Service
- Provides blockchain connectivity for transaction submission
- Supplies gas price information and nonce management

### Event Monitoring Service
- Publishes block and transaction events
- Provides chain status updates

### Analytics Service
- Supplies blockchain data for analytics
- Provides real-time metrics

### Example Integration

```python
import httpx

async def get_ethereum_balance(address: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"http://blockchain-connector:8000/api/v1/chains/ethereum/accounts/{address}/balance"
        )
        return response.json()
```

## Troubleshooting

### Common Issues

1. **RPC Connection Failures**
   - Check RPC endpoint URLs and API keys
   - Verify network connectivity
   - Check rate limits on RPC providers

2. **High Latency**
   - Enable caching for frequently accessed data
   - Use multiple RPC endpoints for load balancing
   - Check Ignite cache performance

3. **Memory Usage**
   - Adjust connection pool sizes
   - Configure cache eviction policies
   - Monitor event buffer sizes

### Debug Mode

Enable debug logging:

```bash
export LOG_LEVEL=DEBUG
export ENABLE_PROFILING=true
```

## Contributing

1. Follow the PlatformQ coding standards
2. Write tests for new features
3. Update API documentation
4. Submit pull requests with clear descriptions

## License

Copyright © 2024 PlatformQ. All rights reserved. 