# Transaction Processor Service

A high-performance microservice that manages the complete lifecycle of blockchain transactions including validation, signing, broadcasting, monitoring, and confirmation across multiple blockchain networks.

## Overview

The Transaction Processor Service handles all aspects of blockchain transaction management for PlatformQ. It provides reliable transaction submission with automatic retry logic, nonce management, gas optimization, and comprehensive monitoring of transaction status across different blockchain networks.

## Key Features

- **Transaction Lifecycle Management**: Complete handling from creation to confirmation
- **Multi-Chain Support**: Unified transaction processing for Ethereum, Solana, Cosmos, and NEAR
- **Nonce Management**: Automatic nonce tracking and gap prevention for account-based chains
- **Gas Optimization**: Dynamic gas price adjustment and optimization strategies
- **Retry Logic**: Intelligent retry with exponential backoff and replacement transactions
- **Queue Management**: Priority-based transaction queuing with Apache Pulsar
- **State Tracking**: Real-time transaction status monitoring and updates
- **Batch Processing**: Support for batching multiple operations in a single transaction
- **MEV Protection**: Flashbots integration for private mempool submission
- **High Availability**: Distributed processing with fault tolerance
- **Comprehensive Monitoring**: Detailed metrics and transaction analytics

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│               Transaction Processor Core                 │
├─────────────┬──────────────┬───────────────────────────┤
│   Validator │ Nonce Manager│    Status Monitor         │
├─────────────┴──────────────┴───────────────────────────┤
│          Transaction Queue (Apache Pulsar)              │
├─────────────────────────────────────────────────────────┤
│  State Store    │  Key Management  │  Gas Optimizer    │
│ (Apache Ignite) │  Service Client  │  Service Client   │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Transaction Management
- `POST /api/v1/transactions` - Submit a new transaction
- `GET /api/v1/transactions/{tx_id}` - Get transaction details
- `GET /api/v1/transactions/{tx_id}/status` - Get transaction status
- `PUT /api/v1/transactions/{tx_id}/cancel` - Cancel pending transaction
- `POST /api/v1/transactions/{tx_id}/retry` - Retry failed transaction
- `POST /api/v1/transactions/{tx_id}/speed-up` - Speed up pending transaction

### Batch Operations
- `POST /api/v1/batches` - Submit batch transaction
- `GET /api/v1/batches/{batch_id}` - Get batch status
- `GET /api/v1/batches/{batch_id}/transactions` - List transactions in batch

### Queue Management
- `GET /api/v1/queue/status` - Get queue statistics
- `GET /api/v1/queue/pending` - List pending transactions
- `POST /api/v1/queue/pause` - Pause transaction processing
- `POST /api/v1/queue/resume` - Resume transaction processing

### Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics endpoint
- `GET /api/v1/stats` - Transaction processing statistics

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=transaction-processor-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8001
ENVIRONMENT=production

# Processing Configuration
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=30
CONFIRMATION_BLOCKS=12
BATCH_SIZE=100
PROCESSING_INTERVAL_MS=1000

# External Services
BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
KEY_MANAGEMENT_URL=http://key-management:8002
GAS_OPTIMIZATION_URL=http://gas-optimization:8003

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_TRANSACTION_TOPIC=persistent://platformq/blockchain/transactions
PULSAR_STATUS_TOPIC=persistent://platformq/blockchain/tx-status

# Ignite Cache Configuration
IGNITE_ENDPOINTS=ignite-node1:10800,ignite-node2:10800
IGNITE_TX_CACHE=transaction-cache
IGNITE_NONCE_CACHE=nonce-cache

# Consul Configuration
CONSUL_HOST=consul
CONSUL_PORT=8500

# Vault Configuration
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=${VAULT_TOKEN}

# MEV Protection (Optional)
ENABLE_FLASHBOTS=true
FLASHBOTS_RPC_URL=https://rpc.flashbots.net

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
```

## Dependencies

- **FastAPI**: REST API framework
- **aiopulsar**: Message queue for transaction processing
- **pyignite**: Distributed state storage
- **httpx**: Async HTTP client for service communication
- **prometheus-client**: Metrics collection
- **python-consul**: Service discovery
- **tenacity**: Retry logic implementation
- **structlog**: Structured logging

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t transaction-processor-service .

# Run the container
docker run -d \
  --name transaction-processor \
  -p 8001:8001 \
  -e BLOCKCHAIN_CONNECTOR_URL="http://blockchain-connector:8000" \
  -e KEY_MANAGEMENT_URL="http://key-management:8002" \
  -e PULSAR_SERVICE_URL="pulsar://pulsar:6650" \
  -e IGNITE_ENDPOINTS="ignite:10800" \
  transaction-processor-service
```

### Using Docker Compose

```yaml
services:
  transaction-processor:
    build: ./services/blockchain/transaction-processor-service
    ports:
      - "8001:8001"
    environment:
      - BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
      - KEY_MANAGEMENT_URL=http://key-management:8002
      - GAS_OPTIMIZATION_URL=http://gas-optimization:8003
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
      - IGNITE_ENDPOINTS=ignite:10800
    depends_on:
      - blockchain-connector
      - key-management
      - gas-optimization
      - pulsar
      - ignite
      - consul
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export BLOCKCHAIN_CONNECTOR_URL="http://localhost:8000"
export KEY_MANAGEMENT_URL="http://localhost:8002"
export PULSAR_SERVICE_URL="pulsar://localhost:6650"

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
```

## Testing

```bash
# Run unit tests
pytest tests/unit -v

# Run integration tests
pytest tests/integration -v

# Run with coverage
pytest --cov=app --cov-report=html

# Stress testing
python tests/stress/transaction_load_test.py --tps 100 --duration 300
```

## Transaction Flow

1. **Submission**: Transaction request received via API
2. **Validation**: Transaction parameters validated
3. **Nonce Assignment**: Nonce allocated for account-based chains
4. **Gas Estimation**: Gas price and limit calculated
5. **Signing**: Transaction signed via Key Management Service
6. **Queue**: Transaction added to processing queue
7. **Broadcasting**: Transaction sent to blockchain
8. **Monitoring**: Transaction status tracked until confirmation
9. **Notification**: Status updates published via Pulsar

## Monitoring

### Health Checks

The service provides comprehensive health checks at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "queue": {
    "pending": 42,
    "processing": 5,
    "failed": 2
  },
  "processors": {
    "active": 10,
    "idle": 5
  },
  "dependencies": {
    "blockchain_connector": "connected",
    "key_management": "connected",
    "pulsar": "connected",
    "ignite": "connected"
  }
}
```

### Metrics

Prometheus metrics available at `/metrics`:

- `tx_processor_transactions_total` - Total transactions by chain and status
- `tx_processor_processing_duration_seconds` - Transaction processing time
- `tx_processor_queue_size` - Current queue size by priority
- `tx_processor_nonce_gaps_total` - Nonce gap occurrences
- `tx_processor_retries_total` - Transaction retry attempts
- `tx_processor_gas_price_wei` - Current gas price by chain

### Transaction Status Updates

Real-time status updates published to Pulsar:

```json
{
  "tx_id": "550e8400-e29b-41d4-a716-446655440000",
  "chain": "ethereum",
  "status": "confirmed",
  "tx_hash": "0x123...",
  "block_number": 18500000,
  "confirmations": 12,
  "gas_used": "21000",
  "timestamp": "2024-01-10T10:00:00Z"
}
```

## Error Handling

### Retry Strategies

1. **Network Errors**: Exponential backoff with jitter
2. **Nonce Issues**: Automatic nonce recalculation
3. **Gas Price Spikes**: Dynamic gas adjustment
4. **Chain Congestion**: Priority-based reordering

### Transaction States

- `pending` - Awaiting processing
- `signing` - Being signed by key management
- `broadcasting` - Submitted to network
- `confirming` - In mempool/awaiting confirmation
- `confirmed` - Successfully confirmed
- `failed` - Permanently failed
- `cancelled` - Cancelled by user

## Security Considerations

1. **Transaction Validation**: All transactions validated before processing
2. **Rate Limiting**: Per-account rate limits enforced
3. **Access Control**: API key authentication required
4. **Secure Communication**: TLS for all service-to-service calls
5. **Audit Logging**: Complete transaction history maintained

## Performance Optimization

1. **Batch Processing**: Group similar transactions
2. **Parallel Processing**: Multiple processor workers
3. **Caching**: Nonce and gas price caching
4. **Connection Pooling**: Reuse blockchain connections
5. **Priority Queues**: High-value transactions prioritized

## Troubleshooting

### Common Issues

1. **Stuck Transactions**
   - Check nonce sequence
   - Verify gas price adequacy
   - Review transaction logs

2. **High Failure Rate**
   - Monitor blockchain network status
   - Check account balances
   - Verify key management service

3. **Queue Backlog**
   - Scale processor workers
   - Adjust batch sizes
   - Check downstream services

### Debug Commands

```bash
# View queue status
curl http://localhost:8001/api/v1/queue/status

# Check specific transaction
curl http://localhost:8001/api/v1/transactions/{tx_id}

# Export metrics
curl http://localhost:8001/metrics | grep tx_processor
```

## Contributing

1. Follow PlatformQ coding standards
2. Add tests for new transaction types
3. Update documentation
4. Performance test changes

## License

Copyright © 2024 PlatformQ. All rights reserved. 