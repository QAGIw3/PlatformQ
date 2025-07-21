# Cross-Chain Bridge Service

A secure and scalable microservice that enables asset transfers between different blockchain networks through a decentralized bridge architecture with multi-validator attestation and configurable security parameters.

## Overview

The Cross-Chain Bridge Service facilitates seamless asset transfers between supported blockchain networks. It implements a secure bridging protocol with multiple validators, threshold signatures, and comprehensive monitoring to ensure safe and reliable cross-chain transactions. The service supports various token standards and provides both lock-and-mint and burn-and-mint mechanisms.

## Key Features

- **Multi-Chain Support**: Bridge assets between Ethereum, BSC, Polygon, Avalanche, and more
- **Secure Architecture**: Multi-validator attestation with configurable thresholds
- **Token Standards**: Support for ERC-20, ERC-721, ERC-1155, and native tokens
- **Flexible Mechanisms**: Lock-and-mint, burn-and-mint, and liquidity pool models
- **Batch Processing**: Optimize gas costs by batching multiple transfers
- **Fee Management**: Dynamic fee calculation and distribution
- **Monitoring & Analytics**: Real-time transfer tracking and statistics
- **Disaster Recovery**: Comprehensive pause and recovery mechanisms
- **Audit Trail**: Complete history of all bridge operations
- **MEV Protection**: Private relayer network to prevent front-running
- **High Performance**: Process thousands of transfers per hour

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                  Bridge Manager Core                     │
├──────────────┬────────────┬────────────┬───────────────┤
│  Validator   │ Attestation│   Token    │    Fee        │
│  Network     │  Manager   │  Registry  │  Calculator   │
├──────────────┴────────────┴────────────┴───────────────┤
│              Bridge Implementations                      │
├──────────────┬────────────┬────────────┬───────────────┤
│     EVM      │   Solana   │   Cosmos   │    NEAR      │
│    Bridge    │   Bridge   │   Bridge   │   Bridge     │
├──────────────┴────────────┴────────────┴───────────────┤
│   Event Monitor │  State Store  │  Message Queue       │
│                 │ (Apache Ignite)│ (Apache Pulsar)      │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Transfer Operations
- `POST /api/v1/transfers` - Initiate cross-chain transfer
- `GET /api/v1/transfers/{transfer_id}` - Get transfer status
- `GET /api/v1/transfers/{transfer_id}/proof` - Get transfer proof
- `POST /api/v1/transfers/{transfer_id}/retry` - Retry failed transfer
- `POST /api/v1/transfers/{transfer_id}/refund` - Request refund

### Bridge Information
- `GET /api/v1/routes` - List available bridge routes
- `GET /api/v1/routes/{source}/{destination}` - Get specific route details
- `GET /api/v1/tokens` - List supported tokens
- `GET /api/v1/tokens/{token}/mappings` - Get token mappings across chains

### Validator Operations
- `GET /api/v1/validators` - List active validators
- `GET /api/v1/validators/{address}/performance` - Get validator metrics
- `POST /api/v1/validators/rotate` - Initiate validator rotation

### Analytics & Monitoring
- `GET /api/v1/stats` - Bridge statistics
- `GET /api/v1/stats/volume` - Transfer volume metrics
- `GET /api/v1/stats/fees` - Fee collection statistics
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics

### Admin Operations
- `POST /api/v1/admin/pause` - Pause bridge operations
- `POST /api/v1/admin/resume` - Resume bridge operations
- `POST /api/v1/admin/tokens/add` - Add new supported token
- `POST /api/v1/admin/validators/add` - Add new validator

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=cross-chain-bridge-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8004
ENVIRONMENT=production

# Bridge Configuration
MIN_VALIDATORS=3
ATTESTATION_THRESHOLD=66  # Percentage
TRANSFER_TIMEOUT_HOURS=24
MAX_BATCH_SIZE=50
BATCH_TIMEOUT_SECONDS=300

# Chain Configuration
ETHEREUM_BRIDGE_CONTRACT=0x1234...
BSC_BRIDGE_CONTRACT=0x5678...
POLYGON_BRIDGE_CONTRACT=0x9ABC...
AVALANCHE_BRIDGE_CONTRACT=0xDEF0...

# Validator Configuration
VALIDATOR_REGISTRY_CONTRACT=0xABCD...
VALIDATOR_REWARD_PERCENT=0.1
SLASHING_ENABLED=true
SLASHING_AMOUNT_ETH=1.0

# External Services
BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
KEY_MANAGEMENT_URL=http://key-management:8002
TRANSACTION_PROCESSOR_URL=http://transaction-processor:8001

# Token Registry
TOKEN_REGISTRY_URL=http://token-registry:8005
PRICE_ORACLE_URL=http://price-oracle:8006

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_TRANSFER_TOPIC=persistent://platformq/bridge/transfers
PULSAR_ATTESTATION_TOPIC=persistent://platformq/bridge/attestations

# Ignite Cache Configuration
IGNITE_ENDPOINTS=ignite-node1:10800,ignite-node2:10800
IGNITE_TRANSFER_CACHE=bridge-transfers
IGNITE_TOKEN_CACHE=bridge-tokens

# Security
ENABLE_RATE_LIMITING=true
MAX_TRANSFERS_PER_ADDRESS_DAILY=100
MIN_TRANSFER_AMOUNT_USD=10
MAX_TRANSFER_AMOUNT_USD=1000000

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
ALERT_WEBHOOK_URL=${ALERT_WEBHOOK_URL}
```

## Dependencies

- **FastAPI**: REST API framework
- **web3.py**: EVM chain interaction
- **aiopulsar**: Message queue for events
- **pyignite**: Distributed state storage
- **httpx**: Async HTTP client
- **eth-abi**: ABI encoding/decoding
- **prometheus-client**: Metrics collection
- **python-consul**: Service discovery
- **tenacity**: Retry logic
- **structlog**: Structured logging

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t cross-chain-bridge-service .

# Run the container
docker run -d \
  --name cross-chain-bridge \
  -p 8004:8004 \
  -e BLOCKCHAIN_CONNECTOR_URL="http://blockchain-connector:8000" \
  -e KEY_MANAGEMENT_URL="http://key-management:8002" \
  -e PULSAR_SERVICE_URL="pulsar://pulsar:6650" \
  -e IGNITE_ENDPOINTS="ignite:10800" \
  cross-chain-bridge-service
```

### Using Docker Compose

```yaml
services:
  cross-chain-bridge:
    build: ./services/blockchain/cross-chain-bridge-service
    ports:
      - "8004:8004"
    environment:
      - BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
      - KEY_MANAGEMENT_URL=http://key-management:8002
      - TRANSACTION_PROCESSOR_URL=http://transaction-processor:8001
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
      - IGNITE_ENDPOINTS=ignite:10800
    depends_on:
      - blockchain-connector
      - key-management
      - transaction-processor
      - pulsar
      - ignite
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Deploy bridge contracts (first time)
python scripts/deploy_bridges.py

# Set environment variables
export BLOCKCHAIN_CONNECTOR_URL="http://localhost:8000"
export KEY_MANAGEMENT_URL="http://localhost:8002"

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8004 --reload
```

## Bridge Protocol

### Transfer Flow

1. **Initiation**: User initiates transfer on source chain
2. **Lock/Burn**: Assets locked or burned on source chain
3. **Event Detection**: Bridge monitors detect the event
4. **Attestation**: Validators create and sign attestations
5. **Threshold**: Wait for required number of attestations
6. **Mint/Release**: Assets minted or released on destination
7. **Confirmation**: Transfer marked as complete

### Security Mechanisms

- **Multi-Validator Consensus**: Requires m-of-n validator signatures
- **Time Locks**: Configurable delays for large transfers
- **Rate Limiting**: Per-user and global transfer limits
- **Pause Mechanism**: Emergency pause functionality
- **Slashing**: Penalize misbehaving validators

### Fee Structure

```json
{
  "base_fee": "0.1%",
  "chain_fees": {
    "ethereum": "0.05%",
    "bsc": "0.03%",
    "polygon": "0.02%"
  },
  "volume_discounts": [
    {"min": 100000, "discount": "10%"},
    {"min": 1000000, "discount": "20%"}
  ]
}
```

## Smart Contracts

### Bridge Contract Interface

```solidity
interface IBridge {
    function deposit(
        address token,
        uint256 amount,
        uint256 destChainId,
        address recipient
    ) external payable;
    
    function withdraw(
        bytes32 transferId,
        bytes[] calldata signatures
    ) external;
    
    function pause() external onlyAdmin;
    function unpause() external onlyAdmin;
}
```

### Token Registry Interface

```solidity
interface ITokenRegistry {
    function getTokenMapping(
        address token,
        uint256 chainId
    ) external view returns (address);
    
    function addTokenMapping(
        address localToken,
        uint256 remoteChainId,
        address remoteToken
    ) external onlyAdmin;
}
```

## Monitoring

### Health Checks

The service provides comprehensive health status at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "bridges": {
    "ethereum": "active",
    "bsc": "active",
    "polygon": "active",
    "avalanche": "maintenance"
  },
  "validators": {
    "active": 7,
    "required": 5,
    "online": 7
  },
  "transfers": {
    "pending": 42,
    "completed_24h": 1523,
    "failed_24h": 3
  }
}
```

### Metrics

Prometheus metrics available at `/metrics`:

- `bridge_transfers_total` - Total transfers by status and chain
- `bridge_transfer_value_usd` - Transfer value in USD
- `bridge_attestations_total` - Attestations by validator
- `bridge_validator_performance` - Validator response times
- `bridge_fee_collected_usd` - Fees collected by token
- `bridge_processing_duration_seconds` - Transfer processing time

### Alerts

Automated alerts for:
- Validator offline/unresponsive
- Transfer stuck for > 1 hour
- Large transfer (> $100k)
- Attestation threshold not met
- Contract pause events
- Abnormal fee spikes

## Security Considerations

1. **Validator Security**
   - Validators run in secure enclaves
   - Private keys managed by Key Management Service
   - Regular key rotation

2. **Transfer Security**
   - All transfers require multiple attestations
   - Large transfers have additional delays
   - Suspicious patterns trigger manual review

3. **Contract Security**
   - Audited by multiple firms
   - Formal verification of critical paths
   - Bug bounty program active

4. **Operational Security**
   - Rate limiting on all endpoints
   - DDoS protection
   - Regular security assessments

## Troubleshooting

### Common Issues

1. **Stuck Transfers**
   - Check validator attestations
   - Verify destination chain status
   - Review transfer logs

2. **Missing Attestations**
   - Check validator health
   - Verify event detection
   - Review validator logs

3. **Fee Calculation Errors**
   - Verify price oracle connectivity
   - Check fee configuration
   - Review calculation logs

### Recovery Procedures

1. **Failed Transfer Recovery**
   ```bash
   # Retry transfer
   curl -X POST http://localhost:8004/api/v1/transfers/{id}/retry
   
   # Request refund
   curl -X POST http://localhost:8004/api/v1/transfers/{id}/refund
   ```

2. **Validator Recovery**
   ```bash
   # Check validator status
   curl http://localhost:8004/api/v1/validators
   
   # Rotate validators
   curl -X POST http://localhost:8004/api/v1/validators/rotate
   ```

## Performance Optimization

1. **Batch Processing**: Group transfers to reduce gas costs
2. **Parallel Attestation**: Process attestations concurrently
3. **Caching**: Cache token mappings and validator sets
4. **Event Filtering**: Optimize event queries
5. **Connection Pooling**: Reuse blockchain connections

## Contributing

1. Review bridge protocol documentation
2. Test with testnet deployments
3. Security review required for protocol changes
4. Update integration tests

## License

Copyright © 2024 PlatformQ. All rights reserved. 