# Blockchain Gateway Service

Unified blockchain abstraction layer for the platformQ ecosystem, providing a single point of integration for all blockchain operations.

## Overview

The Blockchain Gateway Service consolidates all blockchain-related functionality into a single, comprehensive service that provides:

- **Multi-Chain Support**: Seamless interaction with Ethereum, Solana, Cosmos, NEAR, Avalanche, and more
- **Oracle Aggregation**: Price feeds from multiple oracle providers (Chainlink, Band Protocol, Pyth)
- **Cross-Chain Bridge**: Secure asset transfers between different blockchains
- **Gas Optimization**: Intelligent gas price prediction and transaction batching
- **Smart Contract Management**: Deployment, interaction, and monitoring
- **KYC/AML Compliance**: On-chain compliance checks and risk scoring
- **Proposal Execution**: Cross-chain governance proposal execution

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Blockchain Gateway Service                  │
├─────────────────────────────────────────────────────────────┤
│ • Multi-chain abstraction layer                             │
│ • Transaction pooling & optimization                        │
│ • Gas optimization & MEV protection                         │
│ • Smart contract registry                                   │
│ • Event indexing with Ignite                               │
│ • Security analysis (Slither, Mythril)                     │
│ • Cross-chain bridges                                       │
│ • Oracle aggregation                                        │
│ • Chain adapters (Ethereum, Solana, Cosmos, etc.)         │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Multi-Chain Abstraction
- Unified interface for interacting with multiple blockchains
- Automatic chain selection based on gas costs and performance
- Connection pooling for optimal resource usage
- Automatic retry with exponential backoff

### Oracle Aggregation
- Price feeds from multiple oracle providers
- Median price calculation for accuracy
- Custom price feed creation
- Real-time and historical price data

### Cross-Chain Bridge
- Secure asset transfers between chains
- Atomic swaps with rollback protection
- Bridge fee optimization
- Transfer tracking and status monitoring

### Gas Optimization
- Predictive gas pricing using ML models
- Transaction batching for cost savings
- MEV protection through Flashbots
- Dynamic gas strategy selection

### Smart Contract Management
- Multi-chain contract deployment
- ABI management and versioning
- Contract interaction abstraction
- Event monitoring and indexing

### Compliance & Security
- KYC/AML checks for addresses
- Risk scoring and flagging
- Sanctions list checking
- Transaction monitoring

## API Endpoints

### Chain Management
- `GET /api/v1/chains` - List supported chains
- `GET /api/v1/chains/{chain_id}/status` - Get chain status
- `POST /api/v1/chains/{chain_id}/connect` - Connect to a chain

### Transactions
- `POST /api/v1/transactions` - Submit transaction
- `GET /api/v1/transactions/{tx_hash}` - Get transaction status
- `POST /api/v1/transactions/batch` - Submit batch transactions

### Smart Contracts
- `POST /api/v1/contracts/deploy` - Deploy contract
- `POST /api/v1/contracts/{address}/call` - Call contract method
- `GET /api/v1/contracts/{address}/events` - Get contract events

### Oracle Services
- `GET /api/v1/oracle/price/{asset}` - Get aggregated price
- `POST /api/v1/oracle/feed/create` - Create price feed
- `GET /api/v1/oracle/feed/{feed_id}` - Get feed data

### Cross-Chain Bridge
- `POST /api/v1/bridge/transfer` - Initiate bridge transfer
- `GET /api/v1/bridge/transfer/{transfer_id}` - Get transfer status
- `GET /api/v1/bridge/routes` - Get available bridge routes

### Compliance
- `POST /api/v1/compliance/check` - Check address compliance
- `GET /api/v1/compliance/risk/{address}` - Get risk score

## Configuration

### Environment Variables

```bash
# Chain RPC URLs
ETHEREUM_RPC_URL=https://eth-mainnet.example.com
ETHEREUM_WS_URL=wss://eth-mainnet.example.com
POLYGON_RPC_URL=https://polygon-mainnet.example.com
SOLANA_RPC_URL=https://solana-mainnet.example.com
COSMOS_RPC_URL=https://cosmos-mainnet.example.com

# Oracle Configuration
CHAINLINK_NODE_URL=https://chainlink.example.com
BAND_PROTOCOL_URL=https://band.example.com
PYTH_NETWORK_URL=https://pyth.example.com

# Infrastructure
IGNITE_HOST=ignite:10800
PULSAR_URL=pulsar://pulsar:6650

# Security
SLITHER_ENABLED=true
MYTHRIL_ENABLED=true
MEV_PROTECTION_ENABLED=true
```

## Usage Examples

### Submit Transaction
```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://blockchain-gateway:8000/api/v1/transactions",
        json={
            "chain": "ethereum",
            "to": "0x...",
            "value": "1000000000000000000",  # 1 ETH in wei
            "data": "0x..."
        }
    )
    tx_hash = response.json()["transaction_hash"]
```

### Get Oracle Price
```python
response = await client.get(
    "http://blockchain-gateway:8000/api/v1/oracle/price/ETH",
    params={"chains": ["ethereum", "polygon"]}
)
price_data = response.json()
```

### Bridge Transfer
```python
response = await client.post(
    "http://blockchain-gateway:8000/api/v1/bridge/transfer",
    json={
        "from_chain": "ethereum",
        "to_chain": "polygon",
        "asset": "USDC",
        "amount": "1000000000",  # 1000 USDC
        "recipient": "0x..."
    }
)
transfer_id = response.json()["transfer_id"]
```

## Development

### Running Locally
```bash
cd services/blockchain-gateway-service
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t platformq/blockchain-gateway-service .
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `blockchain_transactions_total` - Total transactions by chain
- `blockchain_gas_price` - Current gas price by chain
- `oracle_price_updates` - Oracle price update count
- `bridge_transfers_total` - Total bridge transfers
- `compliance_checks_total` - Total compliance checks

## Security Considerations

- All private keys are stored in HashiCorp Vault
- Transaction signing happens in secure enclaves
- Multi-signature support for high-value transactions
- Rate limiting and DDoS protection
- Audit logs for all operations

## Migration from Legacy Services

This service replaces:
- `blockchain-event-bridge` - Use chain adapters and bridge endpoints
- `oracle-aggregator-service` - Use oracle endpoints

Update service references:
- `http://blockchain-event-bridge:8001` → `http://blockchain-gateway-service:8000`
- `http://oracle-aggregator-service:8000` → `http://blockchain-gateway-service:8000` 