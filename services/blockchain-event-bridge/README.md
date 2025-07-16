# Blockchain Event Bridge Service

## Overview

The Blockchain Event Bridge is a multi-chain service that enables cross-chain DAO governance across Ethereum, Solana, and Hyperledger Fabric networks. It provides unified interfaces for creating proposals, casting votes, and synchronizing reputation scores across different blockchain ecosystems.

## Features

- **Multi-Chain Support**: Ethereum/EVM chains (Ethereum, Polygon, Arbitrum), Solana, and Hyperledger Fabric
- **Cross-Chain Governance**: Create and manage proposals that span multiple blockchains
- **Reputation Synchronization**: Bridge reputation scores across chains for unified voting power
- **Event Streaming**: Real-time event streaming via Apache Pulsar
- **State Management**: High-performance caching with Apache Ignite
- **Verifiable Credentials**: Integration with VC service for access control

## Architecture

The service uses a modular adapter pattern where each blockchain type has its own adapter implementing a common interface:

```
ChainManager
├── EthereumAdapter (supports Ethereum, Polygon, Arbitrum)
├── SolanaAdapter
└── HyperledgerAdapter
```

## API Endpoints

### Chain Management
- `GET /api/v1/chains` - List all connected chains
- `POST /api/v1/chains/register` - Register a new chain dynamically

### Cross-Chain Proposals
- `POST /api/v1/proposals/cross-chain` - Create a cross-chain proposal
- `GET /api/v1/proposals/{id}/state` - Get aggregated proposal state

### Reputation
- `POST /api/v1/reputation/sync` - Sync user reputation across chains
- `GET /api/v1/reputation/{user_id}` - Get aggregated reputation score

### Voting
- `POST /api/v1/vote` - Cast a vote on a specific chain

## Configuration

Environment variables for chain configuration:

### Ethereum/EVM Chains
```bash
ETHEREUM_NODE_URL=wss://mainnet.infura.io/ws/v3/YOUR_KEY
ETHEREUM_GOVERNOR_ADDRESS=0x...
ETHEREUM_REPUTATION_ORACLE=0x...

POLYGON_NODE_URL=wss://polygon-mainnet.infura.io/ws/v3/YOUR_KEY
POLYGON_GOVERNOR_ADDRESS=0x...
```

### Solana
```bash
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
SOLANA_GOVERNANCE_PROGRAM=GovER5Lthms3bLBqWub97yVrMmEogzX7xNjdXpPPCVZw
SOLANA_GOVERNANCE_ACCOUNT=YOUR_GOVERNANCE_ACCOUNT
SOLANA_REPUTATION_PROGRAM=YOUR_REPUTATION_PROGRAM
```

### Hyperledger Fabric
```bash
FABRIC_NETWORK_PROFILE=/path/to/network-profile.yaml
FABRIC_CONNECTION_PROFILE=/path/to/connection-profile.json
FABRIC_CHANNEL=governance
FABRIC_ORG=Org1
FABRIC_USER=User1
```

### Infrastructure
```bash
PULSAR_URL=pulsar://localhost:6650
IGNITE_HOSTS=localhost:10800
CROSS_CHAIN_EVENTS_TOPIC=persistent://public/default/cross-chain-governance
```

## Data Flow

1. **Proposal Creation**:
   - User creates cross-chain proposal via API
   - Proposal submitted to each specified chain
   - State stored in Ignite cache
   - Events published to Pulsar

2. **Voting**:
   - Votes cast on individual chains
   - Events captured by chain adapters
   - Aggregated in Ignite
   - Real-time updates via Pulsar

3. **Reputation Sync**:
   - Reputation scores fetched from each chain
   - Aggregated and cached in Ignite
   - VC issued for cross-chain reputation

## Development

### Running Locally

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start infrastructure:
```bash
docker-compose up -d pulsar ignite
```

3. Configure chains (see Configuration section)

4. Run the service:
```bash
python -m app.main
```

### Testing

For development, use the test endpoint:
```bash
curl -X POST http://localhost:8001/api/v1/test/create-proposal
```

## Smart Contract Integration

### Ethereum/EVM
- Requires deployed PlatformQGovernor contract
- Optional ReputationOracle for reputation scores

### Solana
- Uses Anchor framework
- Requires deployed governance and reputation programs

### Hyperledger Fabric
- Requires installed governance and reputation chaincodes

## Monitoring

- Metrics exposed at `/metrics`
- Health check at `/`
- Event stream monitoring via Pulsar admin

## Security Considerations

- Private keys should never be stored in the service
- Use hardware wallets or key management services
- Enable TLS for all blockchain connections
- Implement rate limiting for API endpoints

## Future Enhancements

- Support for additional chains (Cosmos, Near, etc.)
- Advanced voting strategies (quadratic voting, delegation)
- Cross-chain treasury management
- Automated proposal execution 