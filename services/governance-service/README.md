# Unified Governance Service

Comprehensive governance system for the platformQ ecosystem, combining DAO governance, proposal management, multi-chain support, and advanced voting strategies.

## Overview

The Unified Governance Service consolidates all governance-related functionality into a single service that provides:

- **DAO Governance**: Decentralized Autonomous Organization management with multi-tenant support
- **Proposal Management**: Create, vote on, and execute proposals both on-chain and off-chain
- **Multi-Chain Support**: Execute governance decisions across multiple blockchains
- **Advanced Voting Strategies**: Quadratic, conviction, delegation, and time-weighted voting
- **Reputation Management**: Track and manage user reputation scores
- **Document Management**: Integration with Nextcloud for proposal documents
- **Cross-Chain Execution**: Coordinate proposal execution across different blockchains

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Unified Governance Service                   │
├─────────────────────────────────────────────────────────────┤
│                     Core Components                          │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │  Governance │   Proposal   │   Voting    │ Reputation │ │
│  │   Manager   │   Manager    │ Strategies  │  Manager   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Integrations                              │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │ Blockchain  │  Nextcloud   │   Apache    │  Apache    │ │
│  │   Gateway   │   Storage    │   Ignite    │  Pulsar    │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Features

### DAO Governance
- Create and manage DAOs with custom parameters
- Multi-tenant support with complete isolation
- Flexible role-based permissions
- Treasury management with value transfer proposals

### Proposal Management
- Create proposals with on-chain and off-chain options
- Document attachment via Nextcloud integration
- Multi-stage proposal lifecycle (draft → active → passed/failed → executed)
- Cross-chain proposal coordination
- Automatic execution of passed proposals

### Voting Strategies
- **Quadratic Voting**: Square root of voting power for fairness
- **Conviction Voting**: Time-weighted voting for long-term commitment
- **Delegation**: Delegate voting power to trusted representatives
- **Time-Weighted**: Voting power increases with holding duration
- **Custom Strategies**: Extensible framework for new voting mechanisms

### Reputation System
- Track user contributions and participation
- Reputation-based access control
- Minimum reputation thresholds for treasury proposals
- Cross-service reputation synchronization

## API Endpoints

### Governance Endpoints
- `GET /api/v1/daos` - List all DAOs
- `POST /api/v1/daos` - Create a new DAO
- `GET /api/v1/daos/{dao_id}` - Get DAO details
- `PUT /api/v1/daos/{dao_id}` - Update DAO settings
- `GET /api/v1/voting-strategies` - List available voting strategies

### Proposal Endpoints
- `POST /api/v1/proposals` - Create a new proposal
- `GET /api/v1/proposals` - List proposals with filtering
- `GET /api/v1/proposals/{proposal_id}` - Get proposal details
- `POST /api/v1/proposals/{proposal_id}/vote` - Vote on a proposal
- `POST /api/v1/proposals/{proposal_id}/execute` - Execute a passed proposal
- `POST /api/v1/proposals/{proposal_id}/cancel` - Cancel a proposal
- `GET /api/v1/proposals/{proposal_id}/voting-stats` - Get voting statistics
- `POST /api/v1/proposals/cross-chain` - Create cross-chain proposal

### Reputation Endpoints
- `GET /api/v1/reputation/{user_id}` - Get user reputation
- `POST /api/v1/reputation/sync` - Sync reputation from blockchain

## Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql://user:password@localhost/governance

# Blockchain
BLOCKCHAIN_GATEWAY_URL=http://blockchain-gateway-service:8000

# Storage
NEXTCLOUD_URL=http://nextcloud:80
NEXTCLOUD_USER=admin
NEXTCLOUD_PASSWORD=admin

# Infrastructure
IGNITE_HOST=ignite:10800
PULSAR_URL=pulsar://pulsar:6650

# Service Configuration
SERVICE_NAME=governance-service
LOG_LEVEL=INFO
```

## Usage Examples

### Create a Proposal
```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://governance-service:8000/api/v1/proposals",
        json={
            "title": "Upgrade Platform Infrastructure",
            "description": "Proposal to upgrade our cloud infrastructure",
            "targets": ["0x..."],  # Contract addresses
            "values": [0],         # ETH values
            "calldatas": ["0x..."] # Encoded function calls
        },
        params={"onchain": True}
    )
    proposal_id = response.json()["id"]
```

### Vote on a Proposal
```python
response = await client.post(
    f"http://governance-service:8000/api/v1/proposals/{proposal_id}/vote",
    params={
        "vote": "for",
        "voting_power": 100
    }
)
```

### Create Cross-Chain Proposal
```python
response = await client.post(
    "http://governance-service:8000/api/v1/proposals/cross-chain",
    json={
        "title": "Multi-Chain Treasury Rebalancing",
        "description": "Rebalance treasury across Ethereum and Polygon",
        "chains": ["ethereum", "polygon"],
        "chain_proposals": {
            "ethereum": {
                "targets": ["0x..."],
                "values": [1000000000000000000],  # 1 ETH
                "calldatas": ["0x..."]
            },
            "polygon": {
                "targets": ["0x..."],
                "values": [2000000000000000000000],  # 2000 MATIC
                "calldatas": ["0x..."]
            }
        }
    }
)
```

## Development

### Running Locally
```bash
cd services/governance-service
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Running Tests
```bash
pytest tests/
```

### Database Migrations
```bash
alembic upgrade head
```

### Building Docker Image
```bash
docker build -t platformq/governance-service .
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `governance_proposals_total` - Total proposals by status
- `governance_votes_total` - Total votes cast
- `governance_dao_count` - Number of active DAOs
- `governance_execution_duration` - Proposal execution time
- `governance_reputation_updates` - Reputation update count

## Security Considerations

- Proposals with value transfers require minimum reputation threshold
- Multi-signature support for high-value proposals
- Time locks for critical governance actions
- Role-based access control for administrative functions
- Audit logs for all governance actions

## Migration from Legacy Services

This service replaces:
- `dao-governance-service` - All functionality integrated
- `proposals-service` - Proposals now managed here

Update service references:
- `http://dao-governance-service:8000` → `http://governance-service:8000`
- `http://proposals-service:8000` → `http://governance-service:8000` 