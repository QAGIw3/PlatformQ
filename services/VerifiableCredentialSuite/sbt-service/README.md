# SBT Service

SoulBound Token (SBT) service for managing non-transferable tokens representing verifiable credentials, identity, and reputation on-chain. This service provides APIs for minting, managing, and querying SBTs across multiple blockchain networks.

## Overview

The SBT Service provides:

- **SBT Minting**: Create non-transferable tokens linked to credentials
- **Multi-chain Support**: Deploy and manage SBTs across different blockchains
- **Recovery Mechanisms**: Social recovery for lost accounts
- **Revocation Management**: Issuer-controlled revocation
- **Metadata Handling**: On-chain and off-chain metadata management
- **Cross-chain Bridging**: Port SBTs across chains while maintaining properties
- **Batch Operations**: Efficient bulk minting and management

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   SBT Service                        │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │   FastAPI   │  │ SBT Manager  │  │   Cache    │ │
│  │     API     │  │              │  │  Manager   │ │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘ │
│         │                 │                 │        │
│  ┌──────▼─────────────────▼────────────────▼──────┐ │
│  │         Blockchain Integration Layer            │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐        │ │
│  │  │Ethereum │  │ Polygon │  │  Other  │  ...   │ │
│  │  └─────────┘  └─────────┘  └─────────┘        │ │
│  └─────────────────────────────────────────────────┘ │
│                                                      │
│  ┌──────────────┐  ┌────────────┐  ┌─────────────┐ │
│  │   Storage    │  │  Events    │  │  Monitoring │ │
│  │              │  │            │  │             │ │
│  └──────────────┘  └────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────┘
```

## API Endpoints

### SBT Management

#### Mint SBT
```http
POST /api/v1/sbt/mint
Content-Type: application/json

{
  "recipient": "0x742d35Cc6634C0532925a3b844Bc9e7595f5b899",
  "credential_id": "cred-123",
  "metadata": {
    "type": "UniversityDegree",
    "issuer": "did:example:university",
    "achievement": "Bachelor of Science"
  },
  "chain": "ethereum"
}
```

#### Mint Batch
```http
POST /api/v1/sbt/mint-batch
Content-Type: application/json

{
  "recipients": [
    {
      "address": "0x...",
      "credential_id": "cred-123",
      "metadata": {...}
    }
  ],
  "chain": "polygon"
}
```

#### Revoke SBT
```http
POST /api/v1/sbt/{token_id}/revoke
Content-Type: application/json

{
  "reason": "Credential expired",
  "chain": "ethereum"
}
```

#### Query SBTs
```http
GET /api/v1/sbt/owner/{address}?chain=ethereum
```

#### Get SBT Metadata
```http
GET /api/v1/sbt/{token_id}/metadata?chain=ethereum
```

### Cross-chain Operations

#### Initiate Bridge
```http
POST /api/v1/sbt/{token_id}/bridge
Content-Type: application/json

{
  "source_chain": "ethereum",
  "target_chain": "polygon",
  "recipient": "0x..."
}
```

#### Get Bridge Status
```http
GET /api/v1/sbt/bridge/{bridge_id}/status
```

### Recovery

#### Initiate Recovery
```http
POST /api/v1/sbt/recovery/initiate
Content-Type: application/json

{
  "lost_address": "0x...",
  "new_address": "0x...",
  "recovery_proofs": [...]
}
```

## Configuration

### Environment Variables

```bash
# Service Configuration
SBT_SERVICE_NAME=sbt-service
SBT_SERVICE_VERSION=1.0.0
SBT_HOST=0.0.0.0
SBT_PORT=8053

# Database
SBT_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost/sbt

# Blockchain Connector
SBT_BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector-service:8086

# Apache Ignite Cache
SBT_IGNITE_HOST=localhost
SBT_IGNITE_PORT=10800
SBT_ENABLE_CACHE=true

# IPFS/Storage
SBT_STORAGE_SERVICE_URL=http://storage-service:8084
SBT_IPFS_GATEWAY=https://ipfs.io/ipfs/

# Event Streaming
SBT_PULSAR_URL=pulsar://localhost:6650
SBT_EVENT_TOPIC=sbt-events

# HashiCorp Vault
SBT_VAULT_ADDR=http://vault:8200
SBT_VAULT_TOKEN=<token>

# Consul
SBT_CONSUL_HOST=localhost
SBT_CONSUL_PORT=8500
```

## Smart Contract Integration

### Supported Chains
- Ethereum Mainnet
- Polygon
- Arbitrum
- Optimism
- Custom EVM chains

### Contract Features
- ERC-5192: Minimal Soulbound NFTs
- ERC-5484: Consensual Soulbound Tokens
- Custom recovery mechanisms
- Batch minting support
- Gas optimization

## Metadata Management

### On-chain Metadata
- Token ID
- Recipient address
- Issuer address
- Issuance timestamp
- Revocation status

### Off-chain Metadata
- Credential details
- Achievement descriptions
- Issuer information
- Visual representations
- Proof references

### Storage Options
1. **IPFS**: Decentralized storage for metadata
2. **MinIO**: Self-hosted object storage
3. **Hybrid**: Critical data on-chain, details off-chain

## Recovery Mechanisms

### Social Recovery
- Multi-signature recovery
- Time-locked recovery
- Trusted guardian system

### Issuer Recovery
- Direct issuer intervention
- Proof of identity
- Credential re-issuance

## Integration Examples

### With Core Credential Service

```python
# After credential issuance, mint SBT
response = await http_client.post(
    "http://sbt-service:8053/api/v1/sbt/mint",
    json={
        "recipient": holder_address,
        "credential_id": credential["id"],
        "metadata": {
            "type": credential["type"],
            "issuer": credential["issuer"],
            "claims": credential["credentialSubject"]
        },
        "chain": "polygon"
    }
)
sbt_token_id = response.json()["token_id"]
```

### With Event System

```python
# Subscribe to SBT events
async for event in pulsar_consumer.subscribe("sbt-events"):
    if event["type"] == "SBT_MINTED":
        # Update credential with SBT reference
        await update_credential_sbt(
            credential_id=event["credential_id"],
            sbt_token_id=event["token_id"],
            chain=event["chain"]
        )
```

## Security Considerations

1. **Access Control**: Only authorized issuers can mint/revoke
2. **Rate Limiting**: Prevent spam minting
3. **Gas Management**: Optimize for cost efficiency
4. **Privacy**: Minimal on-chain data exposure
5. **Recovery Security**: Multi-factor recovery verification

## Performance Optimization

1. **Batch Operations**: Group mints for gas efficiency
2. **Caching**: Cache frequently accessed SBT data
3. **Event Streaming**: Asynchronous blockchain operations
4. **Multi-chain Parallelism**: Concurrent chain operations

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run migrations
alembic upgrade head

# Start service
uvicorn app.main:app --reload --port 8053
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run contract tests
pytest tests/contracts -v
```

## Monitoring

- Health endpoint: `GET /health`
- Metrics endpoint: `GET /metrics`
- Chain status: `GET /api/v1/chains/status`

## Related Services

- **blockchain-connector-service**: Blockchain interaction layer
- **core-credential-service**: Credential issuance
- **storage-service**: Metadata storage
- **event-router-service**: Event distribution 