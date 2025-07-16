# Verifiable Credential Service

## Purpose

The **Verifiable Credential Service** provides a comprehensive "Trust Engine" for the platform. It issues W3C Verifiable Credentials to create immutable, cryptographically-provable records of key business events. The service now features advanced blockchain integration including multi-chain support, decentralized identities (DIDs), smart contract verification, zero-knowledge proofs for privacy-preserving credential presentation, cross-chain bridges, IPFS storage, trust networks, and support for emerging standards.

## Key Features

### 1. Multi-Chain Blockchain Support
- **Ethereum**: Full support for mainnet and testnets
- **Polygon**: Optimized for lower gas costs
- **Hyperledger Fabric**: Enterprise-grade permissioned blockchain
- **Multi-chain anchoring**: Anchor credentials across multiple chains simultaneously

### 2. Cross-Chain Bridges
- **Credential Portability**: Transfer credentials between different blockchain networks
- **Atomic Swaps**: Guaranteed cross-chain transfers using HTLCs
- **Bridge Verification**: Cryptographic proofs ensure secure transfers
- **Multi-Chain Identity**: Maintain consistent identity across chains

### 3. Decentralized Identity (DID) Support
- **W3C DID Standards**: Full compliance with DID specifications
- **Multiple DID Methods**:
  - `did:web`: Web-based DIDs for easy integration
  - `did:key`: Self-contained cryptographic DIDs
  - `did:ethr`: Ethereum-based DIDs
- **DID Resolution**: Universal resolver for all supported methods
- **DID Documents**: Automatic generation and management

### 4. IPFS Integration
- **Decentralized Storage**: Store credentials on IPFS with encryption
- **Content Addressing**: Immutable credential references via CIDs
- **Distributed Network**: Replicate across multiple IPFS nodes
- **Merkle Trees**: Efficient batch verification of credentials
- **Credential DAGs**: Link related credentials in directed acyclic graphs

### 5. Trust Networks & Reputation
- **Trust Graphs**: Build networks of trusted issuers and verifiers
- **Reputation Scoring**: Multi-factor reputation system for entities
- **Transitive Trust**: Calculate trust paths between entities
- **Consensus Verification**: Network-based credential verification
- **Trust Propagation**: PageRank-style trust distribution

### 6. Smart Contract Integration
- **Credential Registry**: On-chain registry for credential anchoring
- **Automated Verification**: Smart contract-based verification rules
- **Batch Operations**: Efficient batch anchoring and verification
- **Revocation Support**: On-chain revocation with reasons

### 7. Zero-Knowledge Proofs
- **Selective Disclosure**: Reveal only required attributes
- **BBS+ Signatures**: Privacy-preserving credential signatures
- **Multiple ZKP Types**:
  - BBS+ for efficient selective disclosure
  - CL Signatures (Hyperledger Indy compatible)
  - Groth16 zkSNARKs for complex proofs
- **Verifiable Presentations**: Create presentations with minimal disclosure

### 8. Advanced Standards Support
- **Verifiable Presentations**: W3C VP 2.0 compliant presentations
- **SoulBound Tokens (SBTs)**: Non-transferable on-chain credentials
- **Credential Manifest**: Discovery and issuance workflows
- **Presentation Exchange**: Standardized credential requests

## Architecture

The service is composed of several modules:

```
verifiable-credential-service/
├── app/
│   ├── blockchain/          # Blockchain integration
│   │   ├── base.py         # Abstract blockchain client
│   │   ├── bridge.py       # Cross-chain bridge implementation
│   │   ├── ethereum.py     # Ethereum implementation
│   │   ├── polygon.py      # Polygon implementation
│   │   └── fabric.py       # Hyperledger Fabric implementation
│   ├── did/                # DID management
│   │   ├── did_manager.py  # DID creation and management
│   │   ├── did_methods.py  # DID method implementations
│   │   └── did_resolver.py # DID resolution
│   ├── storage/            # Decentralized storage
│   │   └── ipfs_storage.py # IPFS integration with encryption
│   ├── trust/              # Trust network
│   │   └── reputation.py   # Trust graphs and reputation system
│   ├── standards/          # Standards compliance
│   │   └── advanced_standards.py # VP, SBT, Manifest support
│   ├── zkp/                # Zero-knowledge proofs
│   │   ├── zkp_manager.py  # ZKP orchestration
│   │   ├── bbs_plus.py     # BBS+ implementation
│   │   └── selective_disclosure.py # Selective disclosure
│   └── contracts/          # Smart contracts
│       ├── CredentialRegistry.sol
│       └── CredentialVerifier.sol
```

## Event-Driven Architecture

The service operates both synchronously via REST API and asynchronously via platform events.

### Events Consumed

- **Topic Pattern**: `persistent://platformq/.*/verifiable-credential-issuance-requests`
- **Schema**: `IssueVerifiableCredential`
- **Description**: Consumes requests to issue new credentials, typically published by the `workflow-service` when a business process requires a formal, auditable record.

### Events Produced

- **Topic**: `persistent://platformq/<tenant_id>/verifiable-credential-issued-events`
- **Schema**: `VerifiableCredentialIssued`
- **Description**: Published after a credential has been successfully created and anchored on blockchain.

## API Endpoints

### Credential Issuance

- `POST /api/v1/issue`: Issue a new Verifiable Credential with blockchain anchoring
  ```json
  {
    "subject": {...},
    "type": "ProposalApprovalCredential",
    "blockchain": "ethereum",
    "multi_chain": false,
    "issuer_did": "did:web:platformq.com:tenants:123",
    "store_on_ipfs": true,
    "encrypt_storage": true,
    "create_sbt": false,
    "owner_address": "0x..."
  }
  ```

### Cross-Chain Bridge

- `POST /api/v1/bridge/transfer`: Transfer credential between blockchains
  ```json
  {
    "credential_id": "urn:uuid:...",
    "credential_data": {...},
    "source_chain": "ethereum",
    "target_chain": "polygon"
  }
  ```

- `GET /api/v1/bridge/status/{request_id}`: Check bridge transfer status

### Trust Network

- `POST /api/v1/trust/relationships`: Create trust relationship
- `GET /api/v1/trust/reputation/{entity_id}`: Get entity reputation score
- `GET /api/v1/trust/network/stats`: Get network statistics
- `POST /api/v1/verify/network`: Request network consensus verification

### IPFS Storage

- `GET /api/v1/credentials/ipfs/{cid}`: Retrieve credential from IPFS

### Verifiable Presentations

- `POST /api/v1/presentations/create`: Create a Verifiable Presentation
  ```json
  {
    "credentials": [...],
    "challenge": "random-challenge",
    "domain": "example.com",
    "purpose": "authentication",
    "disclosed_claims": {
      "0": ["name", "age"]
    }
  }
  ```

### SoulBound Tokens

- `POST /api/v1/sbt/mint`: Mint a SoulBound Token
- `GET /api/v1/sbt/{token_id}/verify`: Verify SBT ownership

### Credential Manifest

- `POST /api/v1/manifests`: Create credential manifest for discovery

### DID Management

- `POST /api/v1/dids`: Create a new DID
- `GET /api/v1/dids/{did}`: Resolve DID to document

### Blockchain Verification

- `GET /api/v1/credentials/{credential_id}/verify`: Verify blockchain anchor
- `GET /api/v1/credentials/{credential_id}/history`: Get credential history

## Configuration

The service supports extensive configuration via environment variables:

### Blockchain Configuration
- `ETHEREUM_ENABLED`: Enable Ethereum support
- `ETHEREUM_PROVIDER_URL`: Ethereum node URL
- `ETHEREUM_PRIVATE_KEY`: Private key for transactions
- `POLYGON_ENABLED`: Enable Polygon support
- `FABRIC_ENABLED`: Enable Hyperledger Fabric support

### Storage Configuration
- `STORAGE_PROXY_URL`: IPFS storage proxy service URL
- `CREDENTIAL_ENCRYPTION_KEY`: Encryption key for IPFS storage
- `IPFS_STORAGE_NODES`: List of IPFS nodes for replication

### Trust Network Configuration
- `TRUST_NETWORK_ENABLED`: Enable trust network features
- `MIN_VERIFIERS`: Minimum verifiers for consensus
- `CONSENSUS_THRESHOLD`: Required consensus ratio

## How to Run Locally

1. **Install Requirements**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variables**:
   ```bash
   export PULSAR_URL="pulsar://localhost:6650"
   export ETHEREUM_ENABLED=true
   export ETHEREUM_PROVIDER_URL="http://localhost:8545"
   export STORAGE_PROXY_URL="http://localhost:8001"
   ```

3. **Deploy Smart Contracts** (if using blockchain features):
   ```bash
   cd app/contracts
   npx hardhat deploy --network local
   ```

4. **Run the Service**:
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```

## Usage Examples

### Issue a Credential with Full Features
```python
import httpx

# Issue credential with blockchain anchor, IPFS storage, and SBT
response = httpx.post(
    "http://localhost:8000/api/v1/issue",
    json={
        "subject": {
            "id": "did:example:123",
            "name": "Alice",
            "achievement": "Completed Training"
        },
        "type": "AchievementCredential",
        "blockchain": "ethereum",
        "multi_chain": true,
        "store_on_ipfs": true,
        "create_sbt": true,
        "owner_address": "0x742d35Cc6634C0532925a3b844Bc9e7595f5b41"
    }
)
```

### Create a Privacy-Preserving Presentation
```python
# Create presentation with selective disclosure
response = httpx.post(
    "http://localhost:8000/api/v1/presentations/create",
    json={
        "credentials": [credential],
        "challenge": "abc123",
        "domain": "verifier.example.com",
        "disclosed_claims": {
            "0": ["name"]  # Only reveal name, not achievement
        }
    }
)
```

### Bridge Credential Cross-Chain
```python
# Transfer credential from Ethereum to Polygon
response = httpx.post(
    "http://localhost:8000/api/v1/bridge/transfer",
    json={
        "credential_id": "urn:uuid:123",
        "credential_data": credential_data,
        "source_chain": "ethereum",
        "target_chain": "polygon"
    }
)
```

## Security Considerations

1. **Private Key Management**: Use hardware security modules (HSM) or key management services in production
2. **Encryption**: All credentials stored on IPFS are encrypted by default
3. **Access Control**: Multi-tenant isolation ensures credentials are segregated
4. **Consensus Verification**: Use multiple verifiers to prevent single points of failure
5. **Bridge Security**: Cross-chain transfers use cryptographic proofs and time-locks

## Future Enhancements

1. **Layer 2 Support**: Integration with Optimism, Arbitrum, zkSync
2. **Advanced Privacy**: Integration with Aztec Protocol for private credentials
3. **Decentralized Identifiers**: Support for more DID methods (ION, Element)
4. **Interoperability**: Integration with other credential standards (mDL, EBSI)
5. **AI Integration**: ML-based fraud detection and trust scoring 