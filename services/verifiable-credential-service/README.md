# Verifiable Credential Service

The Verifiable Credential Service provides W3C-compliant credential issuance, verification, and cross-chain bridging capabilities for PlatformQ. It enables privacy-preserving authentication and portable digital identity across multiple blockchains.

## 🎯 Overview

This service manages:
- W3C Verifiable Credential issuance and verification
- Zero-knowledge proof generation for privacy
- Cross-chain credential exports as SoulBound Tokens
- Verifiable Presentations for credential bundling
- Multi-chain DID management
- Trust network integration

## 🏗️ Architecture

### Technology Stack
- **Framework**: FastAPI (Python)
- **Blockchain**: Web3.py, Ethereum/Polygon/Arbitrum
- **Smart Contracts**: Solidity, Hardhat
- **Cryptography**: py-ecc, cryptography, pynacl
- **Standards**: W3C VC, DID, JSON-LD
- **Storage**: IPFS, PostgreSQL
- **Cache**: Apache Ignite

### Key Components

1. **Credential Management**
   - W3C VC Data Model 1.1 compliance
   - Multiple credential types (achievements, qualifications, reputation)
   - Blockchain anchoring for tamper-evidence
   - IPFS storage for decentralization

2. **Zero-Knowledge Proofs**
   - Selective disclosure of attributes
   - Range proofs (age > 18, score > threshold)
   - Set membership proofs
   - BBS+ signatures for privacy

3. **Cross-Chain Bridge**
   - Export VCs as SoulBound Tokens
   - Multi-chain support (Ethereum, Polygon, Arbitrum)
   - Gas optimization for L2 networks
   - HTLC-based atomic swaps

4. **Verifiable Presentations**
   - Bundle multiple credentials
   - Purpose-based disclosure
   - Time-limited validity
   - Challenge-response authentication

## 📡 API Endpoints

### Credential Operations
- `POST /api/v1/issue` - Issue new verifiable credential
- `GET /api/v1/verify` - Verify credential validity
- `GET /api/v1/dids/{did}/credentials` - List user's credentials
- `POST /api/v1/credentials/revoke` - Revoke credential

### Zero-Knowledge Proofs
- `POST /api/v1/zkp/generate` - Generate ZK proof
- `POST /api/v1/zkp/verify` - Verify ZK proof
- `POST /api/v1/credentials/present` - Create derived credential

### Cross-Chain Bridge
- `POST /api/v1/cross-chain/export` - Export credential to blockchain
- `GET /api/v1/cross-chain/chains` - List supported chains
- `GET /api/v1/cross-chain/chains/{network}/fee` - Estimate gas fees
- `GET /api/v1/cross-chain/export/{credential_id}/status` - Check export status

### Presentations
- `POST /api/v1/presentations` - Create verifiable presentation
- `POST /api/v1/presentations/verify` - Verify presentation
- `GET /api/v1/presentations/templates` - Get presentation templates
- `POST /api/v1/presentations/from-template/{name}` - Create from template

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Node.js 18+ (for smart contracts)
- Ethereum wallet with testnet ETH
- IPFS node (optional)
- PostgreSQL

### Development Setup

1. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install Node dependencies**
   ```bash
   npm install
   ```

3. **Compile smart contracts**
   ```bash
   npx hardhat compile
   ```

4. **Deploy contracts (local)**
   ```bash
   npx hardhat node  # In one terminal
   npx hardhat run scripts/deploy_contracts.js --network localhost
   ```

5. **Set environment variables**
   ```bash
   export DATABASE_URL="postgresql://user:pass@localhost/verifiable_credentials"
   export BRIDGE_PRIVATE_KEY="0x..."  # Your wallet private key
   export IPFS_API_URL="http://localhost:5001"
   export ETHEREUM_RPC_URL="http://localhost:8545"
   ```

6. **Start the service**
   ```bash
   uvicorn app.main:app --reload --port 8002
   ```

## 📄 Smart Contracts

### Deployed Contracts
1. **SoulBoundToken.sol** - Non-transferable credential tokens
2. **CrossChainBridge.sol** - HTLC-based cross-chain transfers
3. **ReputationOracle.sol** - Multi-dimensional reputation scoring
4. **PlatformQGovernor.sol** - DAO governance with weighted voting

### Contract Addresses
See `deployments/{network}.json` after deployment.

## 🔐 Security

### Cryptographic Security
- **Signatures**: Ed25519, ECDSA
- **Hashing**: SHA-256, Keccak-256
- **Encryption**: AES-256-GCM
- **ZKP**: BBS+, Bulletproofs

### Blockchain Security
- **Smart Contract Audits**: OpenZeppelin standards
- **Multi-sig Controls**: Gnosis Safe integration
- **Gas Optimization**: L2-first design
- **Reentrancy Protection**: Check-Effects-Interactions

## 🧪 Testing

### Backend Tests
```bash
pytest tests/ -v --cov=app
```

### Smart Contract Tests
```bash
npx hardhat test
npx hardhat coverage
```

### Integration Tests
```bash
pytest tests/integration/ -v
```

## 📊 Credential Types

### Achievement Credentials
```json
{
  "@context": ["https://www.w3.org/2018/credentials/v1"],
  "type": ["VerifiableCredential", "AchievementCredential"],
  "credentialSubject": {
    "achievement": "First Asset Created",
    "level": "Bronze",
    "points": 100
  }
}
```

### Reputation Credentials
```json
{
  "@context": ["https://www.w3.org/2018/credentials/v1"],
  "type": ["VerifiableCredential", "ReputationCredential"],
  "credentialSubject": {
    "dimensions": {
      "technical": 85,
      "collaboration": 90,
      "governance": 75
    }
  }
}
```

### Asset Credentials
```json
{
  "@context": ["https://www.w3.org/2018/credentials/v1"],
  "type": ["VerifiableCredential", "AssetOwnershipCredential"],
  "credentialSubject": {
    "assetId": "uuid",
    "assetType": "3D_MODEL",
    "ownership": 100
  }
}
```

## 🌐 Supported Blockchain Networks

| Network | Chain ID | Features | Status |
|---------|----------|----------|--------|
| Ethereum Mainnet | 1 | Full support | 🟢 Active |
| Polygon | 137 | Low fees, fast | 🟢 Active |
| Arbitrum One | 42161 | L2 scaling | 🟢 Active |
| Local Hardhat | 31337 | Development | 🟢 Active |

## 📈 Performance Metrics

- **Credential Issuance**: < 100ms
- **ZKP Generation**: < 500ms
- **Blockchain Export**: 15-60s (network dependent)
- **Presentation Creation**: < 200ms
- **Verification**: < 50ms

## 🔧 Configuration

Key configuration options in `app/core/config.py`:
- `CREDENTIAL_TTL`: Default credential validity (365 days)
- `MAX_PRESENTATION_SIZE`: Maximum credentials in presentation (10)
- `ZKP_SECURITY_LEVEL`: Cryptographic security parameter (128)
- `IPFS_TIMEOUT`: IPFS operation timeout (30s)
- `GAS_PRICE_MULTIPLIER`: Gas price buffer for transactions (1.2x)

## 📝 License

This service is part of the PlatformQ project and is licensed under the MIT License. 