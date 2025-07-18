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
- **NEW**: AML compliance zero-knowledge proofs
- **NEW**: Distributed ZKP computation using Apache Ignite

## 🏗️ Architecture

### Technology Stack
- **Framework**: FastAPI (Python)
- **Blockchain**: Web3.py, Ethereum/Polygon/Arbitrum
- **Smart Contracts**: Solidity, Hardhat
- **Cryptography**: py-ecc, cryptography, pynacl
- **Standards**: W3C VC, DID, JSON-LD
- **Storage**: IPFS, PostgreSQL
- **Cache**: Apache Ignite
- **Distributed Compute**: Apache Ignite Compute Grid

### Key Components

1. **Credential Management**
   - W3C VC Data Model 1.1 compliance
   - Multiple credential types (achievements, qualifications, reputation, compliance)
   - Blockchain anchoring for tamper-evidence
   - IPFS storage for decentralization

2. **Zero-Knowledge Proofs**
   - Selective disclosure of attributes
   - Range proofs (age > 18, score > threshold, risk < limit)
   - Set membership proofs
   - BBS+ signatures for privacy
   - **NEW**: AML compliance proofs
   - **NEW**: Distributed proof generation

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

5. **Compliance Integration** (NEW)
   - KYC credential issuance and verification
   - AML risk assessment credentials
   - Sanctions check credentials
   - Transaction monitoring summaries
   - Privacy-preserving compliance proofs

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

### KYC Zero-Knowledge Proofs
- `POST /api/v1/credentials/kyc/create` - Create KYC credential
- `POST /api/v1/zkp/kyc/generate-proof` - Generate KYC ZK proof
- `POST /api/v1/zkp/kyc/verify-proof` - Verify KYC proof
- `POST /api/v1/zkp/kyc/selective-disclosure` - Selective attribute disclosure

### AML Zero-Knowledge Proofs (NEW)
- `POST /api/v1/credentials/aml/create` - Create AML compliance credential
- `POST /api/v1/zkp/aml/generate-proof` - Generate AML compliance proof
- `POST /api/v1/zkp/aml/verify-proof` - Verify AML compliance proof
- `POST /api/v1/compliance/aml/check` - Check AML compliance status
- `GET /api/v1/zkp/aml/templates` - Get available proof templates
- `POST /api/v1/zkp/aml/batch-verify` - Batch verify multiple proofs

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
- Apache Ignite 2.14+ (for distributed compute)
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

3. **Start Apache Ignite**
   ```bash
   ignite.sh config/ignite-config.xml
   ```

4. **Compile smart contracts**
   ```bash
   npx hardhat compile
   ```

5. **Deploy contracts (local)**
   ```bash
   npx hardhat node  # In one terminal
   npx hardhat run scripts/deploy_contracts.js --network localhost
   ```

6. **Set environment variables**
   ```bash
   export DATABASE_URL="postgresql://user:pass@localhost/verifiable_credentials"
   export BRIDGE_PRIVATE_KEY="0x..."  # Your wallet private key
   export IPFS_API_URL="http://localhost:5001"
   export ETHEREUM_RPC_URL="http://localhost:8545"
   export IGNITE_HOSTS="localhost:10800"
   ```

7. **Start the service**
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
- **ZKP**: BBS+, Bulletproofs, Groth16

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

### KYC Credentials
```json
{
  "@context": ["https://www.w3.org/2018/credentials/v1"],
  "type": ["VerifiableCredential", "KYCCredential"],
  "credentialSubject": {
    "kycLevel": 2,
    "verifiedAttributes": ["identity", "address", "age_over_18"],
    "jurisdiction": "US"
  }
}
```

### AML Compliance Credentials (NEW)
```json
{
  "@context": ["https://www.w3.org/2018/credentials/v1"],
  "type": ["VerifiableCredential", "AMLComplianceCredential"],
  "credentialSubject": {
    "riskScore": 0.3,
    "riskLevel": "LOW",
    "sanctionsCheck": "CLEAR",
    "lastAssessment": "2024-01-15T10:00:00Z"
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
- **ZKP Generation**: < 500ms (single core), < 100ms (distributed)
- **Blockchain Export**: 15-60s (network dependent)
- **Presentation Creation**: < 200ms
- **Verification**: < 50ms
- **Batch ZKP Generation**: Linear scaling with Ignite nodes

## 🚀 Distributed ZKP Computation (NEW)

### Apache Ignite Integration
The service now supports distributed zero-knowledge proof generation using Apache Ignite's compute grid:

```python
# Submit batch ZKP tasks for parallel processing
task_ids = await zkp_compute.submit_batch_tasks([
    {"proof_type": "kyc", "input_data": {...}, "circuit_name": "kyc_circuit"},
    {"proof_type": "aml", "input_data": {...}, "circuit_name": "aml_circuit"},
    # ... more tasks
])

# Get results
results = await zkp_compute.get_batch_results(task_ids)
```

### Performance Benefits
- **Parallel Processing**: Generate multiple proofs simultaneously
- **Horizontal Scaling**: Add Ignite nodes for linear performance improvement
- **Fault Tolerance**: Automatic failover and retry
- **Resource Optimization**: Efficient CPU utilization across cluster

### Configuration
```yaml
# ignite-config.xml
<property name="cacheConfiguration">
    <list>
        <bean class="org.apache.ignite.configuration.CacheConfiguration">
            <property name="name" value="zkp_tasks"/>
            <property name="cacheMode" value="PARTITIONED"/>
            <property name="backups" value="1"/>
        </bean>
    </list>
</property>
```

## 🔧 Configuration

Key configuration options in `app/core/config.py`:
- `CREDENTIAL_TTL`: Default credential validity (365 days)
- `MAX_PRESENTATION_SIZE`: Maximum credentials in presentation (10)
- `ZKP_SECURITY_LEVEL`: Cryptographic security parameter (128)
- `IPFS_TIMEOUT`: IPFS operation timeout (30s)
- `GAS_PRICE_MULTIPLIER`: Gas price buffer for transactions (1.2x)
- `IGNITE_WORKER_COUNT`: Number of ZKP compute workers (4)
- `ZKP_TASK_TIMEOUT`: Maximum proof generation time (300s)

## 🔗 Compliance Service Integration

### Seamless KYC/AML Flow
1. User completes KYC verification in Compliance Service
2. Compliance Service issues KYC credential via this service
3. User can generate ZK proofs for any platform
4. Platforms verify compliance without accessing personal data

### Privacy-Preserving Compliance
- Prove age without revealing birthdate
- Prove non-sanctioned status without revealing identity
- Prove risk level without revealing transaction history
- Prove jurisdiction without revealing address

## 📝 License

This service is part of the PlatformQ project and is licensed under the MIT License. 