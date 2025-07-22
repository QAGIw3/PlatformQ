# Core Credential Service

The Core Credential Service handles W3C-compliant verifiable credential issuance, verification, and management for PlatformQ. This service focuses exclusively on core credential operations, delegating specialized functions to other microservices.

## 🎯 Overview

This service manages:
- W3C Verifiable Credential issuance
- Credential verification and validation
- Credential revocation management
- Credential status checking
- Basic credential storage and retrieval

## 🏗️ Architecture

### Technology Stack
- **Framework**: FastAPI (Python)
- **Standards**: W3C VC Data Model 1.1
- **Storage**: PostgreSQL for metadata, IPFS/MinIO for credential data
- **Cache**: Apache Ignite for performance
- **Messaging**: Apache Pulsar for event-driven architecture

### Key Features

1. **W3C Compliance**
   - Full W3C VC Data Model 1.1 support
   - JSON-LD context handling
   - Standard proof formats (Ed25519Signature2020)

2. **Credential Management**
   - Issue credentials with proper signatures
   - Verify credential validity and signatures
   - Manage credential lifecycle (issuance to revocation)

3. **Integration Points**
   - Uses blockchain services for anchoring
   - Delegates ZKP operations to zkp-service
   - Integrates with DID service for identity resolution
   - Publishes events via Pulsar

## 📡 API Endpoints

### Credential Operations
- `POST /api/v1/credentials/issue` - Issue new verifiable credential
- `POST /api/v1/credentials/verify` - Verify credential validity
- `GET /api/v1/credentials/{id}` - Retrieve credential by ID
- `POST /api/v1/credentials/{id}/revoke` - Revoke a credential
- `GET /api/v1/credentials/{id}/status` - Check credential status

### Batch Operations
- `POST /api/v1/credentials/batch/issue` - Issue multiple credentials
- `POST /api/v1/credentials/batch/verify` - Verify multiple credentials

### Query Operations
- `GET /api/v1/credentials` - List credentials with filters
- `GET /api/v1/credentials/search` - Search credentials

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL
- Apache Ignite
- Apache Pulsar

### Development Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**
   ```bash
   export DATABASE_URL="postgresql://user:pass@localhost/credentials"
   export PULSAR_URL="pulsar://localhost:6650"
   export IGNITE_HOST="localhost"
   export IGNITE_PORT="10800"
   export STORAGE_SERVICE_URL="http://storage-service:8000"
   ```

3. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

4. **Start the service**
   ```bash
   uvicorn app.main:app --reload --port 8050
   ```

## 📊 Data Models

### Credential Issuance Request
```json
{
  "credential_type": "AchievementCredential",
  "subject": {
    "id": "did:platformq:user123",
    "achievement": "First Asset Created",
    "level": "Bronze",
    "points": 100
  },
  "issuer_did": "did:platformq:issuer",
  "validity_days": 365
}
```

### Verifiable Credential Response
```json
{
  "@context": [
    "https://www.w3.org/2018/credentials/v1",
    "https://platformq.com/contexts/v1"
  ],
  "id": "urn:uuid:12345678-1234-5678-1234-567812345678",
  "type": ["VerifiableCredential", "AchievementCredential"],
  "issuer": "did:platformq:issuer",
  "issuanceDate": "2024-01-01T00:00:00Z",
  "credentialSubject": {
    "id": "did:platformq:user123",
    "achievement": "First Asset Created",
    "level": "Bronze",
    "points": 100
  },
  "proof": {
    "type": "Ed25519Signature2020",
    "created": "2024-01-01T00:00:00Z",
    "verificationMethod": "did:platformq:issuer#key-1",
    "proofPurpose": "assertionMethod",
    "proofValue": "..."
  }
}
```

## 🔐 Security

- All credentials are signed using issuer's private keys
- Keys are managed through the key-management-service
- Credentials can be encrypted at rest if configured
- API authentication via JWT tokens

## 🧪 Testing

```bash
# Run unit tests
pytest tests/unit -v

# Run integration tests
pytest tests/integration -v

# Run with coverage
pytest --cov=app tests/
```

## 📈 Performance

- Credential issuance: < 50ms (cached issuer)
- Credential verification: < 30ms (cached)
- Batch operations: Linear scaling with Ignite
- Storage: Async operations for non-blocking performance

## 🔧 Configuration

Key configuration options:
- `CREDENTIAL_DEFAULT_VALIDITY_DAYS`: Default credential validity (365)
- `MAX_BATCH_SIZE`: Maximum credentials in batch operation (100)
- `CACHE_TTL_SECONDS`: Cache time-to-live (3600)
- `ENABLE_BLOCKCHAIN_ANCHORING`: Enable blockchain anchoring (true)

## 🔗 Integration with Other Services

1. **Storage Service**: For IPFS/MinIO credential storage
2. **Blockchain Services**: For optional anchoring
3. **DID Service**: For DID resolution
4. **Key Management Service**: For signing operations
5. **Event Monitoring Service**: For blockchain events

## 📝 License

This service is part of the PlatformQ project. 