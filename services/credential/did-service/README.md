# DID Service

The DID Service manages Decentralized Identifiers (DIDs) for PlatformQ, providing creation, resolution, and management of DIDs across multiple methods.

## 🎯 Overview

This service manages:
- DID creation (did:key, did:web, did:platformq methods)
- DID resolution and document retrieval
- Key management for DIDs
- DID document updates
- Multi-method DID support

## 🏗️ Architecture

### Technology Stack
- **Framework**: FastAPI (Python)
- **Standards**: W3C DID Core 1.0
- **Storage**: PostgreSQL for DID registry
- **Cache**: Apache Ignite for DID document caching
- **Key Storage**: HashiCorp Vault via key-management-service

### Supported DID Methods

1. **did:key**
   - Deterministic DIDs from public keys
   - No blockchain required
   - Instant creation and resolution

2. **did:web**
   - DIDs hosted on web domains
   - DNS-based trust
   - Easy integration with existing infrastructure

3. **did:platformq**
   - Platform-specific DIDs
   - Optimized for internal use
   - Fast resolution via Ignite cache

4. **did:ethr** (via blockchain services)
   - Ethereum-based DIDs
   - On-chain registry
   - Decentralized control

## 📡 API Endpoints

### DID Operations
- `POST /api/v1/dids/create` - Create new DID
- `GET /api/v1/dids/{did}` - Resolve DID to document
- `PUT /api/v1/dids/{did}` - Update DID document
- `DELETE /api/v1/dids/{did}` - Deactivate DID

### Key Management
- `POST /api/v1/dids/{did}/keys/add` - Add verification method
- `DELETE /api/v1/dids/{did}/keys/{key_id}` - Remove key
- `POST /api/v1/dids/{did}/keys/rotate` - Rotate keys

### Query Operations
- `GET /api/v1/dids` - List DIDs (filtered)
- `GET /api/v1/dids/tenant/{tenant_id}/issuer` - Get tenant's issuer DID

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL
- Apache Ignite
- Key Management Service running

### Development Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set environment variables**
   ```bash
   export DATABASE_URL="postgresql://user:pass@localhost/dids"
   export KEY_MANAGEMENT_URL="http://key-management-service:8088"
   export IGNITE_HOST="localhost"
   export IGNITE_PORT="10800"
   ```

3. **Run database migrations**
   ```bash
   alembic upgrade head
   ```

4. **Start the service**
   ```bash
   uvicorn app.main:app --reload --port 8051
   ```

## 📊 Data Models

### DID Creation Request
```json
{
  "method": "key",
  "key_type": "Ed25519",
  "options": {
    "key_alias": "signing-key-1"
  }
}
```

### DID Document Response
```json
{
  "@context": [
    "https://www.w3.org/ns/did/v1",
    "https://w3id.org/security/v2"
  ],
  "id": "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH",
  "verificationMethod": [{
    "id": "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH#key-1",
    "type": "Ed25519VerificationKey2020",
    "controller": "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH",
    "publicKeyBase58": "B12NYF8RrR3h41TDCTJojY59usg3mbtbjnFs7Eud1Y6u"
  }],
  "authentication": ["#key-1"],
  "assertionMethod": ["#key-1"]
}
```

## 🔐 Security

- All keys stored in HashiCorp Vault
- DIDs are immutable once created (only updates allowed)
- Access control via JWT tokens
- Audit logging for all DID operations

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

- DID creation: < 100ms (did:key), < 200ms (did:platformq)
- DID resolution: < 20ms (cached), < 50ms (uncached)
- Batch operations supported via Ignite
- Horizontal scaling supported

## 🔧 Configuration

Key configuration options:
- `DEFAULT_DID_METHOD`: Default method for DID creation (key)
- `ENABLE_DID_ETHR`: Enable Ethereum DIDs (false)
- `CACHE_TTL_SECONDS`: DID document cache TTL (3600)
- `MAX_KEYS_PER_DID`: Maximum verification methods (10)

## 🔗 Integration with Other Services

1. **Key Management Service**: For secure key generation and storage
2. **Core Credential Service**: DIDs used as issuers and subjects
3. **Blockchain Services**: For did:ethr operations
4. **Storage Service**: For did:web hosting

## 📝 License

This service is part of the PlatformQ project. 