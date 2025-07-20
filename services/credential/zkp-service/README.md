# ZKP Service

Zero-Knowledge Proof (ZKP) service for generating and verifying privacy-preserving proofs for verifiable credentials. This service implements BBS+ signatures, selective disclosure, and distributed proof generation using Apache Ignite.

## Overview

The ZKP Service provides:

- **BBS+ Signature Scheme**: Generate and verify BBS+ signatures for credentials
- **Selective Disclosure**: Create proofs revealing only specific attributes  
- **Range Proofs**: Prove attributes fall within ranges without revealing values
- **Set Membership**: Prove membership in sets without revealing exact values
- **Predicate Proofs**: Prove conditions (age > 18) without revealing age
- **Distributed Generation**: Leverage Apache Ignite compute grid for scalability
- **Proof Caching**: Cache frequently used proofs for performance

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   ZKP Service                        │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │   FastAPI   │  │ Proof Engine │  │   Cache    │ │
│  │     API     │  │              │  │  Manager   │ │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘ │
│         │                 │                 │        │
│  ┌──────▼──────────────────▼───────────────▼──────┐ │
│  │           Apache Ignite Compute Grid            │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐        │ │
│  │  │ Worker 1│  │ Worker 2│  │ Worker N│  ...   │ │
│  │  └─────────┘  └─────────┘  └─────────┘        │ │
│  └─────────────────────────────────────────────────┘ │
│                                                      │
│  ┌──────────────┐  ┌────────────┐  ┌─────────────┐ │
│  │   BBS+ Lib   │  │  Storage   │  │   Metrics   │ │
│  │              │  │            │  │             │ │
│  └──────────────┘  └────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────┘
```

## API Endpoints

### Proof Generation

#### Generate BBS+ Signature
```http
POST /api/v1/proofs/bbs/sign
Content-Type: application/json

{
  "credential": {
    "@context": [...],
    "type": ["VerifiableCredential"],
    "credentialSubject": {...}
  },
  "private_key_id": "key-123"
}
```

#### Create Selective Disclosure Proof
```http
POST /api/v1/proofs/selective-disclosure
Content-Type: application/json

{
  "credential": {...},
  "disclosed_attributes": ["name", "dateOfBirth"],
  "nonce": "random-challenge"
}
```

#### Generate Range Proof
```http
POST /api/v1/proofs/range
Content-Type: application/json

{
  "attribute": "age",
  "min": 18,
  "max": 100,
  "credential": {...}
}
```

#### Create Predicate Proof
```http
POST /api/v1/proofs/predicate
Content-Type: application/json

{
  "predicate": {
    "attribute": "age",
    "operator": ">=",
    "value": 21
  },
  "credential": {...}
}
```

### Proof Verification

#### Verify BBS+ Proof
```http
POST /api/v1/proofs/bbs/verify
Content-Type: application/json

{
  "proof": {...},
  "public_key": {...},
  "nonce": "random-challenge"
}
```

#### Verify Selective Disclosure
```http
POST /api/v1/proofs/selective-disclosure/verify
Content-Type: application/json

{
  "proof": {...},
  "disclosed_data": {...},
  "public_key": {...}
}
```

### Distributed Computation

#### Submit Batch Proof Generation
```http
POST /api/v1/proofs/batch
Content-Type: application/json

{
  "proofs": [
    {"type": "selective-disclosure", "params": {...}},
    {"type": "range", "params": {...}}
  ],
  "priority": "high"
}
```

#### Get Computation Status
```http
GET /api/v1/proofs/batch/{batch_id}/status
```

## Configuration

### Environment Variables

```bash
# Service Configuration
ZKP_SERVICE_NAME=zkp-service
ZKP_SERVICE_VERSION=1.0.0
ZKP_HOST=0.0.0.0
ZKP_PORT=8052

# Database
ZKP_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost/zkp

# Apache Ignite
ZKP_IGNITE_HOST=localhost
ZKP_IGNITE_PORT=10800
ZKP_ENABLE_COMPUTE_GRID=true
ZKP_WORKER_THREADS=4

# Key Management
ZKP_KEY_MANAGEMENT_URL=http://key-management-service:8088

# Caching
ZKP_CACHE_TTL_SECONDS=3600
ZKP_ENABLE_PROOF_CACHE=true

# HashiCorp Vault
ZKP_VAULT_ADDR=http://vault:8200
ZKP_VAULT_TOKEN=<token>

# Consul
ZKP_CONSUL_HOST=localhost
ZKP_CONSUL_PORT=8500
```

## Proof Types

### 1. BBS+ Signatures
- Multi-message signatures
- Selective disclosure without re-issuing
- Unlinkable proofs

### 2. Range Proofs
- Prove value in range without revealing
- Support for integers and dates
- Configurable precision

### 3. Set Membership
- Prove membership without revealing which element
- Efficient for large sets
- Bloom filter optimization

### 4. Predicate Proofs
- Greater than/less than comparisons
- Equality without revealing value
- Complex boolean expressions

### 5. Composite Proofs
- Combine multiple proof types
- AND/OR logical operations
- Nested conditions

## Apache Ignite Integration

### Compute Grid Setup

```python
# Distributed proof generation
@ignite.compute()
async def generate_proof_distributed(
    proof_type: str,
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """Generate proof on compute grid"""
    # Proof generation logic
    pass
```

### Caching Strategy

1. **Proof Templates**: Cache common proof structures
2. **Public Keys**: Cache issuer public keys
3. **Computation Results**: Cache expensive computations
4. **Verification Results**: Short-term cache for verifications

## Security Considerations

1. **Key Management**: Private keys never leave key-management-service
2. **Nonce Handling**: Fresh nonces for unlinkability
3. **Timing Attacks**: Constant-time operations where possible
4. **Resource Limits**: Prevent DoS through computation limits

## Performance Optimization

1. **Batch Processing**: Group similar proofs
2. **Parallel Generation**: Use Ignite compute grid
3. **Proof Caching**: Cache frequently requested proofs
4. **Pre-computation**: Pre-compute common values

## Integration Examples

### With Core Credential Service

```python
# Request ZKP for credential
response = await http_client.post(
    "http://zkp-service:8052/api/v1/proofs/selective-disclosure",
    json={
        "credential": credential,
        "disclosed_attributes": ["name", "dateOfBirth"],
        "nonce": generate_nonce()
    }
)
proof = response.json()
```

### With Presentation Service

```python
# Include ZKP in presentation
presentation["proof"].append({
    "type": "BbsBlsSignatureProof2020",
    "proofValue": proof["proofValue"],
    "nonce": proof["nonce"]
})
```

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run migrations
alembic upgrade head

# Start service
uvicorn app.main:app --reload --port 8052
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run performance tests
pytest tests/performance -v
```

## Monitoring

- Health endpoint: `GET /health`
- Metrics endpoint: `GET /metrics`
- Computation stats: `GET /api/v1/stats/compute`

## Related Services

- **core-credential-service**: Issues credentials requiring ZKPs
- **key-management-service**: Manages cryptographic keys
- **presentation-service**: Uses ZKPs in presentations
- **did-service**: Resolves DIDs for key discovery 