# Presentation Service

Verifiable Presentation (VP) service for creating, sharing, and verifying presentations of verifiable credentials. This service handles presentation requests, selective disclosure, multi-credential presentations, and verification workflows.

## Overview

The Presentation Service provides:

- **Presentation Creation**: Build VPs from one or more credentials
- **Selective Disclosure**: Choose which attributes to reveal
- **Presentation Exchange**: Request/response workflows
- **Multi-party Verification**: Support for complex verification scenarios
- **QR Code Generation**: Mobile-friendly presentation sharing
- **Session Management**: Stateful presentation sessions
- **Template Management**: Reusable presentation templates

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Presentation Service                  │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │   FastAPI   │  │ Presentation │  │  Session   │ │
│  │     API     │  │   Manager    │  │  Manager   │ │
│  └──────┬──────┘  └──────┬───────┘  └─────┬──────┘ │
│         │                 │                 │        │
│  ┌──────▼─────────────────▼────────────────▼──────┐ │
│  │            Service Integration Layer            │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐        │ │
│  │  │Credential│  │   ZKP   │  │   DID   │  ...   │ │
│  │  └─────────┘  └─────────┘  └─────────┘        │ │
│  └─────────────────────────────────────────────────┘ │
│                                                      │
│  ┌──────────────┐  ┌────────────┐  ┌─────────────┐ │
│  │   Storage    │  │   Cache    │  │   Events    │ │
│  │              │  │            │  │             │ │
│  └──────────────┘  └────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────┘
```

## API Endpoints

### Presentation Creation

#### Create Presentation
```http
POST /api/v1/presentations
Content-Type: application/json

{
  "credential_ids": ["cred-123", "cred-456"],
  "holder_did": "did:example:holder",
  "verifier_did": "did:example:verifier",
  "selective_disclosure": {
    "cred-123": ["name", "dateOfBirth"],
    "cred-456": ["degree", "university"]
  },
  "proof_type": "BbsBlsSignatureProof2020"
}
```

#### Create from Template
```http
POST /api/v1/presentations/from-template
Content-Type: application/json

{
  "template_id": "kyc-template",
  "credential_ids": ["cred-123"],
  "holder_did": "did:example:holder"
}
```

### Presentation Exchange

#### Create Presentation Request
```http
POST /api/v1/presentation-requests
Content-Type: application/json

{
  "verifier_did": "did:example:verifier",
  "requirements": {
    "credentials": [
      {
        "type": "IDDocument",
        "attributes": ["name", "dateOfBirth", "nationality"]
      }
    ],
    "purpose": "KYC Verification"
  },
  "challenge": "random-nonce-12345"
}
```

#### Submit Presentation
```http
POST /api/v1/presentation-requests/{request_id}/submit
Content-Type: application/json

{
  "presentation": {
    "@context": [...],
    "type": ["VerifiablePresentation"],
    "verifiableCredential": [...],
    "proof": {...}
  }
}
```

### Verification

#### Verify Presentation
```http
POST /api/v1/presentations/verify
Content-Type: application/json

{
  "presentation": {...},
  "options": {
    "check_status": true,
    "verify_signature": true,
    "validate_schema": true
  }
}
```

#### Get Verification Result
```http
GET /api/v1/verifications/{verification_id}
```

### Session Management

#### Create Session
```http
POST /api/v1/sessions
Content-Type: application/json

{
  "type": "presentation_exchange",
  "participants": ["did:example:holder", "did:example:verifier"],
  "expires_in": 3600
}
```

#### Get Session Status
```http
GET /api/v1/sessions/{session_id}
```

### Templates

#### Create Template
```http
POST /api/v1/templates
Content-Type: application/json

{
  "name": "Employment Verification",
  "description": "Template for employment verification",
  "requirements": {
    "credentials": [
      {
        "type": "EmploymentCredential",
        "required_attributes": ["employer", "position", "startDate"],
        "optional_attributes": ["salary", "department"]
      }
    ]
  }
}
```

## Configuration

### Environment Variables

```bash
# Service Configuration
PRESENTATION_SERVICE_NAME=presentation-service
PRESENTATION_SERVICE_VERSION=1.0.0
PRESENTATION_HOST=0.0.0.0
PRESENTATION_PORT=8054

# Database
PRESENTATION_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost/presentations

# Service Dependencies
PRESENTATION_CREDENTIAL_SERVICE_URL=http://core-credential-service:8050
PRESENTATION_ZKP_SERVICE_URL=http://zkp-service:8052
PRESENTATION_DID_SERVICE_URL=http://did-service:8051

# Apache Ignite Cache
PRESENTATION_IGNITE_HOST=localhost
PRESENTATION_IGNITE_PORT=10800
PRESENTATION_ENABLE_CACHE=true

# Session Management
PRESENTATION_SESSION_TIMEOUT_SECONDS=3600
PRESENTATION_MAX_SESSIONS_PER_USER=10

# QR Code Generation
PRESENTATION_QR_CODE_SIZE=400
PRESENTATION_QR_CODE_ERROR_CORRECTION=M

# Event Streaming
PRESENTATION_PULSAR_URL=pulsar://localhost:6650
PRESENTATION_EVENT_TOPIC=presentation-events

# HashiCorp Vault
PRESENTATION_VAULT_ADDR=http://vault:8200
PRESENTATION_VAULT_TOKEN=<token>

# Consul
PRESENTATION_CONSUL_HOST=localhost
PRESENTATION_CONSUL_PORT=8500
```

## Presentation Types

### 1. Basic Presentation
- Single credential
- Full disclosure
- Standard signature proof

### 2. Selective Disclosure
- Choose specific attributes
- Hide sensitive data
- BBS+ signatures

### 3. Derived Presentations
- Zero-knowledge proofs
- Range proofs
- Predicate proofs

### 4. Composite Presentations
- Multiple credentials
- Cross-credential constraints
- Complex requirements

### 5. Delegated Presentations
- Present on behalf of others
- Proxy authorization
- Chain of custody

## Verification Process

1. **Syntax Check**: Valid JSON-LD structure
2. **Proof Verification**: Cryptographic signature validation
3. **Status Check**: Revocation and expiry
4. **Schema Validation**: Conformance to credential types
5. **Business Rules**: Custom verification logic
6. **Trust Chain**: Issuer and holder validation

## Session Management

### Session Types
- **Ephemeral**: Single-use presentations
- **Persistent**: Multi-use sessions
- **Interactive**: Real-time exchange

### Session Security
- Time-based expiration
- Challenge-response
- Mutual authentication
- Encrypted channels

## Integration Examples

### With Core Credential Service

```python
# Fetch credentials for presentation
credentials = []
for cred_id in credential_ids:
    response = await http_client.get(
        f"http://core-credential-service:8050/api/v1/credentials/{cred_id}"
    )
    credentials.append(response.json())
```

### With ZKP Service

```python
# Generate selective disclosure proof
proof_response = await http_client.post(
    "http://zkp-service:8052/api/v1/proofs/selective-disclosure",
    json={
        "credential": credential,
        "disclosed_attributes": disclosed_attrs,
        "nonce": challenge
    }
)
```

### With DID Service

```python
# Resolve verifier DID
did_response = await http_client.get(
    f"http://did-service:8051/api/v1/dids/{verifier_did}"
)
verifier_doc = did_response.json()["did_document"]
```

## Security Considerations

1. **Replay Protection**: Nonces and timestamps
2. **Privacy**: Minimal disclosure by default
3. **Correlation**: Unlinkable presentations
4. **Authorization**: Proper consent management
5. **Encryption**: Transport and storage security

## Standards Compliance

- **W3C VC Data Model**: Verifiable Presentations
- **DIF Presentation Exchange**: Request/response protocol
- **OpenID4VP**: OpenID for Verifiable Presentations
- **CHAPI**: Credential Handler API integration

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run migrations
alembic upgrade head

# Start service
uvicorn app.main:app --reload --port 8054
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run e2e tests
pytest tests/e2e -v
```

## Monitoring

- Health endpoint: `GET /health`
- Metrics endpoint: `GET /metrics`
- Active sessions: `GET /api/v1/stats/sessions`

## Related Services

- **core-credential-service**: Source of credentials
- **zkp-service**: Privacy-preserving proofs
- **did-service**: Identity resolution
- **verification-service**: Third-party verification 