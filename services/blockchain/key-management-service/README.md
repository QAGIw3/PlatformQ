# Key Management Service

A secure, high-performance microservice that provides cryptographic key management and transaction signing capabilities for blockchain operations, integrated with HashiCorp Vault for maximum security.

## Overview

The Key Management Service is a critical security component of PlatformQ's blockchain infrastructure. It manages cryptographic keys, performs transaction signing, and provides secure key generation and storage capabilities. All private keys are stored in HashiCorp Vault's Transit engine, ensuring keys never exist in plaintext outside of Vault's secure environment.

## Key Features

- **Secure Key Storage**: Private keys stored in HashiCorp Vault Transit engine
- **Multi-Chain Support**: Key management for Ethereum, Solana, Cosmos, and NEAR
- **Transaction Signing**: Secure signing without exposing private keys
- **Key Generation**: Cryptographically secure key pair generation
- **HD Wallet Support**: Hierarchical Deterministic wallet derivation
- **Key Rotation**: Automated key rotation with zero downtime
- **Multi-Signature**: Support for multi-sig wallets and threshold signing
- **Hardware Security Module (HSM)**: Optional HSM backend support
- **Audit Logging**: Complete audit trail of all key operations
- **Access Control**: Fine-grained permissions with Vault policies
- **High Availability**: Clustered deployment with automatic failover

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                  Key Manager Core                        │
├─────────────┬──────────────┬───────────────────────────┤
│ Key Store   │   Signer     │    Access Controller      │
├─────────────┴──────────────┴───────────────────────────┤
│              HashiCorp Vault Client                     │
├─────────────────────────────────────────────────────────┤
│     Vault Transit Engine    │    Vault KV Engine       │
│    (Signing & Encryption)   │   (Metadata Storage)     │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Key Management
- `POST /api/v1/keys` - Generate new key pair
- `GET /api/v1/keys` - List keys (metadata only)
- `GET /api/v1/keys/{key_id}` - Get key details (public key only)
- `DELETE /api/v1/keys/{key_id}` - Mark key for deletion
- `POST /api/v1/keys/{key_id}/rotate` - Rotate key

### Signing Operations
- `POST /api/v1/sign/transaction` - Sign blockchain transaction
- `POST /api/v1/sign/message` - Sign arbitrary message
- `POST /api/v1/sign/typed-data` - Sign EIP-712 typed data
- `POST /api/v1/sign/batch` - Batch signing operation

### HD Wallet Operations
- `POST /api/v1/wallets/hd` - Create HD wallet
- `POST /api/v1/wallets/hd/{wallet_id}/derive` - Derive child key
- `GET /api/v1/wallets/hd/{wallet_id}/addresses` - List derived addresses

### Verification
- `POST /api/v1/verify/signature` - Verify signature
- `POST /api/v1/verify/address` - Verify address ownership

### Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics endpoint

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=key-management-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8002
ENVIRONMENT=production

# Vault Configuration
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=${VAULT_TOKEN}
VAULT_TRANSIT_MOUNT=transit
VAULT_KV_MOUNT=secret
VAULT_NAMESPACE=platformq

# Security Configuration
ENABLE_HSM=false
HSM_MODULE_PATH=/usr/lib/softhsm/libsofthsm2.so
KEY_ROTATION_DAYS=90
MAX_KEY_AGE_DAYS=365

# Rate Limiting
RATE_LIMIT_ENABLED=true
RATE_LIMIT_PER_MINUTE=100
RATE_LIMIT_PER_HOUR=1000

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_AUDIT_TOPIC=persistent://platformq/security/key-audit

# Consul Configuration
CONSUL_HOST=consul
CONSUL_PORT=8500

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
AUDIT_LOG_ENABLED=true
```

## Dependencies

- **FastAPI**: REST API framework
- **hvac**: HashiCorp Vault client
- **cryptography**: Cryptographic operations
- **eth-account**: Ethereum account management
- **py-sr25519-bindings**: Substrate/Polkadot key management
- **ed25519**: Ed25519 signature operations
- **prometheus-client**: Metrics collection
- **python-consul**: Service discovery
- **structlog**: Structured logging

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t key-management-service .

# Run the container
docker run -d \
  --name key-management \
  -p 8002:8002 \
  -e VAULT_ADDR="http://vault:8200" \
  -e VAULT_TOKEN="your-vault-token" \
  -e PULSAR_SERVICE_URL="pulsar://pulsar:6650" \
  key-management-service
```

### Using Docker Compose

```yaml
services:
  key-management:
    build: ./services/blockchain/key-management-service
    ports:
      - "8002:8002"
    environment:
      - VAULT_ADDR=http://vault:8200
      - VAULT_TOKEN=${VAULT_TOKEN}
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
    depends_on:
      - vault
      - pulsar
      - consul
    volumes:
      - /dev/bus/usb:/dev/bus/usb  # For HSM support
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export VAULT_ADDR="http://localhost:8200"
export VAULT_TOKEN="your-dev-token"

# Initialize Vault (first time only)
python scripts/init_vault.py

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8002 --reload
```

## Security Architecture

### Key Storage

1. **Transit Engine**: All private keys stored in Vault's Transit engine
2. **Encryption**: Keys encrypted at rest with AES-256-GCM
3. **Key Wrapping**: Optional key wrapping for additional security
4. **Memory Protection**: Keys never loaded into application memory

### Access Control

```hcl
# Example Vault Policy
path "transit/sign/platformq-*" {
  capabilities = ["create", "update"]
}

path "transit/keys/platformq-*" {
  capabilities = ["read"]
}

path "secret/data/platformq/keys/*" {
  capabilities = ["create", "read", "update", "delete"]
}
```

### Audit Trail

All operations logged with:
- Timestamp
- Operation type
- Key identifier
- Requester identity
- IP address
- Success/failure status

## Monitoring

### Health Checks

The service provides detailed health status at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "vault": {
    "connected": true,
    "sealed": false,
    "version": "1.15.0"
  },
  "keys": {
    "total": 1250,
    "active": 1200,
    "rotating": 5,
    "expired": 45
  },
  "operations": {
    "signing_queue": 3,
    "rate_limit_remaining": 97
  }
}
```

### Metrics

Prometheus metrics available at `/metrics`:

- `key_mgmt_operations_total` - Total operations by type
- `key_mgmt_operation_duration_seconds` - Operation latency
- `key_mgmt_keys_total` - Total keys by status
- `key_mgmt_signing_errors_total` - Signing errors by type
- `key_mgmt_vault_requests_total` - Vault API requests
- `key_mgmt_rate_limit_exceeded_total` - Rate limit violations

### Audit Events

Audit events published to Pulsar:

```json
{
  "timestamp": "2024-01-10T10:00:00Z",
  "service": "key-management-service",
  "operation": "sign_transaction",
  "key_id": "platformq-eth-prod-001",
  "chain": "ethereum",
  "requester": "transaction-processor-service",
  "ip_address": "10.0.1.50",
  "success": true,
  "duration_ms": 125
}
```

## Key Operations

### Key Generation Flow

1. **Request**: Client requests new key generation
2. **Validation**: Request validated and authorized
3. **Generation**: Key generated in Vault Transit engine
4. **Metadata**: Key metadata stored in KV engine
5. **Response**: Public key and key ID returned

### Transaction Signing Flow

1. **Request**: Unsigned transaction received
2. **Validation**: Transaction structure validated
3. **Authorization**: Requester permissions verified
4. **Signing**: Transaction signed in Vault
5. **Audit**: Operation logged
6. **Response**: Signed transaction returned

## Security Best Practices

1. **Vault Token Rotation**: Rotate Vault tokens regularly
2. **TLS Everywhere**: All communication over TLS
3. **Principle of Least Privilege**: Minimal permissions per service
4. **Key Rotation**: Automatic key rotation based on age/usage
5. **Audit Review**: Regular audit log analysis
6. **Backup Strategy**: Regular Vault backup to secure storage

## Troubleshooting

### Common Issues

1. **Vault Connection Errors**
   - Verify Vault is unsealed
   - Check network connectivity
   - Validate authentication token

2. **Signing Failures**
   - Check key permissions in Vault
   - Verify transaction format
   - Review rate limits

3. **Performance Issues**
   - Monitor Vault performance
   - Check connection pool settings
   - Review concurrent request limits

### Debug Commands

```bash
# Check Vault status
vault status

# List keys in transit engine
vault list transit/keys

# View key configuration
vault read transit/keys/platformq-eth-prod-001

# Test signing
curl -X POST http://localhost:8002/api/v1/sign/message \
  -H "Content-Type: application/json" \
  -d '{"key_id": "test-key", "message": "Hello World"}'
```

## Disaster Recovery

1. **Vault Backup**: Regular automated backups
2. **Key Recovery**: Secure recovery process
3. **Failover**: Automatic failover to standby
4. **Audit Preservation**: Audit logs backed up separately

## Contributing

1. Security review required for all changes
2. Follow cryptographic best practices
3. Comprehensive testing mandatory
4. Update security documentation

## License

Copyright © 2024 PlatformQ. All rights reserved. 