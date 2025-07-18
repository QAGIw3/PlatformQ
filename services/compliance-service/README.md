# Compliance Service

## Overview

The Compliance Service provides comprehensive KYC (Know Your Customer), AML (Anti-Money Laundering), and regulatory compliance capabilities for PlatformQ. It integrates with the Verifiable Credential Service to enable privacy-preserving compliance through zero-knowledge proofs.

## Features

### Core Capabilities
- **Multi-tier KYC Verification**: Support for Tier 1 (Basic), Tier 2 (Enhanced), and Tier 3 (Institutional) verification levels
- **AML Monitoring**: Real-time transaction monitoring and risk assessment
- **Sanctions Screening**: Integration with OFAC, EU, UN, and other sanctions databases
- **Risk Scoring**: Dynamic risk assessment based on multiple factors
- **Regulatory Reporting**: Automated SAR, CTR, and other regulatory report generation
- **Identity Verification**: Integration with multiple identity verification providers (Jumio, Onfido)

### Privacy-Preserving Features (NEW)
- **Verifiable Credentials Integration**: Issue W3C-compliant credentials for compliance status
- **Zero-Knowledge Proofs**: Prove compliance without revealing personal data
- **Selective Disclosure**: Share only required compliance attributes
- **Cross-Platform Portability**: Reusable compliance credentials across services

## Architecture

### Technology Stack
- **Framework**: FastAPI (Python 3.10+)
- **Caching**: Apache Ignite (distributed cache and compute grid)
- **Messaging**: Apache Pulsar
- **Configuration**: HashiCorp Vault & Consul
- **Storage**: PostgreSQL, MinIO (documents)
- **Search**: Elasticsearch

### Integration Points
- **Verifiable Credential Service**: For issuing and verifying compliance credentials
- **Blockchain Gateway Service**: For on-chain compliance checks
- **Auth Service**: For user authentication and authorization
- **Notification Service**: For compliance alerts and notifications

## API Endpoints

### KYC Endpoints
```
POST   /api/v1/kyc/initiate               - Start KYC verification
POST   /api/v1/kyc/{id}/documents         - Upload verification documents
POST   /api/v1/kyc/{id}/submit            - Submit for verification
GET    /api/v1/kyc/status                 - Get KYC status
GET    /api/v1/kyc/history                - Get verification history
POST   /api/v1/kyc/refresh                - Refresh expired KYC
DELETE /api/v1/kyc/documents/{id}         - Delete pending document
```

### AML Endpoints
```
POST   /api/v1/aml/screen-transaction     - Screen transaction for AML risks
POST   /api/v1/aml/check-sanctions        - Check entity against sanctions
GET    /api/v1/aml/risk-assessment/{id}   - Get risk assessment
POST   /api/v1/aml/report-suspicious      - Report suspicious activity
GET    /api/v1/aml/transaction-patterns   - Analyze patterns
POST   /api/v1/aml/enhanced-due-diligence - Trigger EDD
GET    /api/v1/aml/watchlist-matches      - Get watchlist matches
```

### Risk Management
```
GET    /api/v1/risk/profile/{id}          - Get risk profile
POST   /api/v1/risk/calculate             - Calculate risk score
GET    /api/v1/risk/thresholds            - Get risk thresholds
PUT    /api/v1/risk/thresholds            - Update thresholds
GET    /api/v1/risk/history/{id}          - Get risk history
```

### Transaction Monitoring
```
GET    /api/v1/monitoring/alerts          - Get monitoring alerts
POST   /api/v1/monitoring/alerts/{id}/investigate - Investigate alert
POST   /api/v1/monitoring/alerts/{id}/resolve     - Resolve alert
GET    /api/v1/monitoring/rules           - Get monitoring rules
POST   /api/v1/monitoring/rules           - Create monitoring rule
PUT    /api/v1/monitoring/rules/{id}      - Update rule
GET    /api/v1/monitoring/statistics      - Get statistics
POST   /api/v1/monitoring/test-rule       - Test rule
```

### Regulatory Reporting
```
POST   /api/v1/reporting/generate         - Generate regulatory report
POST   /api/v1/reporting/sar/submit       - Submit SAR
GET    /api/v1/reporting/history          - Get reporting history
GET    /api/v1/reporting/deadlines        - Get upcoming deadlines
GET    /api/v1/reporting/jurisdictions    - Get supported jurisdictions
GET    /api/v1/reporting/download/{id}    - Download report
POST   /api/v1/reporting/schedule         - Schedule recurring report
GET    /api/v1/reporting/statistics       - Get statistics
```

## Configuration

### Environment Variables
```bash
# Service Configuration
SERVICE_HOST=localhost
SERVICE_PORT=8001

# HashiCorp Vault
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=<your-vault-token>

# HashiCorp Consul
CONSUL_HOST=localhost
CONSUL_PORT=8500

# Apache Ignite
IGNITE_HOSTS=localhost:10800,localhost:10801

# Apache Pulsar
PULSAR_URL=pulsar://localhost:6650

# Compliance Providers
CHAINALYSIS_API_KEY=<stored-in-vault>
ELLIPTIC_API_KEY=<stored-in-vault>
TRM_LABS_API_KEY=<stored-in-vault>
```

### Vault Configuration Structure
```
compliance-service/
├── config/
│   ├── identity_providers
│   ├── identity_api_keys
│   ├── sanctions_databases
│   ├── document_storage_url
│   ├── kyc_encryption_key
│   ├── ignite_hosts
│   ├── pulsar_url
│   └── issuer_did
└── providers/
    ├── chainalysis/api_key
    ├── elliptic/api_key
    └── trm_labs/api_key
```

## Verifiable Credentials Integration

### Issuing Compliance Credentials
The service automatically issues verifiable credentials upon successful compliance checks:

1. **KYC Credentials**: Issued after identity verification
2. **AML Risk Credentials**: Issued after risk assessment
3. **Sanctions Check Credentials**: Time-bounded clearance certificates
4. **Transaction Monitoring Credentials**: Compliance summaries

### Zero-Knowledge Proof Support
Users can prove compliance without revealing personal data:

```python
# Example: Prove KYC compliance for DeFi
proof = await vc_integration.generate_kyc_proof(
    credential_id="vc_kyc_123",
    holder_did="did:platform:user123",
    attributes_to_prove=["age_over_18", "identity_verified", "country_not_sanctioned"],
    min_kyc_level=2
)

# DeFi protocol verifies without accessing personal data
result = await vc_integration.verify_kyc_proof(proof_id, required_attributes, min_level)
```

## Apache Ignite Integration

### Distributed Caching
The service uses Apache Ignite for high-performance distributed caching:
- Transaction history caching
- Risk assessment results
- Pattern detection data
- User compliance status

### Compute Grid
Ignite's compute grid enables:
- Parallel ZKP generation
- Distributed risk calculations
- Map-reduce pattern analysis
- Batch compliance checks

### Cache Configuration
```python
# Cache names and purposes
caches = {
    "aml_transactions": "Transaction history and patterns",
    "aml_risk_assessments": "Risk assessment results",
    "aml_alerts": "Monitoring alerts",
    "aml_patterns": "Pattern detection data",
    "zkp_tasks": "ZKP generation tasks",
    "zkp_results": "Generated proof results"
}
```

## Installation

### Prerequisites
- Python 3.10+
- Apache Ignite 2.14+
- Apache Pulsar 2.11+
- HashiCorp Vault 1.12+
- HashiCorp Consul 1.14+
- PostgreSQL 14+
- Elasticsearch 8.0+

### Setup

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Vault**
   ```bash
   vault kv put compliance-service/config @config.json
   vault kv put compliance-service/providers/chainalysis api_key="your-key"
   ```

3. **Register with Consul**
   ```bash
   consul services register -name=compliance-service -port=8001
   ```

4. **Start Apache Ignite**
   ```bash
   ignite.sh config/ignite-config.xml
   ```

5. **Run the service**
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8001
   ```

## Testing

### Unit Tests
```bash
pytest tests/unit -v --cov=app
```

### Integration Tests
```bash
pytest tests/integration -v
```

### Test Coverage
- KYC VC Integration: `tests/test_kyc_vc_integration.py`
- AML VC Integration: `tests/test_aml_vc_integration.py`
- Ignite Cache: `tests/test_ignite_cache.py`

## Monitoring

### Health Check
```
GET /health
```

### Metrics
The service exposes Prometheus metrics:
- `compliance_kyc_verifications_total`
- `compliance_aml_checks_total`
- `compliance_risk_scores`
- `compliance_zkp_generation_duration`
- `ignite_cache_hit_rate`

### Logging
Structured logging with correlation IDs for request tracing.

## Security

### Data Protection
- All PII encrypted at rest using AES-256
- Document encryption before storage
- Secure key management via HashiCorp Vault

### API Security
- OAuth2/JWT authentication
- Rate limiting per endpoint
- IP whitelisting for admin endpoints

### Compliance
- GDPR compliant with right to erasure
- SOC2 Type II certified infrastructure
- Regular security audits

## Deployment

### Docker
```bash
docker build -t compliance-service .
docker run -p 8001:8001 compliance-service
```

### Kubernetes
```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/configmap.yaml
```

### Scaling
- Horizontal scaling supported via Kubernetes HPA
- Ignite cluster scales independently
- Pulsar handles message partitioning

## Troubleshooting

### Common Issues

1. **Ignite Connection Failed**
   - Check Ignite cluster is running
   - Verify network connectivity
   - Check firewall rules for port 10800

2. **Vault Access Denied**
   - Verify Vault token is valid
   - Check policy permissions
   - Ensure path is correct

3. **VC Service Integration Error**
   - Check VC service is registered in Consul
   - Verify network connectivity
   - Check API compatibility

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 