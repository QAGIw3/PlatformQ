# Data Governance Service

Unified data governance service providing quality management, compliance, privacy, and policy enforcement capabilities.

## Overview

The Data Governance Service consolidates all data governance functionality:
- **Data Quality Management**: Comprehensive quality checks, profiling, and remediation
- **Compliance Management**: Multi-framework compliance scanning and reporting
- **Privacy Management**: GDPR, CCPA, and other privacy request handling
- **Policy Engine**: Flexible policy definition and enforcement
- **Data Contracts**: SLA management between data producers and consumers
- **Access Control**: Fine-grained data access management

## Architecture

The service is built using:
- **FastAPI** for REST APIs
- **data-intelligence-common** library for shared functionality
- **Apache Ignite** for distributed caching
- **Elasticsearch** for search and analytics
- **Cassandra** for persistent storage
- **MinIO** for object storage
- **Apache Pulsar** for event streaming
- **ML Platform Service** for ML-powered quality insights

## Features

### Data Quality
- Multi-dimensional quality assessment (completeness, accuracy, consistency, etc.)
- ML-powered anomaly detection
- Custom rule definition (SQL, Python, ML-based)
- Automated remediation workflows
- Quality trend analysis and predictions
- Integration with Great Expectations and Soda Core

### Compliance
- Support for multiple frameworks (GDPR, CCPA, HIPAA, SOC2, etc.)
- Automated compliance scanning
- Evidence collection and reporting
- Policy violation detection
- Audit trail management

### Privacy
- Privacy request handling (access, deletion, portability)
- PII detection and classification
- Data masking and anonymization
- Consent management
- Right to be forgotten implementation

### Policy Management
- Declarative policy definition
- Real-time policy evaluation
- Policy versioning and approval workflows
- Exception handling
- Integration with OPA (Open Policy Agent)

### Data Contracts
- Contract definition and versioning
- SLA monitoring
- Schema evolution management
- Quality guarantees
- Breach detection and notifications

## API Endpoints

### Quality Management
- `POST /api/v1/quality/check` - Run quality checks
- `GET /api/v1/quality/profile/{entity_id}` - Get quality profile
- `GET /api/v1/quality/history/{entity_id}` - Get quality history
- `POST /api/v1/quality/rules` - Create quality rule
- `GET /api/v1/quality/incidents` - List quality incidents
- `POST /api/v1/quality/remediation/trigger` - Trigger remediation

### Governance
- `GET /api/v1/governance/policies` - List policies
- `POST /api/v1/governance/policies` - Create policy
- `POST /api/v1/governance/evaluate` - Evaluate policy

### Compliance
- `GET /api/v1/compliance/reports` - List compliance reports
- `POST /api/v1/compliance/scan` - Run compliance scan
- `GET /api/v1/compliance/frameworks` - List supported frameworks

### Privacy
- `GET /api/v1/privacy/requests` - List privacy requests
- `POST /api/v1/privacy/requests` - Submit privacy request
- `GET /api/v1/privacy/pii/scan` - Scan for PII

### Contracts
- `GET /api/v1/contracts` - List data contracts
- `POST /api/v1/contracts` - Create contract
- `GET /api/v1/contracts/{id}/compliance` - Check contract compliance

## Configuration

Environment variables:
```bash
# Service Configuration
SERVICE_NAME=data-governance-service
SERVICE_VERSION=2.0.0
ENVIRONMENT=production

# Quality Engine
QUALITY_ENGINE_ENABLED=true
ML_QUALITY_ENABLED=true
AUTO_REMEDIATION_ENABLED=false

# Compliance
COMPLIANCE_FRAMEWORKS=["GDPR", "CCPA", "HIPAA"]
COMPLIANCE_SCAN_INTERVAL_HOURS=24

# Infrastructure
IGNITE_HOST=ignite
IGNITE_PORT=10800
ELASTICSEARCH_HOSTS=["elasticsearch:9200"]
CASSANDRA_HOSTS=["cassandra"]
MINIO_ENDPOINT=minio:9000
PULSAR_URL=pulsar://pulsar:6650

# Integration Services
ML_PLATFORM_SERVICE_URL=http://ml-platform-service:8000
DATA_PLATFORM_SERVICE_URL=http://data-platform-service:8000

# Vault/Consul
VAULT_URL=http://vault:8200
CONSUL_URL=http://consul:8500
```

## Development

### Setup
```bash
cd services/DataIntelligenceSuite/data-governance-service
pip install -r requirements.txt
```

### Run locally
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Run tests
```bash
pytest tests/
```

## Deployment

### Docker
```bash
docker build -t data-governance-service .
docker run -p 8000:8000 data-governance-service
```

### Kubernetes
```bash
kubectl apply -f iac/kubernetes/charts/data-governance-service/
```

## Migration Notes

This service consolidates functionality from:
- `unified-quality-service` → Quality management features
- Legacy quality modules → Enhanced with ML capabilities
- New governance features → Compliance, privacy, contracts

### Key Improvements
1. **Unified Architecture**: Single service for all governance needs
2. **Common Library**: Leverages `data-intelligence-common` for shared functionality
3. **ML Integration**: ML-powered quality insights and anomaly detection
4. **Enhanced Security**: Vault/Consul integration for secrets and configuration
5. **Scalability**: Distributed caching and processing capabilities
6. **Extensibility**: Plugin architecture for custom rules and policies

## Monitoring

- Prometheus metrics at `/metrics`
- Health check at `/health`
- Readiness check at `/ready`
- Service-specific health checks:
  - `/health/quality-engine`
  - `/health/compliance`

## License

Proprietary - Platform Q
