# PlatformQ

A cutting-edge cloud platform for digital asset management, compute resource provisioning, and decentralized blockchain integration.

## 🏗️ Architecture Overview

PlatformQ is built on a microservices architecture with the following core components:

### Core Services

1. **Auth Service** - Authentication and identity management
2. **Analytics Service** - Real-time analytics and insights
3. **Asset Service** - Digital asset management
4. **Compute Allocation Service** - Resource provisioning and management
5. **Connector Service** - Integration with external systems
6. **Search Service** - Full-text search and discovery
7. **Storage Service** - Distributed storage management
8. **Tenant Service** - Multi-tenancy support
9. **Functions Service** - Serverless function execution

### DataIntelligenceSuite (Unified Platform)

The DataIntelligenceSuite provides a **unified data intelligence platform** with 9 consolidated services:

1. **Data Ingestion Service** (Port 8010) - CDC, streaming, and batch ingestion
2. **Stream Processing Service** (Port 8011) - Real-time processing with Flink
3. **Batch Processing Service** (Port 8012) - Large-scale analytics with Spark
4. **Graph Processing Service** (Port 8013) - JanusGraph and GraphX operations
5. **Quality Engine Service** (Port 8014) - Data validation and remediation
6. **MLOps Service** (Port 8015) - ML lifecycle management
7. **Workflow Engine** (Port 8016) - DAG orchestration
8. **Data Catalog Service** (Port 8017) - Metadata and lineage
9. **Unified API Gateway** (Port 8005) - GraphQL and REST APIs

### Blockchain Services

- **Asset Registry** - NFT and digital asset registration
- **Blockchain Gateway** - Multi-chain integration
- **Cross-Chain Bridge** - Asset interoperability
- **Token Service** - Token creation and management

### Infrastructure Components

- **Apache Pulsar** - Event streaming and messaging
- **Apache Cassandra** - Distributed database
- **Apache Ignite** - In-memory computing
- **MinIO** - Object storage
- **JanusGraph** - Graph database
- **Elasticsearch** - Search and analytics
- **HashiCorp Vault** - Secrets management
- **HashiCorp Consul** - Service discovery

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Kubernetes (for production deployment)
- Python 3.9+
- Node.js 16+

### Local Development

```bash
# Clone the repository
git clone https://github.com/your-org/platformq.git
cd platformq

# Start infrastructure services
docker-compose up -d

# Start DataIntelligenceSuite
docker-compose -f docker-compose.dataintelligence.yml up -d

# Verify services are running
curl http://localhost:8005/health  # API Gateway
curl http://localhost:8011/health  # Stream Processing
curl http://localhost:8012/health  # Batch Processing
```

### Submit a Job

```python
# Stream processing job
from dataintelligence import StreamClient

client = StreamClient()
job = client.submit_job({
    "type": "cep",
    "pattern": "fraud_detection",
    "input_topics": ["transactions"],
    "output_topic": "fraud_alerts"
})

# Batch processing job
from dataintelligence import BatchClient

client = BatchClient()
job = client.submit_job({
    "type": "ml_training",
    "model": "asset_classifier",
    "training_data": "s3://data/train"
})
```

## 📊 Key Features

### Unified Processing Platform
- **Stream & Batch**: Single platform for both real-time and batch processing
- **Consolidated Jobs**: 30+ standalone jobs unified into manageable services
- **Resource Optimization**: 60% reduction in infrastructure costs

### Enterprise Ready
- **Multi-tenant**: Complete isolation between tenants
- **Scalable**: Horizontal scaling for all services
- **Secure**: End-to-end encryption, mTLS, and audit trails
- **Compliant**: GDPR, SOC2, and regulatory compliance

### Advanced Capabilities
- **ML/AI Integration**: Built-in MLOps with model versioning and monitoring
- **Graph Analytics**: Real-time graph processing and analytics
- **Quality First**: Automated data quality checks and remediation
- **Event-Driven**: Comprehensive event streaming architecture

## 📁 Project Structure

```
platformq/
├── services/                   # Microservices
│   ├── DataIntelligenceSuite/ # Unified data platform (9 services)
│   ├── BlockchainServices/    # Blockchain integration
│   ├── MarketServices/        # Trading and marketplace
│   └── TenantServices/        # Multi-tenancy
├── libs/                      # Shared libraries
├── iac/                       # Infrastructure as Code
│   ├── kubernetes/           # K8s manifests
│   └── terraform/           # Cloud provisioning
├── processing/               # Legacy (migrated to DataIntelligenceSuite)
└── docs/                     # Documentation
```

## 🛠️ Development

### Service Development

Each service follows a standard structure:
```
service-name/
├── app/           # Application code
├── tests/         # Unit and integration tests
├── Dockerfile     # Container definition
├── requirements.txt # Dependencies
└── README.md      # Service documentation
```

### Testing

```bash
# Run unit tests
pytest services/service-name/tests/

# Run integration tests
pytest tests/integration/

# Run end-to-end tests
pytest tests/e2e/
```

## 📈 Performance

The DataIntelligenceSuite consolidation achieved:

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Components | 45+ | 9 | 80% reduction |
| Resource Usage | ~200GB RAM | ~80GB RAM | 60% reduction |
| Deployment Time | 45+ deployments | 9 services | 5x faster |
| Code Duplication | ~40% | <10% | 75% reduction |

## 🔒 Security

- **Authentication**: JWT-based with refresh tokens
- **Authorization**: Fine-grained RBAC with OPA
- **Encryption**: TLS 1.3 for transport, AES-256 for storage
- **Secrets**: Dynamic secrets with HashiCorp Vault
- **Audit**: Comprehensive audit logging

## 📚 Documentation

- [Architecture Overview](docs/architecture/)
- [API Reference](docs/api/)
- [DataIntelligenceSuite Guide](services/DataIntelligenceSuite/README.md)
- [Deployment Guide](docs/deployment/)
- [Security Guide](docs/security/)

## 🤝 Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## 📄 License

This project is proprietary software. All rights reserved.

## 📞 Support

- **Slack**: #platformq-support
- **Email**: support@platformq.io
- **Documentation**: https://docs.platformq.io 