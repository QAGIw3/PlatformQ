# DataIntelligenceSuite v2.0

Enterprise-scale data intelligence platform with consolidated domain-driven services.

## Overview

DataIntelligenceSuite v2.0 represents a major architectural evolution, consolidating 25+ legacy services into 7 powerful domain-driven services. Built on the enhanced `data-intelligence-common` v2.0 framework, it delivers enterprise-grade performance, scalability, and maintainability.

## Architecture

### Core Services

1. **Analytics Engine Service**
   - Unified analytics with quantum optimization, neuromorphic computing, and stream processing
   - Multi-engine support (Trino, Druid, ClickHouse, Pinot)
   - Real-time and batch analytics
   - Advanced algorithms with extensible architecture

2. **Data Governance Service**
   - Comprehensive quality management and validation
   - Automated remediation and anomaly detection
   - Data profiling and quality scoring
   - Policy enforcement and compliance

3. **Data Platform Service**
   - Digital Integration Hub (DIH) with Apache Ignite
   - Feature store with online/offline serving
   - Storage service with multi-backend support
   - High-performance data synchronization

4. **Integration Hub Service**
   - GraphQL federation gateway
   - Graph analytics with JanusGraph and Spark GraphX
   - Temporal analysis and trust networks
   - Data lineage tracking

5. **ML Platform Service**
   - Unified ML/AI operations and training orchestration
   - Model serving and MLOps lifecycle management
   - Federated learning framework
   - AutoML capabilities

6. **Orchestration Service**
   - Workflow management with Airflow integration
   - Pipeline orchestration and dependency resolution
   - ML-driven optimization
   - Event-driven automation with SeaTunnel

7. **Stream Processing Service v2**
   - Multi-engine streaming (Flink, Beam, Bytewax)
   - Complex event processing
   - Real-time analytics
   - Stateful computations

### Core Framework: data-intelligence-common v2.0

The enhanced common library provides:

- **Base Classes**: Standardized patterns for services, engines, algorithms, and processors
- **Utilities**: Graph algorithms, quantum computing helpers, datetime utils, encryption
- **Processing Framework**: Unified interface for batch, stream, and quality processing
- **Event System**: Pulsar-based event bus with saga orchestration
- **Integration Patterns**: Digital Integration Hub abstractions, CDC processing
- **ML Framework**: Base models, training, inference, and AutoML patterns
- **Monitoring**: Metrics, structured logging, health checks, and tracing

## Technology Stack

- **Languages**: Python 3.10+, TypeScript (for GraphQL)
- **APIs**: FastAPI, GraphQL (Apollo Federation)
- **Messaging**: Apache Pulsar
- **Caching**: Apache Ignite (not Redis)
- **Storage**: MinIO for object storage, Cassandra, Elasticsearch
- **Graph**: JanusGraph with Cassandra backend
- **Processing**: Apache Spark, Flink, Ray, Dask
- **ML**: PyTorch, TensorFlow, scikit-learn, MLflow
- **Security**: HashiCorp Vault & Consul
- **Monitoring**: Prometheus, Grafana, Jaeger

## Security & Compliance

- **Zero-Trust Architecture**: mTLS, service mesh, policy enforcement
- **Dynamic Secrets**: Vault integration for all credentials
- **Encryption**: At-rest and in-transit encryption
- **Audit Logging**: Comprehensive audit trails
- **RBAC**: Fine-grained access control

## Getting Started

### Prerequisites

```bash
# Core dependencies
- Python 3.10+
- Docker & Docker Compose
- Apache Pulsar
- Apache Ignite
- HashiCorp Vault & Consul
```

### Installation

1. Clone the repository
```bash
git clone <repository-url>
cd platformQ/services/DataIntelligenceSuite
```

2. Install common library
```bash
cd data-intelligence-common
pip install -e .
```

3. Install service dependencies
```bash
# For each service
cd <service-name>
pip install -r requirements.txt
```

4. Configure Vault and Consul
```bash
# See docs/integration-guides/ for detailed setup
```

5. Start services
```bash
# Using docker-compose
docker-compose up -d

# Or individually
cd <service-name>
python -m app.main
```

## API Documentation

Each service provides:
- REST API documentation at `http://<service>:8000/docs`
- GraphQL playground at `http://<service>:8000/graphql` (if enabled)
- Health checks at `http://<service>:8000/health`

## Development

### Project Structure

```
DataIntelligenceSuite/
├── data-intelligence-common/     # Shared library v2.0
│   ├── base_service/            # Base service classes
│   ├── core/                    # Core frameworks
│   │   ├── algorithms/          # Algorithm base classes
│   │   ├── engines/             # Engine base classes
│   │   ├── processing/          # Processing framework
│   │   ├── ml/                  # ML framework
│   │   └── integration/         # Integration patterns
│   └── utils/                   # Common utilities
├── analytics-engine-service/     # Consolidated analytics
├── data-governance-service/     # Quality & governance
├── data-platform-service/       # Data infrastructure
├── integration-hub-service/     # Integration & GraphQL
├── ml-platform-service/         # ML operations
├── orchestration-service/       # Workflow orchestration
└── stream-processing-service/   # Stream processing v2
```

### Adding New Features

1. **New Algorithm**: Extend `BaseAlgorithm` in the appropriate service
2. **New Engine**: Extend `BaseEngine` for processing engines
3. **New API**: Use `BaseRouter` for consistent API patterns
4. **New Integration**: Extend `BaseDigitalIntegrationHub` for data sources

## Performance

- **Scalability**: Horizontal scaling with distributed processing
- **Caching**: Multi-tier caching with Ignite
- **Optimization**: ML-driven resource allocation
- **Monitoring**: Real-time performance metrics

## Contributing

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

[License details]

## Support

- Documentation: `docs/`
- Integration Guides: `docs/integration-guides/`
- API Reference: Service-specific `/docs` endpoints

## Migration Status

✅ **v2.0 Migration Complete**
- All 25+ legacy services consolidated into 7 domain services
- Enhanced common library with shared utilities
- Improved performance and maintainability
- Enterprise-grade security and monitoring

For migration details, see `MIGRATION_GUIDE_V2.md`. 