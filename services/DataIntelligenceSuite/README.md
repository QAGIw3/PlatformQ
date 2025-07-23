# DataIntelligenceSuite v2.0

Enterprise-scale data intelligence platform with consolidated domain-driven services.

## Overview

DataIntelligenceSuite v2.0 represents a major architectural evolution, consolidating 25+ legacy services into 7 powerful domain-driven services. Built on the enhanced `data-intelligence-common` v2.0 framework, it delivers enterprise-grade performance, scalability, and maintainability.

## Architecture

### Consolidated Services

1. **Data Platform Service** [[memory:3406972]]
   - Unified data ingestion (CDC, streaming, batch)
   - Multi-format lakehouse (Delta, Iceberg, Hudi)
   - Metadata catalog and discovery
   - Storage abstraction layer

2. **Analytics Engine Service**
   - Multi-engine analytics (Trino, Druid, ClickHouse, Pinot)
   - Real-time and batch analytics
   - OLAP and time-series processing
   - Advanced visualization

3. **ML Platform Service**
   - Unified ML/AI operations
   - Federated learning framework
   - AutoML and neuromorphic computing
   - Quantum optimization integration

4. **Data Governance Service**
   - Comprehensive quality management
   - Compliance and privacy controls
   - Lineage tracking
   - Policy enforcement

5. **Stream Processing Service v2**
   - Multi-engine streaming (Flink, Beam, Bytewax)
   - Complex event processing
   - Real-time analytics
   - Stateful computations

6. **Orchestration Service**
   - Workflow management
   - Event-driven automation
   - Resource scheduling
   - Pipeline coordination

7. **Integration Hub Service**
   - GraphQL gateway
   - API aggregation
   - Protocol translation
   - External system connectors

### Core Framework: data-intelligence-common v2.0

The enhanced common library provides:

- **Unified Processing Interface**: Single API for batch, stream, and quality processing
- **Multi-Engine Support**: Seamlessly switch between Spark, Ray, Dask, Flink, Beam, etc.
- **Advanced Features**: Auto-partitioning, backpressure, ML optimization, cost management
- **Enterprise Patterns**: Circuit breakers, retries, caching, monitoring

## Key Features

### 🚀 Performance
- **10x throughput** improvement with multi-engine optimization
- **Sub-second latency** for streaming operations
- **Automatic scaling** based on workload patterns
- **ML-driven resource allocation**

### 🏗️ Architecture
- **Domain-driven design** for clear boundaries
- **Microservices** with shared capabilities
- **Event-driven** communication via Pulsar
- **Zero-trust security** model

### 🛠️ Operations
- **Unified monitoring** with Prometheus/Grafana
- **Distributed tracing** via OpenTelemetry
- **Centralized logging** with structured logs
- **Health checks** and circuit breakers

### 🔧 Developer Experience
- **Consistent APIs** across all services
- **Comprehensive SDKs** for multiple languages
- **Interactive documentation** with examples
- **Local development** environment

## Technology Stack

### Processing Engines
- **Batch**: Apache Spark, Ray, Dask, Pandas
- **Stream**: Apache Flink, Beam, Bytewax, Native async
- **Quality**: Great Expectations, Deequ, Soda, Native

### Storage Systems
- **Object Storage**: MinIO (S3 compatible)
- **Wide Column**: Apache Cassandra
- **Graph**: JanusGraph
- **Search**: Elasticsearch
- **Cache**: Apache Ignite

### Infrastructure
- **Container**: Docker, Kubernetes
- **Service Mesh**: Consul Connect
- **API Gateway**: Kong
- **Secrets**: HashiCorp Vault
- **Messaging**: Apache Pulsar

## Quick Start

### Prerequisites
- Docker 24.0+
- Kubernetes 1.28+ (for production)
- Python 3.11+
- 16GB RAM minimum

### Local Development

```bash
# Clone repository
git clone https://github.com/platformq/platformq.git
cd platformq/services/DataIntelligenceSuite

# Start infrastructure
docker-compose -f ../../infra/docker-compose/docker-compose.yml up -d

# Install common library
cd data-intelligence-common
pip install -e .
cd ..

# Start a service (e.g., data-platform-service)
cd data-platform-service
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Production Deployment

```bash
# Deploy infrastructure
kubectl apply -f ../../iac/kubernetes/

# Deploy services
kubectl apply -f k8s/

# Verify deployment
kubectl get pods -n dataintelligence
```

## Migration from Legacy Services

### Service Mapping

| Legacy Services | New Consolidated Service |
|----------------|-------------------------|
| data-ingestion-service<br>storage-service<br>data-catalog-hub | **data-platform-service** |
| analytics-service<br>Real-time components | **analytics-engine-service** |
| unified-ml-platform-service<br>neuromorphic-computing-service<br>feature-store-service | **ml-platform-service** |
| unified-quality-service<br>Governance components | **data-governance-service** |
| stream-processing-service<br>Real-time analytics | **stream-processing-service-v2** |
| unified-orchestration-service<br>Workflow components | **orchestration-service** |
| dih-service<br>graphql-gateway | **integration-hub-service** |

### Migration Tools

```bash
# Analyze legacy services
python migration/analyze_services.py

# Generate migration plan
python migration/migrate_to_v2.py --dry-run

# Execute migration
python migration/migrate_to_v2.py --execute
```

## API Documentation

Each service provides comprehensive API documentation:

- Data Platform: http://localhost:8010/docs
- Analytics Engine: http://localhost:8011/docs
- ML Platform: http://localhost:8012/docs
- Data Governance: http://localhost:8013/docs
- Stream Processing: http://localhost:8014/docs
- Orchestration: http://localhost:8015/docs
- Integration Hub: http://localhost:8016/docs

## Monitoring

### Dashboards
- Overall Health: http://localhost:3000/d/dis-overview
- Service Metrics: http://localhost:3000/d/dis-services
- Processing Stats: http://localhost:3000/d/dis-processing

### Alerts
Configured alerts for:
- Service availability
- Processing delays
- Resource utilization
- Error rates
- Data quality issues

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

Proprietary - PlatformQ

## Support

- Documentation: https://docs.platformq.io
- Issues: https://github.com/platformq/platformq/issues
- Slack: #dataintelligence channel 