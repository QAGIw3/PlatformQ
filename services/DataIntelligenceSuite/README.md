# Data Intelligence Suite

A comprehensive suite of data and AI services providing advanced analytics, machine learning, and intelligent data processing capabilities for PlatformQ.

## Overview

The Data Intelligence Suite is a collection of specialized microservices that work together to provide:
- Real-time and batch data analytics
- Machine learning model training and serving
- Feature engineering and management
- Neuromorphic computing capabilities
- Data integration and ETL pipelines
- ML model marketplace

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Data Intelligence Suite                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐   │
│  │   Analytics     │  │  Batch Process  │  │   Data Ingestion    │   │
│  │    Service      │  │     Service     │  │      Service        │   │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬─────────┘   │
│           │                     │                       │              │
│  ┌────────┴────────┐  ┌────────┴────────┐  ┌──────────┴─────────┐   │
│  │  Feature Store  │  │  Neuromorphic   │  │  ML Marketplace    │   │
│  │    Service      │  │    Service      │  │     Service        │   │
│  └────────┬────────┘  └────────┬────────┘  └──────────┬─────────┘   │
│           │                     │                       │              │
│  ┌────────┴────────────────────┴───────────────────────┴─────────┐   │
│  │                    Shared Infrastructure                       │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │   │
│  │  │  Apache  │  │  Apache  │  │  Apache  │  │   Apache     │ │   │
│  │  │  Ignite  │  │  Pulsar  │  │  Flink   │  │   Spark      │ │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘ │   │
│  └────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Services

### Core Services

#### 1. **Analytics Service** (`analytics-service/`)
Real-time analytics and monitoring with Apache Druid integration.
- Time-series analysis
- Real-time dashboards
- Anomaly detection
- Predictive analytics

#### 2. **Batch Processing Service** (`batch-processing-service/`)
Large-scale batch data processing using Apache Spark.
- ETL pipelines
- Data transformations
- Batch ML training
- Data quality checks

#### 3. **Data Ingestion Service** (`data-ingestion-service/`)
Unified data ingestion from multiple sources.
- Multi-source connectors
- Data validation
- Schema management
- Real-time streaming

#### 4. **Data Catalog Hub** (`data-catalog-hub/`)
Centralized metadata management and data discovery.
- Data lineage tracking
- Schema registry
- Data quality metrics
- Access control

#### 5. **Data Query Service** (`data-query-service/`)
Unified query interface for all data sources.
- SQL query engine
- Cross-source joins
- Query optimization
- Result caching

#### 6. **DIH Service** (`dih-service/`)
Digital Integration Hub for real-time data access.
- In-memory caching with Apache Ignite
- Multi-source data integration
- CDC (Change Data Capture)
- ACID transactions

### ML Services

#### 7. **Unified ML Platform Service** (`unified-ml-platform-service/`)
Comprehensive ML platform for model lifecycle management.
- Model training orchestration
- Model serving and deployment
- MLOps and monitoring
- Federated learning
- AutoML capabilities

#### 8. **Feature Store Service** (`feature-store-service/`)
Centralized feature management for ML pipelines.
- Online/offline feature serving
- Feature versioning
- Real-time feature updates
- Feature statistics

#### 9. **Neuromorphic Computing Service** (`neuromorphic-computing-service/`)
Brain-inspired computing with spiking neural networks.
- Ultra-low power AI
- Event-driven processing
- Real-time anomaly detection
- Hardware acceleration support

#### 10. **ML Marketplace Service** (`ml-marketplace-service/`)
Decentralized marketplace for ML models.
- Model publishing and discovery
- Ratings and reviews
- Monetization support
- Personalized recommendations

### Supporting Components

#### 11. **Data Intelligence Common** (`data-intelligence-common/`)
Shared libraries and utilities for all services.
- Common data models
- Utility functions
- Integration helpers
- Base service classes

## Technology Stack

### Data Processing
- **Apache Spark**: Large-scale batch processing and ML
- **Apache Flink**: Stream processing and real-time analytics
- **Apache SeaTunnel**: Data integration and ETL

### Storage & Caching
- **Apache Ignite**: In-memory computing and caching
- **Apache Cassandra**: Distributed wide-column store
- **MinIO**: Object storage for ML artifacts
- **JanusGraph**: Graph database for relationships

### Messaging & Streaming
- **Apache Pulsar**: Event streaming and messaging
- **Apache Avro**: Data serialization

### Analytics
- **Apache Druid**: Real-time analytics database
- **Elasticsearch**: Search and analytics engine
- **Apache Superset**: Data visualization

### Machine Learning
- **PyTorch**: Deep learning framework
- **Scikit-learn**: Traditional ML algorithms
- **MLflow**: ML lifecycle management
- **Milvus**: Vector database for embeddings

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- Java 11+ (for Spark and Flink)
- Node.js 16+ (for dashboards)

### Quick Start

1. **Start Infrastructure Services**
```bash
# Start Ignite, Pulsar, and other dependencies
docker-compose -f docker-compose.dataintelligence.yml up -d
```

2. **Deploy Individual Services**
```bash
# Deploy analytics service
cd analytics-service
docker build -t analytics-service:latest .
docker run -d --name analytics-service -p 8001:8000 analytics-service:latest

# Deploy feature store service
cd ../feature-store-service
docker build -t feature-store-service:latest .
docker run -d --name feature-store-service -p 8002:8000 feature-store-service:latest

# Deploy other services similarly...
```

3. **Verify Services**
```bash
# Check health endpoints
curl http://localhost:8001/health
curl http://localhost:8002/health
```

## Development

### Local Development Setup

1. **Install Python Dependencies**
```bash
cd analytics-service
pip install -r requirements.txt
```

2. **Run Service Locally**
```bash
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/

# Run with coverage
pytest --cov=app tests/
```

## Configuration

Each service can be configured via environment variables:

```bash
# Common configurations
IGNITE_HOST=ignite
IGNITE_PORT=10800
PULSAR_URL=pulsar://pulsar:6650
LOG_LEVEL=INFO

# Service-specific configurations
# See individual service README files
```

## API Documentation

Each service exposes OpenAPI documentation at:
- `http://<service-host>:<port>/docs` - Swagger UI
- `http://<service-host>:<port>/redoc` - ReDoc

## Monitoring

### Metrics
- Prometheus metrics exposed at `/metrics`
- Custom dashboards in Grafana
- Service-specific metrics per README

### Logging
- Structured JSON logging
- Centralized log aggregation
- Log levels: DEBUG, INFO, WARNING, ERROR

### Tracing
- OpenTelemetry integration
- Distributed tracing with Jaeger
- Request correlation IDs

## Security

- Service-to-service mTLS
- API authentication via JWT
- Role-based access control (RBAC)
- Data encryption at rest and in transit

## Contributing

Please refer to the [Contributing Guide](../../CONTRIBUTING.md) for development standards and practices.

## License

This suite is part of the PlatformQ project and follows the project's licensing terms. 