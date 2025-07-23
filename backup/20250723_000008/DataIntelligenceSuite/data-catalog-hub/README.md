# Data Catalog Hub

A unified metadata management and intelligent search platform built with modern clean architecture principles.

## 🚀 Overview

The Data Catalog Hub is a comprehensive metadata management service that provides:

- **🗂️ Unified Metadata Management**: Central repository for all data assets using Apache Atlas
- **🔍 Intelligent Search**: AI-powered search with vector similarity, RAG, and semantic understanding
- **🔄 Data Lineage**: Track data flow, transformations, and impact analysis
- **📚 Business Glossary**: Map business terms to technical assets with AI assistance
- **✅ Quality Integration**: Monitor and improve data quality across the platform
- **📊 Access Analytics**: Understand data usage patterns and optimize access
- **🌐 GraphQL Federation**: Unified GraphQL API for cross-service metadata queries

## 🏗️ Architecture

### Clean Architecture Principles

The service follows Domain-Driven Design (DDD) and clean architecture patterns:

```
app/
├── api/v1/           # 🌐 API Layer (REST endpoints)
│   ├── routers/      # FastAPI routers (thin controllers)
│   ├── models/       # Request/Response DTOs
│   └── dependencies.py # Dependency injection setup
│
├── services/         # 💼 Business Logic Layer
│   ├── catalog/      # Core catalog services
│   ├── search/       # Unified search implementation
│   ├── ai/          # AI/ML components
│   └── adapters/    # Migration adapters
│
├── domain/          # 🎯 Domain Layer
│   ├── models/      # Domain entities
│   ├── events/      # Domain events
│   └── value_objects/ # Value objects
│
├── infrastructure/  # 🔧 Infrastructure Layer
│   ├── repositories/ # Data persistence
│   └── caching/     # Cache implementations
│
├── core/           # ⚙️ Core Components
│   ├── container.py # DI container configuration
│   ├── config.py   # Application settings
│   └── logging_config.py # Logging setup
│
└── events/         # 📡 Event System
    └── event_bus.py # Event-driven architecture
```

### Key Design Patterns

- **🏭 Application Factory**: Clean separation between app creation and running
- **💉 Dependency Injection**: Using `dependency-injector` for IoC
- **📦 Repository Pattern**: Abstract data access from business logic
- **🎯 Service Layer**: Encapsulate business logic
- **📢 Event-Driven**: Loose coupling through domain events
- **🔄 Strategy Pattern**: Pluggable search strategies

### Technology Stack

- **FastAPI**: Modern async web framework with automatic OpenAPI docs
- **Apache Atlas**: Enterprise metadata repository
- **Elasticsearch 8**: Advanced search with native vector support
- **Apache Ignite**: Distributed in-memory caching
- **Dependency Injector**: Powerful IoC container
- **Pydantic**: Data validation and serialization
- **Python 3.11+**: Latest Python features

## 🚀 Quick Start

### Using Docker Compose (Recommended)

```bash
# Clone repository
git clone <repository-url>
cd services/DataIntelligenceSuite/data-catalog-hub

# Start all services
docker-compose up -d

# Wait for services to be ready
sleep 30

# Check health
curl http://localhost:8000/health

# View API documentation
open http://localhost:8000/api/docs
```

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ATLAS_URL=http://localhost:21000
export ATLAS_USERNAME=admin
export ATLAS_PASSWORD=admin
export ELASTICSEARCH_HOSTS='["http://localhost:9200"]'
export IGNITE_HOST=localhost
export IGNITE_PORT=10800

# Run application
python -m app.main

# In another terminal, run tests
pytest
```

## 📚 API Documentation

### Interactive Documentation

When the service is running, visit:
- **Swagger UI**: http://localhost:8000/api/docs
- **ReDoc**: http://localhost:8000/api/redoc
- **GraphQL Playground**: http://localhost:8000/graphql
- **Federation SDL**: http://localhost:8000/graphql/sdl

### Entity Management

```bash
# Create an entity
curl -X POST http://localhost:8000/api/v1/entities \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{
    "typeName": "table",
    "attributes": {
      "name": "customer_data",
      "qualifiedName": "prod.sales.customer_data",
      "description": "Customer transaction data"
    }
  }'

# Get entity by GUID
curl http://localhost:8000/api/v1/entities/{guid}

# Update entity
curl -X PUT http://localhost:8000/api/v1/entities/{guid} \
  -H "Content-Type: application/json" \
  -d '{"attributes": {"description": "Updated description"}}'

# Search entities
curl "http://localhost:8000/api/v1/entities/search?query=customer&limit=10"
```

### Unified Search

```bash
# Text-based search
curl -X POST http://localhost:8000/api/v1/search/text \
  -H "Content-Type: application/json" \
  -d '{
    "query": "customer orders",
    "fields": ["name", "description"],
    "limit": 20
  }'

# Vector similarity search
curl -X POST http://localhost:8000/api/v1/search/vector \
  -H "Content-Type: application/json" \
  -d '{
    "query": "find similar customer datasets",
    "entity_types": ["table", "dataset"],
    "threshold": 0.8
  }'

# Hybrid search (text + vector)
curl -X POST http://localhost:8000/api/v1/search/hybrid \
  -H "Content-Type: application/json" \
  -d '{
    "query": "customer purchase patterns",
    "text_weight": 0.6,
    "vector_weight": 0.4
  }'

# AI-powered search with RAG
curl -X POST http://localhost:8000/api/v1/search/ai \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how do we calculate customer lifetime value?",
    "use_rag": true,
    "include_explanations": true
  }'
```

### Data Lineage

```bash
# Create lineage
curl -X POST http://localhost:8000/api/v1/lineage \
  -H "Content-Type: application/json" \
  -d '{
    "process_name": "customer_etl",
    "process_type": "ETL",
    "inputs": ["guid-1", "guid-2"],
    "outputs": ["guid-3"]
  }'

# Get lineage graph
curl "http://localhost:8000/api/v1/lineage/{entity_guid}?direction=BOTH&depth=3"

# Impact analysis
curl -X POST http://localhost:8000/api/v1/lineage/impact/{entity_guid} \
  -d '{"change_type": "schema_change"}'
```

### Business Glossary

```bash
# Create glossary term
curl -X POST http://localhost:8000/api/v1/glossary/terms \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Customer LTV",
    "definition": "Total revenue expected from a customer over their lifetime",
    "abbreviation": "LTV",
    "status": "APPROVED"
  }'

# AI-powered term suggestions
curl -X POST http://localhost:8000/api/v1/glossary/suggest-terms \
  -d '{"technical_name": "cust_lifetime_val"}'

# Auto-map terms to dataset
curl -X POST http://localhost:8000/api/v1/glossary/auto-map/{dataset_guid}
```

### GraphQL Queries

```graphql
# Get data asset with lineage and quality
query GetDataAsset($guid: String!) {
  dataAsset(guid: $guid) {
    guid
    name
    qualifiedName
    description
    owner
    classifications
    
    lineage {
      upstreamEntities {
        name
        typeName
      }
      downstreamEntities {
        name
        typeName
      }
      impactRadius
    }
    
    qualityScore
    
    glossaryTerms {
      name
      definition
    }
    
    accessAnalytics {
      totalAccesses
      uniqueUsers
      accessTrend
    }
  }
}

# Search for data assets
query SearchAssets($query: String!, $searchType: String!) {
  searchAssets(query: $query, searchType: $searchType, limit: 10) {
    score
    entity {
      guid
      name
      description
      typeName
    }
    highlights
    explanation
    source
  }
}

# Get data lineage graph
query GetLineage($entityGuid: String!) {
  dataLineage(entityGuid: $entityGuid, depth: 3) {
    upstreamEntities {
      name
      qualifiedName
    }
    downstreamEntities {
      name
      qualifiedName
    }
    processes {
      name
      processType
    }
    fullGraph(depth: 5)
  }
}

# Create glossary term
mutation CreateTerm($name: String!, $definition: String!) {
  createGlossaryTerm(name: $name, definition: $definition) {
    guid
    name
    definition
    aiSuggestions
  }
}
```

## 🌟 Key Features

### 1. Unified Search Service
- **Strategy Pattern**: Pluggable search strategies (text, vector, hybrid, AI)
- **Caching**: Intelligent result caching with Ignite
- **Query Analysis**: AI-powered query understanding
- **Multi-modal**: Support for text, code, and image embeddings

### 2. Advanced Data Lineage
- **Real-time Tracking**: Capture data transformations as they happen
- **Impact Analysis**: Understand downstream effects of changes
- **Compliance**: Audit trails for regulatory requirements
- **Visualization**: Interactive lineage graphs

### 3. AI-Enhanced Business Glossary
- **Smart Suggestions**: AI recommends business terms for technical fields
- **Auto-mapping**: Automatically map terms to data assets
- **Semantic Understanding**: Context-aware term relationships
- **Version Control**: Track term evolution

### 4. Intelligent Classification
- **Auto-classification**: ML-based PII/PCI/PHI detection
- **Custom Rules**: Define your own classification patterns
- **Propagation**: Classifications flow through lineage
- **Bulk Operations**: Classify thousands of assets at once

### 5. Quality Integration
- **Score Tracking**: Monitor quality metrics over time
- **Automated Checks**: Run quality assessments on schedule
- **Trend Analysis**: Identify quality patterns
- **Alerts**: Notify on quality degradation

### 6. GraphQL Federation
- **Unified Schema**: Part of platform-wide federated GraphQL API
- **Cross-Service Queries**: Query catalog data alongside other services
- **Type Safety**: Strongly typed GraphQL schema
- **Real-time Updates**: Subscription support for live data changes
- **Schema Stitching**: Automatic schema composition with other services

## ⚙️ Configuration

### Core Settings

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ENV` | Environment (development/staging/production) | development | No |
| `DEBUG` | Enable debug mode | false | No |
| `LOG_LEVEL` | Logging level (DEBUG/INFO/WARNING/ERROR) | INFO | No |
| `HOST` | Server host | 0.0.0.0 | No |
| `PORT` | Server port | 8000 | No |
| `WORKERS` | Number of worker processes | 1 | No |

### Service Dependencies

| Variable | Description | Required |
|----------|-------------|----------|
| `ATLAS_URL` | Apache Atlas API URL | Yes |
| `ATLAS_USERNAME` | Atlas username | Yes |
| `ATLAS_PASSWORD` | Atlas password | Yes |
| `ELASTICSEARCH_HOSTS` | ES hosts as JSON array | Yes |
| `IGNITE_HOST` | Apache Ignite host | Yes |
| `IGNITE_PORT` | Ignite port (default: 10800) | No |

### Optional Features

| Variable | Description | Default |
|----------|-------------|---------|
| `ENABLE_VAULT` | Enable HashiCorp Vault for secrets | false |
| `ENABLE_CONSUL` | Enable Consul for service discovery | false |
| `ENABLE_PULSAR` | Enable Apache Pulsar events | false |
| `OPENAI_API_KEY` | OpenAI key for RAG features | - |

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=app --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_entity_service.py -v

# Run only unit tests
pytest tests/unit -v

# Run integration tests (requires services)
pytest tests/integration -v
```

### Test Structure

```
tests/
├── conftest.py          # Shared fixtures
├── test_api_entities.py # API endpoint tests
├── test_entity_service.py # Service layer tests
├── test_unified_search.py # Search functionality tests
└── unit/               # Unit tests
    └── integration/    # Integration tests
```

## 🔧 Development

### Code Quality Tools

```bash
# Format code with Black
black app/ tests/

# Lint with flake8
flake8 app/ tests/

# Type checking with mypy
mypy app/

# Sort imports
isort app/ tests/

# All quality checks
make quality
```

### Project Guidelines

1. **Clean Architecture**: Maintain layer separation
2. **Type Hints**: Use type annotations everywhere
3. **Async First**: Prefer async/await patterns
4. **Test Coverage**: Maintain >80% coverage
5. **Documentation**: Update docs with code changes

### Adding New Features

1. Start with domain models in `app/domain/`
2. Create repository interface in `app/domain/repositories/`
3. Implement repository in `app/infrastructure/repositories/`
4. Create service in `app/services/`
5. Add API router in `app/api/v1/routers/`
6. Register in DI container `app/core/container.py`
7. Write tests for all layers

## 📈 Performance

### Optimization Features

- **Connection Pooling**: Efficient database connections
- **Async Operations**: Non-blocking I/O throughout
- **Caching Strategy**: Multi-level caching with Ignite
- **Batch Processing**: Bulk operations support
- **Query Optimization**: Elasticsearch query tuning

### Monitoring

- **Prometheus Metrics**: Available at `/metrics`
- **Health Checks**: `/health` endpoint
- **Performance Logs**: Detailed timing information
- **Request Tracing**: Correlation IDs for debugging

## 🔒 Security

### Built-in Security Features

- **JWT Authentication**: Token-based auth
- **RBAC**: Role-based access control
- **Input Validation**: Pydantic models
- **SQL Injection Protection**: Parameterized queries
- **XSS Prevention**: Output encoding
- **CORS Configuration**: Configurable origins

### Best Practices

1. Always use environment variables for secrets
2. Enable TLS in production
3. Rotate credentials regularly
4. Audit access logs
5. Keep dependencies updated

## 🚀 Deployment

### Docker

```bash
# Build image
docker build -t data-catalog-hub:latest .

# Run container
docker run -d \
  -p 8000:8000 \
  -e ATLAS_URL=http://atlas:21000 \
  -e ELASTICSEARCH_HOSTS='["http://elasticsearch:9200"]' \
  --name catalog-hub \
  data-catalog-hub:latest
```

### Kubernetes

```yaml
# Simple deployment
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-catalog-hub
spec:
  replicas: 3
  selector:
    matchLabels:
      app: catalog-hub
  template:
    metadata:
      labels:
        app: catalog-hub
    spec:
      containers:
      - name: app
        image: data-catalog-hub:latest
        ports:
        - containerPort: 8000
        env:
        - name: ATLAS_URL
          value: "http://atlas-service:21000"
EOF
```

### Production Checklist

- [ ] Set `ENV=production`
- [ ] Configure proper logging
- [ ] Set up monitoring/alerting
- [ ] Enable HTTPS/TLS
- [ ] Configure backups
- [ ] Set resource limits
- [ ] Enable horizontal scaling
- [ ] Configure health checks

## 🤝 Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone your fork
git clone https://github.com/your-username/data-catalog-hub.git

# Install development dependencies
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install

# Run tests before committing
pytest
```

## 📞 Support

- **Documentation**: This README and inline code docs
- **API Reference**: http://localhost:8000/api/docs
- **Issues**: GitHub Issues for bug reports and features
- **Discussions**: GitHub Discussions for questions

## 📄 License

This project is part of the PlatformQ Data Intelligence Suite.

---

Built with ❤️ using FastAPI, Apache Atlas, and modern Python 