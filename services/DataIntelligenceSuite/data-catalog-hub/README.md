# Data Catalog Hub

Unified metadata management and intelligent search service for PlatformQ. Combines Apache Atlas for comprehensive metadata management with advanced search capabilities including AI-powered search, vector search, and intelligent discovery.

## Architecture

The service follows a clean architecture pattern with clear separation of concerns:

### Layer Structure

```
app/
├── api/                # API Layer - Thin REST endpoints
│   └── v1/            # Version 1 API implementation
│       ├── routers/   # FastAPI routers (entities, schemas, lineage, etc.)
│       ├── models/    # Request/Response models
│       └── dependencies.py  # Dependency injection
│
├── services/          # Service Layer - Business logic
│   ├── catalog/       # Core catalog services
│   │   ├── entity_service.py
│   │   ├── schema_service.py
│   │   ├── lineage_service.py
│   │   ├── classification_service.py
│   │   └── glossary_service.py
│   └── search/        # Search services
│       └── unified_search_service.py
│
├── domain/            # Domain Layer - Business entities
│   └── catalog/
│       ├── entities/  # Entity aggregate
│       ├── glossary/  # Glossary aggregate
│       └── lineage/   # Lineage aggregate
│
├── infrastructure/    # Infrastructure Layer
│   └── repositories/  # Data persistence
│       ├── entity_repository.py
│       ├── schema_repository.py
│       ├── lineage_repository.py
│       └── glossary_repository.py
│
├── core/             # Core components
│   ├── atlas_client.py     # Apache Atlas integration
│   ├── container.py        # Dependency injection container
│   └── config.py          # Configuration
│
└── events/           # Event-driven architecture
    ├── event_bus.py       # Event bus implementation
    └── catalog_events.py  # Domain events
```

## Features

### Core Catalog Features
- **Entity Management**: Create, update, delete, and search metadata entities
- **Schema Registry**: Version control for data schemas with compatibility checking
- **Data Lineage**: Track data flow and dependencies across the platform
- **Classification**: Automatic and manual data classification (PII, PCI, PHI)
- **Business Glossary**: AI-enhanced business term management and mapping

### Search Capabilities
- **Unified Search**: Single endpoint for searching across all metadata
- **AI-Powered Search**: Natural language understanding and semantic search
- **Vector Search**: Similarity search using embeddings
- **Hybrid Search**: Combines text and vector search for optimal results

### Enhanced Features
- **Medallion Architecture Discovery**: Automatic discovery of Bronze, Silver, and Gold layers
- **Quality Integration**: Real-time data quality scoring and trust levels
- **Access Analytics**: Track and analyze data access patterns
- **Compliance Support**: GDPR, CCPA compliance tracking and audit trails

## Technology Stack

- **FastAPI**: Modern async web framework
- **Apache Atlas**: Metadata management engine
- **Elasticsearch**: Search and analytics
- **Apache Ignite**: Distributed caching
- **Milvus**: Vector database for semantic search
- **Apache Pulsar**: Event streaming

## API Overview

### Entity Management
```
POST   /api/v1/entities              # Create entity
GET    /api/v1/entities/{guid}       # Get entity
PUT    /api/v1/entities/{guid}       # Update entity
DELETE /api/v1/entities/{guid}       # Delete entity
GET    /api/v1/entities              # List entities
```

### Schema Registry
```
POST   /api/v1/schemas               # Register schema
GET    /api/v1/schemas/{id}          # Get schema
GET    /api/v1/schemas/{id}/versions # Get schema versions
POST   /api/v1/schemas/infer         # Infer schema from data
```

### Lineage
```
POST   /api/v1/lineage               # Create lineage
GET    /api/v1/lineage/{guid}        # Get lineage graph
POST   /api/v1/lineage/impact        # Impact analysis
GET    /api/v1/lineage/compliance    # Compliance audit trail
```

### Classifications
```
POST   /api/v1/classifications       # Create classification
POST   /api/v1/classify/auto         # Auto-classify entities
GET    /api/v1/classifications       # List classifications
```

### Glossary
```
POST   /api/v1/glossary              # Create glossary
POST   /api/v1/glossary/terms        # Create term
GET    /api/v1/glossary/terms        # Search terms
POST   /api/v1/glossary/suggest      # AI term suggestions
```

### Search
```
POST   /api/v1/search                # Unified search
POST   /api/v1/search/hybrid         # Hybrid search
GET    /api/v1/search/suggestions    # Search suggestions
```

## Configuration

Key configuration options in `app/core/config.py`:

```python
# Apache Atlas
ATLAS_URL = "http://atlas:21000"
ATLAS_USERNAME = "admin"

# Search
ELASTICSEARCH_HOSTS = ["elasticsearch:9200"]
ENABLE_AI_SEARCH = True

# Caching
IGNITE_HOST = "ignite"
IGNITE_PORT = 10800
CACHE_TTL = 300

# Events
PULSAR_URL = "pulsar://pulsar:6650"
```

## Development

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Run service
python app/main_new.py
```

### Testing
```bash
# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/
```

### Code Quality
- Clean architecture with separation of concerns
- Domain-driven design principles
- Event-driven architecture for loose coupling
- Comprehensive error handling and logging
- Type hints throughout the codebase

## Deployment

The service is designed for Kubernetes deployment with support for:
- Horizontal scaling
- Health checks and readiness probes
- Distributed caching with Apache Ignite
- Event streaming with Apache Pulsar
- Secure secrets management with Vault

## Integration

The Data Catalog Hub integrates with:
- **Auth Service**: For authentication and authorization
- **Quality Service**: For data quality metrics
- **Analytics Service**: For usage analytics
- **Search Service**: For advanced search capabilities
- **All Data Services**: As the central metadata repository

## Monitoring

- Prometheus metrics at `/metrics`
- Health check at `/health`
- Readiness check at `/health/ready`
- OpenTelemetry tracing support
- Comprehensive audit logging 