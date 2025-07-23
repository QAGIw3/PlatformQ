# DataIntelligenceSuite

A comprehensive suite of data intelligence services providing advanced analytics, ML capabilities, and data management for the PlatformQ ecosystem.

## Overview

The DataIntelligenceSuite has been significantly enhanced with a powerful common library and consolidated services that provide:

- **Unified Processing Framework**: Common patterns for batch, stream, and quality processing
- **Advanced Caching Layer**: Distributed caching with Apache Ignite
- **Consolidated Services**: Merged similar services for better maintainability
- **Enhanced Integration**: Seamless integration with platform infrastructure

## Architecture

### Enhanced Common Library (`data-intelligence-common`)

The common library now provides:

#### Core Frameworks
- **Caching Framework**: Unified caching with Ignite, supporting multiple patterns
- **Processing Framework**: Base classes for batch, stream, and quality processors
- **Pipeline Builder**: Fluent API for building complex data pipelines

#### Key Components
```
data-intelligence-common/
├── core/
│   ├── caching/          # Distributed caching framework
│   │   ├── cache_manager.py
│   │   ├── cache_decorators.py
│   │   └── distributed_cache.py
│   └── processing/       # Unified processing framework
│       ├── base_processor.py
│       ├── batch_processor.py
│       ├── stream_processor.py
│       ├── quality_processor.py
│       └── pipeline_builder.py
├── base_service/         # Enhanced base service
├── event_handlers/       # Event processing
├── monitoring/           # Metrics and logging
└── vault_consul/         # Security integration
```

### Consolidated Services

#### 1. **Unified Stream Analytics** (`unified-stream-analytics`)
Merges stream-processing-service and real-time analytics:
- Apache Flink for complex stream processing
- Real-time analytics and aggregations
- CEP (Complex Event Processing)
- Low-latency streaming SQL

#### 2. **Unified Batch Analytics** (`unified-batch-analytics`)
Combines batch-processing-service and batch analytics:
- Apache Spark for distributed processing
- Large-scale batch analytics
- ML pipeline integration
- Scheduled report generation

#### 3. **Enhanced Unified Quality Service** (`unified-quality-service`)
Consolidates all quality operations:
- Multi-dimensional quality assessment
- ML-based anomaly detection
- Auto-remediation capabilities
- Quality trend analysis

#### 4. **Data Intelligence Hub (DIH)** (`dih-service`)
Central orchestration and metadata management:
- Service discovery and routing
- Metadata catalog
- Pipeline orchestration
- Cross-service coordination

### Remaining Specialized Services

- **Feature Store Service**: ML feature management
- **Data Catalog Hub**: Comprehensive data catalog
- **Unified ML Platform**: ML model lifecycle
- **Unified Orchestration Service**: Workflow management
- **Real-time Inference Service**: Model serving
- **Semantic Layer Service**: Business logic layer
- **GraphQL Gateway**: Unified API gateway

## Key Improvements

### 1. Separation of Concerns
- Clear boundaries between services
- Shared functionality in common library
- Reduced code duplication

### 2. Scalability
- Distributed caching for performance
- Parallel processing capabilities
- Resource-aware scheduling

### 3. Maintainability
- Consistent patterns across services
- Centralized configuration
- Unified monitoring

### 4. Reusability
- Common processing frameworks
- Shared utilities and helpers
- Pluggable components

## Usage Examples

### Using the Processing Framework

```python
from data_intelligence_common import (
    BatchProcessor, BatchConfig,
    StreamProcessor, StreamConfig,
    QualityProcessor, QualityConfig,
    PipelineBuilder
)

# Create a data pipeline
pipeline = PipelineBuilder("data_processing_pipeline")
    .source(batch_processor, "s3://raw-data/")
    .quality(quality_processor, quality_rules)
    .transform(lambda df: df.filter("status = 'active'"))
    .branch({
        "stream": stream_processor,
        "batch": batch_processor
    })
    .sink(analytics_store, "processed_data")
    .build()

results = await pipeline.execute()
```

### Using the Caching Framework

```python
from data_intelligence_common import CacheManager, cached

# Initialize cache
cache = CacheManager(config)

# Use decorator for automatic caching
@cached(ttl=3600, cache_name="analytics_results")
async def compute_analytics(data_id: str):
    # Expensive computation
    return results

# Manual cache operations
await cache.put("results", key, value, ttl=3600)
value = await cache.get("results", key)
```

## Technology Stack

### Core Technologies
- **Stream Processing**: Apache Flink
- **Batch Processing**: Apache Spark
- **Caching**: Apache Ignite
- **Message Queue**: Apache Pulsar
- **Storage**: MinIO, Cassandra, Elasticsearch
- **ML**: scikit-learn, TensorFlow, PyTorch

### Infrastructure
- **Service Discovery**: Consul
- **Secret Management**: Vault
- **Monitoring**: Prometheus + Grafana
- **Tracing**: Jaeger
- **Container Runtime**: Docker/Kubernetes

## Getting Started

### Prerequisites
- Python 3.8+
- Docker and Docker Compose
- Apache Spark 3.4+
- Apache Flink 1.17+

### Installation

1. Clone the repository
2. Install common library:
   ```bash
   cd libs/data-intelligence-common
   pip install -e .
   ```

3. Start infrastructure:
   ```bash
   docker-compose -f docker-compose.dataintelligence.yml up -d
   ```

4. Start services:
   ```bash
   # Start unified stream analytics
   cd services/DataIntelligenceSuite/unified-stream-analytics
   python -m app.main
   
   # Start unified batch analytics
   cd services/DataIntelligenceSuite/unified-batch-analytics
   python -m app.main
   ```

## Migration Guide

### From Old Services

1. **Update imports** to use common library:
   ```python
   # Old
   from stream_processing import StreamProcessor
   
   # New
   from data_intelligence_common import StreamProcessor
   ```

2. **Migrate configurations** to new unified format

3. **Update API endpoints** to consolidated services

4. **Test thoroughly** with new processing framework

## Performance Considerations

- Use caching for frequently accessed data
- Enable parallel processing where possible
- Monitor resource usage and scale accordingly
- Use appropriate batch sizes for processing
- Enable compression for network transfers

## Security

- All services integrate with Vault for secrets
- mTLS between services via Consul Connect
- Role-based access control (RBAC)
- Audit logging for compliance
- Encrypted data at rest and in transit

## Monitoring and Observability

- Prometheus metrics exposed at `/metrics`
- Distributed tracing with Jaeger
- Centralized logging with Elasticsearch
- Custom dashboards in Grafana
- Alerting via AlertManager

## Contributing

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 