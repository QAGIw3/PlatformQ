# Data Intelligence Common Library v2.0

Enterprise-scale shared library for DataIntelligenceSuite services.

## Overview

The `data-intelligence-common` library provides a comprehensive set of reusable components, patterns, and utilities for building high-performance data intelligence services. Version 2.0 introduces significant enhancements for enterprise-scale operations.

## Key Features

### 🚀 Enhanced Processing Framework
- **Unified Processor Base**: Single interface for batch, stream, and quality processing
- **Automatic Partitioning**: Intelligent data partitioning with multiple strategies
- **Parallel Processing**: Built-in parallelism with backpressure control
- **Resource Management**: Adaptive resource allocation and monitoring
- **Cost Optimization**: Track and optimize processing costs

### 🔧 Multi-Engine Support
- **Batch Engines**: Spark, Ray, Dask, Pandas
- **Stream Engines**: Flink, Beam, Bytewax, Native async
- **Quality Engines**: Great Expectations, Deequ, Soda, Native
- **Automatic Engine Selection**: Choose optimal engine based on workload

### 📊 Advanced Features
- **Lakehouse Integration**: Native support for Iceberg, Delta, Hudi
- **ML Integration**: Built-in ML capabilities for anomaly detection
- **Quality Monitoring**: Real-time data quality assessment
- **Lineage Tracking**: Automatic data lineage capture
- **Event-Driven Architecture**: Pulsar-based event bus

### 🔒 Enterprise Security
- **Vault Integration**: Dynamic secrets and encryption
- **Consul Integration**: Service discovery and configuration
- **Zero-Trust Architecture**: Built-in security patterns
- **Audit Logging**: Comprehensive audit trails

## Installation

```bash
pip install data-intelligence-common
```

For specific engine support:
```bash
# Spark support
pip install data-intelligence-common[spark]

# Flink support
pip install data-intelligence-common[flink]

# ML support
pip install data-intelligence-common[ml]

# All features
pip install data-intelligence-common[all]
```

## Quick Start

### Basic Batch Processing

```python
from data_intelligence_common.core.processing import BatchProcessor, BatchConfig
from data_intelligence_common.core.lakehouse import LakehouseManager

# Configure processor
config = BatchConfig(
    name="my-batch-processor",
    engine="auto",  # Auto-select best engine
    enable_lakehouse=True,
    enable_quality_checks=True
)

# Create processor
processor = BatchProcessor(config)
await processor.initialize()

# Process data
result = await processor.process("s3://my-bucket/data.parquet")
print(f"Processed {result.metrics.records_processed} records")
```

### Stream Processing

```python
from data_intelligence_common.core.processing import StreamProcessor, StreamConfig

# Configure stream processor
config = StreamConfig(
    name="my-stream-processor",
    source_type="pulsar",
    enable_exactly_once=True,
    enable_ml_quality=True
)

# Create processor
processor = StreamProcessor(config)
await processor.initialize()

# Process stream with fluent API
await processor \
    .filter(lambda event: event["value"] > 100) \
    .map(lambda event: transform(event)) \
    .window("tumbling", timedelta(minutes=5)) \
    .to_lakehouse("processed_events")
```

### Data Quality

```python
from data_intelligence_common.core.processing import QualityProcessor, QualityConfig
from data_intelligence_common.core.processing import QualityRule, DataQualityDimension

# Configure quality processor
config = QualityConfig(
    name="my-quality-processor",
    enable_ml_quality=True,
    enable_auto_remediation=True
)

# Define quality rules
rules = [
    QualityRule(
        rule_id="null_check_email",
        name="Email Null Check",
        check_type="null_check",
        dimension=DataQualityDimension.COMPLETENESS,
        column="email",
        severity="error"
    )
]

# Create processor
processor = QualityProcessor(config)
processor.add_rules(rules)

# Run quality assessment
result = await processor.process(df)
print(f"Quality Score: {result.overall_score}")
```

## Architecture

```
data-intelligence-common/
├── base_service/          # Service templates and patterns
├── core/
│   ├── processing/        # Enhanced processing framework
│   ├── lakehouse/         # Lakehouse integrations
│   ├── ml/                # ML capabilities
│   ├── quality/           # Quality framework
│   ├── events/            # Event-driven patterns
│   ├── orchestration/     # Workflow orchestration
│   └── caching/           # Distributed caching
├── integrations/          # External service integrations
├── monitoring/            # Observability components
└── vault_consul/          # Security integrations
```

## Core Components

### Processing Framework

The enhanced processing framework provides:
- Unified interface for all processing types
- Automatic optimization and partitioning
- Built-in monitoring and metrics
- Fault tolerance and retry logic

### Lakehouse Integration

Native support for modern lakehouse formats:
- Apache Iceberg
- Delta Lake
- Apache Hudi

Features:
- ACID transactions
- Time travel
- Schema evolution
- Automatic optimization

### ML Integration

Built-in ML capabilities:
- Anomaly detection
- Feature engineering
- Model serving
- Online learning

### Event-Driven Architecture

Pulsar-based event bus with:
- Exactly-once semantics
- Event sourcing
- Saga pattern support
- Dead letter queues

## Best Practices

### 1. Use Dependency Injection

```python
from dependency_injector import containers, providers
from data_intelligence_common.core.caching import CacheManager

class Container(containers.DeclarativeContainer):
    cache = providers.Singleton(CacheManager)
```

### 2. Leverage Async/Await

```python
async def process_data():
    async with processor:
        result = await processor.process(data)
    return result
```

### 3. Enable Monitoring

```python
from data_intelligence_common.monitoring import setup_monitoring

app = create_app()
setup_monitoring(app)
```

### 4. Use Structured Logging

```python
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)
logger.info("Processing started", extra={"job_id": job_id})
```

## Configuration

### Environment Variables

```bash
# Service configuration
SERVICE_NAME=my-service
ENVIRONMENT=production

# Consul configuration
CONSUL_URL=http://consul:8500
CONSUL_TOKEN=xxx

# Vault configuration
VAULT_URL=http://vault:8200
VAULT_TOKEN=xxx

# Processing configuration
MAX_WORKERS=8
ENABLE_CACHING=true
CACHE_TTL=3600
```

### Consul Configuration

```json
{
  "data-intelligence/processors/my-processor/config": {
    "parallelism": 16,
    "partition_size_mb": 256,
    "enable_optimization": true
  }
}
```

## Performance Tuning

### Batch Processing

1. **Partition Size**: Adjust `partition_size_mb` based on data characteristics
2. **Parallelism**: Set based on available CPU cores
3. **Engine Selection**: Use Spark for large datasets, Pandas for small

### Stream Processing

1. **Buffer Size**: Tune based on throughput requirements
2. **Checkpoint Interval**: Balance between performance and fault tolerance
3. **Watermark Delay**: Set based on expected out-of-order data

### Resource Management

1. **Memory Limits**: Set to 70-80% of available memory
2. **CPU Limits**: Leave headroom for system processes
3. **Adaptive Scaling**: Enable for variable workloads

## Monitoring and Debugging

### Metrics

Key metrics exposed via Prometheus:
- `processing_records_total`: Total records processed
- `processing_duration_seconds`: Processing duration histogram
- `processing_errors_total`: Error count by type
- `resource_usage_percent`: CPU/Memory usage

### Logging

Structured logs with correlation IDs:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "level": "INFO",
  "service": "my-processor",
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "message": "Processing completed",
  "records_processed": 1000000,
  "duration_ms": 5432
}
```

### Tracing

OpenTelemetry integration for distributed tracing:
- Automatic span creation
- Context propagation
- Performance profiling

## Migration Guide

### From v1.x to v2.0

1. **Update imports**:
```python
# Old
from data_intelligence_common.processing import BatchProcessor

# New
from data_intelligence_common.core.processing import BatchProcessor
```

2. **Update configuration**:
```python
# Old
config = ProcessorConfig(parallelism=4)

# New
config = BatchConfig(
    parallelism=4,
    engine="auto",
    enable_optimization=True
)
```

3. **Use new features**:
```python
# Enable ML-based quality
config.enable_ml_quality = True

# Enable cost tracking
config.enable_cost_tracking = True
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

See [LICENSE](LICENSE) for details. 