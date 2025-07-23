# Data Intelligence Common Library

A comprehensive shared library for all Data Intelligence Suite services, providing unified patterns for data processing, caching, event handling, and infrastructure integration with modern data architectures.

## 🚀 Major Enhancements (v2.0.0)

### 🏔️ Lakehouse Architecture
- **Apache Iceberg**: ACID transactions, time travel, schema evolution
- **Delta Lake**: Unified batch and streaming with ACID guarantees  
- **Apache Hudi**: Incremental processing and CDC support
- **Unified Manager**: Seamless switching between lakehouse formats

### 📊 Data Quality & Governance
- **Great Expectations**: Comprehensive data validation and profiling
- **Apache Deequ**: Unit tests for data quality with Spark
- **Soda Core**: Data quality monitoring and alerting
- **Data Contracts**: Enforce quality SLAs and schema contracts

### 🔍 Metadata & Lineage
- **DataHub**: Centralized metadata management platform
- **OpenLineage**: Cross-platform data lineage standard
- **Column-level Lineage**: Track data transformations at field level
- **ML Model Registry**: Track models, features, and experiments

### ⚡ Real-time Analytics
- **Apache Pinot**: Real-time distributed OLAP datastore
- **ClickHouse**: Column-oriented database for analytics
- **Star-tree Indexes**: Fast aggregation queries
- **Materialized Views**: Pre-computed analytics

### 🔌 Event Processing
- **Pluggable Backends**: Pulsar, Kafka, Redis Streams, NATS
- **Exactly-once Semantics**: Reliable event processing
- **Event Sourcing**: Complete audit trail
- **Saga Pattern**: Distributed transaction coordination

### 🏗️ Architecture Improvements
- **Plugin System**: Reduce code duplication across clients
- **Async-first**: Built for high concurrency
- **Circuit Breakers**: Fault tolerance patterns
- **Connection Pooling**: Optimized resource usage

## 📦 Installation

```bash
# Core installation
pip install -e services/DataIntelligenceSuite/data-intelligence-common

# With all features
pip install -e "services/DataIntelligenceSuite/data-intelligence-common[all]"

# Specific feature sets
pip install -e "services/DataIntelligenceSuite/data-intelligence-common[lakehouse]"      # Lakehouse support
pip install -e "services/DataIntelligenceSuite/data-intelligence-common[quality]"        # Data quality tools
pip install -e "services/DataIntelligenceSuite/data-intelligence-common[realtime]"       # Real-time analytics
pip install -e "services/DataIntelligenceSuite/data-intelligence-common[ml]"             # ML features
pip install -e "services/DataIntelligenceSuite/data-intelligence-common[streaming]"      # Stream processing
```

## 🛠️ Core Features

### 🔐 Security & Configuration
- **HashiCorp Vault Integration**
  - Dynamic database credentials with automatic rotation
  - Transit encryption for sensitive data
  - PKI certificate management for mTLS
  - Secure secret storage and retrieval
  - Encryption key management

- **HashiCorp Consul Integration**  
  - Service discovery with health checking
  - Dynamic configuration management
  - Distributed key-value storage
  - Service mesh capabilities
  - Leader election and distributed locks

### 🏗️ Enhanced Components

#### Base Service Framework
- Standardized service initialization with Vault/Consul
- Health checking and monitoring
- Graceful shutdown handling
- Automatic credential renewal
- Service registration and discovery

#### Plugin-based Client Architecture
```
BaseServiceClient
    ├── ClientPlugin (Interface)
    │   ├── PulsarPlugin
    │   ├── KafkaPlugin
    │   ├── IgnitePlugin
    │   └── CustomPlugin
    └── PluginRegistry
        └── Dynamic plugin discovery
```

#### Event Processing Pipeline
```
Event Source → Backend Adapter → Event Bus → Handlers
                    ↓                ↓           ↓
                 Pulsar          Saga Manager  Processors
                 Kafka           Event Store   Analytics
                 Redis           Audit Log     ML Pipeline
                 NATS
```

## 💡 Usage Examples

### Lakehouse Operations

```python
from data_intelligence_common.core.lakehouse import LakehouseManager, LakehouseFormat

# Initialize manager
manager = LakehouseManager()
await manager.initialize()

# Create Iceberg table with time travel
table = await manager.create_table(
    name="events",
    schema={"user_id": "string", "event": "string", "timestamp": "timestamp"},
    format=LakehouseFormat.ICEBERG,
    partition_by=["date(timestamp)"]
)

# Write data with ACID guarantees
await manager.write_data("events", data)

# Query with time travel
yesterday_data = await manager.read_table(
    "events",
    timestamp=datetime.now() - timedelta(days=1)
)

# Migrate between formats
await manager.migrate_table(
    source_table="events",
    target_table="events_delta",
    target_format=LakehouseFormat.DELTA
)
```

### Data Quality Validation

```python
from data_intelligence_common.integrations import DeequClient, CheckBuilder, CheckLevel

# Initialize Deequ
deequ = DeequClient()
await deequ.connect()

# Build quality checks
checks = [
    CheckBuilder("completeness", CheckLevel.ERROR)
        .is_complete("user_id")
        .is_complete("email")
        .build(),
    
    CheckBuilder("uniqueness", CheckLevel.ERROR)
        .is_unique("transaction_id")
        .build(),
    
    CheckBuilder("validity", CheckLevel.WARNING)
        .is_non_negative("amount")
        .is_contained_in("status", ["pending", "completed", "failed"])
        .satisfies("email", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
        .build()
]

# Verify data quality
result = await deequ.verify_data(spark_df, checks)
print(f"Quality check {'passed' if result.status == 'SUCCESS' else 'failed'}")
print(f"Metrics: {result.metrics}")

# Get constraint suggestions
suggestions = await deequ.suggest_constraints(spark_df)
for suggestion in suggestions:
    print(f"Suggested: {suggestion['code']} for column {suggestion['column']}")
```

### Event Processing with Multiple Backends

```python
from data_intelligence_common.core.events.backends import (
    EventBackendFactory, BackendType, EventBackendConfig,
    Event, ConsumerConfig
)

# Create event backend (Pulsar, Kafka, Redis, NATS)
config = EventBackendConfig(
    backend_type=BackendType.KAFKA,
    connection_url="kafka://localhost:9092",
    delivery_guarantee="exactly_once"
)

backend = EventBackendFactory.create_backend(config)
await backend.connect()

# Publish events with exactly-once semantics
event = Event(
    id="evt-123",
    topic="user.events",
    data={"action": "purchase", "amount": 99.99},
    headers={"source": "web", "version": "1.0"}
)

result = await backend.publish(event)
print(f"Published to partition {result.partition} at offset {result.offset}")

# Subscribe with consumer group
async def handle_event(event: Event):
    print(f"Processing: {event.data}")
    # Process event
    return True

subscription = await backend.subscribe(
    ConsumerConfig(
        consumer_group="analytics-processor",
        topics=["user.events", "system.events"],
        enable_dead_letter=True,
        max_redeliveries=3
    ),
    handler=handle_event
)

# Stream events as async iterator
async for event in backend.stream(consumer_config):
    await process_event(event)
```

### Real-time Analytics

```python
from data_intelligence_common.integrations.realtime import (
    PinotClient, TableSchema, TableConfig, TableType
)

# Initialize Pinot
pinot = PinotClient()
await pinot.connect()

# Create real-time table
schema = TableSchema(
    schema_name="user_metrics",
    dimension_fields=[
        {"name": "user_id", "dataType": "STRING"},
        {"name": "segment", "dataType": "STRING"}
    ],
    metric_fields=[
        {"name": "revenue", "dataType": "DOUBLE"},
        {"name": "events", "dataType": "LONG"}
    ],
    time_field={"name": "timestamp", "dataType": "TIMESTAMP"}
)

config = TableConfig(
    table_name="user_metrics",
    table_type=TableType.REALTIME,
    time_column="timestamp",
    stream_type="kafka",
    stream_topic="user.metrics",
    stream_bootstrap_servers="localhost:9092"
)

await pinot.create_schema(schema)
await pinot.create_table(config)

# Query real-time data
result = await pinot.query("""
    SELECT 
        segment,
        COUNT(DISTINCT user_id) as unique_users,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_revenue
    FROM user_metrics
    WHERE timestamp >= ago('1h')
    GROUP BY segment
    ORDER BY total_revenue DESC
""")

df = result.to_dataframe()
print(df)
```

### Metadata Management with DataHub

```python
from data_intelligence_common.integrations import (
    DataHubClient, DatasetMetadata, MLModelMetadata,
    DataPlatform, DataQualityMetric
)

datahub = DataHubClient()
await datahub.connect()

# Register dataset with full metadata
dataset = DatasetMetadata(
    platform=DataPlatform.SPARK,
    name="customer_360",
    env="PROD",
    schema=[
        {"name": "customer_id", "type": "string", "nullable": False},
        {"name": "lifetime_value", "type": "double", "nullable": True},
        {"name": "segment", "type": "string", "nullable": False}
    ],
    properties={
        "description": "Unified customer view",
        "refresh_schedule": "0 2 * * *"
    },
    tags=["pii", "gdpr", "customer-data"],
    owners=["data-team", "analytics-team"],
    upstream_datasets=[
        "urn:li:dataset:(urn:li:dataPlatform:kafka,events.customer,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:postgres,crm.customers,PROD)"
    ]
)

await datahub.ingest_dataset(dataset)

# Register ML model
model = MLModelMetadata(
    name="customer_churn_predictor",
    version="2.1.0",
    algorithm="XGBoost",
    hyperparameters={
        "max_depth": 6,
        "learning_rate": 0.3,
        "n_estimators": 100
    },
    metrics={
        "auc": 0.92,
        "precision": 0.87,
        "recall": 0.89
    },
    features=["lifetime_value", "days_since_last_purchase", "support_tickets"],
    tags=["production", "churn-prediction"]
)

await datahub.ingest_ml_model(model)

# Track data quality metrics
metrics = [
    DataQualityMetric(
        dataset_urn=dataset.get_urn(),
        metric_name="completeness",
        value=0.98,
        dimension="completeness"
    ),
    DataQualityMetric(
        dataset_urn=dataset.get_urn(),
        metric_name="freshness_hours",
        value=2.5,
        dimension="timeliness"
    )
]

await datahub.ingest_data_quality_metrics(metrics)

# Search and discover
results = await datahub.search_datasets(
    query="customer",
    filters={"tags": ["pii"], "platform": "spark"}
)

# Get lineage
lineage = await datahub.get_dataset_lineage(
    dataset.get_urn(),
    direction="BOTH",
    depth=3
)
```

### Cross-platform Lineage with OpenLineage

```python
from data_intelligence_common.integrations import (
    OpenLineageClient, LineageJob, LineageRun, LineageDataset,
    JobType
)

# Initialize OpenLineage
lineage_client = OpenLineageClient(
    backend="http",
    endpoint="http://marquez:5000"
)
await lineage_client.connect()

# Create job with metadata
job = await lineage_client.create_job_with_metadata(
    namespace="etl",
    name="customer_aggregation",
    job_type=JobType.BATCH,
    description="Daily customer metrics aggregation",
    source_code_location="https://github.com/company/etl/blob/main/customer_agg.py"
)

# Define datasets
input_dataset = await lineage_client.create_dataset_with_schema(
    namespace="warehouse",
    name="raw_events",
    schema_fields=[
        {"name": "event_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "timestamp", "type": "timestamp"}
    ]
)

output_dataset = await lineage_client.create_dataset_with_schema(
    namespace="warehouse",
    name="customer_metrics",
    schema_fields=[
        {"name": "customer_id", "type": "string"},
        {"name": "total_events", "type": "long"},
        {"name": "last_activity", "type": "timestamp"}
    ]
)

# Add column-level lineage
output_dataset = await lineage_client.add_column_lineage(
    output_dataset,
    {
        "customer_id": [
            {"namespace": "warehouse", "dataset": "raw_events", "field": "customer_id"}
        ],
        "total_events": [
            {"namespace": "warehouse", "dataset": "raw_events", "field": "event_id"}
        ]
    }
)

# Track job execution
run = await lineage_client.create_run_with_parent(
    parent_job_name="daily_etl_orchestrator",
    nominal_time=datetime.now()
)

# Emit start event
await lineage_client.emit_start_event(
    job, run, 
    inputs=[input_dataset],
    outputs=[output_dataset]
)

# ... job execution ...

# Add quality metrics
output_dataset = await lineage_client.add_data_quality_metrics(
    output_dataset,
    metrics={"row_count": 1000000, "null_count": 0},
    assertions=[
        {"assertion": "customer_id IS NOT NULL", "success": True}
    ]
)

# Emit complete event
await lineage_client.emit_complete_event(
    job, run,
    inputs=[input_dataset],
    outputs=[output_dataset]
)
```

### Advanced Caching with Encryption

```python
from data_intelligence_common.core.caching import (
    DistributedCacheManager, CacheConfig, CacheStrategy
)

# Initialize distributed cache
cache_manager = DistributedCacheManager(
    ignite_client=ignite_client,
    vault_client=vault_client,
    consul_client=consul_client
)

# Create encrypted cache with access control
cache_config = CacheConfig(
    name="sensitive_data",
    strategy=CacheStrategy.WRITE_THROUGH,
    encrypt_data=True,
    encryption_key="pii-encryption",
    ttl_seconds=3600,
    access_roles=["data-scientist", "analyst"]
)

cache = await cache_manager.create_cache(cache_config)

# Use cache with automatic encryption
await cache.put("user:123", {"ssn": "123-45-6789", "income": 75000})
data = await cache.get("user:123")  # Automatically decrypted

# Batch operations
batch_data = {f"user:{i}": {"data": i} for i in range(1000)}
await cache.put_all(batch_data)

# Cache statistics
stats = await cache.get_statistics()
print(f"Hit rate: {stats.hit_rate:.2%}")
print(f"Avg get time: {stats.avg_get_time_ms}ms")
```

## 🔧 Configuration

### Environment Variables
```bash
# Core settings
SERVICE_NAME=my-service
SERVICE_PORT=8000

# Vault Configuration
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=your-token
VAULT_NAMESPACE=data-intelligence

# Consul Configuration  
CONSUL_HTTP_ADDR=http://localhost:8500
CONSUL_HTTP_TOKEN=your-token
CONSUL_DATACENTER=dc1

# Feature flags
ENABLE_LAKEHOUSE=true
ENABLE_QUALITY_CHECKS=true
ENABLE_REALTIME_ANALYTICS=true
```

### Vault Setup for New Features

```bash
# Lakehouse credentials
vault write database/config/iceberg \
    plugin_name=postgresql-database-plugin \
    allowed_roles="lakehouse-reader,lakehouse-writer" \
    connection_url="postgresql://{{username}}:{{password}}@iceberg-catalog:5432/catalog"

# Real-time analytics
vault write database/config/pinot \
    plugin_name=http-database-plugin \
    allowed_roles="pinot-admin,pinot-user" \
    url="http://pinot-controller:9000"

# Data quality
vault write kv/data-intelligence/great-expectations \
    datasource_config=@ge-datasources.json \
    expectation_stores=@ge-stores.json
```

## 📊 Performance Optimizations

- **Connection Pooling**: Reuse connections across requests
- **Async I/O**: Non-blocking operations throughout
- **Batch Processing**: Efficient bulk operations
- **Caching**: Multi-level caching with TTL
- **Circuit Breakers**: Prevent cascade failures
- **Compression**: Automatic data compression
- **Lazy Loading**: Load components on demand
- **Resource Limits**: Prevent resource exhaustion

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test categories
pytest -m "lakehouse"
pytest -m "quality"
pytest -m "realtime"
pytest -m "events"

# Run with coverage
pytest --cov=data_intelligence_common --cov-report=html

# Run integration tests
pytest tests/integration/ --integration

# Run performance tests
pytest tests/performance/ --benchmark
```

## 📚 Architecture

```
data-intelligence-common/
├── base_service/          # Base service framework
├── clients/               # Service client integrations
├── core/                  # Core functionality
│   ├── caching/          # Distributed cache management
│   ├── catalog/          # Data catalog integration
│   ├── clients/          # Plugin-based client architecture
│   ├── config/           # Configuration management
│   ├── events/           # Event processing
│   │   └── backends/     # Pluggable event backends
│   ├── integration/      # Data integration patterns
│   ├── lakehouse/        # Lakehouse table formats
│   ├── ml/               # ML utilities
│   ├── orchestration/    # Workflow orchestration
│   └── processing/       # Data processing
├── integrations/         # External service clients
│   └── realtime/        # Real-time analytics
├── monitoring/           # Observability
├── utils/                # Utilities
└── vault_consul/         # Vault/Consul integration
```

## 🤝 Contributing

1. Follow the established patterns
2. Add comprehensive tests for new features
3. Update documentation with examples
4. Ensure backward compatibility
5. Run linting and type checking
6. Add performance benchmarks for critical paths

## 📈 Roadmap

- [ ] Apache Doris integration for real-time analytics
- [ ] Databricks Unity Catalog support
- [ ] Streaming SQL with Flink SQL
- [ ] GraphQL federation for metadata
- [ ] Policy-based data access control
- [ ] Data mesh integration patterns
- [ ] Federated learning support

## 📄 License

Proprietary - PlatformQ 