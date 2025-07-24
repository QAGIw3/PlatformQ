# DataIntelligenceSuite Migration Guide

This guide explains how to migrate all DataIntelligenceSuite services to fully utilize the refactored `data-intelligence-common` library.

## Overview of Changes

The `data-intelligence-common` library has been significantly refactored with:

1. **Unified Base Service** - All services now inherit from `DataIntelligenceBaseService`
2. **Mixin Architecture** - Common functionality provided through mixins
3. **Unified Processing** - Single processor handles both batch and stream
4. **Consolidated Patterns** - Resilience, caching, and factory patterns unified
5. **Unified Event System** - Consistent event handling across services
6. **Unified Configuration** - Composition-based configuration system
7. **Quality Stages** - Reusable quality checking components

## Migration Steps for Each Service

### 1. Analytics Engine Service

**Key Changes:**
- Inherit from `DataIntelligenceBaseService`
- Use `UnifiedProcessor` for stream processing
- Leverage built-in caching and metrics

**Migration Example:**
```python
# Before
class AnalyticsService:
    def __init__(self):
        self.metrics = MetricsCollector()
        self.cache = CacheManager()
        # ... manual setup

# After
from data_intelligence_common.base_service import DataIntelligenceBaseService

class AnalyticsBaseService(DataIntelligenceBaseService):
    def __init__(self, config: AnalyticsServiceConfig):
        super().__init__(config)
        # Metrics, cache, events all provided by base class
```

**Benefits:**
- Automatic health checks
- Built-in metrics collection
- Unified event publishing
- Consistent error handling

### 2. Data Platform Service

**Key Changes:**
- Use `UnifiedProcessor` for ETL pipelines
- Replace custom quality checks with quality stages
- Use unified configuration

**Migration Example:**
```python
# Create ETL pipeline with quality checks
processor = UnifiedProcessor.pipeline(config)\
    .from_source(FileSource("input.csv"))\
    .transform(DataCleaningStage())\
    .transform(QualityCheckStage(rules))\
    .transform(DeduplicationStage(["id"]))\
    .to_sink(DatabaseSink(client, "clean_data"))\
    .build()
```

### 3. ML Platform Service

**Key Changes:**
- Use mixins for ML-specific functionality
- Leverage unified event system for model lifecycle
- Use factory patterns for model creation

**Migration Example:**
```python
from data_intelligence_common.core.mixins import ServiceMixin, StateMixin

class MLPlatformService(DataIntelligenceBaseService, StateMixin):
    async def train_model(self, config):
        # Automatic event publishing
        await self.publish_event(
            event_type="model.training.started",
            data={"model_id": model_id}
        )
        
        # Use state mixin for tracking
        await self.set_state(f"model:{model_id}:status", "training")
```

### 4. Data Governance Service

**Key Changes:**
- Use quality stages for all quality checks
- Leverage `CommonQualityRules` for standard rules
- Use unified event system for quality alerts

**Migration Example:**
```python
# Create quality pipeline
rules = [
    CommonQualityRules.not_null("customer_id"),
    CommonQualityRules.in_range("age", 0, 150),
    CommonQualityRules.matches_pattern("email", r"^[\w\.-]+@[\w\.-]+\.\w+$")
]

pipeline = await quality_service.create_quality_pipeline(
    name="customer_quality",
    source=DatabaseSource(client, "SELECT * FROM customers"),
    sink=DatabaseSink(client, "clean_customers"),
    rules=rules,
    enable_anomaly_detection=True
)
```

### 5. Orchestration Service

**Key Changes:**
- Use unified event system for workflow orchestration
- Leverage `EventRouter` for event-driven workflows
- Use `ProcessingContext` for workflow state

**Migration Example:**
```python
from data_intelligence_common.core.events import EventRouter, BaseEventProcessor

class WorkflowOrchestrator(BaseEventProcessor):
    def __init__(self):
        super().__init__()
        self.router = EventRouter()
        
        # Register workflow triggers
        self.router.add_route(
            "data.ingestion.completed",
            self._trigger_quality_check
        )
```

### 6. Integration Hub Service

**Key Changes:**
- Use unified configuration for all connectors
- Leverage factory patterns for connector creation
- Use resilience patterns for reliable integrations

**Migration Example:**
```python
from data_intelligence_common.core.patterns.factory import PluginFactory

class ConnectorFactory(PluginFactory):
    def __init__(self):
        super().__init__(
            plugin_dir="connectors",
            base_class=BaseConnector
        )
        
# Usage
connector = factory.create("salesforce", config)
```

## Common Migration Patterns

### 1. Service Initialization

```python
# Old way
class MyService:
    def __init__(self):
        self.config = load_config()
        self.metrics = MetricsCollector()
        self.cache = CacheManager()
        self.event_bus = EventBus()
        # ... lots of manual setup

# New way
class MyService(DataIntelligenceBaseService):
    def __init__(self, config: MyServiceConfig):
        super().__init__(config)
        # Everything is provided!
```

### 2. Event Publishing

```python
# Old way
event = Event(
    event_type="my.event",
    source=self.service_name,
    data=data
)
await self.event_bus.publish("events", event)

# New way
await self.publish_event(
    event_type="my.event",
    data=data
)
```

### 3. Metrics Recording

```python
# Old way
self.metrics.increment_counter(
    "operations_total",
    {"operation": "process", "status": "success"}
)

# New way
self.record_operation("process", {"status": "success"})
```

### 4. Caching

```python
# Old way
cache_key = f"result:{query_id}"
result = await cache.get(cache_key)
if not result:
    result = await process_query(query)
    await cache.set(cache_key, result, ttl=300)

# New way
@cached(ttl=300)
async def process_query(self, query):
    return await self._execute_query(query)
```

### 5. Health Checks

```python
# Old way
async def health_check(self):
    checks = []
    checks.append(await check_database())
    checks.append(await check_cache())
    return all(checks)

# New way
self.register_health_check(
    "database",
    self._check_database_health,
    critical=True
)
# Health endpoint automatically provided
```

## Configuration Migration

### Old Configuration
```python
class ServiceConfig:
    def __init__(self):
        self.host = os.getenv("HOST", "localhost")
        self.port = int(os.getenv("PORT", 8080))
        self.db_host = os.getenv("DB_HOST")
        # ... many manual configs
```

### New Configuration
```python
from data_intelligence_common.core.config.unified import UnifiedServiceConfig

@dataclass
class MyServiceConfig(UnifiedServiceConfig):
    # Service-specific settings only
    my_custom_setting: str = "default"
    
    # Database config using composition
    database_config: DatabaseConnectionConfig = field(
        default_factory=lambda: DatabaseConnectionConfig(
            host="db.local",
            port=5432
        )
    )
```

## Testing Migration

### Old Testing
```python
class TestService:
    def setup(self):
        self.service = MyService()
        self.mock_db = Mock()
        self.service.db = self.mock_db
```

### New Testing
```python
class TestService:
    async def setup(self):
        config = MyServiceConfig(name="test", enable_metrics=False)
        self.service = MyService(config)
        # Use built-in test helpers
        await self.service.initialize_service()
```

## Benefits After Migration

1. **Reduced Code** - 50-70% less boilerplate code
2. **Consistency** - All services work the same way
3. **Reliability** - Built-in resilience patterns
4. **Observability** - Automatic metrics and tracing
5. **Maintainability** - Clear separation of concerns
6. **Testability** - Easier to mock and test

## Migration Checklist

- [ ] Update service to inherit from `DataIntelligenceBaseService`
- [ ] Create service configuration extending `UnifiedServiceConfig`
- [ ] Replace manual initialization with base class methods
- [ ] Update event publishing to use `publish_event()`
- [ ] Replace custom metrics with `record_operation()` and `record_error()`
- [ ] Use quality stages for data validation
- [ ] Update processors to use `UnifiedProcessor`
- [ ] Register health checks using `register_health_check()`
- [ ] Update tests to use new initialization
- [ ] Remove redundant utility functions

## Support

For questions or issues during migration:
1. Check the updated documentation in `data-intelligence-common/README.md`
2. Review example implementations in this guide
3. Contact the platform team for assistance 