# Integration Patterns

This directory contains **integration patterns** - abstract frameworks and base classes for building data integration solutions.

## What belongs here?

- **Patterns & Frameworks**: Abstract base classes and patterns for integration
- **Common Logic**: Shared integration logic that can be reused across multiple implementations
- **Interfaces**: Contracts and interfaces that specific integrations must implement

## Contents

### base_dih.py
**Digital Integration Hub (DIH)** pattern for building high-performance data access layers:
- Multi-source data integration
- Cache management and optimization
- Data synchronization patterns
- Query federation

### cdc_processor.py
**Change Data Capture (CDC)** pattern for real-time data synchronization:
- CDC event processing
- Stream-based data replication
- Event ordering and deduplication
- State management for CDC positions

### data_source_manager.py
**Data Source Management** pattern for managing connections:
- Connection pooling
- Health checking
- Load balancing
- Credential management

### cache_patterns.py
**Caching Patterns** specific to integration scenarios:
- Cache warming strategies
- Cache invalidation patterns
- Multi-level caching
- Cache optimization

## How is this different from `integrations/`?

| Aspect | `core/integration/` | `integrations/` |
|--------|-------------------|----------------|
| **Purpose** | Patterns & frameworks | Concrete implementations |
| **Content** | Abstract base classes | Service-specific clients |
| **Examples** | `BaseDigitalIntegrationHub`, `BaseCDCProcessor` | `CassandraClient`, `PulsarClient` |
| **Usage** | Extended by implementations | Used directly by services |

## Example Usage

```python
# Using a pattern from core/integration/
from data_intelligence_common.core.integration import BaseDigitalIntegrationHub

class MyDIH(BaseDigitalIntegrationHub):
    async def _initialize_impl(self):
        # Custom initialization
        pass

# Using a client from integrations/
from data_intelligence_common.integrations import CassandraClient

client = CassandraClient(config)
await client.connect()
```

## Design Principles

1. **Separation of Concerns**: Patterns are separate from implementations
2. **Reusability**: Patterns can be reused across multiple integration scenarios
3. **Extensibility**: Easy to add new patterns without affecting existing implementations
4. **Testability**: Patterns can be tested independently of specific integrations 