# Data Catalog Hub Refactoring Plan

## Overview
This document outlines a comprehensive refactoring plan to improve the data-catalog-hub service's architecture, focusing on separation of concerns, maintainability, organization, scalability, efficiency, and performance.

## 1. API Layer Refactoring

### Current Issues
- Large API files (glossary.py: 700+ lines, lineage.py: 600+ lines)
- Business logic embedded in API endpoints
- Global dependency management using `set_dependencies()`
- Tight coupling between layers

### Proposed Structure

```
app/
├── api/
│   ├── v1/
│   │   ├── routers/
│   │   │   ├── entities.py (thin router, ~100 lines)
│   │   │   ├── glossary/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── terms.py
│   │   │   │   ├── categories.py
│   │   │   │   └── mappings.py
│   │   │   └── lineage/
│   │   │       ├── __init__.py
│   │   │       ├── graph.py
│   │   │       ├── impact.py
│   │   │       └── compliance.py
│   │   ├── dependencies.py (DI container)
│   │   └── models/
│   │       ├── requests/
│   │       └── responses/
│   └── middleware/
```

### Example Refactored API Endpoint

```python
# app/api/v1/routers/entities.py
from fastapi import APIRouter, Depends, HTTPException
from app.api.v1.dependencies import get_entity_service
from app.api.v1.models.requests import CreateEntityRequest
from app.api.v1.models.responses import EntityResponse
from app.services.catalog import EntityService

router = APIRouter(prefix="/entities", tags=["entities"])

@router.post("", response_model=EntityResponse)
async def create_entity(
    request: CreateEntityRequest,
    entity_service: EntityService = Depends(get_entity_service)
):
    """Create a new entity - thin wrapper around service"""
    try:
        entity = await entity_service.create(request)
        return EntityResponse.from_domain(entity)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
```

## 2. Service Layer Architecture

### Current Issues
- Mixed responsibilities in service classes
- Duplicate implementations (3 vector search services)
- Legacy code maintained alongside new code

### Proposed Service Organization

```
app/
├── services/
│   ├── catalog/
│   │   ├── entity_service.py
│   │   ├── schema_service.py
│   │   ├── lineage_service.py
│   │   └── classification_service.py
│   ├── search/
│   │   ├── search_facade.py
│   │   ├── strategies/
│   │   │   ├── base.py
│   │   │   ├── text_search.py
│   │   │   ├── vector_search.py
│   │   │   └── hybrid_search.py
│   │   └── query_processing/
│   │       ├── analyzer.py
│   │       └── enhancer.py
│   └── integration/
│       ├── atlas_integration.py
│       ├── elasticsearch_integration.py
│       └── quality_integration.py
```

### Service Layer Principles
1. **Single Responsibility**: Each service handles one domain concept
2. **Interface Segregation**: Small, focused interfaces
3. **Dependency Inversion**: Depend on abstractions, not concretions

## 3. Domain Layer Introduction

### Proposed Domain Structure

```
app/
├── domain/
│   ├── catalog/
│   │   ├── entities/
│   │   │   ├── entity.py
│   │   │   ├── entity_repository.py
│   │   │   └── entity_specification.py
│   │   ├── glossary/
│   │   │   ├── term.py
│   │   │   ├── term_repository.py
│   │   │   └── term_service.py
│   │   └── lineage/
│   │       ├── lineage_graph.py
│   │       ├── impact_analyzer.py
│   │       └── lineage_repository.py
│   └── search/
│       ├── search_query.py
│       ├── search_result.py
│       └── search_strategy.py
```

## 4. Dependency Injection Refactoring

### Current Issue
- Global variables and manual dependency wiring
- Difficult to test in isolation

### Proposed Solution: Dependency Injection Container

```python
# app/core/container.py
from dependency_injector import containers, providers
from app.core.config import Settings
from app.services.catalog import EntityService, SchemaService
from app.infrastructure import AtlasRepository, ElasticsearchClient

class Container(containers.DeclarativeContainer):
    # Configuration
    config = providers.Configuration()
    
    # Infrastructure
    atlas_client = providers.Singleton(
        AtlasClient,
        settings=config.atlas
    )
    
    es_client = providers.Singleton(
        ElasticsearchClient,
        hosts=config.elasticsearch.hosts
    )
    
    cache_manager = providers.Singleton(
        IgniteCacheManager,
        config=config.cache
    )
    
    # Repositories
    entity_repository = providers.Singleton(
        AtlasEntityRepository,
        client=atlas_client,
        cache=cache_manager
    )
    
    # Services
    entity_service = providers.Factory(
        EntityService,
        repository=entity_repository,
        event_publisher=providers.DependsOn("event_publisher")
    )
```

## 5. Main Application Simplification

### Current Issue
- DataCatalogHub class with too many responsibilities
- Complex initialization logic

### Proposed Application Factory

```python
# app/application.py
from fastapi import FastAPI
from app.core.container import Container
from app.api.v1 import create_v1_router

def create_application() -> FastAPI:
    """Application factory pattern"""
    # Create container
    container = Container()
    container.config.from_env()
    
    # Create app
    app = FastAPI(
        title="Data Catalog Hub",
        version="3.0.0"
    )
    
    # Wire dependencies
    app.container = container
    
    # Add routers
    app.include_router(create_v1_router())
    
    # Add middleware
    setup_middleware(app)
    
    # Add event handlers
    setup_event_handlers(app, container)
    
    return app

# app/main.py - simplified
from app.application import create_application

app = create_application()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8017)
```

## 6. Event-Driven Architecture Enhancement

### Current Issue
- Tight coupling through direct method calls
- Difficult to scale individual components

### Proposed Event Bus

```python
# app/events/event_bus.py
from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from datetime import datetime
import asyncio

@dataclass
class DomainEvent:
    aggregate_id: str
    event_type: str
    occurred_at: datetime
    data: Dict[str, Any]

class EventBus:
    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        
    def register_handler(self, event_type: str, handler: Callable):
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
        
    async def publish(self, event: DomainEvent):
        handlers = self._handlers.get(event.event_type, [])
        await asyncio.gather(*[
            handler(event) for handler in handlers
        ])

# Domain events
class EntityCreated(DomainEvent):
    def __init__(self, entity_id: str, entity_data: Dict[str, Any]):
        super().__init__(
            aggregate_id=entity_id,
            event_type="entity.created",
            occurred_at=datetime.utcnow(),
            data=entity_data
        )
```

## 7. Caching Strategy Optimization

### Current Issues
- Inconsistent caching patterns
- No cache invalidation strategy

### Proposed Caching Layer

```python
# app/infrastructure/caching/cache_decorator.py
from functools import wraps
from typing import Optional, Callable
import hashlib
import json

def cached(
    ttl: int = 300,
    key_prefix: Optional[str] = None,
    invalidate_on: Optional[List[str]] = None
):
    """Intelligent caching decorator"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Generate cache key
            cache_key = _generate_cache_key(
                key_prefix or f"{self.__class__.__name__}.{func.__name__}",
                args,
                kwargs
            )
            
            # Try to get from cache
            if hasattr(self, 'cache_manager'):
                cached_value = await self.cache_manager.get(cache_key)
                if cached_value is not None:
                    return cached_value
            
            # Execute function
            result = await func(self, *args, **kwargs)
            
            # Store in cache
            if hasattr(self, 'cache_manager'):
                await self.cache_manager.set(cache_key, result, ttl=ttl)
                
            return result
        
        # Store invalidation events
        if invalidate_on:
            wrapper._invalidate_on = invalidate_on
            
        return wrapper
    return decorator
```

## 8. Search Service Consolidation

### Current Issues
- Three different vector search implementations
- Duplicate embedding logic
- Inconsistent search interfaces

### Unified Search Architecture

```python
# app/services/search/search_facade.py
class UnifiedSearchService:
    """Facade for all search operations"""
    
    def __init__(
        self,
        strategy_factory: SearchStrategyFactory,
        query_analyzer: QueryAnalyzer,
        result_merger: ResultMerger
    ):
        self.strategy_factory = strategy_factory
        self.query_analyzer = query_analyzer
        self.result_merger = result_merger
        
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> SearchResults:
        # Analyze query
        analyzed_query = await self.query_analyzer.analyze(query)
        
        # Select strategies based on query analysis
        strategies = self.strategy_factory.get_strategies(
            analyzed_query.intent,
            options
        )
        
        # Execute searches in parallel
        results = await asyncio.gather(*[
            strategy.search(analyzed_query, options)
            for strategy in strategies
        ])
        
        # Merge and rank results
        return self.result_merger.merge(results, analyzed_query)
```

## 9. Testing Strategy

### Unit Testing Structure

```
tests/
├── unit/
│   ├── domain/
│   │   ├── test_entity.py
│   │   └── test_lineage_graph.py
│   ├── services/
│   │   ├── test_entity_service.py
│   │   └── test_search_service.py
│   └── api/
│       └── test_entity_endpoints.py
├── integration/
│   ├── test_atlas_integration.py
│   └── test_search_integration.py
└── e2e/
    └── test_catalog_workflows.py
```

### Example Unit Test with Mocking

```python
# tests/unit/services/test_entity_service.py
import pytest
from unittest.mock import Mock, AsyncMock
from app.services.catalog import EntityService
from app.domain.catalog import Entity

@pytest.fixture
def mock_repository():
    repo = Mock()
    repo.save = AsyncMock()
    repo.find_by_id = AsyncMock()
    return repo

@pytest.fixture
def entity_service(mock_repository):
    return EntityService(repository=mock_repository)

async def test_create_entity(entity_service, mock_repository):
    # Arrange
    entity_data = {"name": "test", "type": "dataset"}
    mock_repository.save.return_value = Entity(id="123", **entity_data)
    
    # Act
    result = await entity_service.create(entity_data)
    
    # Assert
    assert result.id == "123"
    mock_repository.save.assert_called_once()
```

## 10. Performance Optimizations

### Database Query Optimization

```python
# app/infrastructure/repositories/optimized_repository.py
class OptimizedEntityRepository:
    def __init__(self, atlas_client, cache_manager):
        self.atlas_client = atlas_client
        self.cache_manager = cache_manager
        
    async def find_with_relations(
        self,
        entity_id: str,
        include: List[str]
    ) -> Optional[Entity]:
        """Batch load related data to avoid N+1 queries"""
        # Use DataLoader pattern
        async with self.atlas_client.batch_context():
            entity = await self.atlas_client.get_entity(entity_id)
            
            if not entity:
                return None
                
            # Batch load relations
            if "lineage" in include:
                entity.lineage = await self._load_lineage_batch([entity_id])
            if "classifications" in include:
                entity.classifications = await self._load_classifications_batch([entity_id])
                
        return Entity.from_atlas(entity)
```

### Async Processing for Heavy Operations

```python
# app/services/catalog/async_processing.py
from app.tasks import celery_app

class AsyncEntityService:
    async def bulk_classify(self, entity_ids: List[str]):
        """Offload heavy classification to background task"""
        task = celery_app.send_task(
            'classify_entities',
            args=[entity_ids]
        )
        return {
            "task_id": task.id,
            "status": "processing",
            "entity_count": len(entity_ids)
        }
```

## Implementation Plan

### Phase 1: Foundation (Weeks 1-2)
1. Implement dependency injection container
2. Create domain models and repositories
3. Set up new project structure

### Phase 2: Service Layer (Weeks 3-4)
1. Extract business logic from API to services
2. Implement service interfaces
3. Create service tests

### Phase 3: API Refactoring (Weeks 5-6)
1. Break down large API files
2. Implement thin API routers
3. Update API documentation

### Phase 4: Search Consolidation (Weeks 7-8)
1. Merge vector search implementations
2. Implement unified search facade
3. Migrate existing search calls

### Phase 5: Performance & Monitoring (Weeks 9-10)
1. Implement caching strategy
2. Add performance monitoring
3. Optimize database queries

## Success Metrics

1. **Code Quality**
   - No file exceeds 300 lines
   - Test coverage > 80%
   - Cyclomatic complexity < 10

2. **Performance**
   - API response time < 200ms (p95)
   - Search latency < 100ms
   - Cache hit rate > 70%

3. **Maintainability**
   - Deployment frequency increased by 50%
   - Bug resolution time reduced by 40%
   - New feature development time reduced by 30% 