"""
Common Design Patterns for DataIntelligenceSuite

Provides reusable implementations of common patterns:
- Retry with backoff
- Circuit breaker
- Rate limiting
- Bulkhead
- Saga orchestration
- CQRS
- Event sourcing
- Repository pattern
- Unit of work
"""

from .resilience import (
    RetryPattern,
    CircuitBreakerPattern,
    BulkheadPattern,
    TimeoutPattern,
    FallbackPattern,
    ResilienceConfig,
    ResiliencePolicy
)

from .saga import (
    SagaOrchestrator,
    SagaStep,
    CompensationStrategy,
    SagaContext,
    SagaTransaction
)

from .cqrs import (
    Command,
    Query,
    CommandHandler,
    QueryHandler,
    CommandBus,
    QueryBus,
    CQRSMediator
)

from .repository import (
    Repository,
    RepositoryBase,
    UnitOfWork,
    Specification,
    AggregateRoot
)

from .observer import (
    Observer,
    Observable,
    EventPublisher,
    EventSubscriber
)

from .factory import (
    Factory,
    AbstractFactory,
    FactoryRegistry,
    Builder
)

from .strategy import (
    Strategy,
    StrategyContext,
    StrategyRegistry
)

__all__ = [
    # Resilience
    "RetryPattern",
    "CircuitBreakerPattern",
    "BulkheadPattern",
    "TimeoutPattern",
    "FallbackPattern",
    "ResilienceConfig",
    "ResiliencePolicy",
    
    # Saga
    "SagaOrchestrator",
    "SagaStep",
    "CompensationStrategy",
    "SagaContext",
    "SagaTransaction",
    
    # CQRS
    "Command",
    "Query",
    "CommandHandler",
    "QueryHandler",
    "CommandBus",
    "QueryBus",
    "CQRSMediator",
    
    # Repository
    "Repository",
    "RepositoryBase",
    "UnitOfWork",
    "Specification",
    "AggregateRoot",
    
    # Observer
    "Observer",
    "Observable",
    "EventPublisher",
    "EventSubscriber",
    
    # Factory
    "Factory",
    "AbstractFactory",
    "FactoryRegistry",
    "Builder",
    
    # Strategy
    "Strategy",
    "StrategyContext",
    "StrategyRegistry"
] 